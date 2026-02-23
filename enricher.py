"""
Edikte-Monitor – Gutachten-Enricher
=====================================
Liest alle Notion-Einträge mit gesetztem Flag "Gutachten automatisch herunterladen?"
und füllt aus dem PDF-Gutachten folgende Properties:

  Aus dem Grundbuch-Auszug (Section B):
    Verpflichtende Partei  – Name des Eigentümers (genauer als vom Edikt)
    Zustell Adresse        – Wohnadresse des Eigentümers
    Zustell PLZ/Ort        – PLZ + Ort des Eigentümers

  Aus Section C (Pfandrechte):
    Notizen                – Gläubiger / Bank (Pfandrechtsgläubiger)

  Meta:
    Gutachten analysiert?  – wird auf True gesetzt wenn erfolgreich
    Gutachten-URL          – Link zum PDF (in Notizen ergänzt)

Das Skript wird als separater GitHub-Actions-Job ausgeführt.
Trigger: manuell (workflow_dispatch) ODER automatisch nach dem Haupt-Scraper.
"""

import os
import re
import io
import json
import time
import urllib.request
import urllib.parse
from typing import Optional

try:
    import fitz          # PyMuPDF
except ImportError:
    fitz = None          # Fehler wird unten abgefangen

from notion_client import Client


# =============================================================================
# KONFIGURATION
# =============================================================================

BASE_URL = "https://edikte.justiz.gv.at"

# Notion-Feldnamen (exakt wie in der DB)
N_TITLE           = "Liegenschaftsadresse"
N_HASH            = "Hash-ID / Vergleichs-ID"
N_LINK            = "Link"
N_VERPFL          = "Verpflichtende Partei"
N_ZUSTELL_ADR     = "Zustell Adresse"
N_ZUSTELL_PLZ     = "Zustell PLZ/Ort"
N_NOTIZEN         = "Notizen"
N_GUTACHTEN_FLAG  = "Gutachten automatisch herunterladen?"
N_ANALYSIERT      = "Gutachten analysiert?"
N_RELEVANT        = "Für uns relevant?"
N_WORKFLOW        = "Workflow-Phase"


# =============================================================================
# HILFSFUNKTIONEN
# =============================================================================

def env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Fehlende Umgebungsvariable: {name}")
    return value


def clean_notion_db_id(raw: str) -> str:
    raw = raw.split("?")[0].strip().rstrip("/").split("/")[-1]
    clean = re.sub(r"[^0-9a-fA-F]", "", raw)
    if len(clean) == 32:
        return f"{clean[0:8]}-{clean[8:12]}-{clean[12:16]}-{clean[16:20]}-{clean[20:32]}"
    return raw


def rich_text_val(props: dict, key: str) -> str:
    """Liest den Textinhalt einer rich_text-Property."""
    items = props.get(key, {}).get("rich_text", [])
    return "".join(i.get("plain_text", "") for i in items).strip()


def title_val(props: dict, key: str) -> str:
    """Liest den Textinhalt einer title-Property."""
    items = props.get(key, {}).get("title", [])
    return "".join(i.get("plain_text", "") for i in items).strip()


def url_val(props: dict, key: str) -> Optional[str]:
    """Liest eine URL-Property."""
    return props.get(key, {}).get("url")


def checkbox_val(props: dict, key: str) -> bool:
    """Liest eine Checkbox-Property."""
    return props.get(key, {}).get("checkbox", False)


def rt_prop(text: str) -> dict:
    """Erzeugt ein rich_text-Property-Dict für Notion."""
    return {"rich_text": [{"text": {"content": text[:2000]}}]}


# =============================================================================
# EDIKT-DETAILSEITE: PDF-LINKS FINDEN
# =============================================================================

def fetch_attachment_links(edikt_url: str) -> dict:
    """
    Öffnet die Edikt-Detailseite und gibt alle Anhang-Links zurück.
    Rückgabe: {"pdfs": [...], "images": [...]}
    """
    req = urllib.request.Request(
        edikt_url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; EdikteMonitor/1.0)"}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        html = r.read().decode("utf-8", errors="replace")

    # Anhänge: /edikte/ex/exedi3.nsf/0/HEX/$file/FILENAME
    pattern = re.compile(
        r'href="(/edikte/ex/exedi3\.nsf/0/[^"]+\$file/([^"]+))"',
        re.IGNORECASE
    )
    pdfs   = []
    images = []
    for path, raw_fname in pattern.findall(html):
        fname = urllib.parse.unquote(raw_fname)
        full  = f"{BASE_URL}{path}"
        if fname.lower().endswith(".pdf"):
            pdfs.append({"url": full, "filename": fname})
        elif fname.lower().endswith((".jpg", ".jpeg", ".png")):
            images.append({"url": full, "filename": fname})

    return {"pdfs": pdfs, "images": images}


def pick_gutachten_pdf(pdfs: list[dict]) -> Optional[dict]:
    """
    Wählt das wahrscheinlichste Gutachten-PDF aus der Liste.
    Heuristik: Dateiname enthält 'G', 'Gutachten', 'SV' o.Ä.
    Wenn unklar: erstes PDF.
    """
    preferred_keywords = ["gutachten", " g ", "sachverst", "sv-", "/g-", "g "]
    for pdf in pdfs:
        fname_lower = pdf["filename"].lower()
        if any(kw in fname_lower for kw in preferred_keywords):
            return pdf
    # Fallback: erstes PDF das nicht "Anlagen" heißt
    for pdf in pdfs:
        if "anlagen" not in pdf["filename"].lower():
            return pdf
    # Letzter Fallback: einfach das erste
    return pdfs[0] if pdfs else None


def download_pdf(url: str) -> Optional[bytes]:
    """Lädt ein PDF herunter und gibt die Bytes zurück."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; EdikteMonitor/1.0)"}
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


# =============================================================================
# PDF PARSEN – GRUNDBUCH-AUSZUG
# =============================================================================

def extract_grundbuch_info(pdf_bytes: bytes) -> dict:
    """
    Extrahiert relevante Daten aus dem PDF-Gutachten.

    Unterstützt zwei Formate:
    1. Einfaches Grundbuchs-PDF (Kärnten-Stil):
       Enthält direkt den Grundbuchauszug mit Sektionen A1/A2/B/C.
    2. Professionelles Verkehrswertgutachten (Wien-Stil, 100+ Seiten):
       Enthält auf Seite 1 "Verpflichtete Partei: NAME" und
       weitere Infos im Fließtext.

    Gibt zurück:
      eigentümer_name    – Name des Eigentümers
      eigentümer_adresse – Straße + Hausnummer
      eigentümer_plz_ort – PLZ + Ort
      eigentümer_geb     – Geburtsdatum (falls vorhanden)
      gläubiger          – Liste der Pfandrechtsgläubiger aus Section C
      forderung_betrag   – Forderungsbetrag
      volltext           – Gesamter extrahierter Text (für Logging)
    """
    if fitz is None:
        raise ImportError("PyMuPDF (fitz) nicht installiert. Bitte: pip install pymupdf")

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")

    # Gesamttext aller Seiten sammeln
    all_text = []
    for page in doc:
        text = page.get_text()
        if text and text.strip():
            all_text.append(text)
    full_text = "\n".join(all_text)

    result = {
        "eigentümer_name":    "",
        "eigentümer_adresse": "",
        "eigentümer_plz_ort": "",
        "eigentümer_geb":     "",
        "gläubiger":          [],
        "forderung_betrag":   "",
        "volltext":           full_text[:5000],
    }

    # ── Format 1: Grundbuchauszug mit Sektionen B / C ───────────────────────
    section_b = _extract_section(full_text, "** B ***", "** C ***")
    if not section_b:
        section_b = _extract_section(full_text, "** B **", "** C **")

    if section_b:
        owner_info = _parse_owner_from_section_b(section_b)
        result.update(owner_info)

    section_c = _extract_section(full_text, "** C ***", "** HINWEIS ***")
    if not section_c:
        section_c = _extract_section(full_text, "** C **", "HINWEIS")
    if section_c:
        gläubiger, betrag = _parse_creditors_from_section_c(section_c)
        result["gläubiger"]        = gläubiger
        result["forderung_betrag"] = betrag

    # ── Format 2: Professionelles Gutachten (z.B. Strafella-Stil) ───────────
    # Auch wenn Section-B-Parsing erfolgreich war, ergänzen wir falls nötig
    if not result["eigentümer_name"]:
        # "Verpflichtete Partei: XYZ GmbH" auf Seite 1
        vp_match = re.search(
            r'Verpflichtete(?:\s+Partei)?:\s*(.+?)(?:\n|Betreibende|Auftraggeber)',
            full_text[:3000],
            re.IGNORECASE | re.DOTALL
        )
        if vp_match:
            name = vp_match.group(1).strip().split("\n")[0].strip()
            result["eigentümer_name"] = name

    # Zustelladresse aus dem Gutachten-Titelblatt falls vorhanden
    # z.B. "Verpflichtete Partei: XYZ GmbH\nMusterstraße 5, 1010 Wien"
    if result["eigentümer_name"] and not result["eigentümer_adresse"]:
        # Suche nach Adresse direkt nach dem Eigentümernamen
        name_escaped = re.escape(result["eigentümer_name"][:30])
        adr_after = re.search(
            name_escaped + r'[^\n]*\n\s*([A-ZÄÖÜ][^\n]{5,60})\s*\n',
            full_text[:3000],
            re.IGNORECASE
        )
        if adr_after:
            adr_raw = adr_after.group(1).strip()
            # PLZ am Ende?
            plz_m = re.search(r'\b(\d{4,5})\b', adr_raw)
            if plz_m:
                result["eigentümer_plz_ort"] = plz_m.group(1)
                result["eigentümer_adresse"] = adr_raw

    # Gläubiger aus professionellem Gutachten: "Betreibende Partei: NAME"
    if not result["gläubiger"]:
        bp_match = re.search(
            r'Betreibende(?:\s+Partei)?:\s*(.+?)(?:\n|Verpflichtete)',
            full_text[:3000],
            re.IGNORECASE | re.DOTALL
        )
        if bp_match:
            glaeu = bp_match.group(1).strip().split("\n")[0].strip()
            if glaeu:
                result["gläubiger"] = [glaeu]

    return result


def _extract_section(text: str, start_marker: str, end_marker: str) -> str:
    """Extrahiert Text zwischen zwei Markierungen (case-insensitive)."""
    start = text.lower().find(start_marker.lower())
    if start == -1:
        return ""
    end = text.lower().find(end_marker.lower(), start + len(start_marker))
    if end == -1:
        return text[start:]
    return text[start:end]


def _parse_owner_from_section_b(section_b: str) -> dict:
    """
    Parst Namen und Adresse des/der Eigentümer(s) aus Section B des Grundbuchs.

    Das Grundbuch-Format ist immer:
        1 ANTEIL: 1/1
          Vorname Nachname          ← 5 Leerzeichen Einrückung
          GEB: YYYY-MM-DD ADR: Straße X, Ort   PLZ
           a TZ/JAHR Kaufvertrag ...
    """
    result = {
        "eigentümer_name":    "",
        "eigentümer_adresse": "",
        "eigentümer_plz_ort": "",
        "eigentümer_geb":     "",
    }

    # Muster: "GEB: YYYY-MM-DD ADR: Straße X, Ort   PLZ"
    adr_pattern = re.compile(
        r'GEB:\s*(\d{4}-\d{2}-\d{2})\s+ADR:\s*(.+?)\s{2,}(\d{4,5})\s*$',
        re.IGNORECASE
    )
    adr_no_geb = re.compile(
        r'ADR:\s*(.+?)\s{2,}(\d{4,5})\s*$',
        re.IGNORECASE
    )
    adr_simple = re.compile(r'ADR:\s*(.+)', re.IGNORECASE)

    # Direkt im Rohtext mit Einrückung arbeiten (NICHT strip() die Zeilen)
    lines = section_b.splitlines()

    # Suche nach "ANTEIL:" dann die nächste Zeile die mit >=5 Leerzeichen
    # beginnt und NICHT mit Zahl+Leerzeichen beginnt = Eigentumsname
    for i, line in enumerate(lines):
        if "ANTEIL:" not in line.upper():
            continue

        # Nächste Zeilen durchsuchen
        for j in range(i + 1, min(i + 8, len(lines))):
            candidate = lines[j]

            # Name-Zeile: beginnt mit Leerzeichen, dann Großbuchstabe
            # und enthält KEIN "GEB:", "ADR:", kein Datum-Muster, keine Nummer
            stripped = candidate.strip()
            if not stripped:
                continue
            if re.match(r'^\d', stripped):          # "1 ANTEIL..." oder "a 7321/..."
                continue
            if re.match(r'^[a-z]\s+\d', stripped):  # "a 7321/2006..."
                continue
            if "GEB:" in stripped.upper():
                continue
            if "ADR:" in stripped.upper():
                continue
            if re.match(r'^\*+', stripped):          # "**** B ****"
                continue

            # Das ist der Name
            result["eigentümer_name"] = stripped
            # Nächste Zeile = GEB/ADR
            for k in range(j + 1, min(j + 4, len(lines))):
                adr_line = lines[k].strip()
                if not adr_line:
                    continue
                m = adr_pattern.search(adr_line)
                if m:
                    result["eigentümer_geb"]     = m.group(1)
                    result["eigentümer_adresse"] = m.group(2).strip().rstrip(",")
                    result["eigentümer_plz_ort"] = m.group(3)
                    break
                m2 = adr_no_geb.search(adr_line)
                if m2:
                    result["eigentümer_adresse"] = m2.group(1).strip().rstrip(",")
                    result["eigentümer_plz_ort"] = m2.group(2)
                    break
                m3 = adr_simple.search(adr_line)
                if m3:
                    adr_raw = m3.group(1).strip()
                    plz_m = re.search(r'\s+(\d{4,5})\s*$', adr_raw)
                    if plz_m:
                        result["eigentümer_plz_ort"] = plz_m.group(1)
                        result["eigentümer_adresse"] = adr_raw[:plz_m.start()].strip().rstrip(",")
                    else:
                        result["eigentümer_adresse"] = adr_raw
                    break
            break  # Ersten Eigentümer nehmen und aufhören
        break

    return result


def _parse_creditors_from_section_c(section_c: str) -> tuple[list[str], str]:
    """
    Parst Pfandrechtsgläubiger und Forderungsbeträge aus Section C.
    Gibt (gläubiger_liste, forderung_betrag) zurück.
    """
    gläubiger = []
    betrag    = ""

    lines = [l.strip() for l in section_c.splitlines() if l.strip()]

    # "für XYZ-Bank ..." → Gläubiger
    fuer_pattern = re.compile(r'^für\s+(.+)', re.IGNORECASE)
    # "Hereinbringung von EUR 200.000,--" → Betrag
    betrag_pattern = re.compile(
        r'Hereinbringung von\s+(EUR\s+[\d\.,]+)',
        re.IGNORECASE
    )
    # "PFANDRECHT  Höchstbetrag EUR X"
    pfand_pattern = re.compile(
        r'PFANDRECHT\s+Höchstbetrag\s+(EUR\s+[\d\.,]+)',
        re.IGNORECASE
    )

    seen = set()
    for line in lines:
        m_fuer = fuer_pattern.match(line)
        if m_fuer:
            name = m_fuer.group(1).strip().rstrip(".")
            # FN-Nummern und sehr kurze Strings überspringen
            if len(name) > 5 and name not in seen:
                gläubiger.append(name)
                seen.add(name)

        if not betrag:
            m_betrag = betrag_pattern.search(line)
            if m_betrag:
                betrag = m_betrag.group(1).strip()

    # Fallback: Pfandrecht Höchstbetrag
    if not betrag:
        for line in lines:
            m_pfand = pfand_pattern.search(line)
            if m_pfand:
                betrag = m_pfand.group(1).strip()
                break

    return gläubiger, betrag


# =============================================================================
# NOTION – ALLE EINTRÄGE MIT FLAG LADEN
# =============================================================================

def load_flagged_pages(notion: Client, db_id: str) -> list[dict]:
    """
    Lädt alle Notion-Seiten bei denen "Gutachten automatisch herunterladen?"
    gesetzt ist UND "Gutachten analysiert?" noch NICHT gesetzt ist.
    """
    pages  = []
    cursor = None

    while True:
        kwargs = {
            "database_id": db_id,
            "filter": {
                "and": [
                    {
                        "property":  N_GUTACHTEN_FLAG,
                        "checkbox":  {"equals": True},
                    },
                    {
                        "property":  N_ANALYSIERT,
                        "checkbox":  {"equals": False},
                    },
                ]
            },
            "page_size": 100,
        }
        if cursor:
            kwargs["start_cursor"] = cursor

        resp   = notion.databases.query(**kwargs)
        pages += resp.get("results", [])
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return pages


# =============================================================================
# NOTION – SEITE AKTUALISIEREN
# =============================================================================

def update_notion_page(notion: Client, page_id: str, info: dict,
                       gutachten_url: str, success: bool) -> None:
    """
    Schreibt die extrahierten Daten in die Notion-Seite.
    """
    properties = {
        N_ANALYSIERT: {"checkbox": success},
    }

    if success:
        # Eigentümername → Verpflichtende Partei (überschreibt groben Wert aus Edikt)
        if info.get("eigentümer_name"):
            properties[N_VERPFL] = rt_prop(info["eigentümer_name"])

        # Zustelladresse
        if info.get("eigentümer_adresse"):
            properties[N_ZUSTELL_ADR] = rt_prop(info["eigentümer_adresse"])

        # PLZ + Ort → aus Grundbuch kommt oft nur PLZ, aus Edikt haben wir den Ort
        plz_ort = info.get("eigentümer_plz_ort", "")
        if plz_ort:
            properties[N_ZUSTELL_PLZ] = rt_prop(plz_ort)

        # Notizen: Gläubiger + Forderungsbetrag + PDF-Link
        notiz_parts = []
        if info.get("gläubiger"):
            notiz_parts.append("Gläubiger: " + " | ".join(info["gläubiger"]))
        if info.get("forderung_betrag"):
            notiz_parts.append("Forderung: " + info["forderung_betrag"])
        notiz_parts.append(f"Gutachten-PDF: {gutachten_url}")

        notizen_text = "\n".join(notiz_parts)
        properties[N_NOTIZEN] = rt_prop(notizen_text)

    notion.pages.update(page_id=page_id, properties=properties)


# =============================================================================
# HAUPTPROGRAMM
# =============================================================================

def main() -> None:
    if fitz is None:
        print("❌ PyMuPDF nicht installiert. Bitte: pip install pymupdf")
        raise SystemExit(1)

    notion = Client(auth=env("NOTION_TOKEN"))
    db_id  = clean_notion_db_id(env("NOTION_DATABASE_ID"))

    print("=" * 60)
    print("🔍 Edikte-Monitor – Gutachten-Enricher")
    print("=" * 60)

    # 1. Alle markierten Seiten laden
    pages = load_flagged_pages(notion, db_id)
    print(f"\n📋 {len(pages)} Einträge mit gesetztem Download-Flag gefunden\n")

    if not pages:
        print("Nichts zu tun – kein Eintrag hat das Flag gesetzt.")
        return

    ok_count      = 0
    skip_count    = 0
    error_count   = 0

    for page in pages:
        props    = page.get("properties", {})
        page_id  = page["id"]
        title    = title_val(props, N_TITLE) or "(kein Titel)"
        edikt_url = url_val(props, N_LINK)

        print(f"\n▶ {title}")

        if not edikt_url:
            print(f"  ⚠️  Kein Link in Notion – überspringe")
            skip_count += 1
            continue

        # 2. PDF-Links von der Edikt-Seite holen
        try:
            attachments = fetch_attachment_links(edikt_url)
            pdfs = attachments["pdfs"]
            print(f"  📎 {len(pdfs)} PDF(s) gefunden, {len(attachments['images'])} Bilder")
        except Exception as e:
            print(f"  ❌ Fehler beim Abrufen der Edikt-Seite: {e}")
            error_count += 1
            # Als fehlgeschlagen markieren
            notion.pages.update(
                page_id=page_id,
                properties={N_ANALYSIERT: {"checkbox": False}}
            )
            continue

        if not pdfs:
            print(f"  ⚠️  Kein PDF-Anhang gefunden – überspringe")
            skip_count += 1
            continue

        # 3. Bestes Gutachten-PDF auswählen
        gutachten = pick_gutachten_pdf(pdfs)
        print(f"  📄 Gutachten: {gutachten['filename']}")

        # 4. PDF herunterladen
        try:
            pdf_bytes = download_pdf(gutachten["url"])
            print(f"  ⬇️  Heruntergeladen: {len(pdf_bytes):,} Bytes")
        except Exception as e:
            print(f"  ❌ Download-Fehler: {e}")
            error_count += 1
            continue

        # 5. Grundbuch-Infos extrahieren
        try:
            info = extract_grundbuch_info(pdf_bytes)

            print(f"  👤 Eigentümer:  {info['eigentümer_name'] or '(nicht gefunden)'}")
            print(f"  🏠 Adresse:     {info['eigentümer_adresse'] or '(nicht gefunden)'}")
            print(f"  📮 PLZ/Ort:     {info['eigentümer_plz_ort'] or '(nicht gefunden)'}")
            if info["gläubiger"]:
                print(f"  🏦 Gläubiger:   {' | '.join(info['gläubiger'][:2])}")
            if info["forderung_betrag"]:
                print(f"  💶 Forderung:   {info['forderung_betrag']}")

        except Exception as e:
            print(f"  ❌ PDF-Parse-Fehler: {e}")
            error_count += 1
            continue

        # 6. Notion aktualisieren
        try:
            has_owner = bool(info.get("eigentümer_name") or info.get("eigentümer_adresse"))
            update_notion_page(notion, page_id, info, gutachten["url"], success=has_owner)
            if has_owner:
                print(f"  ✅ Notion aktualisiert")
                ok_count += 1
            else:
                print(f"  ⚠️  Kein Eigentümer im PDF gefunden (evtl. gescanntes Dokument)")
                # Flag trotzdem setzen um keine Endlosschleife
                notion.pages.update(
                    page_id=page_id,
                    properties={N_ANALYSIERT: {"checkbox": True},
                                 N_NOTIZEN: rt_prop(f"Gutachten-PDF: {gutachten['url']}\n(Kein Grundbuch-Text lesbar – möglicherweise gescanntes Dokument)")}
                )
                skip_count += 1
        except Exception as e:
            print(f"  ❌ Notion-Update-Fehler: {e}")
            error_count += 1
            continue

        # Kurze Pause um API-Limits zu respektieren
        time.sleep(0.5)

    # ── Zusammenfassung ──────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"✅ Erfolgreich:    {ok_count}")
    print(f"⏭  Übersprungen:  {skip_count}")
    print(f"❌ Fehler:         {error_count}")
    print("=" * 60)


if __name__ == "__main__":
    main()
