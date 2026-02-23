"""
Edikte-Monitor – Österreich
============================
Scraper für https://edikte.justiz.gv.at (Gerichtliche Versteigerungen)
Alle Bundesländer | HTTP-Request (kein Browser nötig) | Notion | Telegram
"""

import os
import re
import json
import time
import asyncio
import urllib.request
import urllib.parse
from datetime import datetime
from notion_client import Client

try:
    import fitz          # PyMuPDF – optionale Abhängigkeit
    FITZ_AVAILABLE = True
except ImportError:
    fitz = None
    FITZ_AVAILABLE = False

# =============================================================================
# KONFIGURATION
# =============================================================================

BASE_URL = "https://edikte.justiz.gv.at"

# Bundesland-Werte aus dem Formular (name=BL)
BUNDESLAENDER = {
    "Wien":           "0",
    "Niederösterreich": "1",
    "Burgenland":     "2",
    "Oberösterreich": "3",
    "Salzburg":       "4",
    "Steiermark":     "5",
    "Kärnten":        "6",
    "Tirol":          "7",
    "Vorarlberg":     "8",
}

# Nur diese Link-Texte werden verarbeitet
RELEVANT_TYPES = ("Versteigerung", "Entfall des Termins", "Verschiebung")

# Schlüsselwörter im Link-Text → Objekt wird NICHT importiert
# (greift auf Ergebnisseite, wo der Text oft nur "Versteigerung (Datum)" ist)
EXCLUDE_KEYWORDS = [
    "landwirtschaft",
    "land- und forst",
    "forstwirtschaft",
    "gewerb",
    "betriebsobjekt",
    "industrie",
    "lager",
    "büro",
    "hotel",
    "pension",
]

# Kategorien aus der Detailseite → Objekt wird NICHT importiert
# Entspricht den Werten im Feld "Kategorie(n)" auf edikte.justiz.gv.at
EXCLUDE_KATEGORIEN = {
    "land- und forstwirtschaftlich genutzte liegenschaft",  # LF
    "gewerbliche liegenschaft",                             # GL
    "betriebsobjekt",
    "superädifikat",                                        # SE – nur wenn gewerblich
}

# Notion-Feldname für PLZ (exakt so wie in der Datenbank angelegt)
NOTION_PLZ_FIELD = "Liegenschafts PLZ"

# Edikt-ID aus dem Link extrahieren
ID_RE = re.compile(r"alldoc/([0-9a-f]+)!OpenDocument", re.IGNORECASE)

# Verkehrswert / Schätzwert
SCHAETZWERT_RE = re.compile(
    r'(?:Schätzwert|Verkehrswert|Schätzungswert|Wert)[:\s]+([\d\.\s,]+(?:EUR|€)?)',
    re.IGNORECASE
)


# =============================================================================
# HILFSFUNKTIONEN
# =============================================================================

def env(name: str) -> str:
    """Liest eine Umgebungsvariable – wirft Fehler wenn nicht gesetzt."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Fehlende Umgebungsvariable: {name}")
    return value


def clean_notion_db_id(raw: str) -> str:
    """Bereinigt die Notion Datenbank-ID (entfernt View-Parameter etc.)."""
    raw = raw.split("?")[0].strip()
    raw = raw.rstrip("/").split("/")[-1]
    clean = re.sub(r"[^0-9a-fA-F]", "", raw)
    if len(clean) == 32:
        return f"{clean[0:8]}-{clean[8:12]}-{clean[12:16]}-{clean[16:20]}-{clean[20:32]}"
    return raw


def is_excluded(text: str) -> bool:
    """Prüft ob ein Objekt anhand des Link-Texts ausgeschlossen werden soll."""
    return any(kw in text.lower() for kw in EXCLUDE_KEYWORDS)


def is_excluded_by_kategorie(kategorie: str) -> bool:
    """Prüft ob ein Objekt anhand der Detailseiten-Kategorie ausgeschlossen werden soll."""
    return kategorie.lower().strip() in EXCLUDE_KATEGORIEN


def parse_euro(raw: str) -> float | None:
    """
    Wandelt einen österreichischen Betragsstring in float um.
    z.B. '180.000,00 EUR' → 180000.0
    """
    try:
        cleaned = re.sub(r"[€EUReur\s]", "", raw.strip())
        cleaned = cleaned.replace(".", "").replace(",", ".")
        return float(cleaned)
    except Exception:
        return None


def parse_flaeche(raw: str) -> float | None:
    """Wandelt '96,72 m²' in 96.72 um."""
    try:
        m = re.search(r"([\d.,]+)", raw)
        if m:
            return float(m.group(1).replace(".", "").replace(",", "."))
    except Exception:
        pass
    return None


def fetch_detail(link: str) -> dict:
    """
    Lädt die Edikt-Detailseite und extrahiert alle strukturierten Felder
    direkt aus dem Bootstrap-Grid (span.col-sm-3 + p.col-sm-9).

    Liefert ein Dict mit den Schlüsseln:
      liegenschaftsadresse, plz_ort, adresse_voll   ← echte Immobilienadresse
      gericht, aktenzeichen, wegen
      termin, termin_iso
      kategorie, grundbuch, ez
      flaeche_objekt, flaeche_grundstueck
      schaetzwert (float), schaetzwert_str
      geringstes_gebot (float)
    """
    try:
        req = urllib.request.Request(
            link,
            headers={"User-Agent": "Mozilla/5.0 (compatible; EdikteMonitor/1.0)"}
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception as exc:
        print(f"    [Detail] ⚠️  Fehler beim Laden: {exc}")
        return {}

    # ── Alle label→value Paare aus dem Bootstrap-Grid extrahieren ────────────
    grid_re = re.compile(
        r'<span[^>]*col-sm-3[^>]*>\s*([^<]+?)\s*</span>\s*<p[^>]*col-sm-9[^>]*>\s*(.*?)\s*</p>',
        re.DOTALL | re.IGNORECASE
    )

    def clean(html_fragment: str) -> str:
        t = re.sub(r"<[^>]+>", " ", html_fragment)
        t = t.replace("\xa0", " ").replace("&nbsp;", " ")
        from html import unescape
        t = unescape(t)
        return " ".join(t.split()).strip()

    fields: dict[str, str] = {}
    for label, value in grid_re.findall(html):
        key = label.strip().rstrip(":").strip()
        fields[key] = clean(value)

    result: dict = {}

    # ── Liegenschaftsadresse (echte Immobilienadresse!) ──────────────────────
    adresse    = fields.get("Liegenschaftsadresse", "")
    plz_ort    = fields.get("PLZ/Ort", "")
    if adresse:
        result["liegenschaftsadresse"] = adresse
        result["plz_ort"]              = plz_ort
        result["adresse_voll"]         = f"{adresse}, {plz_ort}".strip(", ")
        print(f"    [Detail] 📍 Adresse: {result['adresse_voll']}")

    # ── Gericht / Dienststelle ────────────────────────────────────────────────
    if "Dienststelle" in fields:
        result["gericht"] = fields["Dienststelle"]
    elif "Dienststelle:" in fields:
        result["gericht"] = fields["Dienststelle:"]

    # ── Aktenzeichen ──────────────────────────────────────────────────────────
    for k in ("Aktenzeichen", "Aktenzeichen:"):
        if k in fields:
            result["aktenzeichen"] = fields[k]
            break

    # ── wegen ─────────────────────────────────────────────────────────────────
    if "wegen" in fields:
        result["wegen"] = fields["wegen"]

    # ── Versteigerungstermin ──────────────────────────────────────────────────
    termin_raw = fields.get("Versteigerungstermin", "")
    m = re.search(r"(\d{1,2}\.\d{1,2}\.\d{4})\s+um\s+([\d:]+\s*Uhr)", termin_raw)
    if m:
        result["termin"] = f"{m.group(1)} {m.group(2)}"
        try:
            dt = datetime.strptime(m.group(1), "%d.%m.%Y")
            result["termin_iso"] = dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    # ── Kategorie / Objektart ─────────────────────────────────────────────────
    if "Kategorie(n)" in fields:
        result["kategorie"] = fields["Kategorie(n)"]

    # ── Grundbuch / EZ ────────────────────────────────────────────────────────
    if "Grundbuch" in fields:
        result["grundbuch"] = fields["Grundbuch"]
    if "EZ" in fields:
        result["ez"] = fields["EZ"]

    # ── Flächen ───────────────────────────────────────────────────────────────
    fobj = fields.get("Objektgröße", "")
    if fobj:
        parsed = parse_flaeche(fobj)
        if parsed:
            result["flaeche_objekt"] = parsed

    fgrst = fields.get("Grundstücksgröße", "")
    if fgrst:
        parsed = parse_flaeche(fgrst)
        if parsed:
            result["flaeche_grundstueck"] = parsed

    # ── Schätzwert ────────────────────────────────────────────────────────────
    sv_raw = fields.get("Schätzwert", "")
    if sv_raw:
        result["schaetzwert_str"] = sv_raw
        parsed = parse_euro(sv_raw)
        if parsed is not None:
            result["schaetzwert"] = parsed
            print(f"    [Detail] 💰 Schätzwert: {parsed:,.0f} €")

    # ── Geringstes Gebot ──────────────────────────────────────────────────────
    gg_raw = fields.get("Geringstes Gebot", "")
    if gg_raw:
        parsed = parse_euro(gg_raw)
        if parsed is not None:
            result["geringstes_gebot"] = parsed

    return result


# =============================================================================
# TELEGRAM
# =============================================================================

def html_escape(text: str) -> str:
    """Escapt Sonderzeichen für Telegram HTML-Modus."""
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


def _telegram_send_raw(url: str, payload_dict: dict) -> None:
    """Interne Hilfsfunktion: sendet einen JSON-Payload an die Telegram API."""
    payload = json.dumps(payload_dict, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        r.read()


def _truncate_plain(text: str, limit: int = 4096) -> str:
    """Kürzt Plain-Text sicher auf das Zeichenlimit."""
    if len(text) <= limit:
        return text
    return text[:limit - 6] + "\n[...]"


def _strip_html_tags(text: str) -> str:
    """Entfernt alle HTML-Tags und dekodiert HTML-Entities."""
    plain = re.sub(r"<[^>]+>", "", text)
    plain = plain.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return plain


async def send_telegram(message: str) -> None:
    """
    Sendet eine Nachricht via Telegram Bot (HTML-Modus).
    - Wenn die Nachricht > 4096 Zeichen: wird in mehrere Teile aufgeteilt,
      wobei jeder Teil an einer Zeilengrenze getrennt wird (kein halber HTML-Tag).
    - Bei HTML-Fehler (400): Fallback auf reinen Text ohne parse_mode.
    """
    token   = env("TELEGRAM_BOT_TOKEN")
    chat_id = env("TELEGRAM_CHAT_ID")
    url     = f"https://api.telegram.org/bot{token}/sendMessage"

    def split_message(text: str, limit: int = 4000) -> list[str]:
        """Teilt eine Nachricht an Zeilengrenzen auf, sodass kein HTML-Tag zerrissen wird."""
        if len(text) <= limit:
            return [text]
        parts = []
        current = []
        current_len = 0
        for line in text.split("\n"):
            line_len = len(line) + 1  # +1 für \n
            if current_len + line_len > limit and current:
                parts.append("\n".join(current))
                current = [line]
                current_len = line_len
            else:
                current.append(line)
                current_len += line_len
        if current:
            parts.append("\n".join(current))
        return parts

    parts = split_message(message)
    total = len(parts)

    for i, part in enumerate(parts, 1):
        label = f" ({i}/{total})" if total > 1 else ""
        try:
            _telegram_send_raw(url, {
                "chat_id":                  chat_id,
                "text":                     part,
                "parse_mode":               "HTML",
                "disable_web_page_preview": True,
            })
            print(f"[Telegram] ✅ Nachricht{label} gesendet ({len(part)} Zeichen)")
        except Exception as e:
            print(f"[Telegram] ⚠️  HTML-Modus fehlgeschlagen{label} ({e}), versuche Plain Text …")
            # Fallback: HTML-Tags entfernen, kein parse_mode senden
            plain = _truncate_plain(_strip_html_tags(part))
            try:
                _telegram_send_raw(url, {
                    "chat_id":                  chat_id,
                    "text":                     plain,
                    "disable_web_page_preview": True,
                })
                print(f"[Telegram] ✅ Plain-Text{label} gesendet ({len(plain)} Zeichen)")
            except Exception as e2:
                raise RuntimeError(f"Telegram komplett fehlgeschlagen{label}: {e2}") from e2


# =============================================================================
# GUTACHTEN – PDF-DOWNLOAD & PARSING
# =============================================================================

def gutachten_fetch_attachment_links(edikt_url: str) -> dict:
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


def gutachten_pick_best_pdf(pdfs: list) -> dict | None:
    """Wählt das wahrscheinlichste Gutachten-PDF aus der Liste."""
    preferred = ["gutachten", " g ", "sachverst", "sv-", "/g-", "g "]
    for pdf in pdfs:
        if any(kw in pdf["filename"].lower() for kw in preferred):
            return pdf
    for pdf in pdfs:
        if "anlagen" not in pdf["filename"].lower():
            return pdf
    return pdfs[0] if pdfs else None


def gutachten_download_pdf(url: str) -> bytes:
    """Lädt ein PDF herunter und gibt die Bytes zurück."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; EdikteMonitor/1.0)"}
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def _gb_extract_section(text: str, start_marker: str, end_marker: str) -> str:
    """Extrahiert Text zwischen zwei Markierungen."""
    start = text.lower().find(start_marker.lower())
    if start == -1:
        return ""
    end = text.lower().find(end_marker.lower(), start + len(start_marker))
    if end == -1:
        return text[start:]
    return text[start:end]


def _gb_parse_owner(section_b: str) -> dict:
    """Parst Namen und Adresse aus Section B des Grundbuchs."""
    result = {
        "eigentümer_name":    "",
        "eigentümer_adresse": "",
        "eigentümer_plz_ort": "",
        "eigentümer_geb":     "",
    }
    adr_pattern  = re.compile(
        r'GEB:\s*(\d{4}-\d{2}-\d{2})\s+ADR:\s*(.+?)\s{2,}(\d{4,5})\s*$',
        re.IGNORECASE
    )
    adr_no_geb   = re.compile(r'ADR:\s*(.+?)\s{2,}(\d{4,5})\s*$', re.IGNORECASE)
    adr_simple   = re.compile(r'ADR:\s*(.+)', re.IGNORECASE)

    lines = section_b.splitlines()
    for i, line in enumerate(lines):
        if "ANTEIL:" not in line.upper():
            continue
        for j in range(i + 1, min(i + 8, len(lines))):
            candidate = lines[j]
            stripped  = candidate.strip()
            if not stripped:
                continue
            if re.match(r'^\d', stripped):          continue
            if re.match(r'^[a-z]\s+\d', stripped):  continue
            if "GEB:" in stripped.upper():           continue
            if "ADR:" in stripped.upper():           continue
            if re.match(r'^\*+', stripped):          continue
            result["eigentümer_name"] = stripped
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
            break
        break
    return result


def _gb_parse_creditors(section_c: str) -> tuple:
    """Parst Pfandrechtsgläubiger und Forderungsbeträge aus Section C."""
    gläubiger = []
    betrag    = ""
    lines = [l.strip() for l in section_c.splitlines() if l.strip()]
    fuer_pattern   = re.compile(r'^für\s+(.+)', re.IGNORECASE)
    betrag_pattern = re.compile(r'Hereinbringung von\s+(EUR\s+[\d\.,]+)', re.IGNORECASE)
    pfand_pattern  = re.compile(r'PFANDRECHT\s+Höchstbetrag\s+(EUR\s+[\d\.,]+)', re.IGNORECASE)
    seen = set()
    for line in lines:
        m = fuer_pattern.match(line)
        if m:
            name = m.group(1).strip().rstrip(".")
            if len(name) > 5 and name not in seen:
                gläubiger.append(name)
                seen.add(name)
        if not betrag:
            mb = betrag_pattern.search(line)
            if mb:
                betrag = mb.group(1).strip()
    if not betrag:
        for line in lines:
            mp = pfand_pattern.search(line)
            if mp:
                betrag = mp.group(1).strip()
                break
    return gläubiger, betrag


def gutachten_extract_info(pdf_bytes: bytes) -> dict:
    """
    Extrahiert Eigentümer, Adresse, Gläubiger und Forderungsbetrag aus dem PDF.
    Unterstützt Grundbuchauszug-Format (Kärnten-Stil) und professionelle
    Gutachten mit 'Verpflichtete Partei:'-Angabe (Wien-Stil).
    Gibt leeres Dict zurück wenn fitz nicht verfügbar ist.
    """
    if not FITZ_AVAILABLE:
        return {}

    doc      = fitz.open(stream=pdf_bytes, filetype="pdf")
    all_text = [p.get_text() for p in doc if p.get_text().strip()]
    full_text = "\n".join(all_text)

    result = {
        "eigentümer_name":    "",
        "eigentümer_adresse": "",
        "eigentümer_plz_ort": "",
        "eigentümer_geb":     "",
        "gläubiger":          [],
        "forderung_betrag":   "",
    }

    # ── Format 1: Grundbuchauszug Sektionen B / C ────────────────────────────
    sec_b = _gb_extract_section(full_text, "** B ***", "** C ***")
    if not sec_b:
        sec_b = _gb_extract_section(full_text, "** B **", "** C **")
    if sec_b:
        result.update(_gb_parse_owner(sec_b))

    sec_c = _gb_extract_section(full_text, "** C ***", "** HINWEIS ***")
    if not sec_c:
        sec_c = _gb_extract_section(full_text, "** C **", "HINWEIS")
    if sec_c:
        gl, bt = _gb_parse_creditors(sec_c)
        result["gläubiger"]        = gl
        result["forderung_betrag"] = bt

    # ── Format 2: Professionelles Gutachten (Verpflichtete Partei) ───────────
    if not result["eigentümer_name"]:
        vp = re.search(
            r'Verpflichtete(?:\s+Partei)?:\s*(.+?)(?:\n|Betreibende|Auftraggeber)',
            full_text[:3000], re.IGNORECASE | re.DOTALL
        )
        if vp:
            result["eigentümer_name"] = vp.group(1).strip().split("\n")[0].strip()

    if result["eigentümer_name"] and not result["eigentümer_adresse"]:
        name_esc = re.escape(result["eigentümer_name"][:30])
        adr_after = re.search(
            name_esc + r'[^\n]*\n\s*([A-ZÄÖÜ][^\n]{5,60})\s*\n',
            full_text[:3000], re.IGNORECASE
        )
        if adr_after:
            adr_raw = adr_after.group(1).strip()
            plz_m = re.search(r'\b(\d{4,5})\b', adr_raw)
            if plz_m:
                result["eigentümer_plz_ort"] = plz_m.group(1)
                result["eigentümer_adresse"] = adr_raw

    if not result["gläubiger"]:
        bp = re.search(
            r'Betreibende(?:\s+Partei)?:\s*(.+?)(?:\n|Verpflichtete)',
            full_text[:3000], re.IGNORECASE | re.DOTALL
        )
        if bp:
            g = bp.group(1).strip().split("\n")[0].strip()
            if g:
                result["gläubiger"] = [g]

    return result


def gutachten_enrich_notion_page(
    notion: Client,
    page_id: str,
    edikt_url: str,
) -> bool:
    """
    Hauptfunktion: Lädt das Gutachten-PDF von der Edikt-Seite,
    extrahiert Eigentümer/Gläubiger und schreibt sie in die Notion-Seite.

    Gibt True zurück wenn erfolgreich, False bei Fehler oder fehlendem PDF.
    Das Flag 'Gutachten analysiert?' wird immer gesetzt (True/False).
    """
    if not FITZ_AVAILABLE:
        print("    [Gutachten] ⚠️  PyMuPDF nicht verfügbar – überspringe PDF-Analyse")
        return False

    try:
        attachments = gutachten_fetch_attachment_links(edikt_url)
        pdfs = attachments["pdfs"]
    except Exception as exc:
        print(f"    [Gutachten] ⚠️  Fehler beim Laden der Edikt-Seite: {exc}")
        notion.pages.update(
            page_id=page_id,
            properties={"Gutachten analysiert?": {"checkbox": False}}
        )
        return False

    if not pdfs:
        print("    [Gutachten] ℹ️  Kein PDF-Anhang gefunden")
        notion.pages.update(
            page_id=page_id,
            properties={"Gutachten analysiert?": {"checkbox": False}}
        )
        return False

    gutachten = gutachten_pick_best_pdf(pdfs)
    print(f"    [Gutachten] 📄 {gutachten['filename']}")

    try:
        pdf_bytes = gutachten_download_pdf(gutachten["url"])
    except Exception as exc:
        print(f"    [Gutachten] ⚠️  Download-Fehler: {exc}")
        notion.pages.update(
            page_id=page_id,
            properties={"Gutachten analysiert?": {"checkbox": False}}
        )
        return False

    try:
        info = gutachten_extract_info(pdf_bytes)
    except Exception as exc:
        print(f"    [Gutachten] ⚠️  Parse-Fehler: {exc}")
        notion.pages.update(
            page_id=page_id,
            properties={"Gutachten analysiert?": {"checkbox": False}}
        )
        return False

    # ── Notion-Properties aufbauen ───────────────────────────────────────────
    has_owner = bool(info.get("eigentümer_name") or info.get("eigentümer_adresse"))
    properties: dict = {
        "Gutachten analysiert?": {"checkbox": True},
    }

    def _rt(text: str) -> dict:
        return {"rich_text": [{"text": {"content": str(text)[:2000]}}]}

    if info.get("eigentümer_name"):
        print(f"    [Gutachten] 👤 Eigentümer: {info['eigentümer_name']}")
        properties["Verpflichtende Partei"] = _rt(info["eigentümer_name"])

    if info.get("eigentümer_adresse"):
        print(f"    [Gutachten] 🏠 Adresse: {info['eigentümer_adresse']}")
        properties["Zustell Adresse"] = _rt(info["eigentümer_adresse"])

    if info.get("eigentümer_plz_ort"):
        properties["Zustell PLZ/Ort"] = _rt(info["eigentümer_plz_ort"])

    # Notizen: Gläubiger + Forderung + PDF-Link
    notiz_parts = []
    if info.get("gläubiger"):
        print(f"    [Gutachten] 🏦 Gläubiger: {' | '.join(info['gläubiger'][:2])}")
        notiz_parts.append("Gläubiger: " + " | ".join(info["gläubiger"]))
    if info.get("forderung_betrag"):
        notiz_parts.append("Forderung: " + info["forderung_betrag"])
    notiz_parts.append(f"Gutachten-PDF: {gutachten['url']}")
    properties["Notizen"] = _rt("\n".join(notiz_parts))

    if not has_owner:
        # Gescanntes Dokument – trotzdem als analysiert markieren
        properties["Notizen"] = _rt(
            f"Gutachten-PDF: {gutachten['url']}\n"
            "(Kein Grundbuch-Text lesbar – möglicherweise gescanntes Dokument)"
        )
        print("    [Gutachten] ⚠️  Kein Eigentümer gefunden (gescanntes Dokument?)")

    try:
        notion.pages.update(page_id=page_id, properties=properties)
        print("    [Gutachten] ✅ Notion aktualisiert")
    except Exception as exc:
        print(f"    [Gutachten] ⚠️  Notion-Update-Fehler: {exc}")
        return False

    return True


# =============================================================================
# NOTION
# =============================================================================

def notion_load_all_ids(notion: Client, db_id: str) -> dict[str, str]:
    """
    Lädt ALLE bestehenden Einträge aus der Notion-DB und gibt ein Dict
    {edikt_id -> page_id} zurück.

    Zusätzlich werden Einträge mit fortgeschrittener Workflow-Phase
    (z.B. 'Angeschrieben', 'Angebot', 'Gekauft') unter dem Sentinel-Wert
    "(geschuetzt)" gespeichert – der Scraper überspringt diese komplett,
    auch wenn die Hash-ID matcht. So werden bereits bearbeitete Immobilien
    niemals dupliziert oder überschrieben.

    Paginierung: Notion liefert max. 100 Ergebnisse pro Anfrage.
    """
    # Workflow-Phasen die NICHT überschrieben werden dürfen
    GESCHUETZT_PHASEN = {
        "📨 Angeschrieben",
        "🤝 Angebot",
        "📋 Due Diligence",
        "✅ Gekauft",
        "❌ Abgelehnt",
    }

    print("[Notion] 📥 Lade alle bestehenden IDs aus der Datenbank …")
    known: dict[str, str] = {}  # edikt_id -> page_id  (oder "(geschuetzt)")
    has_more = True
    cursor = None
    page_count = 0
    geschuetzt_count = 0

    while has_more:
        kwargs: dict = {"filter": {"value": "page", "property": "object"}, "page_size": 100}
        if cursor:
            kwargs["start_cursor"] = cursor
        try:
            resp = notion.search(**kwargs)
        except Exception as exc:
            print(f"  [Notion] ⚠️  Fehler beim Laden der IDs: {exc}")
            break

        for page in resp.get("results", []):
            # Nur Pages aus unserer DB
            parent = page.get("parent", {})
            if parent.get("database_id", "").replace("-", "") != db_id.replace("-", ""):
                continue

            props = page.get("properties", {})

            # Workflow-Phase prüfen
            phase_sel = props.get("Workflow-Phase", {}).get("select") or {}
            phase = phase_sel.get("name", "")
            ist_geschuetzt = phase in GESCHUETZT_PHASEN

            # Hash-ID auslesen
            hash_rt = props.get("Hash-ID / Vergleichs-ID", {}).get("rich_text", [])
            eid = ""
            if hash_rt:
                eid = hash_rt[0].get("plain_text", "").strip().lower()

            if eid:
                if ist_geschuetzt:
                    known[eid] = "(geschuetzt)"
                    geschuetzt_count += 1
                else:
                    known[eid] = page["id"]

            # Einträge OHNE Hash-ID aber MIT fortgeschrittener Phase:
            # Titel als Ersatz-Fingerprint speichern (verhindert Doppelanlage
            # bei manuell eingetragenen Immobilien ohne Hash-ID)
            elif ist_geschuetzt:
                title_rt = props.get("Liegenschaftsadresse", {}).get("title", [])
                title = title_rt[0].get("plain_text", "").strip().lower() if title_rt else ""
                if title:
                    known[f"__titel__{title}"] = "(geschuetzt)"
                    geschuetzt_count += 1

            page_count += 1

        has_more = resp.get("has_more", False)
        cursor   = resp.get("next_cursor")

    print(f"[Notion] ✅ {len(known)} Einträge geladen "
          f"({geschuetzt_count} geschützt, {page_count} Seiten geprüft)")
    return known


def notion_create_eintrag(notion: Client, db_id: str, data: dict) -> dict:
    """
    Legt einen neuen Eintrag in Notion an.
    Ruft die Detailseite ab, filtert nach Kategorie und befüllt alle Felder.
    Gibt den detail-Dict zurück (oder {} wenn Objekt gefiltert wurde).
    Rückgabe None bedeutet: Objekt wurde durch Kategorie-Filter ausgeschlossen.
    """
    bundesland   = data.get("bundesland", "Unbekannt")
    link         = data.get("link", "")
    edikt_id     = data.get("edikt_id", "")
    beschreibung = data.get("beschreibung", "")
    typ          = data.get("type", "Versteigerung")

    # ── Detailseite abrufen ──────────────────────────────────────────────────
    detail: dict = {}
    if link:
        detail = fetch_detail(link)

    # ── Kategorie-Filter (auf Detailseite, zuverlässiger als Link-Text) ──────
    kategorie = detail.get("kategorie", "")
    if kategorie and is_excluded_by_kategorie(kategorie):
        print(f"  [Filter] ⛔ Kategorie ausgeschlossen: '{kategorie}' ({edikt_id[:8]}…)")
        return None  # Signalisiert: nicht importieren

    # ── Liegenschaftsadresse als Titel ───────────────────────────────────────
    adresse_voll = detail.get("adresse_voll", "")
    if not adresse_voll:
        datum_m = re.search(r"\((\d{2}\.\d{2}\.\d{4})\)", beschreibung)
        adresse_voll = f"{bundesland} – {datum_m.group(1) if datum_m else beschreibung[:60]}"

    titel    = adresse_voll
    objektart = kategorie or beschreibung[:200]

    # ── Kern-Properties (existieren garantiert in jeder Notion-DB) ───────────
    properties: dict = {
        "Liegenschaftsadresse": {
            "title": [{"text": {"content": titel[:200]}}]
        },
        "Hash-ID / Vergleichs-ID": {
            "rich_text": [{"text": {"content": edikt_id}}]
        },
        "Link": {"url": link},
        "Art des Edikts": {
            "select": {
                "name": typ if typ in ("Versteigerung", "Entfall des Termins") else "Versteigerung"
            }
        },
        "Bundesland":              {"select": {"name": bundesland}},
        "Neu eingelangt":          {"checkbox": True},
        "Automatisch importiert?": {"checkbox": True},
        "Workflow-Phase":          {"select": {"name": "🆕 Neu eingelangt"}},
        "Objektart": {
            "rich_text": [{"text": {"content": objektart[:200]}}]
        },
    }

    # ── Optionale Properties – werden einzeln hinzugefügt ────────────────────
    # Schlägt ein Feld fehl, wird nur dieses Feld übersprungen, nicht der ganze Eintrag.

    verkehrswert = detail.get("schaetzwert")
    if verkehrswert is not None:
        vk_str = f"{verkehrswert:,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")
        properties["Verkehrswert"] = {"rich_text": [{"text": {"content": vk_str}}]}

    termin_iso = detail.get("termin_iso")
    if termin_iso:
        properties["Versteigerungstermin"] = {"date": {"start": termin_iso}}

    gericht = detail.get("gericht", "")
    if gericht:
        properties["Verpflichtende Partei"] = {
            "rich_text": [{"text": {"content": gericht[:200]}}]
        }

    plz_ort = detail.get("plz_ort", "")
    if plz_ort:
        # Vollständig: "1120 Wien" → "1120 Wien"
        properties[NOTION_PLZ_FIELD] = {
            "rich_text": [{"text": {"content": plz_ort.strip()[:100]}}]
        }

    flaeche = detail.get("flaeche_objekt") or detail.get("flaeche_grundstueck")
    if flaeche is not None:
        flaeche_str = f"{flaeche:,.2f} m²".replace(",", "X").replace(".", ",").replace("X", ".")
        properties["Fläche"] = {"rich_text": [{"text": {"content": flaeche_str}}]}

    # ── Seite anlegen – erst Kern, dann optionale Felder einzeln ─────────────
    # Strategie: Kern-Properties zuerst. Falls optionale Felder nicht existieren,
    # werden sie weggelassen und der Eintrag trotzdem angelegt.
    created_page = None
    try:
        created_page = notion.pages.create(parent={"database_id": db_id}, properties=properties)
        print(f"  [Notion] ✅ Erstellt: {titel[:80]}")
    except Exception as e:
        err_str = str(e)
        # Herausfinden welches Feld das Problem ist und es entfernen
        optional_fields = [NOTION_PLZ_FIELD, "Fläche", "Verkehrswert",
                           "Versteigerungstermin", "Verpflichtende Partei"]
        removed = []
        for field in optional_fields:
            if field in err_str and field in properties:
                del properties[field]
                removed.append(field)

        if removed:
            print(f"  [Notion] ⚠️  Felder nicht gefunden, übersprungen: {removed}")
            try:
                created_page = notion.pages.create(parent={"database_id": db_id}, properties=properties)
                print(f"  [Notion] ✅ Erstellt (ohne {removed}): {titel[:80]}")
            except Exception as e2:
                raise e2  # Wirklicher Fehler → nach oben weitergeben
        else:
            raise  # Kein bekanntes optionales Feld → nach oben weitergeben

    # Gibt (detail, page_id) zurück damit der Aufrufer das Gutachten anreichern kann
    new_page_id = created_page["id"] if created_page else None
    return detail, new_page_id


def notion_mark_entfall(notion: Client, page_id: str, item: dict) -> None:
    """Markiert ein bestehendes Notion-Objekt als 'Termin entfallen'."""
    notion.pages.update(
        page_id=page_id,
        properties={
            "Art des Edikts": {"select": {"name": "Entfall des Termins"}},
            "Archiviert":     {"checkbox": True},
            "Workflow-Phase": {"select": {"name": "🗄 Archiviert"}},
            "Neu eingelangt": {"checkbox": False},
        },
    )
    print(f"  [Notion] 🔴 Entfall markiert: {item.get('edikt_id', '?')}")


def notion_enrich_urls(notion: Client, db_id: str) -> int:
    """
    Findet Notion-Einträge OHNE Link-URL und versucht, über die Edikte-Suche
    einen passenden Eintrag zu finden.

    Strategie:
    1. Alle Pages aus der DB via search() laden.
    2. Falls die Seite eine Hash-ID hat → Link direkt konstruieren.
    3. Falls nicht → über Titel / Bundesland eine Freitextsuche machen.

    Gibt die Anzahl der erfolgreich ergänzten URLs zurück.
    """
    print("\n[URL-Anreicherung] 🔗 Suche nach Einträgen ohne URL …")

    enriched = 0

    # Alle Seiten via search() laden (notion-client v3 hat kein databases.query)
    pages_without_url: list[dict] = []
    has_more = True
    start_cursor = None

    while has_more:
        kwargs: dict = {
            "filter": {"value": "page", "property": "object"},
            "page_size": 100,
        }
        if start_cursor:
            kwargs["start_cursor"] = start_cursor

        try:
            resp = notion.search(**kwargs)
        except Exception as exc:
            print(f"  [URL-Anreicherung] ❌ Notion-Abfrage fehlgeschlagen: {exc}")
            break

        for page in resp.get("results", []):
            # Nur Pages aus unserer DB
            parent = page.get("parent", {})
            if parent.get("database_id", "").replace("-", "") != db_id.replace("-", ""):
                continue
            # Nur Pages ohne Link
            props    = page.get("properties", {})
            link_val = props.get("Link", {}).get("url")
            if not link_val:
                pages_without_url.append(page)

        has_more = resp.get("has_more", False)
        start_cursor = resp.get("next_cursor")

    print(f"  [URL-Anreicherung] 📋 {len(pages_without_url)} Einträge ohne URL gefunden")

    for page in pages_without_url:
        page_id = page["id"]
        props   = page.get("properties", {})

        # Hash-ID vorhanden? → Link direkt bauen
        hash_rt = props.get("Hash-ID / Vergleichs-ID", {}).get("rich_text", [])
        if hash_rt:
            edikt_id = hash_rt[0].get("plain_text", "").strip()
            if edikt_id and re.fullmatch(r"[0-9a-f]{32}", edikt_id):
                constructed_link = (
                    f"{BASE_URL}/edikte/ex/exedi3.nsf/alldoc/{edikt_id}!OpenDocument"
                )
                try:
                    notion.pages.update(
                        page_id=page_id,
                        properties={"Link": {"url": constructed_link}},
                    )
                    enriched += 1
                    print(f"  [URL-Anreicherung] ✅ Link gesetzt (Hash-ID): {edikt_id}")
                except Exception as exc:
                    print(f"  [URL-Anreicherung] ❌ Update fehlgeschlagen ({edikt_id}): {exc}")
                continue

        # Kein Hash-ID → Titel-Suche auf edikte.at
        title_rt = props.get("Liegenschaftsadresse", {}).get("title", [])
        titel = title_rt[0].get("plain_text", "") if title_rt else ""

        bl_prop = props.get("Bundesland", {}).get("select") or {}
        bundesland_name = bl_prop.get("name", "")
        bl_value = BUNDESLAENDER.get(bundesland_name, "")

        if not titel and not bl_value:
            print(f"  [URL-Anreicherung] ⚠️  Kein Titel/Bundesland für {page_id[:8]}…")
            continue

        # Suche für das Bundesland + Keyword aus dem Titel
        keyword = re.sub(r"(Wien|Niederösterreich|Burgenland|Oberösterreich|Salzburg|"
                         r"Steiermark|Kärnten|Tirol|Vorarlberg)", "", titel).strip()
        keyword = keyword[:40] if keyword else ""

        matches = _search_edikt_by_keyword(bl_value, keyword)
        if len(matches) == 1:
            candidate = matches[0]
            try:
                notion.pages.update(
                    page_id=page_id,
                    properties={
                        "Link": {"url": candidate["link"]},
                        "Hash-ID / Vergleichs-ID": {
                            "rich_text": [{"text": {"content": candidate["edikt_id"]}}]
                        },
                    },
                )
                enriched += 1
                print(
                    f"  [URL-Anreicherung] ✅ Link gefunden (Freitext): "
                    f"{candidate['edikt_id']}"
                )
            except Exception as exc:
                print(f"  [URL-Anreicherung] ❌ Update fehlgeschlagen: {exc}")
        elif len(matches) == 0:
            print(f"  [URL-Anreicherung] 🔍 Kein Treffer für '{titel[:50]}'")
        else:
            print(
                f"  [URL-Anreicherung] ❓ {len(matches)} Treffer (mehrdeutig) "
                f"für '{titel[:50]}' – übersprungen"
            )

    print(f"[URL-Anreicherung] ✅ {enriched} URLs ergänzt")
    return enriched


def _search_edikt_by_keyword(bl_value: str, keyword: str) -> list[dict]:
    """
    Interne Hilfsfunktion: Sucht auf edikte.at für ein Bundesland mit einem
    Freitext-Keyword und gibt die gefundenen Items zurück.
    """
    if not bl_value:
        return []

    query_parts = [f"([BL]=({bl_value}))"]
    if keyword:
        query_parts.append(keyword)

    params = urllib.parse.urlencode({
        "SearchView": "",
        "subf": "eex",
        "SearchOrder": "4",
        "SearchMax": "50",
        "retfields": f"~BL={bl_value}",
        "ftquery": keyword,
        "query": " ".join(query_parts),
    })
    url = f"{BASE_URL}/edikte/ex/exedi3.nsf/suchedi?{params}"

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; EdikteMonitor/1.0)"}
        )
        with urllib.request.urlopen(req, timeout=20) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception:
        return []

    rel_pattern = re.compile(
        r'<a[^>]+href="(alldoc/([0-9a-f]+)!OpenDocument)"[^>]*>([^<]+)</a>',
        re.IGNORECASE
    )

    results = []
    for href_rel, edikt_id, link_text in rel_pattern.findall(html):
        link_text = link_text.strip()
        if not any(link_text.startswith(t) for t in RELEVANT_TYPES):
            continue
        results.append({
            "edikt_id": edikt_id.lower(),
            "link": f"{BASE_URL}/edikte/ex/exedi3.nsf/{href_rel}",
            "beschreibung": link_text,
        })
    return results


def notion_enrich_gutachten(notion: Client, db_id: str) -> int:
    """
    Findet alle Notion-Einträge die:
      - eine URL (Link) haben, UND
      - 'Gutachten analysiert?' = False / nicht gesetzt haben, UND
      - NICHT in einer geschützten Workflow-Phase sind

    Für jeden solchen Eintrag wird das Gutachten-PDF heruntergeladen
    und die Properties (Eigentümer, Adresse, Gläubiger, Forderung) befüllt.

    Das ist der Weg für manuell eingetragene Immobilien:
    Sobald die URL gesetzt wird (entweder vom Nutzer oder durch URL-Anreicherung),
    wird das Gutachten automatisch beim nächsten Lauf analysiert.

    Gibt die Anzahl der erfolgreich angereicherten Einträge zurück.
    """
    GESCHUETZT_PHASEN = {
        "📨 Angeschrieben", "🤝 Angebot",
        "📋 Due Diligence", "✅ Gekauft", "❌ Abgelehnt",
    }

    print("\n[Gutachten-Anreicherung] 📄 Suche nach Einträgen ohne Gutachten-Analyse …")

    to_enrich: list[dict] = []
    has_more     = True
    start_cursor = None

    while has_more:
        kwargs: dict = {
            "filter": {"value": "page", "property": "object"},
            "page_size": 100,
        }
        if start_cursor:
            kwargs["start_cursor"] = start_cursor

        try:
            resp = notion.search(**kwargs)
        except Exception as exc:
            print(f"  [Gutachten-Anreicherung] ❌ Notion-Abfrage fehlgeschlagen: {exc}")
            break

        for page in resp.get("results", []):
            parent = page.get("parent", {})
            if parent.get("database_id", "").replace("-", "") != db_id.replace("-", ""):
                continue

            props = page.get("properties", {})

            # Nur Einträge in nicht-geschützter Phase
            phase = (props.get("Workflow-Phase", {}).get("select") or {}).get("name", "")
            if phase in GESCHUETZT_PHASEN:
                continue

            # Muss eine URL haben
            link_val = props.get("Link", {}).get("url")
            if not link_val:
                continue

            # Noch nicht analysiert
            analysiert = props.get("Gutachten analysiert?", {}).get("checkbox", False)
            if analysiert:
                continue

            to_enrich.append({"page_id": page["id"], "link": link_val})

        has_more     = resp.get("has_more", False)
        start_cursor = resp.get("next_cursor")

    MAX_PER_RUN = 15   # Begrenzung: max. 15 PDFs pro Run (~2–3 Min. Laufzeit)
    total_found = len(to_enrich)
    if total_found > MAX_PER_RUN:
        print(f"  [Gutachten-Anreicherung] ⚠️  {total_found} gefunden – verarbeite nur die ersten {MAX_PER_RUN} (Rest beim nächsten Run)")
        to_enrich = to_enrich[:MAX_PER_RUN]

    print(f"  [Gutachten-Anreicherung] 📋 {len(to_enrich)} Einträge werden jetzt analysiert")

    enriched = 0
    for entry in to_enrich:
        try:
            ok = gutachten_enrich_notion_page(notion, entry["page_id"], entry["link"])
            if ok:
                enriched += 1
        except Exception as exc:
            print(f"  [Gutachten-Anreicherung] ❌ Fehler für {entry['page_id'][:8]}…: {exc}")
        time.sleep(0.3)   # kurze Pause um API-Limits zu schonen

    remaining = total_found - len(to_enrich)
    if remaining > 0:
        print(f"  [Gutachten-Anreicherung] ℹ️  Noch {remaining} Einträge offen – werden in nächsten Runs verarbeitet")
    print(f"[Gutachten-Anreicherung] ✅ {enriched} Gutachten analysiert")
    return enriched


# =============================================================================
# SCRAPING – direkte HTTP-Requests (kein Browser nötig!)
# =============================================================================

def fetch_results_for_state(bundesland: str, bl_value: str) -> list[dict]:
    """
    Ruft die Ergebnisseite für ein Bundesland direkt per HTTP ab.

    Die URL-Struktur wurde durch Analyse des Formulars ermittelt:
    /edikte/ex/exedi3.nsf/suchedi?SearchView&subf=eex&...&query=([BL]=(X))
    """
    print(f"\n[Scraper] 🔍 Suche für: {bundesland} (BL={bl_value})")

    query = f"([BL]=({bl_value}))"
    params = urllib.parse.urlencode({
        "SearchView": "",
        "subf": "eex",
        "SearchOrder": "4",
        "SearchMax": "4999",
        "retfields": f"~BL={bl_value}",
        "ftquery": "",
        "query": query,
    })
    url = f"{BASE_URL}/edikte/ex/exedi3.nsf/suchedi?{params}"

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; EdikteMonitor/1.0)"}
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            html = r.read().decode("utf-8", errors="replace")
    except Exception as exc:
        print(f"  [Scraper] ❌ HTTP-Fehler: {exc}")
        return []

    # Links extrahieren – Format: alldoc/HEX!OpenDocument (relativ, ohne führendes /)
    rel_pattern = re.compile(
        r'<a[^>]+href="(alldoc/([0-9a-f]+)!OpenDocument)"[^>]*>([^<]+)</a>',
        re.IGNORECASE
    )

    results = []
    seen_ids = set()

    for href_rel, edikt_id, link_text in rel_pattern.findall(html):
        link_text = link_text.strip()
        edikt_id  = edikt_id.lower()
        href      = f"{BASE_URL}/edikte/ex/exedi3.nsf/{href_rel}"

        if edikt_id in seen_ids:
            continue
        seen_ids.add(edikt_id)

        # Typ bestimmen
        typ = None
        for t in RELEVANT_TYPES:
            if link_text.startswith(t):
                typ = t
                break
        if not typ:
            continue

        # Ausschlussliste (nur bei Versteigerung relevant)
        if typ == "Versteigerung" and is_excluded(link_text):
            print(f"  [Filter] ⛔ Ausgeschlossen: {link_text[:80]}")
            continue

        results.append({
            "bundesland":   bundesland,
            "type":         typ,
            "beschreibung": link_text,
            "link":         href,
            "edikt_id":     edikt_id,
        })

    print(f"  [Scraper] 📋 {len(results)} relevante Treffer für {bundesland}")
    return results


# =============================================================================
# MAIN
# =============================================================================

async def main() -> None:
    print("=" * 60)
    print(f"Edikte-Monitor gestartet: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    notion = Client(auth=env("NOTION_TOKEN"))
    db_id  = clean_notion_db_id(env("NOTION_DATABASE_ID"))

    neue_eintraege:  list[dict] = []
    entfall_updates: list[dict] = []
    fehler:          list[str]  = []

    # ── 1. Alle bekannten IDs einmalig laden (schnelle lokale Deduplizierung) ─
    try:
        known_ids = notion_load_all_ids(notion, db_id)  # {edikt_id -> page_id}
    except Exception as exc:
        print(f"  [ERROR] Konnte IDs nicht laden: {exc}")
        known_ids = {}

    # ── 2. Edikte scrapen + in Notion eintragen ───────────────────────────────
    for bundesland, bl_value in BUNDESLAENDER.items():
        try:
            results = fetch_results_for_state(bundesland, bl_value)
        except Exception as exc:
            msg = f"Scraper-Fehler {bundesland}: {exc}"
            print(f"  [ERROR] {msg}")
            fehler.append(msg)
            continue

        for item in results:
            try:
                eid = item["edikt_id"].lower()

                if item["type"] == "Versteigerung":
                    if known_ids.get(eid) == "(geschuetzt)":
                        print(f"  [Notion] 🔒 Geschützt (bereits bearbeitet): {eid}")
                    elif eid not in known_ids:
                        result_tuple = notion_create_eintrag(notion, db_id, item)
                        if result_tuple is None:
                            # Kategorie-Filter hat das Objekt ausgeschlossen
                            known_ids[eid] = "(gefiltert)"
                        else:
                            detail, new_page_id = result_tuple
                            item["_detail"] = detail
                            neue_eintraege.append(item)
                            known_ids[eid] = "(neu)"  # sofort als bekannt markieren
                            # ── Gutachten sofort anreichern ──────────────────
                            if new_page_id and item.get("link") and FITZ_AVAILABLE:
                                try:
                                    gutachten_enrich_notion_page(
                                        notion, new_page_id, item["link"]
                                    )
                                except Exception as ge:
                                    print(f"    [Gutachten] ⚠️  Anreicherung fehlgeschlagen: {ge}")
                    else:
                        print(f"  [Notion] ⏭  Bereits vorhanden: {eid}")

                elif item["type"] in ("Entfall des Termins", "Verschiebung"):
                    page_id = known_ids.get(eid)
                    if page_id and page_id != "(neu)":
                        notion_mark_entfall(notion, page_id, item)
                        entfall_updates.append(item)
                    else:
                        print(f"  [Notion] ℹ️  Entfall ohne DB-Eintrag: {eid}")

            except Exception as exc:
                msg = f"Notion-Fehler {item.get('edikt_id', '?')}: {exc}"
                print(f"  [ERROR] {msg}")
                fehler.append(msg)

    # ── 3. URL-Anreicherung für manuell angelegte Einträge ────────────────────
    try:
        enriched_count = notion_enrich_urls(notion, db_id)
    except Exception as exc:
        msg = f"URL-Anreicherung fehlgeschlagen: {exc}"
        print(f"  [ERROR] {msg}")
        fehler.append(msg)
        enriched_count = 0

    # ── 4. Gutachten-Anreicherung für manuell angelegte Einträge ─────────────
    # Betrifft: Einträge die bereits eine URL haben aber noch nicht analysiert wurden.
    # Das sind Immobilien die ihr selbst eingetragen habt (mit oder ohne Hash-ID).
    gutachten_enriched = 0
    if FITZ_AVAILABLE:
        try:
            gutachten_enriched = notion_enrich_gutachten(notion, db_id)
        except Exception as exc:
            msg = f"Gutachten-Anreicherung fehlgeschlagen: {exc}"
            print(f"  [ERROR] {msg}")
            fehler.append(msg)
    else:
        print("[Gutachten] ℹ️  PyMuPDF nicht verfügbar – überspringe Gutachten-Anreicherung")

    # ── 5. Zusammenfassung ────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"✅ Neue Einträge:       {len(neue_eintraege)}")
    print(f"🔴 Entfall-Updates:     {len(entfall_updates)}")
    print(f"🔗 URLs ergänzt:        {enriched_count}")
    print(f"📄 Gutachten analysiert:{gutachten_enriched}")
    print(f"⚠️  Fehler:              {len(fehler)}")
    print("=" * 60)

    if not neue_eintraege and not entfall_updates and not fehler:
        print("Keine neuen relevanten Änderungen – kein Telegram-Versand.")
        return

    # ── 4. Telegram ───────────────────────────────────────────────────────────
    lines = [
        "<b>🏛 Edikte-Monitor</b>",
        f"<i>{datetime.now().strftime('%d.%m.%Y %H:%M')}</i>",
        "",
    ]

    if neue_eintraege:
        lines.append(f"<b>🟢 Neue Versteigerungen: {len(neue_eintraege)}</b>")
        for item in neue_eintraege[:20]:
            detail    = item.get("_detail", {})
            adresse   = html_escape(detail.get("adresse_voll") or item["beschreibung"][:70])
            kategorie = html_escape(detail.get("kategorie", ""))
            vk        = detail.get("schaetzwert")
            vk_str    = f" | 💰 {vk:,.0f} €".replace(",", ".") if vk else ""
            kat_str   = f" [{kategorie}]" if kategorie else ""
            lines.append(f"• <b>{adresse}</b>{kat_str}{vk_str}")
            lines.append(f"  <a href=\"{item['link']}\">→ Edikt öffnen</a>")
        if len(neue_eintraege) > 20:
            lines.append(f"  ... und {len(neue_eintraege) - 20} weitere")
        lines.append("")

    if entfall_updates:
        lines.append(f"<b>🔴 Termin entfallen/verschoben: {len(entfall_updates)}</b>")
        for item in entfall_updates[:10]:
            lines.append(f"• {html_escape(item['bundesland'])} – {html_escape(item['beschreibung'][:60])}")
        lines.append("")

    if enriched_count:
        lines.append(f"<b>🔗 URLs nachgetragen: {enriched_count}</b>")
        lines.append("")

    if gutachten_enriched:
        lines.append(f"<b>📄 Gutachten analysiert: {gutachten_enriched}</b>")
        lines.append("")

    if fehler:
        lines.append(f"<b>⚠️ Fehler ({len(fehler)}):</b>")
        for f_msg in fehler[:5]:
            lines.append(f"• {f_msg[:100]}")

    try:
        await send_telegram("\n".join(lines))
    except Exception as exc:
        print(f"[ERROR] Telegram fehlgeschlagen: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
