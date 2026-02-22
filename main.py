"""
Edikte-Monitor – Österreich
============================
Scraper für https://edikte.justiz.gv.at (Gerichtliche Versteigerungen)
Alle Bundesländer | HTTP-Request (kein Browser nötig) | Notion | Telegram
"""

import os
import re
import time
import asyncio
import urllib.request
import urllib.parse
from datetime import datetime
from notion_client import Client

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

# Schlüsselwörter → Objekt wird NICHT importiert
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
    """Prüft ob ein Objekt durch EXCLUDE_KEYWORDS ausgeschlossen werden soll."""
    return any(kw in text.lower() for kw in EXCLUDE_KEYWORDS)


def parse_schaetzwert(raw: str) -> float | None:
    """
    Wandelt einen Schätzwert-String (z.B. '1.130.698,59') in einen float um.
    Gibt None zurück wenn das Parsen scheitert.
    """
    try:
        # Tausendertrennzeichen (.) entfernen, Komma durch Punkt ersetzen
        cleaned = raw.strip()
        cleaned = re.sub(r"[€EUReur\s]", "", cleaned)
        cleaned = cleaned.replace(".", "").replace(",", ".")
        return float(cleaned)
    except Exception:
        return None


def fetch_detail(link: str) -> dict:
    """
    Lädt die Edikt-Detailseite und extrahiert:
    - Schätzwert / Verkehrswert (als Zahl)
    - Aktenzeichen
    - Versteigerungstermin (Datum + Uhrzeit)
    - Adresse (Liegenschaft / Versteigerungsort)
    - Gericht / Dienststelle
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

    body = re.sub(r"<[^>]+>", " ", html)
    body = " ".join(body.split())

    result = {}

    # --- Schätzwert / Verkehrswert ---
    m = SCHAETZWERT_RE.search(body)
    if m:
        raw_val = m.group(1).strip()
        result["schaetzwert_str"] = raw_val
        parsed = parse_schaetzwert(raw_val)
        if parsed is not None:
            result["schaetzwert"] = parsed

    # --- Aktenzeichen ---
    m = re.search(r"Aktenzeichen:\s*([\w\s\/\.]+?)\s+wegen", body, re.IGNORECASE)
    if m:
        result["aktenzeichen"] = m.group(1).strip()

    # --- Versteigerungstermin ---
    m = re.search(
        r"Versteigerungstermin:\s*am\s+([\d\.]+)\s+um\s+([\d:]+\s+Uhr)",
        body, re.IGNORECASE
    )
    if m:
        result["termin"] = f"{m.group(1)} {m.group(2)}"
        try:
            dt = datetime.strptime(m.group(1), "%d.%m.%Y")
            result["termin_iso"] = dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    # --- Versteigerungsort ---
    m = re.search(
        r"Versteigerungsort:\s*([^;\n]+?)(?:;|Telefonkontakt|$)",
        body, re.IGNORECASE
    )
    if m:
        result["adresse"] = m.group(1).strip()

    # --- Dienststelle / Gericht ---
    m = re.search(r"Dienststelle:\s*([^(]+)", body, re.IGNORECASE)
    if m:
        result["gericht"] = m.group(1).strip()

    return result


# =============================================================================
# TELEGRAM
# =============================================================================

async def send_telegram(message: str) -> None:
    """Sendet eine Nachricht via Telegram Bot."""
    token   = env("TELEGRAM_BOT_TOKEN")
    chat_id = env("TELEGRAM_CHAT_ID")

    if len(message) > 4096:
        message = message[:4090] + "\n[...]"

    text = urllib.parse.quote(message)
    url  = (
        f"https://api.telegram.org/bot{token}/sendMessage"
        f"?chat_id={chat_id}&text={text}&parse_mode=HTML"
    )
    with urllib.request.urlopen(url, timeout=15) as r:
        r.read()
    print(f"[Telegram] ✅ Nachricht gesendet ({len(message)} Zeichen)")


# =============================================================================
# NOTION
# =============================================================================

def notion_find_page(notion: Client, db_id: str, edikt_id: str):
    """Sucht ein bestehendes Notion-Page anhand der Hash-ID."""
    response = notion.search(
        query=edikt_id,
        filter={"value": "page", "property": "object"},
    )
    for page in response.get("results", []):
        parent = page.get("parent", {})
        if parent.get("database_id", "").replace("-", "") != db_id.replace("-", ""):
            continue
        props     = page.get("properties", {})
        hash_prop = props.get("Hash-ID / Vergleichs-ID", {})
        rich_text = hash_prop.get("rich_text", [])
        if rich_text and rich_text[0].get("plain_text", "") == edikt_id:
            return page
    return None


def notion_create_eintrag(notion: Client, db_id: str, data: dict) -> None:
    """
    Legt einen neuen Eintrag in Notion an.
    Ruft vorher die Detailseite ab, um Verkehrswert, Termin etc. zu befüllen.
    """
    bundesland   = data.get("bundesland", "Unbekannt")
    link         = data.get("link", "")
    edikt_id     = data.get("edikt_id", "")
    beschreibung = data.get("beschreibung", "")
    typ          = data.get("type", "Versteigerung")

    # ── Detail abrufen (Schätzwert, Termin, Gericht …) ──────────────────────
    detail = {}
    if link:
        detail = fetch_detail(link)

    # Datum aus Beschreibung oder Detail
    datum_str = re.search(r"\((\d{2}\.\d{2}\.\d{4})\)", beschreibung)
    datum_fmt = datum_str.group(1) if datum_str else detail.get("termin", "")

    # Titel
    titel = f"{bundesland} – {typ}"
    if datum_fmt:
        termin_kurz = datum_fmt[:10]
        titel += f" – {termin_kurz}"
    if beschreibung:
        titel += f" | {beschreibung[:50]}"

    # Adresse aus Detail überschreiben wenn sinnvoll
    adresse = detail.get("adresse", "") or beschreibung[:100]

    properties: dict = {
        "Liegenschaftsadresse": {
            "title": [{"text": {"content": adresse[:200] or titel[:200]}}]
        },
        "Hash-ID / Vergleichs-ID": {
            "rich_text": [{"text": {"content": edikt_id}}]
        },
        "Link": {
            "url": link
        },
        "Art des Edikts": {
            "select": {
                "name": typ if typ in ("Versteigerung", "Entfall des Termins") else "Versteigerung"
            }
        },
        "Bundesland": {
            "select": {"name": bundesland}
        },
        "Neu eingelangt": {
            "checkbox": True
        },
        "Automatisch importiert?": {
            "checkbox": True
        },
        "Workflow-Phase": {
            "select": {"name": "🆕 Neu eingelangt"}
        },
        "Objektart": {
            "rich_text": [{"text": {"content": beschreibung[:200]}}]
        },
    }

    # ── Verkehrswert ─────────────────────────────────────────────────────────
    verkehrswert = detail.get("schaetzwert")
    if verkehrswert is not None:
        properties["Verkehrswert"] = {"number": verkehrswert}
        print(f"    [Detail] 💰 Verkehrswert: {verkehrswert:,.2f} €")

    # ── Versteigerungstermin ─────────────────────────────────────────────────
    termin_iso = detail.get("termin_iso")
    if termin_iso:
        properties["Versteigerungstermin"] = {"date": {"start": termin_iso}}

    # ── Gericht ──────────────────────────────────────────────────────────────
    gericht = detail.get("gericht", "")
    if gericht:
        properties["Verpflichtende Partei"] = {
            "rich_text": [{"text": {"content": gericht[:200]}}]
        }

    notion.pages.create(
        parent={"database_id": db_id},
        properties=properties,
    )
    print(f"  [Notion] ✅ Erstellt: {titel[:80]}")


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

    # ── 1. Edikte scrapen + in Notion eintragen ───────────────────────────────
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
                existing = notion_find_page(notion, db_id, item["edikt_id"])

                if item["type"] == "Versteigerung":
                    if not existing:
                        notion_create_eintrag(notion, db_id, item)
                        neue_eintraege.append(item)
                    else:
                        print(f"  [Notion] ⏭  Bereits vorhanden: {item['edikt_id']}")

                elif item["type"] in ("Entfall des Termins", "Verschiebung"):
                    if existing:
                        notion_mark_entfall(notion, existing["id"], item)
                        entfall_updates.append(item)
                    else:
                        print(f"  [Notion] ℹ️  Entfall ohne DB-Eintrag: {item['edikt_id']}")

            except Exception as exc:
                msg = f"Notion-Fehler {item.get('edikt_id', '?')}: {exc}"
                print(f"  [ERROR] {msg}")
                fehler.append(msg)

    # ── 2. URL-Anreicherung für manuell angelegte Einträge ────────────────────
    try:
        enriched_count = notion_enrich_urls(notion, db_id)
    except Exception as exc:
        msg = f"URL-Anreicherung fehlgeschlagen: {exc}"
        print(f"  [ERROR] {msg}")
        fehler.append(msg)
        enriched_count = 0

    # ── 3. Zusammenfassung ────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"✅ Neue Einträge:       {len(neue_eintraege)}")
    print(f"🔴 Entfall-Updates:     {len(entfall_updates)}")
    print(f"🔗 URLs ergänzt:        {enriched_count}")
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
            detail = item.get("_detail", {})
            vk = detail.get("schaetzwert")
            vk_str = f" | 💰 {vk:,.0f} €" if vk else ""
            lines.append(
                f"• <b>{item['bundesland']}</b> – {item['beschreibung'][:70]}{vk_str}"
            )
            lines.append(f"  <a href=\"{item['link']}\">→ Edikt öffnen</a>")
        if len(neue_eintraege) > 20:
            lines.append(f"  ... und {len(neue_eintraege) - 20} weitere")
        lines.append("")

    if entfall_updates:
        lines.append(f"<b>🔴 Termin entfallen/verschoben: {len(entfall_updates)}</b>")
        for item in entfall_updates[:10]:
            lines.append(f"• {item['bundesland']} – {item['beschreibung'][:60]}")
        lines.append("")

    if enriched_count:
        lines.append(f"<b>🔗 URLs nachgetragen: {enriched_count}</b>")
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
