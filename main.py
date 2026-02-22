"""
Edikte-Monitor – Österreich
============================
Scraper für https://edikte.justiz.gv.at (Gerichtliche Versteigerungen)
Alle Bundesländer | HTTP-Request (kein Browser nötig) | Notion | Telegram
"""

import os
import re
import asyncio
import urllib.request
import urllib.parse
import http.cookiejar
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
    """Legt einen neuen Eintrag in Notion an."""
    bundesland   = data.get("bundesland", "Unbekannt")
    link         = data.get("link", "")
    edikt_id     = data.get("edikt_id", "")
    beschreibung = data.get("beschreibung", "")
    typ          = data.get("type", "Versteigerung")

    datum_str = re.search(r"\((\d{2}\.\d{2}\.\d{4})\)", beschreibung)
    datum_fmt = datum_str.group(1) if datum_str else ""

    titel = f"{bundesland} – {typ}"
    if datum_fmt:
        titel += f" – {datum_fmt}"
    if beschreibung:
        titel += f" | {beschreibung[:50]}"

    properties = {
        "Liegenschaftsadresse": {
            "title": [{"text": {"content": titel[:200]}}]
        },
        "Hash-ID / Vergleichs-ID": {
            "rich_text": [{"text": {"content": edikt_id}}]
        },
        "Link": {
            "url": link
        },
        "Art des Edikts": {
            "select": {"name": typ if typ in ("Versteigerung", "Entfall des Termins") else "Versteigerung"}
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
    link_pattern = re.compile(
        r'href="((?:https?://[^"]*)?(?:/edikte/[^"]*)?alldoc/([0-9a-f]+)!OpenDocument)"',
        re.IGNORECASE
    )
    # Auch relative Links ohne Host
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

    # -------------------------------------------------------------------------
    # Zusammenfassung
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print(f"✅ Neue Einträge:    {len(neue_eintraege)}")
    print(f"🔴 Entfall-Updates:  {len(entfall_updates)}")
    print(f"⚠️  Fehler:           {len(fehler)}")
    print("=" * 60)

    if not neue_eintraege and not entfall_updates and not fehler:
        print("Keine neuen relevanten Änderungen – kein Telegram-Versand.")
        return

    # -------------------------------------------------------------------------
    # Telegram-Nachricht
    # -------------------------------------------------------------------------
    lines = [
        "<b>🏛 Edikte-Monitor</b>",
        f"<i>{datetime.now().strftime('%d.%m.%Y %H:%M')}</i>",
        "",
    ]

    if neue_eintraege:
        lines.append(f"<b>🟢 Neue Versteigerungen: {len(neue_eintraege)}</b>")
        for item in neue_eintraege[:20]:
            lines.append(f"• <b>{item['bundesland']}</b> – {item['beschreibung'][:80]}")
            lines.append(f"  <a href=\"{item['link']}\">→ Edikt öffnen</a>")
        if len(neue_eintraege) > 20:
            lines.append(f"  ... und {len(neue_eintraege) - 20} weitere")
        lines.append("")

    if entfall_updates:
        lines.append(f"<b>🔴 Termin entfallen/verschoben: {len(entfall_updates)}</b>")
        for item in entfall_updates[:10]:
            lines.append(f"• {item['bundesland']} – {item['beschreibung'][:60]}")
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
