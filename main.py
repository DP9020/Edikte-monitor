"""
Edikte-Monitor – Österreich
============================
Scraper für https://edikte.justiz.gv.at (Gerichtliche Versteigerungen)
Alle Bundesländer | Playwright | Notion | Telegram
"""

import os
import re
import asyncio
from datetime import datetime
from notion_client import Client
from playwright.async_api import async_playwright

# =============================================================================
# KONFIGURATION
# =============================================================================

EDIKTE_FORM_URL = (
    "https://edikte.justiz.gv.at/edikte/ex/exedi3.nsf/suche!OpenForm&subf="
)

BUNDESLAENDER = [
    "Kärnten",
    "Salzburg",
    "Steiermark",
    "Oberösterreich",
    "Niederösterreich",
    "Wien",
    "Burgenland",
    "Tirol",
    "Vorarlberg",
]

# Nur diese Typen werden verarbeitet
RELEVANT_TYPES = ("Versteigerung", "Entfall des Termins")

# Schlüsselwörter → Objekt wird NICHT importiert (bei Versteigerungen)
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

# Edikt-ID aus dem Link extrahieren (z.B. /alldoc/abc123ef!OpenDocument)
ID_RE = re.compile(r"/alldoc/([0-9a-f]+)!OpenDocument", re.IGNORECASE)


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
    """
    Bereinigt die Notion Datenbank-ID.
    Entfernt URL-Parameter (?v=...&pvs=...) und gibt nur die reine ID zurück.
    Funktioniert auch wenn der User versehentlich die View-URL kopiert hat.
    """
    # Query-Parameter entfernen (z.B. ?v=abc&pvs=13)
    raw = raw.split("?")[0].strip()
    # Letzten Pfadteil nehmen (falls vollständige URL eingefügt)
    raw = raw.rstrip("/").split("/")[-1]
    # Nur Hex-Zeichen behalten
    clean = re.sub(r"[^0-9a-fA-F]", "", raw)
    if len(clean) == 32:
        return f"{clean[0:8]}-{clean[8:12]}-{clean[12:16]}-{clean[16:20]}-{clean[20:32]}"
    return raw


def is_excluded(text: str) -> bool:
    """Prüft ob ein Objekt durch EXCLUDE_KEYWORDS ausgeschlossen werden soll."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in EXCLUDE_KEYWORDS)


# =============================================================================
# TELEGRAM
# =============================================================================

async def send_telegram(message: str) -> None:
    """Sendet eine Nachricht via Telegram Bot."""
    import urllib.parse
    import urllib.request

    token = env("TELEGRAM_BOT_TOKEN")
    chat_id = env("TELEGRAM_CHAT_ID")

    # Nachricht auf max. 4096 Zeichen kürzen (Telegram-Limit)
    if len(message) > 4096:
        message = message[:4090] + "\n[...]"

    text = urllib.parse.quote(message)
    url = (
        f"https://api.telegram.org/bot{token}/sendMessage"
        f"?chat_id={chat_id}&text={text}&parse_mode=HTML"
    )

    with urllib.request.urlopen(url, timeout=15) as response:
        response.read()

    print(f"[Telegram] Nachricht gesendet ({len(message)} Zeichen)")


# =============================================================================
# NOTION
# =============================================================================

def notion_find_page(notion: Client, db_id: str, edikt_id: str):
    """
    Sucht ein bestehendes Notion-Page anhand der Hash-ID / Vergleichs-ID (via search).
    Nutzt das Feld 'Hash-ID / Vergleichs-ID' (rich_text) in der Datenbank.
    """
    response = notion.search(
        query=edikt_id,
        filter={"value": "page", "property": "object"},
    )
    for page in response.get("results", []):
        # Nur Seiten aus unserer Datenbank
        parent = page.get("parent", {})
        if parent.get("database_id", "").replace("-", "") != db_id.replace("-", ""):
            continue
        # Hash-ID / Vergleichs-ID prüfen
        props = page.get("properties", {})
        hash_prop = props.get("Hash-ID / Vergleichs-ID", {})
        rich_text = hash_prop.get("rich_text", [])
        if rich_text and rich_text[0].get("plain_text", "") == edikt_id:
            return page
    return None


def notion_create_versteigerung(notion: Client, db_id: str, data: dict) -> None:
    """
    Legt einen neuen Eintrag für eine Versteigerung in Notion an.

    data-Keys:
        bundesland  – z.B. "Wien"
        link        – vollständige URL zum Edikt
        edikt_id    – eindeutige Hex-ID
        beschreibung – Linktext / Objektbeschreibung (optional)
        gericht     – extrahiertes Gericht (optional)
    """
    bundesland  = data.get("bundesland", "Unbekannt")
    link        = data.get("link", "")
    edikt_id    = data.get("edikt_id", "")
    beschreibung = data.get("beschreibung", "")
    gericht     = data.get("gericht", "")

    # Seitentitel: Bundesland + Datum
    titel = f"{bundesland} – Versteigerung – {datetime.now().strftime('%d.%m.%Y')}"
    if beschreibung:
        # ersten 60 Zeichen der Beschreibung anhängen
        titel += f" | {beschreibung[:60]}"

    properties: dict = {
        # Titel → echtes Property heißt "Liegenschaftsadresse" (title)
        "Liegenschaftsadresse": {
            "title": [{"text": {"content": titel}}]
        },
        # Eindeutige ID zur Deduplizierung
        "Hash-ID / Vergleichs-ID": {
            "rich_text": [{"text": {"content": edikt_id}}]
        },
        # Link zum Edikt → echtes Property heißt "Link" (url)
        "Link": {
            "url": link
        },
        "Art des Edikts": {
            "select": {"name": "Versteigerung"}
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
        # Import-Datum → echtes Property ist "created_time" (automatisch)
        # Objektart aus Beschreibung
        "Objektart": {
            "rich_text": [{"text": {"content": beschreibung[:200] if beschreibung else ""}}]
        },
    }

    # Optionales Verpflichtende Partei / Gericht
    if gericht:
        properties["Verpflichtende Partei"] = {
            "rich_text": [{"text": {"content": gericht}}]
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
            "Art des Edikts": {
                "select": {"name": "Entfall des Termins"}
            },
            "Archiviert": {
                "checkbox": True
            },
            "Workflow-Phase": {
                "select": {"name": "🗄 Archiviert"}
            },
            "Neu eingelangt": {
                "checkbox": False
            },
        },
    )
    print(f"  [Notion] 🔴 Entfall markiert für ID: {item.get('edikt_id', '?')}")


# =============================================================================
# SCRAPING (Playwright)
# =============================================================================

async def scrape_for_state(page, bundesland: str) -> list[dict]:
    """
    Scraper für edikte.justiz.gv.at – Gerichtliche Versteigerungen.

    Strategie:
    - Direkt das VEX-Formular (Versteigerungs-Suche) aufrufen (subf=vex)
    - Bundesland per select[name='BL'] setzen
    - Submit-Button per Name klicken
    - IBM Domino macht JS-Redirect auf Ergebnisseite → auf URL mit 'alldoc' warten
    """
    print(f"\n[Scraper] 🔍 Suche für: {bundesland}")

    await page.goto(EDIKTE_FORM_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(1500)

    # -------------------------------------------------------------------------
    # Bundesland per name='BL' setzen (bekanntes Feld aus Form-Analyse)
    # -------------------------------------------------------------------------
    bl_select = page.locator('select[name="BL"]')
    if await bl_select.count() == 0:
        print(f"  [Scraper] ⚠️  BL-Dropdown nicht gefunden. Überspringe: {bundesland}")
        return []

    await bl_select.select_option(label=bundesland)
    print(f"  [Scraper] ✔️  Bundesland gesetzt: {bundesland}")

    # -------------------------------------------------------------------------
    # Submit-Button klicken (input[name='sebut'])
    # IBM Domino verarbeitet das Formular server-seitig und macht dann
    # einen JavaScript-Redirect auf die Ergebnisseite
    # -------------------------------------------------------------------------
    submit_btn = page.locator('input[name="sebut"]')
    if await submit_btn.count() > 0:
        await submit_btn.evaluate("el => el.click()")
        print(f"  [Scraper] 🖱️  Submit geklickt")
    else:
        await page.evaluate("document.querySelector('form').submit()")
        print(f"  [Scraper] 📤 Formular per JS abgeschickt")

    # Warten auf Navigation – IBM Domino: CreateDocument → JS-Redirect → Ergebnisseite
    try:
        await page.wait_for_url("**/alldoc/**", timeout=10000)
        print(f"  [Scraper] ✅ Ergebnisseite geladen: {page.url}")
    except Exception:
        # Fallback: einfach warten
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(3000)
        print(f"  [Scraper] 📍 URL nach Warten: {page.url}")

    # Seiteninhalt für Debugging
    page_text = await page.inner_text('body')
    print(f"  [Scraper] 📄 Inhalt (erste 200): {page_text[:200]}")

    # -------------------------------------------------------------------------
    # Ergebnisse auslesen
    # -------------------------------------------------------------------------
    anchors = await page.locator("a[href*='/alldoc/']").all()
    print(f"  [Scraper] 🔗 alldoc-Links: {len(anchors)}")
    results = []

    for anchor in anchors:
        href = await anchor.get_attribute("href")
        text = (await anchor.inner_text()).strip()

        if not href or not text:
            continue

        # Nur relevante Typen (Versteigerung / Entfall des Termins)
        if not any(text.startswith(t) for t in RELEVANT_TYPES):
            continue

        # Ausschlussliste (nur bei Versteigerungen relevant)
        if text.startswith("Versteigerung") and is_excluded(text):
            print(f"  [Filter] ⛔ Ausgeschlossen: {text[:80]}")
            continue

        # Edikt-ID aus URL extrahieren
        match = ID_RE.search(href)
        if not match:
            continue

        edikt_id = match.group(1).lower()

        # Relative URLs ergänzen
        if href.startswith("/"):
            href = "https://edikte.justiz.gv.at" + href

        # Gericht aus Linktext extrahieren (optional, z.B. "BG Wien-Innere Stadt")
        gericht = _extract_gericht(text)

        results.append({
            "bundesland":   bundesland,
            "type":         text.split(" – ")[0].strip() if " – " in text else text[:50],
            "beschreibung": text,
            "link":         href,
            "edikt_id":     edikt_id,
            "gericht":      gericht,
        })

    print(f"  [Scraper] 📋 {len(results)} relevante Treffer für {bundesland}")
    return results


def _extract_gericht(text: str) -> str:
    """
    Versucht ein Gericht aus dem Linktext zu extrahieren.
    Beispiel: "Versteigerung – BG Wien-Innere Stadt – Wohnung ..."
              → "BG Wien-Innere Stadt"
    """
    parts = [p.strip() for p in text.split(" – ")]
    if len(parts) >= 2:
        gericht_kandidat = parts[1]
        # Plausibilitätsprüfung: enthält "BG", "LG", "HG" oder "Gericht"
        if any(kw in gericht_kandidat for kw in ("BG", "LG", "HG", "Gericht")):
            return gericht_kandidat
    return ""


# =============================================================================
# MAIN
# =============================================================================

async def main() -> None:
    print("=" * 60)
    print(f"Edikte-Monitor gestartet: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    notion = Client(auth=env("NOTION_TOKEN"))
    db_id  = clean_notion_db_id(env("NOTION_DATABASE_ID"))

    neue_versteigerungen: list[dict] = []
    entfall_updates:      list[dict] = []
    fehler:               list[str]  = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        for bundesland in BUNDESLAENDER:
            try:
                results = await scrape_for_state(page, bundesland)
            except Exception as exc:
                msg = f"Fehler bei {bundesland}: {exc}"
                print(f"  [ERROR] {msg}")
                fehler.append(msg)
                continue

            for item in results:
                try:
                    existing = notion_find_page(notion, db_id, item["edikt_id"])

                    if item["type"].startswith("Versteigerung"):
                        if not existing:
                            notion_create_versteigerung(notion, db_id, item)
                            neue_versteigerungen.append(item)
                        else:
                            print(f"  [Notion] ⏭  Bereits vorhanden: {item['edikt_id']}")

                    elif item["type"].startswith("Entfall des Termins"):
                        if existing:
                            notion_mark_entfall(notion, existing["id"], item)
                            entfall_updates.append(item)
                        else:
                            print(
                                f"  [Notion] ℹ️  Entfall-Edikt ohne Treffer in DB "
                                f"(noch nicht importiert): {item['edikt_id']}"
                            )

                except Exception as exc:
                    msg = f"Notion-Fehler für {item.get('edikt_id', '?')}: {exc}"
                    print(f"  [ERROR] {msg}")
                    fehler.append(msg)

        await browser.close()

    # -------------------------------------------------------------------------
    # Zusammenfassung
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print(f"✅ Neue Versteigerungen:       {len(neue_versteigerungen)}")
    print(f"🔴 Entfall-Updates:            {len(entfall_updates)}")
    print(f"⚠️  Fehler:                     {len(fehler)}")
    print("=" * 60)

    if not neue_versteigerungen and not entfall_updates and not fehler:
        print("Keine neuen relevanten Änderungen – kein Telegram-Versand.")
        return

    # -------------------------------------------------------------------------
    # Telegram-Nachricht aufbauen
    # -------------------------------------------------------------------------
    lines = [
        f"<b>🏛 Edikte-Monitor</b>",
        f"<i>{datetime.now().strftime('%d.%m.%Y %H:%M')}</i>",
        "",
    ]

    if neue_versteigerungen:
        lines.append(f"<b>🟢 Neue Versteigerungen: {len(neue_versteigerungen)}</b>")
        for item in neue_versteigerungen[:20]:
            kurz = item["beschreibung"][:80] if item.get("beschreibung") else ""
            lines.append(f"• <b>{item['bundesland']}</b> – {kurz}")
            lines.append(f"  <a href=\"{item['link']}\">→ Edikt öffnen</a>")
        if len(neue_versteigerungen) > 20:
            lines.append(f"  ... und {len(neue_versteigerungen) - 20} weitere")
        lines.append("")

    if entfall_updates:
        lines.append(f"<b>🔴 Termin entfallen: {len(entfall_updates)}</b>")
        for item in entfall_updates[:10]:
            lines.append(f"• {item['bundesland']} – ID: {item['edikt_id']}")
        if len(entfall_updates) > 10:
            lines.append(f"  ... und {len(entfall_updates) - 10} weitere")
        lines.append("")

    if fehler:
        lines.append(f"<b>⚠️ Fehler ({len(fehler)}):</b>")
        for f_msg in fehler[:5]:
            lines.append(f"• {f_msg[:100]}")

    message = "\n".join(lines)

    try:
        await send_telegram(message)
    except Exception as exc:
        print(f"[ERROR] Telegram-Versand fehlgeschlagen: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
