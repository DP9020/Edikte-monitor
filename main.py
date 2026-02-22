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
    """Sucht ein bestehendes Notion-Page anhand der Edikt-ID."""
    response = notion.databases.query(
        database_id=db_id,
        filter={
            "property": "Edikt-ID",
            "rich_text": {"equals": edikt_id},
        },
    )
    results = response.get("results", [])
    return results[0] if results else None


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
        "Name": {
            "title": [{"text": {"content": titel}}]
        },
        "Edikt-ID": {
            "rich_text": [{"text": {"content": edikt_id}}]
        },
        "Edikt-Link": {
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
        "Import-Datum": {
            "date": {"start": datetime.now().strftime("%Y-%m-%d")}
        },
    }

    # Optionales Gerichts-Feld
    if gericht:
        properties["Gericht"] = {
            "rich_text": [{"text": {"content": gericht}}]
        }

    # Optionales Beschreibungs-Feld
    if beschreibung:
        properties["Beschreibung"] = {
            "rich_text": [{"text": {"content": beschreibung[:2000]}}]
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
    Öffnet das Suchformular auf edikte.justiz.gv.at,
    wählt das Bundesland aus und gibt alle relevanten Treffer zurück.

    Strategie:
    - Alle <select>-Felder durchgehen
    - Das Dropdown identifizieren das die meisten Bundesländer-Optionen enthält
    - Kein hart codierter Feldname (robust gegen HTML-Änderungen)
    - Formular via JavaScript submitten (kein physischer Klick nötig)
    """
    print(f"\n[Scraper] 🔍 Suche für: {bundesland}")

    await page.goto(EDIKTE_FORM_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(1500)

    # -------------------------------------------------------------------------
    # Bundesland-Dropdown automatisch identifizieren
    # -------------------------------------------------------------------------
    target_states = {s.strip().lower() for s in BUNDESLAENDER}

    selects   = page.locator("select")
    count     = await selects.count()
    best_select = None
    best_score  = 0

    for i in range(count):
        s = selects.nth(i)

        # "multiple"-Dropdowns (z.B. Objektkategorie) überspringen
        multiple = await s.get_attribute("multiple")
        if multiple is not None:
            continue

        option_texts = await s.evaluate(
            "(el) => Array.from(el.options).map(o => (o.textContent || '').trim().toLowerCase())"
        )
        score = len(target_states.intersection(set(option_texts)))

        if score > best_score:
            best_score  = score
            best_select = s

    if best_select is None or best_score < 5:
        print(
            f"  [Scraper] ⚠️  Bundesland-Dropdown nicht gefunden "
            f"(best_score={best_score}). Bundesland übersprungen: {bundesland}"
        )
        return []

    # Bundesland auswählen
    await best_select.select_option(label=bundesland)
    print(f"  [Scraper] ✔️  Dropdown gefunden (Score {best_score}), ausgewählt: {bundesland}")

    # -------------------------------------------------------------------------
    # Formular per JavaScript abschicken (robuster als Klick)
    # -------------------------------------------------------------------------
    await page.evaluate("""
        const form = document.querySelector("form");
        if (form) { form.submit(); }
    """)
    await page.wait_for_load_state("networkidle")
    await page.wait_for_timeout(1500)

    # -------------------------------------------------------------------------
    # Ergebnisse auslesen
    # -------------------------------------------------------------------------
    anchors = await page.locator("a[href*='/alldoc/']").all()
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
    db_id  = env("NOTION_DATABASE_ID")

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
