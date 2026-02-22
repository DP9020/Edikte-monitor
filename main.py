import os
import re
import asyncio
from datetime import datetime
from notion_client import Client
from playwright.async_api import async_playwright

# =========================
# KONFIGURATION
# =========================

EDIKTE_FORM_URL = "https://edikte.justiz.gv.at/edikte/ex/exedi3.nsf/suche!OpenForm&subf="

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

RELEVANT_TYPES = ("Versteigerung", "Entfall des Termins")

EXCLUDE_KEYWORDS = [
    "landwirtschaft",
    "land- und forst",
    "gewerb",
    "betriebsobjekt",
    "industrie",
    "lager",
    "büro",
    "hotel",
    "pension",
]

ID_RE = re.compile(r"/alldoc/([0-9a-f]+)!OpenDocument", re.IGNORECASE)


# =========================
# HILFSFUNKTIONEN
# =========================

def env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


async def send_telegram(message: str):
    import urllib.parse
    import urllib.request

    token = env("TELEGRAM_BOT_TOKEN")
    chat_id = env("TELEGRAM_CHAT_ID")

    text = urllib.parse.quote(message)
    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={text}"

    with urllib.request.urlopen(url) as response:
        response.read()


def notion_find_page(notion: Client, db_id: str, edikt_id: str):
    response = notion.databases.query(
        database_id=db_id,
        filter={
            "property": "Edikt-ID (Formel)",
            "formula": {"string": {"equals": edikt_id}},
        },
    )
    results = response.get("results", [])
    return results[0] if results else None


def notion_create_versteigerung(notion: Client, db_id: str, data: dict):
    notion.pages.create(
        parent={"database_id": db_id},
        properties={
            "Name": {
                "title": [
                    {
                        "text": {
                            "content": f"{data['bundesland']} – Versteigerung"
                        }
                    }
                ]
            },
            "Edikt-Link": {"url": data["link"]},
            "Art des Edikts": {"select": {"name": "Versteigerung"}},
            "Bundesland": {"select": {"name": data["bundesland"]}},
            "Neu eingelangt": {"checkbox": True},
            "Automatisch importiert?": {"checkbox": True},
        },
    )


def notion_mark_entfall(notion: Client, page_id: str):
    notion.pages.update(
        page_id=page_id,
        properties={
            "Art des Edikts": {"select": {"name": "Entfall des Termins"}},
            "Archiviert": {"checkbox": True},
            "Workflow-Phase": {"select": {"name": "🗄 Archiviert"}},
            "Neu eingelangt": {"checkbox": False},
        },
    )


# =========================
# SCRAPING
# =========================

async def scrape_for_state(page, bundesland):
    await page.goto(EDIKTE_FORM_URL)
    await page.wait_for_timeout(1000)

    # Direkt das richtige Bundesland-Dropdown ansprechen
    bundesland_select = page.locator('select[name="VBl"]')
    await bundesland_select.select_option(label=bundesland)

    # Suchen klicken
    # Formular direkt per JS absenden (robuster als Klick)
    await page.evaluate("""
    const form = document.querySelector("form");
    if (form) { form.submit(); }
    """)
    await page.wait_for_load_state("networkidle")

    # warten bis Ergebnis-Links da sind (oder leer bleibt)
    await page.wait_for_timeout(1000)

    anchors = await page.locator("a[href*='/alldoc/']").all()
    results = []

    for anchor in anchors:
        href = await anchor.get_attribute("href")
        text = (await anchor.inner_text()).strip()

        if not href or not text:
            continue

        if not any(text.startswith(t) for t in RELEVANT_TYPES):
            continue

        if any(keyword in text.lower() for keyword in EXCLUDE_KEYWORDS):
            continue

        match = ID_RE.search(href)
        if not match:
            continue

        if href.startswith("/"):
            href = "https://edikte.justiz.gv.at" + href

        results.append({
            "bundesland": bundesland,
            "type": text,
            "link": href,
            "edikt_id": match.group(1).lower(),
        })

    return results
async def scrape_for_state(page, bundesland):
    await page.goto(EDIKTE_FORM_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(1000)

    # --- Bundesland-Dropdown automatisch finden ---
    target_states = {s.strip().lower() for s in BUNDESLAENDER}

    selects = page.locator("select")
    best_select = None
    best_score = 0

    for i in range(await selects.count()):
        s = selects.nth(i)

        # Kategorien-Dropdown ist oft "multiple" -> das ignorieren wir
        multiple = await s.get_attribute("multiple")
        if multiple is not None:
            continue

        # Options-Texte auslesen
        option_texts = await s.evaluate(
            """(el) => Array.from(el.options).map(o => (o.textContent || '').trim().toLowerCase())"""
        )

        score = len(target_states.intersection(set(option_texts)))

        if score > best_score:
            best_score = score
            best_select = s

    if not best_select or best_score < 5:
        raise RuntimeError(
            f"Konnte das Bundesland-Dropdown nicht sicher finden (best_score={best_score})."
        )

    await best_select.select_option(label=bundesland)

    # --- Suche auslösen ---
    await page.locator('input[type="submit"], button[type="submit"]').first.click()
    await page.wait_for_timeout(2000)

    anchors = await page.locator("a[href*='/alldoc/']").all()
    results = []

    for anchor in anchors:
        href = await anchor.get_attribute("href")
        text = (await anchor.inner_text()).strip()

        if not href or not text:
            continue

        if not any(text.startswith(t) for t in RELEVANT_TYPES):
            continue

        if any(keyword in text.lower() for keyword in EXCLUDE_KEYWORDS):
            continue

        match = ID_RE.search(href)
        if not match:
            continue

        if href.startswith("/"):
            href = "https://edikte.justiz.gv.at" + href

        results.append({
            "bundesland": bundesland,
            "type": text,
            "link": href,
            "edikt_id": match.group(1).lower(),
        })

    return results
# =========================
# MAIN LOGIK
# =========================

async def main():
    notion = Client(auth=env("NOTION_TOKEN"))
    db_id = env("NOTION_DATABASE_ID")

    neue_versteigerungen = []
    entfall_updates = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for bundesland in BUNDESLAENDER:
            results = await scrape_for_state(page, bundesland)

            for item in results:
                existing_page = notion_find_page(notion, db_id, item["edikt_id"])

                if item["type"].startswith("Versteigerung"):
                    if not existing_page:
                        notion_create_versteigerung(notion, db_id, item)
                        neue_versteigerungen.append(item)

                elif item["type"].startswith("Entfall des Termins"):
                    if existing_page:
                        notion_mark_entfall(notion, existing_page["id"])
                        entfall_updates.append(item)

        await browser.close()

    if not neue_versteigerungen and not entfall_updates:
        print("Keine neuen relevanten Änderungen.")
        return

    lines = [f"Edikte Update ({datetime.now().strftime('%Y-%m-%d %H:%M')})"]

    if neue_versteigerungen:
        lines.append(f"\n🟢 Neue Versteigerungen: {len(neue_versteigerungen)}")
        for item in neue_versteigerungen[:20]:
            lines.append(f"- {item['bundesland']}\n  {item['link']}")

    if entfall_updates:
        lines.append(f"\n🔴 Termin entfallen bei bestehenden Objekten: {len(entfall_updates)}")
        for item in entfall_updates[:20]:
            lines.append(f"- {item['bundesland']}\n  {item['link']}")

    await send_telegram("\n".join(lines))


if __name__ == "__main__":
    asyncio.run(main())
