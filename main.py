import os
import re
import asyncio
from datetime import datetime
from notion_client import Client
from playwright.async_api import async_playwright

EDIKTE_URL = "https://edikte.justiz.gv.at/edikte/ex/exedi3.nsf/suche!OpenForm&subf="

BUNDESLAENDER = [
    "Kärnten",
    "Salzburg",
    "Steiermark",
    "Oberösterreich",
    "Niederösterreich",
    "Wien",
    "Burgenland",
]

ID_RE = re.compile(r"/alldoc/([0-9a-f]+)!OpenDocument", re.IGNORECASE)


def env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


async def send_telegram(message: str) -> None:
    import urllib.parse
    import urllib.request

    token = env("TELEGRAM_BOT_TOKEN")
    chat_id = env("TELEGRAM_CHAT_ID")
    text = urllib.parse.quote(message)

    url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={text}"
    with urllib.request.urlopen(url, timeout=30) as r:
        r.read()


def notion_has_id(notion: Client, db_id: str, edikt_id: str) -> bool:
    resp = notion.databases.query(
        database_id=db_id,
        filter={
            "property": "Edikt-ID (Formel)",
            "formula": {"string": {"equals": edikt_id}},
        },
        page_size=1,
    )
    return len(resp.get("results", [])) > 0


async def scrape_links_for_state(page, bundesland: str) -> list[str]:
    await page.goto(EDIKTE_URL, wait_until="domcontentloaded")

    await page.wait_for_timeout(1500)

    anchors = await page.locator("a").all()
    links = set()

    for a in anchors:
        href = await a.get_attribute("href")
        if not href:
            continue
        if "/alldoc/" in href and "!OpenDocument" in href:
            if href.startswith("/"):
                href = "https://edikte.justiz.gv.at" + href
            links.add(href)

    return sorted(links)


async def main():
    notion = Client(auth=env("NOTION_TOKEN"))
    db_id = env("NOTION_DATABASE_ID")

    new_hits = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for bl in BUNDESLAENDER:
            links = await scrape_links_for_state(page, bl)
            for link in links:
                m = ID_RE.search(link)
                if not m:
                    continue
                edikt_id = m.group(1).lower()

                if notion_has_id(notion, db_id, edikt_id):
                    continue

                new_hits.append((bl, edikt_id, link))

        await browser.close()

    if not new_hits:
        print("No new items.")
        return

    lines = [f"Neue Edikte: {len(new_hits)} ({datetime.now().strftime('%Y-%m-%d %H:%M')})"]
    for bl, edikt_id, link in new_hits[:20]:
        lines.append(f"- {bl}: {edikt_id}\n  {link}")

    msg = "\n".join(lines)
    await send_telegram(msg)
    print(msg)


if __name__ == "__main__":
    asyncio.run(main())
