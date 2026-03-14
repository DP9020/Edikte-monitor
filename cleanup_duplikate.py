"""
Duplikat-Bereinigung für Edikte-Monitor Notion-Datenbank.

Findet Einträge mit derselben Hash-ID (edikt_id) und behält jeweils
den "besseren" (höhere Phase / mehr Daten), der andere wird archiviert.

Ausführen:
    python cleanup_duplikate.py

Umgebungsvariablen nötig:
    NOTION_TOKEN
    NOTION_DATABASE_ID
"""

import os
import time
from notion_client import Client

# ── Phasen-Priorität: höherer Index = wertvoller ────────────────────────────
PHASE_RANG = {
    "🆕 Neu eingelangt":               0,
    "❌ Nicht relevant":                1,
    "🗄 Archiviert":                    2,
    "🔎 In Prüfung":                    3,
    "📊 Gutachten analysiert":          4,
    "✅ Relevant – Brief vorbereiten":  5,
    "📩 Brief versendet":               6,
    "🟡 Beobachten":                    7,
    "✅ Gekauft":                       8,
}

DRY_RUN = True  # Auf False setzen um wirklich zu archivieren


def env(key: str) -> str:
    val = os.environ.get(key, "")
    if not val:
        raise ValueError(f"Umgebungsvariable {key} fehlt")
    return val


def clean_db_id(db_id: str) -> str:
    db_id = db_id.strip()
    if "notion.so" in db_id:
        db_id = db_id.split("/")[-1].split("?")[0]
    db_id = db_id.replace("-", "")
    if len(db_id) == 32:
        db_id = f"{db_id[:8]}-{db_id[8:12]}-{db_id[12:16]}-{db_id[16:20]}-{db_id[20:]}"
    return db_id


def load_all_pages(notion: Client, db_id: str) -> list[dict]:
    print("Lade alle Pages …")
    pages = []
    cursor = None
    while True:
        kwargs = {"page_size": 100}
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = notion.databases.query(database_id=db_id, **kwargs)
        pages.extend(resp.get("results", []))
        print(f"  {len(pages)} Pages geladen …", end="\r")
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    print(f"\n✅ {len(pages)} Pages geladen")
    return pages


def page_rang(page: dict) -> int:
    """Gibt den Rang einer Page zurück (höher = wertvoller, behalten)."""
    props = page.get("properties", {})
    phase = (props.get("Workflow-Phase", {}).get("select") or {}).get("name", "")
    rang  = PHASE_RANG.get(phase, 0)

    # Bonus-Punkte für vorhandene Daten
    eigentuemer = props.get("Verpflichtende Partei", {}).get("rich_text", [])
    if eigentuemer:
        rang += 100

    adresse = props.get("Zustell Adresse", {}).get("rich_text", [])
    if adresse:
        rang += 50

    relevant = (props.get("Für uns relevant?", {}).get("select") or {}).get("name", "")
    if relevant == "Ja":
        rang += 200

    brief_datum = props.get("Brief erstellt am", {}).get("date")
    if brief_datum and brief_datum.get("start"):
        rang += 300

    return rang


def main():
    notion = Client(auth=env("NOTION_TOKEN"))
    db_id  = clean_db_id(env("NOTION_DATABASE_ID"))

    pages = load_all_pages(notion, db_id)

    # ── Gruppieren nach Hash-ID ───────────────────────────────────────────────
    gruppen: dict[str, list[dict]] = {}
    ohne_hash = 0

    for page in pages:
        props    = page.get("properties", {})
        hash_rt  = props.get("Hash-ID / Vergleichs-ID", {}).get("rich_text", [])
        hash_id  = hash_rt[0].get("plain_text", "").strip().lower() if hash_rt else ""

        if not hash_id:
            ohne_hash += 1
            continue

        gruppen.setdefault(hash_id, []).append(page)

    duplikat_gruppen = {k: v for k, v in gruppen.items() if len(v) > 1}

    print(f"\n📊 Statistik:")
    print(f"   Gesamt:          {len(pages)}")
    print(f"   Ohne Hash-ID:    {ohne_hash}")
    print(f"   Duplikat-Gruppen:{len(duplikat_gruppen)}")
    duplikat_count = sum(len(v) - 1 for v in duplikat_gruppen.values())
    print(f"   Zu archivierende:{duplikat_count}")

    if not duplikat_gruppen:
        print("\n✅ Keine Duplikate gefunden!")
        return

    if DRY_RUN:
        print("\n⚠️  DRY RUN – es wird nichts geändert. DRY_RUN = False setzen zum Ausführen.\n")

    archiviert = 0
    for hash_id, gruppe in duplikat_gruppen.items():
        # Besten Eintrag bestimmen
        gruppe_sortiert = sorted(gruppe, key=page_rang, reverse=True)
        behalten  = gruppe_sortiert[0]
        loeschen  = gruppe_sortiert[1:]

        behalten_props = behalten.get("properties", {})
        behalten_titel = ""
        titel_rt = behalten_props.get("Liegenschaftsadresse", {}).get("title", [])
        if titel_rt:
            behalten_titel = titel_rt[0].get("plain_text", "")
        behalten_phase = (behalten_props.get("Workflow-Phase", {}).get("select") or {}).get("name", "")

        print(f"\n🔑 Hash: {hash_id[:16]}… | Behalten: '{behalten_titel[:50]}' [{behalten_phase}]")

        for dup in loeschen:
            dup_props = dup.get("properties", {})
            dup_phase = (dup_props.get("Workflow-Phase", {}).get("select") or {}).get("name", "")
            dup_id    = dup["id"]

            print(f"   🗑  Archiviere Duplikat [{dup_phase}] page_id={dup_id[:8]}…")

            if not DRY_RUN:
                try:
                    notion.pages.update(
                        page_id=dup_id,
                        properties={
                            "Archiviert":     {"checkbox": True},
                            "Workflow-Phase": {"select": {"name": "🗄 Archiviert"}},
                            "Notizen": {"rich_text": [{"type": "text", "text": {
                                "content": f"[Automatisch archiviert – Duplikat von {behalten['id'][:8]}]"
                            }}]},
                        }
                    )
                    archiviert += 1
                    time.sleep(0.4)
                except Exception as exc:
                    print(f"   ⚠️  Fehler: {exc}")
            else:
                archiviert += 1

    print(f"\n{'[DRY RUN] Würde' if DRY_RUN else '✅'} {archiviert} Duplikat(e) archiviert")


if __name__ == "__main__":
    main()
