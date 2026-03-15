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

# .env Datei einlesen falls vorhanden (lokale Ausführung ohne GitHub Actions)
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

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

DRY_RUN = False  # Auf False setzen um wirklich zu archivieren


def env(key: str) -> str:
    val = os.environ.get(key, "")
    if not val:
        raise ValueError(f"Umgebungsvariable {key} fehlt")
    return val


def clean_db_id(raw: str) -> str:
    import re
    raw = raw.split("?")[0].strip()
    raw = raw.rstrip("/").split("/")[-1]
    clean = re.sub(r"[^0-9a-fA-F]", "", raw)
    if len(clean) == 32:
        return f"{clean[0:8]}-{clean[8:12]}-{clean[12:16]}-{clean[16:20]}-{clean[20:32]}"
    return raw


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


def get_titel(page: dict) -> str:
    titel_rt = page.get("properties", {}).get("Liegenschaftsadresse", {}).get("title", [])
    return titel_rt[0].get("plain_text", "").strip() if titel_rt else ""


def normalize_titel(titel: str) -> str:
    """Normalisiert Adressen für Vergleich: Kleinbuchstaben, Leerzeichen vereinheitlichen."""
    import re
    t = titel.lower().strip()
    t = re.sub(r"\s+", " ", t)
    return t


def archiviere_duplikate(notion, duplikat_gruppen: dict, label: str) -> int:
    archiviert = 0
    for key, gruppe in duplikat_gruppen.items():
        gruppe_sortiert = sorted(gruppe, key=page_rang, reverse=True)
        behalten = gruppe_sortiert[0]
        loeschen = gruppe_sortiert[1:]

        behalten_titel = get_titel(behalten)
        behalten_phase = (behalten.get("properties", {}).get("Workflow-Phase", {}).get("select") or {}).get("name", "")

        print(f"\n🔑 {label}: {str(key)[:20]}… | Behalten: '{behalten_titel[:50]}' [{behalten_phase}]")

        for dup in loeschen:
            dup_props = dup.get("properties", {})
            dup_phase = (dup_props.get("Workflow-Phase", {}).get("select") or {}).get("name", "")
            dup_id    = dup["id"]

            print(f"   🗑  Archiviere [{dup_phase}] page_id={dup_id[:8]}…")

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
    return archiviert


def main():
    notion = Client(auth=env("NOTION_TOKEN"))
    db_id  = clean_db_id(env("NOTION_DATABASE_ID"))

    pages = load_all_pages(notion, db_id)

    if DRY_RUN:
        print("\n⚠️  DRY RUN – es wird nichts geändert. DRY_RUN = False setzen zum Ausführen.\n")

    # ── Pass 1: Gruppieren nach Hash-ID ──────────────────────────────────────
    hash_gruppen: dict[str, list[dict]] = {}
    ohne_hash = 0

    for page in pages:
        props   = page.get("properties", {})
        hash_rt = props.get("Hash-ID / Vergleichs-ID", {}).get("rich_text", [])
        hash_id = hash_rt[0].get("plain_text", "").strip().lower() if hash_rt else ""

        if not hash_id:
            ohne_hash += 1
            continue

        # Bereits archivierte überspringen
        if props.get("Archiviert", {}).get("checkbox", False):
            continue

        hash_gruppen.setdefault(hash_id, []).append(page)

    dup_hash = {k: v for k, v in hash_gruppen.items() if len(v) > 1}

    print(f"\n📊 Pass 1 – Duplikate nach Hash-ID:")
    print(f"   Aktive Pages:    {sum(len(v) for v in hash_gruppen.values())}")
    print(f"   Duplikat-Gruppen:{len(dup_hash)}")
    print(f"   Zu archivieren:  {sum(len(v)-1 for v in dup_hash.values())}")

    archiviert1 = archiviere_duplikate(notion, dup_hash, "Hash")

    # ── Pass 2: Gruppieren nach normalisiertem Titel (gleiche Adresse) ───────
    # Pages neu laden damit archivierte nicht mehr auftauchen
    pages2 = load_all_pages(notion, db_id)
    titel_gruppen: dict[str, list[dict]] = {}

    for page in pages2:
        props = page.get("properties", {})
        if props.get("Archiviert", {}).get("checkbox", False):
            continue

        titel = normalize_titel(get_titel(page))
        if not titel:
            continue

        titel_gruppen.setdefault(titel, []).append(page)

    dup_titel = {k: v for k, v in titel_gruppen.items() if len(v) > 1}

    print(f"\n📊 Pass 2 – Duplikate nach Adresse/Titel:")
    print(f"   Aktive Pages:    {sum(len(v) for v in titel_gruppen.values())}")
    print(f"   Duplikat-Gruppen:{len(dup_titel)}")
    print(f"   Zu archivieren:  {sum(len(v)-1 for v in dup_titel.values())}")

    archiviert2 = archiviere_duplikate(notion, dup_titel, "Titel")

    print(f"\n{'[DRY RUN] Würde' if DRY_RUN else '✅'} {archiviert1 + archiviert2} Duplikat(e) archiviert")
    print(f"   Pass 1 (Hash-ID): {archiviert1}")
    print(f"   Pass 2 (Adresse): {archiviert2}")


if __name__ == "__main__":
    main()
