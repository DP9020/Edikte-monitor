"""
Duplikat-Bereinigung für Edikte-Monitor Notion-Datenbank.

Findet Einträge mit derselben Hash-ID (edikt_id) oder Adresse und behält
jeweils den "besseren" (höhere Phase / mehr Daten), der andere wird archiviert.

Ausführen:
    python cleanup_duplikate.py            # Dry-Run (zeigt nur an)
    python cleanup_duplikate.py --apply    # tatsächlich archivieren

Umgebungsvariablen nötig:
    NOTION_TOKEN
    NOTION_DATABASE_ID
"""

import os
import re
import sys
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

from _notion_helpers import paginated_query, with_retry

# ── Phasen-Priorität: höherer Index = wertvoller ────────────────────────────
# Hinweis: "🟡 Beobachten" wird vom Hauptscraper nicht gesetzt (siehe
# main.py:GESCHUETZT_PHASEN). Falls es manuell vergeben wird, wird der
# Eintrag wegen seiner hohen Priorität bevorzugt behalten – sicheres Default.
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


def env(key: str) -> str:
    val = os.environ.get(key, "")
    if not val:
        raise ValueError(f"Umgebungsvariable {key} fehlt")
    return val


def clean_db_id(raw: str) -> str:
    raw = raw.split("?")[0].strip()
    raw = raw.rstrip("/").split("/")[-1]
    clean = re.sub(r"[^0-9a-fA-F]", "", raw)
    if len(clean) == 32:
        return f"{clean[0:8]}-{clean[8:12]}-{clean[12:16]}-{clean[16:20]}-{clean[20:32]}"
    return raw


def _rt_text(rt: list | None) -> str:
    """Verkettet alle rich_text-Blöcke; schützt gegen None-Blocks und
    Multi-Block-Splits (Notion teilt rich_text bei >2000 Zeichen)."""
    if not rt:
        return ""
    parts: list[str] = []
    for block in rt:
        if not isinstance(block, dict):
            continue
        plain = block.get("plain_text")
        if isinstance(plain, str) and plain:
            parts.append(plain)
            continue
        text_obj = block.get("text") or {}
        content = text_obj.get("content") if isinstance(text_obj, dict) else None
        if isinstance(content, str):
            parts.append(content)
    return "".join(parts)


def load_all_pages(notion: Client, db_id: str) -> list[dict]:
    """Alle Pages mit Retry-geschütztem Paginierungs-Helper."""
    print("Lade alle Pages …")
    pages = paginated_query(notion, db_id)
    print(f"✅ {len(pages)} Pages geladen")
    return pages


def page_rang(page: dict) -> int:
    """Gibt den Rang einer Page zurück (höher = wertvoller, behalten)."""
    props = page.get("properties", {})
    phase = (props.get("Workflow-Phase", {}).get("select") or {}).get("name", "")
    rang  = PHASE_RANG.get(phase, 0)

    # Bonus-Punkte für vorhandene Daten
    if props.get("Verpflichtende Partei", {}).get("rich_text", []):
        rang += 100
    if props.get("Zustell Adresse", {}).get("rich_text", []):
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
    return _rt_text(titel_rt).strip()


def normalize_titel(titel: str) -> str:
    """Normalisiert Adressen für Vergleich: Kleinbuchstaben, Leerzeichen vereinheitlichen."""
    t = titel.lower().strip()
    t = re.sub(r"\s+", " ", t)
    return t


def get_gericht_az(page: dict) -> tuple[str, str]:
    """Gibt (Gericht, Aktenzeichen) einer Page zurück, jeweils leer falls nicht vorhanden."""
    props = page.get("properties", {})
    gericht = _rt_text(props.get("Gericht", {}).get("rich_text", [])).strip()
    aktenzeichen = _rt_text(props.get("Aktenzeichen", {}).get("rich_text", [])).strip()
    return gericht, aktenzeichen


def get_hash_ids(page: dict) -> list[str]:
    """Liest alle Hash-IDs aus dem newline-getrennten Feld als Liste."""
    props = page.get("properties", {})
    hash_rt = props.get("Hash-ID / Vergleichs-ID", {}).get("rich_text", [])
    full = _rt_text(hash_rt).strip().lower()
    if not full:
        return []
    return [eid for eid in (line.strip() for line in full.split("\n")) if eid]


def archiviere_duplikate(
    notion,
    duplikat_gruppen: dict,
    label: str,
    *,
    dry_run: bool,
    pass2: bool = False,
    bereits_archiviert: set | None = None,
) -> int:
    """
    Archiviert die niedrigsten Pages jeder Duplikat-Gruppe.
    `bereits_archiviert` (Set von page_ids) verhindert doppelte Archivierung
    wenn eine Page in mehreren Hash-Gruppen auftaucht.
    """
    if bereits_archiviert is None:
        bereits_archiviert = set()
    archiviert = 0
    for key, gruppe in duplikat_gruppen.items():
        # Pages die durch frühere Gruppen bereits markiert sind ausfiltern
        gruppe = [p for p in gruppe if p["id"] not in bereits_archiviert]
        if len(gruppe) < 2:
            continue

        # ── Pass-2-Schutz: nur archivieren wenn Gericht oder Aktenzeichen übereinstimmen ──
        if pass2:
            gerichte_count: dict[str, int] = {}
            az_count: dict[str, int] = {}
            for p in gruppe:
                g, az = get_gericht_az(p)
                if g:
                    gerichte_count[g] = gerichte_count.get(g, 0) + 1
                if az:
                    az_count[az] = az_count.get(az, 0) + 1

            if not (gerichte_count or az_count):
                print(f"   ⚠️  Pass 2 übersprungen (kein Gericht/AZ vorhanden): {str(key)[:80]}")
                continue
            if not (any(c > 1 for c in gerichte_count.values())
                    or any(c > 1 for c in az_count.values())):
                print(f"   ⚠️  Pass 2 übersprungen (kein gemeinsames Gericht/AZ): {str(key)[:80]}")
                continue

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

            if dry_run:
                bereits_archiviert.add(dup_id)
                archiviert += 1
                continue

            # Bestehende Notiz erhalten – manuelle Notizen am Duplikat sollen
            # nicht verloren gehen, falls der Eintrag später überprüft wird.
            alte_notiz = _rt_text(dup_props.get("Notizen", {}).get("rich_text", [])).strip()
            marker = f"[Automatisch archiviert – Duplikat von {behalten['id'][:8]}]"
            neue_notiz = f"{alte_notiz}\n{marker}".strip() if alte_notiz else marker
            neue_notiz = neue_notiz[:2000]

            try:
                with_retry(
                    notion.pages.update,
                    page_id=dup_id,
                    properties={
                        "Archiviert":     {"checkbox": True},
                        "Workflow-Phase": {"select": {"name": "🗄 Archiviert"}},
                        "Notizen": {"rich_text": [{"type": "text", "text": {
                            "content": neue_notiz
                        }}]},
                    }
                )
                bereits_archiviert.add(dup_id)
                archiviert += 1
                time.sleep(0.4)
            except Exception as exc:
                print(f"   ⚠️  Fehler: {exc}")
    return archiviert


def main():
    apply_changes = "--apply" in sys.argv
    dry_run = not apply_changes

    notion = Client(auth=env("NOTION_TOKEN"))
    db_id  = clean_db_id(env("NOTION_DATABASE_ID"))

    pages = load_all_pages(notion, db_id)

    if dry_run:
        print("\n⚠️  DRY RUN – es wird nichts geändert. --apply zum Ausführen.\n")

    # ── Pass 1: Gruppieren nach JEDER einzelnen Hash-ID ─────────────────────
    # Eine Page kann mehrere edikt_ids haben (newline-getrennt), weil
    # main.py neue IDs anhängt statt zu ersetzen. Set-basiertes Matching:
    # zwei Pages sind Duplikate wenn ihre Hash-Listen sich überschneiden.
    hash_gruppen: dict[str, list[dict]] = {}
    page_ids_with_hash: set[str] = set()
    ohne_hash = 0

    for page in pages:
        if page.get("properties", {}).get("Archiviert", {}).get("checkbox", False):
            continue
        eids = get_hash_ids(page)
        if not eids:
            ohne_hash += 1
            continue
        page_ids_with_hash.add(page["id"])
        for eid in eids:
            hash_gruppen.setdefault(eid, []).append(page)

    dup_hash = {k: v for k, v in hash_gruppen.items() if len(v) > 1}

    print(f"\n📊 Pass 1 – Duplikate nach Hash-ID (Set-Overlap):")
    print(f"   Aktive Pages mit Hash:   {len(page_ids_with_hash)}")
    print(f"   Pages ohne Hash:         {ohne_hash}")
    print(f"   Hash-Gruppen mit Dups:   {len(dup_hash)}")

    bereits_archiviert: set[str] = set()
    archiviert1 = archiviere_duplikate(
        notion, dup_hash, "Hash",
        dry_run=dry_run, bereits_archiviert=bereits_archiviert,
    )

    # ── Pass 2: Gruppieren nach normalisiertem Titel (gleiche Adresse) ───────
    # In-Memory-Filter: bereits archivierte Pages aus Pass 1 ausnehmen,
    # statt die gesamte DB ein zweites Mal aus Notion zu laden.
    titel_gruppen: dict[str, list[dict]] = {}
    for page in pages:
        if page["id"] in bereits_archiviert:
            continue
        if page.get("properties", {}).get("Archiviert", {}).get("checkbox", False):
            continue
        titel = normalize_titel(get_titel(page))
        if not titel:
            continue
        titel_gruppen.setdefault(titel, []).append(page)

    dup_titel = {k: v for k, v in titel_gruppen.items() if len(v) > 1}

    print(f"\n📊 Pass 2 – Duplikate nach Adresse/Titel:")
    print(f"   Aktive Pages:    {sum(len(v) for v in titel_gruppen.values())}")
    print(f"   Duplikat-Gruppen:{len(dup_titel)}")
    print(f"   Zu archivieren:  {sum(len(v)-1 for v in dup_titel.values())} (vor Gericht/AZ-Prüfung)")

    archiviert2 = archiviere_duplikate(
        notion, dup_titel, "Titel",
        dry_run=dry_run, pass2=True, bereits_archiviert=bereits_archiviert,
    )

    print(f"\n{'[DRY RUN] Würde' if dry_run else '✅'} {archiviert1 + archiviert2} Duplikat(e) archivier{'en' if dry_run else 't'}")
    print(f"   Pass 1 (Hash-ID): {archiviert1}")
    print(f"   Pass 2 (Adresse): {archiviert2}")


if __name__ == "__main__":
    main()
