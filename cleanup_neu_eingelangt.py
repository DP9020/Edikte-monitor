"""
Bereinigt Duplikate im '🆕 Neu eingelangt' der Notion-Datenbank.

Problem: Einträge wandern durch seltene Edge-Cases als Duplikat in
'🆕 Neu eingelangt', obwohl dieselbe Immobilie (gleiche Adresse + Bundesland)
bereits in einer anderen Phase bearbeitet wurde (Brief erstellt, als irrelevant
markiert, gekauft etc.).

Dieses Skript:
  1. Lädt alle Pages aus der Notion-DB
  2. Gruppiert nach (Bundesland + normalisierter Adresse)
  3. Findet Gruppen in denen einer in '🆕 Neu eingelangt' ist UND ein Zwilling
     bereits bearbeitet wurde
  4. Archiviert das Duplikat (Phase → 🗄 Archiviert, Checkbox Archiviert = True,
     Notiz mit Verweis auf Original)

Kriterien für 'bereits bearbeitet' (am Zwilling):
  - 'Für uns relevant?' ist gesetzt (Ja/Nein/Beobachten), ODER
  - 'Brief erstellt am' ist befüllt, ODER
  - 'Workflow-Phase' ist eine fortgeschrittene (Brief versendet, Gutachten
    analysiert, Gekauft, Archiviert, Nicht relevant, Relevant – Brief vorbereiten,
    In Prüfung), ODER
  - 'Status' ist 🔴/🟡/🟢

Ausführen (lokal):
    python cleanup_neu_eingelangt.py            # Dry-Run – zeigt nur was passieren würde
    python cleanup_neu_eingelangt.py --apply    # Tatsächlich archivieren

Benötigte Umgebungsvariablen (entweder in .env oder im Shell):
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

from notion_client import Client  # noqa: E402

from _notion_helpers import paginated_query, with_retry  # noqa: E402


NEU_PHASE = "🆕 Neu eingelangt"

BEARBEITET_PHASEN = {
    "🔎 In Prüfung",
    "✅ Relevant – Brief vorbereiten",
    "📩 Brief versendet",
    "📊 Gutachten analysiert",
    "✅ Gekauft",
    "🗄 Archiviert",
    "❌ Nicht relevant",
}

AKTIVER_STATUS = {"🔴 Rot", "🟢 Grün", "🟡 Gelb"}


def env(key: str) -> str:
    val = os.environ.get(key, "")
    if not val:
        raise ValueError(f"Umgebungsvariable {key} fehlt")
    return val


def clean_db_id(raw: str) -> str:
    raw = raw.split("?")[0].strip().rstrip("/").split("/")[-1]
    clean = re.sub(r"[^0-9a-fA-F]", "", raw)
    if len(clean) == 32:
        return f"{clean[0:8]}-{clean[8:12]}-{clean[12:16]}-{clean[16:20]}-{clean[20:32]}"
    return raw


def normalize_address(s: str) -> str:
    """Kleinbuchstaben, Leerzeichen zusammenfassen, trimmen."""
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def load_all_pages(notion: Client, db_id: str) -> list[dict]:
    """Alle Pages mit Retry-geschütztem Paginierungs-Helper."""
    return paginated_query(notion, db_id)


def _rt_text(rt: list | None) -> str:
    """Verkettet alle rich_text-Blöcke; schützt gegen None-Blocks und Multi-Block-Splits."""
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


def summarize(page: dict) -> dict:
    p = page["properties"]
    title_rt = p.get("Liegenschaftsadresse", {}).get("title", [])
    title = _rt_text(title_rt).strip()
    notiz_rt = p.get("Notizen", {}).get("rich_text", [])
    bundesland = (p.get("Bundesland", {}).get("select") or {}).get("name", "")
    phase = (p.get("Workflow-Phase", {}).get("select") or {}).get("name", "")
    relevant = (p.get("Für uns relevant?", {}).get("select") or {}).get("name", "")
    status = (p.get("Status", {}).get("select") or {}).get("name", "")
    brief_date_obj = p.get("Brief erstellt am", {}).get("date") or {}
    brief_date = brief_date_obj.get("start", "") if brief_date_obj else ""
    archiviert = p.get("Archiviert", {}).get("checkbox", False)
    return {
        "id": page["id"],
        "title": title,
        "bundesland": bundesland,
        "phase": phase,
        "relevant": relevant,
        "status": status,
        "brief_date": brief_date,
        "archiviert": archiviert,
        "created": page.get("created_time", ""),
        "last_edited": page.get("last_edited_time", ""),
        "notiz": _rt_text(notiz_rt).strip(),
    }


def ist_bearbeitet(s: dict) -> bool:
    return (
        s["relevant"] != ""
        or s["brief_date"] != ""
        or s["phase"] in BEARBEITET_PHASEN
        or s["status"] in AKTIVER_STATUS
    )


def main() -> None:
    dry_run = "--apply" not in sys.argv

    print(f"{'=' * 70}")
    print(f"  Duplikat-Bereinigung: '🆕 Neu eingelangt'")
    print(f"  Modus: {'DRY-RUN (keine Änderungen)' if dry_run else 'APPLY (echte Änderungen!)'}")
    print(f"{'=' * 70}\n")

    notion = Client(auth=env("NOTION_TOKEN"))
    db_id = clean_db_id(env("NOTION_DATABASE_ID"))

    print("[1/3] Lade alle Pages aus Notion …")
    pages = load_all_pages(notion, db_id)
    print(f"      {len(pages)} Pages insgesamt geladen\n")

    print("[2/3] Gruppiere nach Bundesland + normalisierter Adresse …")
    summaries = [summarize(p) for p in pages]
    aktiv = [s for s in summaries if not s["archiviert"]]

    groups: dict[str, list[dict]] = {}
    for s in aktiv:
        if not s["title"]:
            continue
        key = f"{s['bundesland'].lower()}|{normalize_address(s['title'])}"
        groups.setdefault(key, []).append(s)

    # Finde fehl-platzierte Duplikate
    misplaced: list[tuple[dict, dict]] = []  # (neu_duplikat, bearbeiteter_zwilling)
    for key, group in groups.items():
        if len(group) < 2:
            continue
        neu_entries = [s for s in group if s["phase"] == NEU_PHASE and not ist_bearbeitet(s)]
        bearb_entries = [s for s in group if ist_bearbeitet(s)]
        if not neu_entries or not bearb_entries:
            continue
        # Bester Zwilling: zuletzt bearbeitet (der mit meisten Daten).
        # Tiebreaker auf created, falls last_edited leer/identisch ist –
        # vermeidet unstabile Reihenfolge bei API-Lücken.
        primary = max(bearb_entries, key=lambda x: (x["last_edited"] or "", x["created"] or ""))
        for neu in neu_entries:
            misplaced.append((neu, primary))

    print(f"      Eindeutige Immobilien (Bundesland+Adresse): {len(groups)}")
    print(f"      Aktive Pages insgesamt:                     {len(aktiv)}")
    print(f"      🆕-Duplikate mit bereits bearbeitetem Zwilling: {len(misplaced)}\n")

    if not misplaced:
        print("✅ Keine Duplikate zum Bereinigen gefunden. Alles sauber.")
        return

    print(f"[3/3] Liste der zu archivierenden Duplikate:\n")
    for idx, (neu, primary) in enumerate(misplaced, 1):
        print(f"  [{idx:3d}] {neu['bundesland']:17s} | {neu['title'][:60]}")
        print(
            f"        🗑  Duplikat:  {neu['id'][:8]}…  "
            f"erstellt {neu['created'][:10]}  Phase='{neu['phase']}'"
        )
        print(
            f"        📌  Original:  {primary['id'][:8]}…  "
            f"Phase='{primary['phase']}'  "
            f"Relevant='{primary['relevant'] or '–'}'  "
            f"Brief='{primary['brief_date'][:10] if primary['brief_date'] else '–'}'"
        )

    if dry_run:
        print(f"\n⚠️  DRY-RUN — keine Änderungen vorgenommen.")
        print(f"    Zum tatsächlichen Archivieren:")
        print(f"    python {os.path.basename(sys.argv[0])} --apply")
        return

    print(f"\n🛠  Archiviere {len(misplaced)} Duplikate …\n")
    archiviert = 0
    fehler = 0
    for neu, primary in misplaced:
        try:
            marker = (
                f"[Auto-Dedup] Duplikat zu Notion-Page "
                f"{primary['id'][:8]}… – dort bereits "
                f"bearbeitet (Phase: {primary['phase']})."
            )
            alt_notiz = neu.get("notiz", "").strip()
            neue_notiz = f"{alt_notiz}\n{marker}".strip() if alt_notiz else marker
            neue_notiz = neue_notiz[:2000]
            with_retry(
                notion.pages.update,
                page_id=neu["id"],
                properties={
                    "Workflow-Phase": {"select": {"name": "🗄 Archiviert"}},
                    "Archiviert": {"checkbox": True},
                    "Neu eingelangt": {"checkbox": False},
                    "Notizen": {
                        "rich_text": [
                            {"type": "text", "text": {"content": neue_notiz}}
                        ]
                    },
                },
            )
            archiviert += 1
            print(f"  ✓ {neu['title'][:60]}")
            time.sleep(0.4)  # Notion Rate-Limit
        except Exception as exc:
            fehler += 1
            print(f"  ✗ {neu['title'][:60]}  →  FEHLER: {exc}")

    print(f"\n{'=' * 70}")
    print(f"  Fertig. Archiviert: {archiviert}. Fehler: {fehler}.")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
