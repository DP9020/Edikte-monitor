"""
Tiefe Duplikat-Bereinigung für Edikte-Monitor.

Im Gegensatz zu cleanup_neu_eingelangt.py (nur Bundesland+Adresse-Match)
und cleanup_duplikate.py (nur exakter Hash-Match) verwendet dieses Skript
drei Match-Strategien, kombiniert mit harten Safety-Checks:

  Strategie A: Hash-ID-Overlap
    Zwei Pages gehören zusammen wenn deren Hash-ID-Sets (cumulative Feld
    newline-separiert) mindestens eine ID gemeinsam haben.

  Strategie B: Stark normalisierte Adresse + PLZ
    Adresse wird aggressiv normalisiert (Abkürzungen expandiert, Interpunktion
    weg, ß→ss, Umlaute normalisiert). Match nur wenn PLZ identisch (aus
    'Liegenschafts PLZ' oder aus Titel extrahiert).

  Strategie C: Bundesland + stark normalisierte Adresse (Fallback)
    Wenn keine PLZ verfügbar.

Safety: Innerhalb einer Duplikat-Gruppe muss mindestens eine dieser Bedingungen
erfüllt sein bevor archiviert wird:
  - Alle Pages haben identische PLZ, ODER
  - Mindestens ein Hash-ID-Overlap, ODER
  - Identischer 'Gericht'-Wert

Ranking (höher = behalten):
  - Advanced Workflow-Phase (Brief versendet etc.)
  - 'Für uns relevant?' gesetzt
  - 'Brief erstellt am' gesetzt
  - Mehr Daten (Eigentümer, Zustelladresse, Verkehrswert)

Ausführen:
    python dedup_tief.py             # Dry-Run (default)
    python dedup_tief.py --apply     # tatsächlich archivieren
"""

import os
import re
import sys
import time

# .env einlesen (lokal)
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


# ── Phasen-Ranking (höher = wertvoller, behalten) ───────────────────────────
PHASE_RANG = {
    "🆕 Neu eingelangt": 0,
    "🗄 Archiviert": 1,
    "❌ Nicht relevant": 2,
    "🔎 In Prüfung": 3,
    "📊 Gutachten analysiert": 4,
    "✅ Relevant – Brief vorbereiten": 5,
    "📩 Brief versendet": 6,
    "✅ Gekauft": 8,
}


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
    """Aggressive Adress-Normalisierung für Matching.

    - Kleinbuchstaben
    - Umlaute (ß→ss, ä/ö/ü → a/o/u)
    - Interpunktion → Leerzeichen
    - Abkürzungen expandieren (str. → strasse, platz → platz, etc.)
    - Whitespace zusammenfassen
    """
    s = s.strip().lower()
    s = s.replace("ß", "ss").replace("ä", "a").replace("ö", "o").replace("ü", "u")
    # Interpunktion → Leerzeichen
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s)
    # Abkürzungen normalisieren (vor/nach Leerzeichen)
    s = re.sub(r"\bstr\b\.?", "strasse", s)
    s = re.sub(r"\bstraße\b", "strasse", s)  # sollte durch ß→ss abgefangen sein, aber safe
    s = re.sub(r"\bpl\b\.?", "platz", s)
    s = re.sub(r"\bg\b\.?", "gasse", s)
    s = re.sub(r"\bhnr\b\.?", "", s)
    s = re.sub(r"\btop\b\s*\d*", "", s)  # "Top 3" entfernen — unzuverlässig zwischen Pages
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_plz(title: str, plz_field: str) -> str:
    """Extrahiert eine verlässliche österreichische 4-stellige PLZ.

    Quellen in dieser Reihenfolge:
      1. Notion-Feld 'Liegenschafts PLZ' (vertrauenswürdig).
      2. Titel-Pattern '4-Ziffern gefolgt von Ort-Name' – NUR wenn die
         4-Ziffern-Zahl direkt vor einem Wort mit Großbuchstaben steht,
         z.B. '1120 Wien', '6273 Ried'. Das schließt Jahreszahlen aus
         Datumsangaben ('05.05.2026') sicher aus.
      3. Österreichische PLZ starten nicht mit 0 – Zahlen mit führender
         0 werden verworfen.
    """
    # Primärquelle: explizites PLZ-Feld
    if plz_field:
        m = re.search(r"\b([1-9]\d{3})\b", plz_field)
        if m:
            return m.group(1)
    # Sekundärquelle: Titel, aber nur PLZ + Ort-Pattern
    if title:
        # 4-stellig (erste Ziffer 1-9) gefolgt von Whitespace und Großbuchstaben
        m = re.search(r"\b([1-9]\d{3})\s+([A-ZÄÖÜ][\wäöüß.\-/]+)", title)
        if m:
            return m.group(1)
    return ""


_SYNTHETIC_TITLE_RE = re.compile(
    r"^(Wien|Niederösterreich|Oberösterreich|Burgenland|Steiermark|Kärnten|Salzburg|Tirol|Vorarlberg)\s*[–-]\s*\d{2}\.\d{2}\.\d{4}",
    re.IGNORECASE,
)


def ist_synthetischer_titel(title: str) -> bool:
    """True wenn der Titel ein Fallback aus 'Bundesland – DD.MM.YYYY' ist.

    Solche Titel enthalten keine echte Adresse — sie dürfen NIE als
    Dedup-Signal dienen, weil unterschiedliche Immobilien zufällig am
    selben Datum im selben Bundesland versteigert werden können.
    """
    return bool(_SYNTHETIC_TITLE_RE.match(title.strip()))


def hash_ids_of(page: dict) -> set[str]:
    """Gibt alle Hash-IDs einer Page als Set zurück (cumulatives Feld)."""
    hash_rt = page.get("properties", {}).get("Hash-ID / Vergleichs-ID", {}).get("rich_text", [])
    if not hash_rt:
        return set()
    full = hash_rt[0].get("plain_text", "").strip().lower()
    return {e.strip() for e in full.split("\n") if e.strip()}


def get_titel(page: dict) -> str:
    title_rt = page.get("properties", {}).get("Liegenschaftsadresse", {}).get("title", [])
    return title_rt[0].get("plain_text", "").strip() if title_rt else ""


def get_select(page: dict, field: str) -> str:
    return (page.get("properties", {}).get(field, {}).get("select") or {}).get("name", "")


def get_rt(page: dict, field: str) -> str:
    rt = page.get("properties", {}).get(field, {}).get("rich_text", [])
    return rt[0].get("plain_text", "").strip() if rt else ""


def page_rang(page: dict) -> int:
    """Ranking für 'behalten' — höher = wertvoller."""
    phase = get_select(page, "Workflow-Phase")
    rang = PHASE_RANG.get(phase, 0)

    if get_rt(page, "Verpflichtende Partei"):
        rang += 100
    if get_rt(page, "Zustell Adresse"):
        rang += 50
    if get_select(page, "Für uns relevant?") == "Ja":
        rang += 200
    brief_date = (page.get("properties", {}).get("Brief erstellt am", {}).get("date") or {}).get("start")
    if brief_date:
        rang += 300
    # Bonus für kumulative Hash-ID (ältere, reichhaltige Page)
    rang += len(hash_ids_of(page)) * 5
    return rang


def load_all_pages(notion: Client, db_id: str) -> list[dict]:
    """Alle Pages mit Retry-geschütztem Paginierungs-Helper."""
    return paginated_query(notion, db_id)


def build_groups(active_pages: list[dict]) -> tuple[list[list[dict]], dict]:
    """Union-Find-basiertes Gruppieren über 3 Strategien.

    Gibt (gruppen, stats) zurück. Nur Gruppen mit ≥2 Pages werden zurückgegeben.
    """
    # Page-Index anlegen
    n = len(active_pages)
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    # Strategie A: Hash-ID Overlap (Union-Find über alle Hash-IDs die eine Page hat)
    hash_to_idx: dict[str, int] = {}
    for i, p in enumerate(active_pages):
        for h in hash_ids_of(p):
            if h in hash_to_idx:
                union(i, hash_to_idx[h])
            else:
                hash_to_idx[h] = i

    # Strategie B: Normalisierte Adresse + PLZ
    addr_plz_to_idx: dict[tuple[str, str], int] = {}
    addr_bl_to_idx: dict[tuple[str, str], int] = {}
    for i, p in enumerate(active_pages):
        title = get_titel(p)
        if not title:
            continue
        # Synthetische Fallback-Titel nie als Dedup-Signal verwenden –
        # sie enthalten keine echte Adresse, Hash-Overlap bleibt erlaubt.
        if ist_synthetischer_titel(title):
            continue
        norm = normalize_address(title)
        if not norm:
            continue
        plz_field = get_rt(p, "Liegenschafts PLZ")
        plz = extract_plz(title, plz_field)
        bundesland = get_select(p, "Bundesland").strip().lower()

        if plz:
            key_b = (norm, plz)
            if key_b in addr_plz_to_idx:
                union(i, addr_plz_to_idx[key_b])
            else:
                addr_plz_to_idx[key_b] = i
        # Strategie C (Fallback, wenn keine PLZ): Bundesland + norm
        # Nur zulässig wenn norm nicht-trivial (min. 10 Zeichen, kein reiner Ortsname)
        if bundesland and len(norm) >= 10:
            key_c = (norm, bundesland)
            if key_c in addr_bl_to_idx:
                union(i, addr_bl_to_idx[key_c])
            else:
                addr_bl_to_idx[key_c] = i

    # Gruppen sammeln
    from collections import defaultdict
    clusters: dict[int, list[int]] = defaultdict(list)
    for i in range(n):
        clusters[find(i)].append(i)

    gruppen = [[active_pages[i] for i in idxs] for idxs in clusters.values() if len(idxs) >= 2]

    stats = {
        "num_hash_ids_registered": len(hash_to_idx),
        "num_addr_plz_groups": len(addr_plz_to_idx),
        "num_addr_bl_groups": len(addr_bl_to_idx),
        "num_duplicate_groups": len(gruppen),
    }
    return gruppen, stats


def gruppe_ist_sicher(gruppe: list[dict]) -> tuple[bool, str]:
    """Prüft ob eine Duplikat-Gruppe sicher archiviert werden kann.

    Sicher wenn mindestens EINES der Kriterien erfüllt ist:
      1. Alle Pages haben identische PLZ (falls vorhanden), ODER
      2. Mindestens ein Hash-ID-Overlap über alle Pages hinweg, ODER
      3. Identischer 'Gericht'-Wert über alle Pages, ODER
      4. Identisches 'Aktenzeichen' über alle Pages

    Gibt (safe, reason) zurück.
    """
    # Kriterium 1: PLZ identisch
    plzs = set()
    for p in gruppe:
        title = get_titel(p)
        plz_field = get_rt(p, "Liegenschafts PLZ")
        plz = extract_plz(title, plz_field)
        if plz:
            plzs.add(plz)
    plz_ok = len(plzs) == 1 and len(plzs) > 0

    # Kriterium 2: Hash-Overlap
    hash_sets = [hash_ids_of(p) for p in gruppe]
    any_overlap = False
    if all(h for h in hash_sets):
        # Jede Page hat Hashes, prüfen ob pro Paar Overlap existiert
        # (Union-Find hat die bereits gruppiert, das hier ist Safety-Doublecheck)
        union_all = set().union(*hash_sets)
        # Min. eine ID die in mehreren Pages vorkommt
        id_count: dict[str, int] = {}
        for hs in hash_sets:
            for h in hs:
                id_count[h] = id_count.get(h, 0) + 1
        any_overlap = any(c >= 2 for c in id_count.values())

    # Kriterium 3: Gericht identisch
    gerichte = {get_rt(p, "Gericht") for p in gruppe if get_rt(p, "Gericht")}
    gericht_ok = len(gerichte) == 1

    # Kriterium 4: Aktenzeichen identisch
    az = {get_rt(p, "Aktenzeichen") for p in gruppe if get_rt(p, "Aktenzeichen")}
    az_ok = len(az) == 1

    # Synthetische Fallback-Titel haben keinen echten Adressinhalt –
    # deshalb dürfen PLZ/Gericht/Aktenzeichen-Matches dort NICHT als
    # Safety dienen. Nur Hash-Overlap (dasselbe Edikt) ist dann sicher.
    alle_synthetisch = all(ist_synthetischer_titel(get_titel(p)) for p in gruppe)
    hat_synthetisch = any(ist_synthetischer_titel(get_titel(p)) for p in gruppe)

    # Priorität: Hash-Overlap zuerst (stärkste Evidenz dass es dasselbe
    # Edikt ist). Danach erst die schwächeren Kriterien.
    if any_overlap:
        return True, "Hash-ID-Overlap"
    if alle_synthetisch:
        return False, "alle Titel synthetisch & kein Hash-Overlap"
    if hat_synthetisch:
        return False, "mindestens 1 Titel synthetisch & kein Hash-Overlap"
    if plz_ok:
        return True, f"PLZ identisch ({list(plzs)[0]})"
    if gericht_ok:
        return True, f"Gericht identisch ({list(gerichte)[0][:30]})"
    if az_ok:
        return True, f"Aktenzeichen identisch ({list(az)[0][:30]})"
    return False, "kein Hash/PLZ/Gericht/AZ-Match"


def archive_page(notion: Client, page_id: str, primary_id: str, primary_phase: str) -> None:
    with_retry(
        notion.pages.update,
        page_id=page_id,
        properties={
            "Workflow-Phase": {"select": {"name": "🗄 Archiviert"}},
            "Archiviert": {"checkbox": True},
            "Neu eingelangt": {"checkbox": False},
            "Notizen": {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {
                            "content": (
                                f"[Auto-Dedup-Tief] Duplikat zu Notion-Page "
                                f"{primary_id[:8]}… – dort bereits bearbeitet "
                                f"(Phase: {primary_phase})."
                            )
                        },
                    }
                ]
            },
        },
    )


def main() -> None:
    dry_run = "--apply" not in sys.argv

    print("=" * 72)
    print(f"  Tiefe Duplikat-Bereinigung")
    print(f"  Modus: {'DRY-RUN (keine Änderungen)' if dry_run else 'APPLY (echte Änderungen!)'}")
    print("=" * 72)
    print()

    notion = Client(auth=env("NOTION_TOKEN"))
    db_id = clean_db_id(env("NOTION_DATABASE_ID"))

    print("[1/4] Lade alle Pages aus Notion …")
    all_pages = load_all_pages(notion, db_id)
    print(f"      {len(all_pages)} Pages insgesamt")

    # Archivierte raus
    active = [p for p in all_pages if not p.get("properties", {}).get("Archiviert", {}).get("checkbox", False)]
    print(f"      {len(active)} aktive Pages (nicht archiviert)\n")

    print("[2/4] Gruppiere via 3 Strategien (Hash-Overlap, Adresse+PLZ, Adresse+Bundesland) …")
    gruppen, stats = build_groups(active)
    print(f"      Hash-IDs registriert:          {stats['num_hash_ids_registered']}")
    print(f"      Adresse+PLZ-Gruppen:           {stats['num_addr_plz_groups']}")
    print(f"      Adresse+Bundesland-Gruppen:    {stats['num_addr_bl_groups']}")
    print(f"      Gefundene Duplikat-Gruppen:    {len(gruppen)}\n")

    if not gruppen:
        print("✅ Keine Duplikate gefunden. Alles sauber.")
        return

    print("[3/4] Safety-Check pro Gruppe + Ranking:")
    to_archive: list[tuple[dict, dict, str]] = []  # (duplicate, primary, reason)
    unsafe_count = 0
    for gruppe in gruppen:
        safe, reason = gruppe_ist_sicher(gruppe)
        gruppe_sorted = sorted(gruppe, key=page_rang, reverse=True)
        primary = gruppe_sorted[0]
        dups = gruppe_sorted[1:]
        if not safe:
            unsafe_count += 1
            print(f"  ⚠️  ÜBERSPRUNGEN (unsicher: {reason}): {get_titel(primary)[:60]}")
            for d in dups:
                print(f"        - Kandidat: {d['id'][:8]}… Phase='{get_select(d, 'Workflow-Phase')}'")
            continue
        for d in dups:
            to_archive.append((d, primary, reason))

    print(f"\n      → {len(to_archive)} Duplikate zum Archivieren freigegeben")
    print(f"      → {unsafe_count} Gruppen als unsicher übersprungen\n")

    if not to_archive:
        print("✅ Keine sicher archivierbaren Duplikate gefunden.")
        return

    print("[4/4] Detail-Liste der sicheren Duplikate:\n")
    for idx, (dup, primary, reason) in enumerate(to_archive, 1):
        dup_phase = get_select(dup, "Workflow-Phase")
        prim_phase = get_select(primary, "Workflow-Phase")
        prim_relevant = get_select(primary, "Für uns relevant?")
        prim_brief_obj = primary.get("properties", {}).get("Brief erstellt am", {}).get("date") or {}
        prim_brief = prim_brief_obj.get("start", "") if prim_brief_obj else ""
        print(f"  [{idx:3d}] {get_titel(dup)[:70]}")
        print(
            f"        🗑  Duplikat:  {dup['id'][:8]}…  Phase='{dup_phase}'  "
            f"Relevant='{get_select(dup, 'Für uns relevant?') or '–'}'"
        )
        print(
            f"        📌  Original:  {primary['id'][:8]}…  Phase='{prim_phase}'  "
            f"Relevant='{prim_relevant or '–'}'  Brief='{prim_brief[:10] if prim_brief else '–'}'"
        )
        print(f"        🔗  Match:     {reason}")

    if dry_run:
        print(f"\n⚠️  DRY-RUN — keine Änderungen vorgenommen.")
        print(f"    Mit --apply tatsächlich archivieren:")
        print(f"    python {os.path.basename(sys.argv[0])} --apply")
        return

    print(f"\n🛠  Archiviere {len(to_archive)} Duplikate …\n")
    ok = 0
    fehler = 0
    for dup, primary, reason in to_archive:
        try:
            archive_page(notion, dup["id"], primary["id"], get_select(primary, "Workflow-Phase"))
            ok += 1
            print(f"  ✓ {get_titel(dup)[:70]}")
            time.sleep(0.4)
        except Exception as exc:
            fehler += 1
            print(f"  ✗ {get_titel(dup)[:70]}  →  FEHLER: {exc}")

    print(f"\n{'=' * 72}")
    print(f"  Fertig. Archiviert: {ok}. Fehler: {fehler}. Unsicher übersprungen: {unsafe_count}.")
    print("=" * 72)


if __name__ == "__main__":
    main()
