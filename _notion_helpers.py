"""
Gemeinsame Notion-Helper für die Dedup-/Cleanup-Skripte.

Wird von cleanup_duplikate.py, cleanup_neu_eingelangt.py und
dedup_tief.py importiert. Zuvor hatten diese Skripte gar keinen Retry
und sind bei einzelnen 429/5xx-Errors abgebrochen – bei großen Läufen
(100+ Archivierungen) statistisch ~20% Ausfallrate.
"""
from __future__ import annotations

import time


import re

# HTTP-Statuscode 5xx als isoliertes Token statt Substring – sonst matcht "500"
# auch in Body-Texten wie "limit 5000".
_TRANSIENT_5XX_RE = re.compile(r'\b5(0[0-9]|1[0-9])\b')


def is_transient_error(exc: Exception) -> tuple[bool, str]:
    """True wenn der Fehler retry-würdig ist (Rate-Limit, 5xx, Timeout)."""
    err = str(exc).lower()
    if "429" in err or "rate_limited" in err or "rate limit" in err:
        return True, "429 Rate-Limit"
    m = _TRANSIENT_5XX_RE.search(err)
    if m:
        return True, f"{m.group(0)} Server-Fehler"
    if "timeout" in err or "timed out" in err:
        return True, "Timeout"
    if "connection reset" in err or "connection aborted" in err:
        return True, "Connection reset"
    return False, ""


def with_retry(fn, *args, max_retries: int = 3, label: str = "Notion", **kwargs):
    """Führt einen Notion-API-Call mit Retry bei transienten Fehlern aus.

    Backoff: 5s, 15s, 30s, dann 30s-Plateau. 4xx-Fehler (außer 429) werden
    sofort propagiert.
    """
    delays = [5, 15, 30]
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            transient, reason = is_transient_error(exc)
            if transient and attempt < max_retries - 1:
                wait = delays[min(attempt, len(delays) - 1)]
                print(f"  [{label}] ⏳ {reason} – warte {wait}s (Versuch {attempt+1}/{max_retries})")
                time.sleep(wait)
            else:
                raise


# Notion API 2025-09-03: data_source_id ersetzt database_id für Queries.
# Wir cachen die Resolution pro db_id, damit wir databases.retrieve() nicht
# bei jedem Query-Aufruf wiederholen müssen.
_data_source_id_cache: dict[str, str] = {}


def resolve_data_source_id(notion, db_id: str) -> str:
    """Resolved data_source_id für eine Notion-Datenbank (cached, mit Retry)."""
    cached = _data_source_id_cache.get(db_id)
    if cached:
        return cached

    db = with_retry(notion.databases.retrieve, database_id=db_id, label="Notion-DB-Retrieve")
    sources = db.get("data_sources") or []
    if not sources:
        raise RuntimeError(
            f"Notion-Datenbank {db_id[:8]}… liefert keine data_sources – "
            "ist die Integration auf API-Version 2025-09-03+ konfiguriert?"
        )
    ds_id = sources[0].get("id")
    if not ds_id:
        raise RuntimeError(f"data_sources[0].id fehlt für DB {db_id[:8]}…")

    _data_source_id_cache[db_id] = ds_id
    return ds_id


def query_with_retry(notion, db_id: str, **kwargs) -> dict:
    """data_sources.query() mit Retry. Nimmt weiterhin die database_id und resolved intern."""
    ds_id = resolve_data_source_id(notion, db_id)
    return with_retry(notion.data_sources.query, data_source_id=ds_id, label="Notion-Query", **kwargs)


def paginated_query(notion, db_id: str, page_size: int = 100) -> list[dict]:
    """Paginiert durch die gesamte Data Source und gibt alle Results zurück.

    Verwendet query_with_retry für jeden Einzelaufruf.
    """
    pages: list[dict] = []
    cursor = None
    while True:
        kwargs: dict = {"page_size": page_size}
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = query_with_retry(notion, db_id, **kwargs)
        pages.extend(resp.get("results", []))
        print(f"  {len(pages)} Pages geladen …", end="\r")
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
    print()
    return pages
