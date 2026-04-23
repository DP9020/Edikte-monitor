"""
Gemeinsame Notion-Helper für die Dedup-/Cleanup-Skripte.

Wird von cleanup_duplikate.py, cleanup_neu_eingelangt.py und
dedup_tief.py importiert. Zuvor hatten diese Skripte gar keinen Retry
und sind bei einzelnen 429/5xx-Errors abgebrochen – bei großen Läufen
(100+ Archivierungen) statistisch ~20% Ausfallrate.
"""
from __future__ import annotations

import time


def is_transient_error(exc: Exception) -> tuple[bool, str]:
    """True wenn der Fehler retry-würdig ist (Rate-Limit, 5xx, Timeout)."""
    err = str(exc).lower()
    if "429" in err or "rate_limited" in err or "rate limit" in err:
        return True, "429 Rate-Limit"
    for code in ("500", "502", "503", "504"):
        if code in err:
            return True, f"{code} Server-Fehler"
    if "timeout" in err or "timed out" in err:
        return True, "Timeout"
    if "connection reset" in err or "connection aborted" in err:
        return True, "Connection reset"
    return False, ""


def with_retry(fn, *args, max_retries: int = 3, label: str = "Notion", **kwargs):
    """Führt einen Notion-API-Call mit Retry bei transienten Fehlern aus.

    Backoff: 5s, 15s, 30s. 4xx-Fehler (außer 429) werden sofort propagiert.
    """
    delays = [5, 15, 30]
    for attempt in range(max_retries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            transient, reason = is_transient_error(exc)
            if transient and attempt < max_retries - 1:
                wait = delays[attempt]
                print(f"  [{label}] ⏳ {reason} – warte {wait}s (Versuch {attempt+1}/{max_retries})")
                time.sleep(wait)
            else:
                raise


def query_with_retry(notion, db_id: str, **kwargs) -> dict:
    """databases.query() mit Retry."""
    return with_retry(notion.databases.query, database_id=db_id, label="Notion-Query", **kwargs)


def paginated_query(notion, db_id: str, page_size: int = 100) -> list[dict]:
    """Paginiert durch die gesamte DB und gibt alle Results zurück.

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
