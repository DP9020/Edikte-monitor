"""
diagnose_notion.py — Schnell-Diagnose: warum so wenige Eval-Kandidaten?
Zeigt Pipeline-Stats für Notion-DB.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent

# .env laden
for line in (REPO_ROOT / ".env").read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

from notion_client import Client

notion = Client(auth=os.environ["NOTION_TOKEN"])
db_id  = os.environ["NOTION_DATABASE_ID"]


def _resolve_data_source_id(notion, db_id: str) -> str:
    """Notion API 2025-09-03: queries laufen über data_source_id."""
    db = notion.databases.retrieve(database_id=db_id)
    sources = db.get("data_sources") or []
    if not sources:
        raise RuntimeError(f"Notion-DB {db_id[:8]}… liefert keine data_sources")
    return sources[0]["id"]


ds_id = _resolve_data_source_id(notion, db_id)


def _rt(rt):
    parts = []
    for t in rt or []:
        plain = t.get("plain_text") if isinstance(t, dict) else None
        if isinstance(plain, str) and plain:
            parts.append(plain); continue
        text_obj = (t.get("text") if isinstance(t, dict) else None) or {}
        c = text_obj.get("content") if isinstance(text_obj, dict) else None
        if isinstance(c, str):
            parts.append(c)
    return "".join(parts).strip()


total = 0
buckets = {
    "alle": 0,
    "analysiert_true": 0,
    "analysiert_true_mit_drive": 0,
    "analysiert_true_mit_gueltigem_drive": 0,
    "analysiert_true_ohne_drive": 0,
    "analysiert_true_drive_nicht_verfuegbar": 0,
    "vision": 0,
    "edge_multi_owner": 0,
    "edge_foreign_addr": 0,
    "status_count": {},
    "workflow_count": {},
}

cursor = None
while True:
    kwargs = {"data_source_id": ds_id, "page_size": 100}
    if cursor:
        kwargs["start_cursor"] = cursor
    resp = notion.data_sources.query(**kwargs)
    for p in resp.get("results", []):
        buckets["alle"] += 1
        props = p.get("properties", {})

        analysiert = props.get("Gutachten analysiert?", {}).get("checkbox") or False
        drive_link = props.get("Google Drive Link", {}).get("url") or ""
        notizen    = _rt(props.get("Notizen", {}).get("rich_text", []))
        eig_name   = _rt(props.get("Verpflichtende Partei", {}).get("rich_text", []))
        eig_plz    = _rt(props.get("Zustell PLZ/Ort", {}).get("rich_text", []))
        status_raw = (props.get("Status", {}).get("select") or {}).get("name", "")
        wf_raw     = (props.get("Workflow-Phase", {}).get("select") or {}).get("name", "")

        buckets["status_count"][status_raw] = buckets["status_count"].get(status_raw, 0) + 1
        buckets["workflow_count"][wf_raw]   = buckets["workflow_count"].get(wf_raw, 0)   + 1

        if analysiert:
            buckets["analysiert_true"] += 1
            if drive_link:
                buckets["analysiert_true_mit_drive"] += 1
                if "nicht-verfuegbar" in drive_link:
                    buckets["analysiert_true_drive_nicht_verfuegbar"] += 1
                else:
                    buckets["analysiert_true_mit_gueltigem_drive"] += 1
            else:
                buckets["analysiert_true_ohne_drive"] += 1

            if "via gpt-4o vision" in notizen.lower():
                buckets["vision"] += 1
            if " | " in eig_name:
                buckets["edge_multi_owner"] += 1
            if eig_plz.startswith("D-") or eig_plz.startswith("CH-"):
                buckets["edge_foreign_addr"] += 1

    if not resp.get("has_more"):
        break
    cursor = resp.get("next_cursor")

print(f"\n[Diagnose] Notion-DB {db_id[:8]}…")
print(f"  Pages gesamt:                                {buckets['alle']}")
print(f"  davon 'Gutachten analysiert?'=true:          {buckets['analysiert_true']}")
print(f"    + mit Drive-Link gefüllt:                  {buckets['analysiert_true_mit_drive']}")
print(f"    + Drive-Link valide (nicht 'nicht-verfuegbar'): {buckets['analysiert_true_mit_gueltigem_drive']}")
print(f"    + Drive-Link 'nicht-verfuegbar':            {buckets['analysiert_true_drive_nicht_verfuegbar']}")
print(f"    + Drive-Link leer:                          {buckets['analysiert_true_ohne_drive']}")
print()
print(f"  Modality-Aufschlüsselung (innerhalb analysiert):")
print(f"    vision (via GPT-4o Vision):                 {buckets['vision']}")
print(f"    edge: mehrere Eigentümer (' | '):           {buckets['edge_multi_owner']}")
print(f"    edge: ausländische Adresse (D-/CH-):        {buckets['edge_foreign_addr']}")

print()
print(f"  Status-Verteilung (top 10):")
for k, v in sorted(buckets["status_count"].items(), key=lambda x: -x[1])[:10]:
    print(f"    {k!r:35s} {v:>5}")

print()
print(f"  Workflow-Phase-Verteilung (top 10):")
for k, v in sorted(buckets["workflow_count"].items(), key=lambda x: -x[1])[:10]:
    print(f"    {k!r:35s} {v:>5}")
