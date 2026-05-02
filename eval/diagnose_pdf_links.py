"""
diagnose_pdf_links.py — Schaut wie viele Notion-Pages einen PDF-Link in Notizen
haben und wie viele davon HEAD-200 zurückgeben (also live abrufbar sind).
"""
from __future__ import annotations

import os
import random
import re
import sys
import urllib.request
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent

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


PDF_LINK_RE = re.compile(r"Gutachten-PDF:\s*(\S+)")

with_link = []
cursor = None
while True:
    kwargs = {"data_source_id": ds_id, "page_size": 100}
    if cursor:
        kwargs["start_cursor"] = cursor
    resp = notion.data_sources.query(**kwargs)
    for p in resp.get("results", []):
        props = p.get("properties", {})
        if not (props.get("Gutachten analysiert?", {}).get("checkbox") or False):
            continue
        notizen = _rt(props.get("Notizen", {}).get("rich_text", []))
        m = PDF_LINK_RE.search(notizen)
        if not m:
            continue
        url = m.group(1).strip()
        eig_name = _rt(props.get("Verpflichtende Partei", {}).get("rich_text", []))
        eig_plz  = _rt(props.get("Zustell PLZ/Ort",       {}).get("rich_text", []))
        is_vision = "via gpt-4o vision" in notizen.lower()
        is_multi  = " | " in eig_name
        is_foreign = eig_plz.startswith("D-") or eig_plz.startswith("CH-")
        if is_multi or is_foreign:
            mod = "edge"
        elif is_vision:
            mod = "vision"
        else:
            mod = "text"
        with_link.append({"url": url, "modality": mod})
    if not resp.get("has_more"):
        break
    cursor = resp.get("next_cursor")

print(f"[Diagnose] Pages mit 'Gutachten-PDF:'-Link in Notizen: {len(with_link)}")
mod_count = {"text":0,"vision":0,"edge":0}
for x in with_link:
    mod_count[x["modality"]] += 1
for k,v in mod_count.items():
    print(f"    {k}: {v}")

# Live-Stichprobe: 12 random URLs HEAD-checken
rng = random.Random(42)
sample = rng.sample(with_link, min(12, len(with_link)))
print(f"\n[Diagnose] Live-Verfügbarkeit (Stichprobe von {len(sample)}):")
ok, dead = 0, 0
for s in sample:
    try:
        req = urllib.request.Request(s["url"], method="HEAD",
                                     headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            status = r.status
            if status == 200:
                ok += 1
                tag = "OK"
            else:
                dead += 1
                tag = f"HTTP {status}"
    except Exception as exc:
        dead += 1
        tag = f"FAIL ({type(exc).__name__})"
    print(f"  [{s['modality']:6s}] {tag:25s}  {s['url'][:80]}")

print(f"\n[Diagnose] Sample: {ok} live / {dead} tot")
if sample:
    print(f"  Hochrechnung: ~{int(len(with_link) * ok / len(sample))} live PDFs verfügbar")
else:
    print("  Hochrechnung: – (keine Sample-Pages verfügbar)")
