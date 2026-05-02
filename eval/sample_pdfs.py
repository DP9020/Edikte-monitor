"""
sample_pdfs.py — Stratifiziertes Sampling von 40 PDFs für die Eval.

Quelle: PDF-Links aus dem Notizen-Feld jeder Notion-Page (Format: "Gutachten-PDF: <url>").
Hintergrund: Drive enthält nur ~6 PDFs (nur "🟡 Gelb"-Status); die ~557 anderen
analysierten Edikte haben PDF-Links nur in Notizen. Live-Verfügbarkeit ~33 %.

Workflow:
1. Notion → alle analysierten Pages mit `Gutachten-PDF:`-Link.
2. Pro Modality (text/vision/edge_case) 3× Übersample, shuffle (Seed 42).
3. Lade nacheinander PDFs herunter; bei 4xx/5xx weiter zum nächsten Kandidaten.
4. Stop wenn pro Modality das Ziel erreicht ist (28/8/4).
5. Schreibt eval/data/eval-set.jsonl mit Status-Quo-GT aus Notion-Properties.

Aufruf:  python eval/sample_pdfs.py
ENV:     NOTION_TOKEN, NOTION_DATABASE_ID (aus .env)
"""

from __future__ import annotations

import io
import json
import os
import random
import re
import sys
import urllib.request
import urllib.error
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
EVAL_DIR  = REPO_ROOT / "eval"
PDF_DIR   = EVAL_DIR / "data" / "pdfs"
JSONL_OUT = EVAL_DIR / "data" / "eval-set.jsonl"

SEED = 42
TARGETS = {"text": 28, "vision": 8, "edge_case": 4}
OVERSAMPLE = 3   # 3× weil ~33 % Live-Verfügbarkeit
TOTAL = sum(TARGETS.values())

PDF_LINK_RE = re.compile(r"Gutachten-PDF:\s*(\S+)")
FORDERUNG_RE = re.compile(r"Forderung:\s*([^\n]+)")
USER_AGENT = "Mozilla/5.0 (Edikte-Monitor Eval-Sampling)"
DOWNLOAD_TIMEOUT = 30
MAX_PDF_BYTES = 50 * 1024 * 1024  # 50 MB


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k:
            os.environ.setdefault(k, v)


def _rt(rt) -> str:
    parts: list[str] = []
    for t in rt or []:
        if not isinstance(t, dict):
            continue
        plain = t.get("plain_text")
        if isinstance(plain, str) and plain:
            parts.append(plain)
            continue
        text_obj = t.get("text") or {}
        c = text_obj.get("content") if isinstance(text_obj, dict) else None
        if isinstance(c, str):
            parts.append(c)
    return "".join(parts).strip()


def fetch_candidates(notion, db_id: str) -> list[dict]:
    """Alle analysierten Pages mit `Gutachten-PDF:`-Link aus Notizen."""
    out: list[dict] = []
    cursor = None
    while True:
        kwargs = {"database_id": db_id, "page_size": 100}
        if cursor:
            kwargs["start_cursor"] = cursor
        resp = notion.databases.query(**kwargs)
        for p in resp.get("results", []):
            props = p.get("properties", {})
            if not (props.get("Gutachten analysiert?", {}).get("checkbox") or False):
                continue
            notizen = _rt(props.get("Notizen", {}).get("rich_text", []))
            mlink = PDF_LINK_RE.search(notizen)
            if not mlink:
                continue
            pdf_url = mlink.group(1).strip()

            adr_title = props.get("Liegenschaftsadresse", {}).get("title", [])
            liegenschaftsadresse = _rt(adr_title)
            eig_name = _rt(props.get("Verpflichtende Partei", {}).get("rich_text", []))
            eig_adr  = _rt(props.get("Zustell Adresse",      {}).get("rich_text", []))
            eig_plz  = _rt(props.get("Zustell PLZ/Ort",      {}).get("rich_text", []))
            gl_text  = _rt(props.get("Betreibende Partei",   {}).get("rich_text", []))
            mforderung = FORDERUNG_RE.search(notizen)
            forderung  = (mforderung.group(1).strip() if mforderung else "") or None

            is_vision  = "via gpt-4o vision" in notizen.lower()
            multi_owner = " | " in eig_name
            foreign_addr = eig_plz.startswith("D-") or eig_plz.startswith("CH-")

            if multi_owner or foreign_addr:
                modality = "edge_case"
            elif is_vision:
                modality = "vision"
            else:
                modality = "text"

            gl_list = [g.strip() for g in gl_text.split("|") if g.strip()] if gl_text else []

            out.append({
                "page_id": p["id"],
                "liegenschaftsadresse": liegenschaftsadresse,
                "modality": modality,
                "pdf_url": pdf_url,
                "ground_truth": {
                    "eigentümer_name":    eig_name or None,
                    "eigentümer_adresse": eig_adr  or None,
                    "eigentümer_plz_ort": eig_plz  or None,
                    "gläubiger":          gl_list,
                    "forderung_betrag":   forderung,
                },
            })

        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")

    return out


def download_pdf(url: str) -> tuple[bytes, str] | tuple[None, str]:
    """Lädt PDF; retourniert (bytes, '') bei Erfolg oder (None, fail_reason)."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=DOWNLOAD_TIMEOUT) as r:
            ctype = r.headers.get("Content-Type", "")
            if "pdf" not in ctype.lower() and not url.lower().endswith(".pdf"):
                # Trotzdem versuchen — manche Server schicken application/octet-stream
                pass
            data = r.read(MAX_PDF_BYTES + 1)
            if len(data) > MAX_PDF_BYTES:
                return None, f"too_large_{len(data)}"
            if not data.startswith(b"%PDF"):
                return None, "not_pdf"
            return data, ""
    except urllib.error.HTTPError as e:
        return None, f"http_{e.code}"
    except urllib.error.URLError as e:
        return None, f"url_{type(e.reason).__name__}"
    except Exception as e:
        return None, f"err_{type(e).__name__}"


def main() -> int:
    load_env(REPO_ROOT / ".env")
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    notion_token = os.environ.get("NOTION_TOKEN")
    notion_dbid  = os.environ.get("NOTION_DATABASE_ID")
    if not (notion_token and notion_dbid):
        print("FEHLER: NOTION_TOKEN, NOTION_DATABASE_ID müssen in .env gesetzt sein.")
        return 1

    from notion_client import Client as NotionClient
    notion = NotionClient(auth=notion_token)

    print(f"[Sample] Lese Kandidaten aus Notion-DB {notion_dbid[:8]}...")
    candidates = fetch_candidates(notion, notion_dbid)

    by_mod: dict[str, list[dict]] = {"text": [], "vision": [], "edge_case": []}
    for c in candidates:
        by_mod[c["modality"]].append(c)
    print(f"[Sample] {len(candidates)} Kandidaten gesamt:")
    for m, n in TARGETS.items():
        print(f"           {m:10s}: {len(by_mod[m]):3d}  (Ziel: {n})")

    # Übersample je Modality, gemixt
    rng = random.Random(SEED)
    queue: list[dict] = []
    for m, n_target in TARGETS.items():
        bucket = by_mod[m][:]
        rng.shuffle(bucket)
        # Markiere mit Modality + Ziel-Counter
        queue.extend(bucket[: n_target * OVERSAMPLE])

    # Sortier-Trick: rotiere Modalities, damit wir früh in jeder ein wenig Fortschritt haben
    rng.shuffle(queue)

    # Download-Loop — stop pro Modality wenn Target erreicht
    have: dict[str, int] = {m: 0 for m in TARGETS}
    fails: dict[str, int] = {}
    written = 0

    JSONL_OUT.parent.mkdir(parents=True, exist_ok=True)
    fout = JSONL_OUT.open("w", encoding="utf-8")

    try:
        for idx, item in enumerate(queue, start=1):
            mod = item["modality"]
            if have[mod] >= TARGETS[mod]:
                continue
            if all(have[m] >= TARGETS[m] for m in TARGETS):
                break

            data, reason = download_pdf(item["pdf_url"])
            if data is None:
                fails[reason] = fails.get(reason, 0) + 1
                print(f"  [{idx:03d}] [{mod:10s}] FAIL {reason:20s}  {item['pdf_url'][:70]}")
                continue

            have[mod] += 1
            eid = f"edikt-{written+1:03d}"
            pdf_path = PDF_DIR / f"{eid}.pdf"
            pdf_path.write_bytes(data)

            record = {
                "id": eid,
                "pdf_filename": pdf_path.name,
                "pdf_url": item["pdf_url"],
                "modality": mod,
                "liegenschaftsadresse": item["liegenschaftsadresse"],
                "notion_page_id": item["page_id"],
                "ground_truth": item["ground_truth"],
                "ground_truth_source": "status_quo_uncorrected",
            }
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
            fout.flush()
            written += 1
            print(f"  [{idx:03d}] [{mod:10s}] OK   {pdf_path.name} ({len(data)//1024}kB) "
                  f"[have: text={have['text']}/{TARGETS['text']} "
                  f"vision={have['vision']}/{TARGETS['vision']} "
                  f"edge={have['edge_case']}/{TARGETS['edge_case']}]")

    finally:
        fout.close()

    print(f"\n[Sample] Geschrieben: {written}/{TOTAL} PDFs in {PDF_DIR}")
    print(f"[Sample] Eval-Set:    {JSONL_OUT}")
    if fails:
        print(f"[Sample] Fail-Verteilung:")
        for k, v in sorted(fails.items(), key=lambda x: -x[1]):
            print(f"           {k:25s} {v}")
    return 0 if written >= TOTAL // 2 else 2


if __name__ == "__main__":
    sys.exit(main())
