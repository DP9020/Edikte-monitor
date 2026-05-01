"""
runner.py — Haupt-Eval-Lauf.

Pro Eval-Item (PDF):
- Modality entscheidet welche Modell-Configs aufgerufen werden
- Text-Items: alle 5 TEXT_CONFIGS (Status Quo + 4 NIM)
- Vision-Items: nur status_quo_vision (NIM hat kein Vision-Modell)
- Edge-Cases: behandeln wir wie Text (sind Text-PDFs mit komplexer Structure)

Ausgabe:
- eval/runs/<timestamp>/raw/<config_id>/<edikt_id>.json   – Roh-Output + Latenz
- eval/runs/<timestamp>/per_call.jsonl                     – flach für Analyse
- eval/runs/<timestamp>/summary.json                       – Aggregate je Modell

Aufruf:
- python eval/runner.py                – Vollläufer (alle PDFs)
- python eval/runner.py --limit 3      – Smoke-Run mit 3 PDFs
- python eval/runner.py --skip-warmup  – Cold-Start nicht vorwärmen (Debug)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    sys.stderr.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)

REPO_ROOT = Path(__file__).resolve().parent.parent
EVAL_DIR  = REPO_ROOT / "eval"
JSONL_IN  = EVAL_DIR / "data" / "eval-set.jsonl"
PDF_DIR   = EVAL_DIR / "data" / "pdfs"
RUNS_DIR  = EVAL_DIR / "runs"


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


def load_eval_set(limit: int | None = None) -> list[dict]:
    items = [json.loads(ln) for ln in JSONL_IN.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if limit:
        items = items[:limit]
    return items


def pdf_text_snippet(pdf_path: Path, max_chars: int = 12000) -> str:
    """Wie main.py:1141: erste 12000 Zeichen des PyMuPDF-Volltexts."""
    import fitz
    doc = fitz.open(str(pdf_path))
    try:
        chunks = []
        for page in doc:
            chunks.append(page.get_text())
            if sum(len(c) for c in chunks) >= max_chars:
                break
        return "".join(chunks)[:max_chars]
    finally:
        doc.close()


def pdf_to_images_b64(pdf_path: Path, max_pages: int = 8) -> list[str]:
    """Wie main.py:2954: 2.5x Zoom = ~190 DPI, JPEG q=80."""
    import fitz
    doc = fitz.open(str(pdf_path))
    try:
        out = []
        for i in range(min(max_pages, len(doc))):
            page = doc[i]
            pix  = page.get_pixmap(matrix=fitz.Matrix(2.5, 2.5), colorspace=fitz.csRGB)
            jpg  = pix.tobytes("jpeg", jpg_quality=80)
            out.append(base64.b64encode(jpg).decode("utf-8"))
        return out
    finally:
        doc.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                        help="Max # PDFs (für Smoke-Run; default: alle)")
    parser.add_argument("--skip-warmup", action="store_true",
                        help="Cold-Start-Warmup überspringen")
    parser.add_argument("--only", default="",
                        help="Komma-Liste von Config-IDs, sonst alle")
    parser.add_argument("--label", default=None, help="Run-Label (Default: Timestamp)")
    args = parser.parse_args()

    load_env(REPO_ROOT / ".env")

    sys.path.insert(0, str(EVAL_DIR))
    from models  import CONFIGS, TEXT_CONFIGS, VISION_CONFIGS, call_text, call_vision, warmup, liveness_check
    from metrics import evaluate, aggregate, composite_score, knockouts

    only_filter = {x.strip() for x in args.only.split(",") if x.strip()}

    label = args.label or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    run_dir = RUNS_DIR / label
    run_dir.mkdir(parents=True, exist_ok=True)
    raw_dir = run_dir / "raw"
    raw_dir.mkdir(exist_ok=True)
    per_call_path = run_dir / "per_call.jsonl"
    summary_path  = run_dir / "summary.json"

    print(f"[Runner] Run-Dir: {run_dir}")

    items = load_eval_set(limit=args.limit)
    print(f"[Runner] Eval-Set: {len(items)} Items")

    # Warmup pro genutzter Config — ein Call ist gnug, nicht pro Item
    used_configs: set[str] = set()
    for item in items:
        if item["modality"] in ("text", "edge_case"):
            for c in TEXT_CONFIGS:
                used_configs.add(c.id)
        elif item["modality"] == "vision":
            for c in VISION_CONFIGS:
                used_configs.add(c.id)

    if only_filter:
        used_configs &= only_filter

    # ── Pre-flight Liveness-Check: tote Modelle aussortieren ──
    # Parallel + 30s Timeout: gibt NIM-Cold-Start (10-20s im Free Tier) eine faire Chance
    from concurrent.futures import ThreadPoolExecutor, as_completed
    LIVENESS_TIMEOUT = 30
    print(f"[Runner] Liveness-Check ({len(used_configs)} Configs, parallel, ~{LIVENESS_TIMEOUT}s Timeout)...", flush=True)
    dead_configs: set[str] = set()
    with ThreadPoolExecutor(max_workers=len(used_configs)) as ex:
        futures = {ex.submit(liveness_check, CONFIGS[cid], timeout_s=LIVENESS_TIMEOUT): cid
                   for cid in used_configs}
        for fut in as_completed(futures):
            cid = futures[fut]
            r = fut.result()
            if r.error:
                dead_configs.add(cid)
                print(f"           {cid:24s} DEAD ({r.error[:60]})", flush=True)
            else:
                print(f"           {cid:24s} alive ({r.latency_ms}ms)", flush=True)

    used_configs -= dead_configs
    if dead_configs:
        print(f"[Runner] {len(dead_configs)} Config(s) ausgeschlossen: {sorted(dead_configs)}", flush=True)
    if not used_configs:
        print("[Runner] Alle Configs DEAD — Abbruch.", flush=True)
        return 1

    # ── Warmup für lebendige Configs (sequentiell, aber nur Lebendige) ──
    if not args.skip_warmup:
        print(f"[Runner] Warmup ({len(used_configs)} lebendige Configs, kann je Config 30-180s dauern)...", flush=True)
        for cid in sorted(used_configs):
            cfg = CONFIGS[cid]
            print(f"           {cid:24s} ... ", end="", flush=True)
            r = warmup(cfg)
            tag = f"{r.latency_ms}ms" if r.error is None else f"ERR {r.error[:50]}"
            print(tag, flush=True)

    # Pro-Call-Records
    per_call_fp = per_call_path.open("w", encoding="utf-8")
    aggregates_input: dict[str, list[dict]] = {cid: [] for cid in used_configs}

    try:
        for ix, item in enumerate(items, start=1):
            eid       = item["id"]
            pdf_path  = PDF_DIR / item["pdf_filename"]
            modality  = item["modality"]
            gt        = item["ground_truth"]

            if not pdf_path.exists():
                print(f"  [{ix:02d}] {eid}  PDF MISSING", flush=True)
                continue

            print(f"\n  [{ix:02d}/{len(items):02d}] {eid}  modality={modality}  liegen={item.get('liegenschaftsadresse','')[:50]}", flush=True)

            # Configs für dieses Item — nur lebendige aus used_configs
            if modality in ("text", "edge_case"):
                configs = [c for c in TEXT_CONFIGS if c.id in used_configs]
                # PDF-Text einmal extrahieren
                try:
                    snippet = pdf_text_snippet(pdf_path)
                except Exception as exc:
                    print(f"      [PDF] FAIL extract text: {exc}", flush=True)
                    continue
            elif modality == "vision":
                configs = [c for c in VISION_CONFIGS if c.id in used_configs]
                try:
                    images = pdf_to_images_b64(pdf_path)
                except Exception as exc:
                    print(f"      [PDF] FAIL render images: {exc}", flush=True)
                    continue
            else:
                print(f"      Unbekannte modality: {modality} -> skip")
                continue

            for cfg in configs:
                cfg_dir = raw_dir / cfg.id
                cfg_dir.mkdir(exist_ok=True)
                if modality in ("text", "edge_case"):
                    res = call_text(cfg, snippet)
                else:
                    res = call_vision(cfg, images)

                eval_metrics = evaluate(res.raw_text, gt)

                rec = {
                    "edikt_id": eid,
                    "config_id": cfg.id,
                    "modality": modality,
                    "latency_ms": res.latency_ms,
                    "prompt_tokens": res.prompt_tokens,
                    "completion_tokens": res.completion_tokens,
                    "error": res.error,
                    "metrics": eval_metrics,
                }

                # Roh-Output separat speichern (für späteres Disagreement-Reading)
                (cfg_dir / f"{eid}.json").write_text(
                    json.dumps({
                        "edikt_id": eid,
                        "config_id": cfg.id,
                        "raw_output": res.raw_text,
                        "latency_ms": res.latency_ms,
                        "error": res.error,
                        "ground_truth": gt,
                        "metrics": eval_metrics,
                    }, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

                per_call_fp.write(json.dumps(rec, ensure_ascii=False) + "\n")
                per_call_fp.flush()

                aggregates_input[cfg.id].append(eval_metrics)

                # Kompakte Zeile
                m = eval_metrics
                if not m["json_valid"]:
                    tag = f"JSON_FAIL:{m.get('json_error','')}"
                else:
                    parts = []
                    if m.get("name_exact") is not None:    parts.append(f"name={'OK' if m['name_exact'] else 'X '}")
                    if m.get("addr_exact") is not None:    parts.append(f"addr={'OK' if m['addr_exact'] else 'X '}")
                    if m.get("glaubiger_f1") is not None:  parts.append(f"glF1={m['glaubiger_f1']:.2f}")
                    tag = " ".join(parts) or "ok"
                err_tag = f" ERR={res.error[:40]}" if res.error else ""
                print(f"      [{cfg.id:24s}] {res.latency_ms:>5}ms  {tag}{err_tag}", flush=True)
    finally:
        per_call_fp.close()

    # ── Aggregate berechnen ──
    summary = {}
    print("\n[Runner] Aggregate je Modell:")
    for cid in sorted(used_configs):
        agg = aggregate(aggregates_input[cid])
        score = composite_score(agg)
        ko    = knockouts(agg) if agg.get("n") and agg["json_valid_rate"] is not None else []
        summary[cid] = {"agg": agg, "composite_score": score, "knockouts": ko}
        ko_str = ", ".join(ko) if ko else "(none)"
        print(f"  {cid:24s}  n={agg['n']:>2}  "
              f"json={agg.get('json_valid_rate', 0):.0%}  "
              f"name={agg.get('name_exact', 0):.0%}  "
              f"addr={agg.get('addr_exact', 0):.0%}  "
              f"score={(score or 0):.3f}  KO=[{ko_str}]")

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[Runner] Summary: {summary_path}")
    print(f"[Runner] Per-Call: {per_call_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
