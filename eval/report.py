"""
report.py — Generiert eval/03-EVAL-REPORT.md aus einem Run.

Aufruf: python eval/report.py [<run_label>]
        Default: letzter Run in eval/runs/

Output:
- Tabelle: Config × Dimension × Score
- Knock-Out-Verletzungen markiert
- Migration-Empfehlung in einem Satz nach Phase-1-Triggerregel
- Disagreement-Sichtung (5-10 Fälle wo Configs auseinanderlaufen)
- Latenz-Stats (Cold-Start sichtbar)
- Kosten-Schätzung
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
EVAL_DIR  = REPO_ROOT / "eval"
RUNS_DIR  = EVAL_DIR / "runs"
REPORT_OUT = EVAL_DIR / "03-EVAL-REPORT.md"

# Konsistent mit metrics.knockouts() / composite_score()
KO_THRESHOLDS = {
    "json_valid_rate": (">=", 0.99, "D1 JSON-Validität"),
    "name_exact":      (">=", 0.85, "D3 Name-Exact-Match"),
    "addr_exact":      (">=", 0.90, "D5 Adresse"),
    "halluc_name_rate":("<=", 0.03, "D9 Halluz. Name"),
    "halluc_addr_rate":("<=", 0.03, "D10 Halluz. Adresse"),
}

# Token-Preise pro 1M (Stand 2026-04, geschätzt)
TOKEN_PRICES = {
    # OpenAI
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "gpt-4o":      {"input": 2.50, "output": 10.00},
    # NIM Free Tier
    "qwen/qwen3-coder-480b-a35b-instruct": {"input": 0.0, "output": 0.0},
    "z-ai/glm5":                             {"input": 0.0, "output": 0.0},
    "deepseek-ai/deepseek-v3.2":             {"input": 0.0, "output": 0.0},
    "deepseek-ai/deepseek-v4-pro":           {"input": 0.0, "output": 0.0},
}


def latest_run() -> Path:
    runs = sorted([p for p in RUNS_DIR.iterdir() if p.is_dir()],
                  key=lambda p: p.stat().st_mtime, reverse=True)
    if not runs:
        raise SystemExit("Keine Runs in eval/runs/ gefunden.")
    return runs[0]


def load_run(run_dir: Path) -> tuple[dict, list[dict]]:
    summary_p = run_dir / "summary.json"
    pcalls_p  = run_dir / "per_call.jsonl"
    if not summary_p.exists() or not pcalls_p.exists():
        raise SystemExit(f"summary.json oder per_call.jsonl fehlt in {run_dir}")
    summary = json.loads(summary_p.read_text(encoding="utf-8"))
    per_call = [json.loads(l) for l in pcalls_p.read_text(encoding="utf-8").splitlines() if l.strip()]
    return summary, per_call


def fmt_pct(v: float | None, digits: int = 1) -> str:
    if v is None:
        return "—"
    return f"{v*100:.{digits}f}%"


def fmt_num(v: float | None, digits: int = 2) -> str:
    if v is None:
        return "—"
    return f"{v:.{digits}f}"


def detect_disagreements(per_call: list[dict], top_n: int = 8) -> list[dict]:
    """Sucht Edikte mit größtem Score-Auseinanderlaufen zwischen Configs."""
    by_edikt: dict[str, list[dict]] = {}
    for r in per_call:
        eid = r["edikt_id"]
        if r.get("metrics", {}).get("json_valid"):
            by_edikt.setdefault(eid, []).append(r)

    disagreements: list[dict] = []
    for eid, calls in by_edikt.items():
        if len(calls) < 2:
            continue
        # Spreiz auf Name-Exact (binär — bool zu int)
        flags = [(c["config_id"],
                  c["metrics"].get("name_exact"),
                  c["metrics"].get("addr_exact"))
                 for c in calls]
        # Wenn mind. 1 Config name=True und 1 Config name=False → Disagreement
        names = [f[1] for f in flags if f[1] is not None]
        addrs = [f[2] for f in flags if f[2] is not None]
        spread = (len(set(names)) > 1) or (len(set(addrs)) > 1)
        if spread:
            disagreements.append({"edikt_id": eid, "calls": flags})

    return disagreements[:top_n]


def latency_stats(per_call: list[dict]) -> dict[str, dict]:
    """Latenz-Stats pro Config (median, p95, max). Erste Calls als 'Cold' markiert."""
    by_cfg: dict[str, list[int]] = {}
    cold_starts: dict[str, int] = {}
    for r in per_call:
        cid = r["config_id"]
        if r["error"]:
            continue
        by_cfg.setdefault(cid, []).append(r["latency_ms"])

    out = {}
    for cid, lats in by_cfg.items():
        s = sorted(lats)
        if not s:
            continue
        n = len(s)
        out[cid] = {
            "n": n,
            "median": s[n//2],
            "p95":    s[min(int(n*0.95), n-1)],
            "max":    s[-1],
            "cold_first": s[-1] if max(lats) > 30000 else None,  # heuristisch
        }
    return out


def cost_estimate(per_call: list[dict], summary: dict) -> dict[str, float]:
    """Geschätzte API-Kosten pro Config für die Test-Run-Größe.

    Substring-Heuristik (alt) hat z.B. die Preise von DeepSeek V3.2 und V4-Pro
    vertauscht, weil beide auf 'deepseek' matchten. Stattdessen direkter
    Mapping-Lookup über models.CONFIGS[cid].model.
    """
    # Lazy-Import um Zirkular-Imports und sys.path-Mutation auf Modul-Ebene
    # zu vermeiden – report.py wird auch standalone aus eval/ ausgeführt.
    sys.path.insert(0, str(EVAL_DIR))
    try:
        import models as _models  # type: ignore
        cfg_to_model = {cid: cfg.model for cid, cfg in _models.CONFIGS.items()}
    finally:
        if str(EVAL_DIR) in sys.path:
            sys.path.remove(str(EVAL_DIR))

    by_cfg: dict[str, dict[str, int]] = {}
    for r in per_call:
        cid = r["config_id"]
        if r["error"]:
            continue
        b = by_cfg.setdefault(cid, {"prompt": 0, "completion": 0})
        b["prompt"]     += r.get("prompt_tokens")     or 0
        b["completion"] += r.get("completion_tokens") or 0

    cost: dict[str, float] = {}
    for cid, tokens in by_cfg.items():
        model_id = cfg_to_model.get(cid, "")
        unit_cost = TOKEN_PRICES.get(model_id, {"input": 0.0, "output": 0.0})
        cost[cid] = (tokens["prompt"] * unit_cost["input"] +
                     tokens["completion"] * unit_cost["output"]) / 1_000_000
    return cost


def determine_recommendation(summary: dict) -> str:
    """Migrations-Triggerregel laut 01-EVAL-CRITERIA.md §5."""
    sq_id = "status_quo_text"
    sq    = summary.get(sq_id, {}).get("agg")
    if not sq or sq.get("n", 0) == 0:
        return "Status Quo (Call A) konnte nicht gemessen werden — keine Empfehlung möglich."

    survivors = []
    for cid, data in summary.items():
        if cid == sq_id or cid == "status_quo_vision":
            continue
        if data.get("knockouts"):
            continue
        agg = data["agg"]
        # Mindest-Anforderung: ≥ Status Quo - 1pp in name/addr
        if agg["name_exact"] < sq["name_exact"] - 0.01:  continue
        if agg["addr_exact"] < sq["addr_exact"] - 0.01:  continue
        # Wunsch-Bedingung: ≥ +3pp in name ODER addr ODER glaubiger F1 +5pp
        wins = []
        if agg["name_exact"] >= sq["name_exact"] + 0.03: wins.append("name")
        if agg["addr_exact"] >= sq["addr_exact"] + 0.03: wins.append("addr")
        if (agg.get("glaubiger_f1_mean") or 0) >= (sq.get("glaubiger_f1_mean") or 0) + 0.05: wins.append("glaubiger")
        if (agg["halluc_name_rate"] <= sq["halluc_name_rate"] - 0.01
            and agg["halluc_addr_rate"] <= sq["halluc_addr_rate"] - 0.01): wins.append("halluz")
        if wins:
            survivors.append((cid, data["composite_score"] or 0, wins))

    if not survivors:
        return ("Bei Status Quo (gpt-4o-mini) bleiben — kein NIM-Kandidat erfüllt "
                "alle Knock-Out-Schwellen UND eine Wunsch-Bedingung.")

    survivors.sort(key=lambda x: x[1], reverse=True)
    winner_id, winner_score, winner_wins = survivors[0]
    return (f"Wechsel auf **{winner_id}** für Call A (gpt-4o-mini-Ersatz). "
            f"Composite-Score {winner_score:.3f}, gewinnt in: {', '.join(winner_wins)}. "
            "Call B (Vision) bleibt mangels NIM-Free-Tier-Vision-Modell auf gpt-4o.")


# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_label", nargs="?", default=None)
    args = parser.parse_args()

    run_dir = (RUNS_DIR / args.run_label) if args.run_label else latest_run()
    print(f"[Report] Lese Run: {run_dir.name}")
    summary, per_call = load_run(run_dir)

    lat = latency_stats(per_call)
    cost = cost_estimate(per_call, summary)
    disagreements = detect_disagreements(per_call)
    recommendation = determine_recommendation(summary)

    md = []
    md.append(f"# Phase 4 — Eval-Report\n")
    md.append(f"**Run:** `{run_dir.name}`")
    md.append(f"**Eingeschwungene Schwellen:** siehe `01-EVAL-CRITERIA.md` (gefroren).")
    md.append(f"**Eval-Set:** {len(set(r['edikt_id'] for r in per_call))} Edikte × "
              f"{len(set(r['config_id'] for r in per_call))} Configs = "
              f"{len(per_call)} Calls\n")

    # ── Empfehlung früh, weil das das Wichtigste ist ──
    md.append("## Empfehlung\n")
    md.append(f"> {recommendation}\n")

    # ── Hauptmatrix ──
    md.append("## Hauptmatrix — Genauigkeit\n")
    md.append("| Config | n | JSON | Schema | Name | Adresse | PLZ | Gläubiger F1 | Forderung | Halluz. Name | Halluz. Adr. | Score | Knock-Outs |")
    md.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")

    cfg_order = ["status_quo_text", "status_quo_vision",
                 "nim_qwen", "nim_glm5", "nim_deepseek_v32", "nim_deepseek_v4pro"]
    for cid in cfg_order:
        if cid not in summary:
            continue
        a = summary[cid]["agg"]
        if a.get("n", 0) == 0:
            continue
        score = summary[cid]["composite_score"]
        ko = summary[cid]["knockouts"]
        ko_str = "🔴 " + ", ".join(ko) if ko else "✓"
        md.append(f"| `{cid}` | {a['n']} | "
                  f"{fmt_pct(a.get('json_valid_rate'))} | "
                  f"{fmt_pct(a.get('schema_compliant'))} | "
                  f"{fmt_pct(a.get('name_exact'))} | "
                  f"{fmt_pct(a.get('addr_exact'))} | "
                  f"{fmt_pct(a.get('plz_exact'))} | "
                  f"{fmt_num(a.get('glaubiger_f1_mean'))} | "
                  f"{fmt_pct(a.get('forderung_exact'))} | "
                  f"{fmt_pct(a.get('halluc_name_rate'))} | "
                  f"{fmt_pct(a.get('halluc_addr_rate'))} | "
                  f"**{fmt_num(score, 3) if score else '—'}** | "
                  f"{ko_str} |")
    md.append("")

    # ── Latenz ──
    md.append("## Latenz-Stats (Cold-Start sichtbar)\n")
    md.append("| Config | n | Median | p95 | Max | über Knock-Out (30s)? |")
    md.append("|---|---|---|---|---|---|")
    for cid in cfg_order:
        if cid not in lat:
            continue
        s = lat[cid]
        ko_lat = "🔴 ja" if s["p95"] > 30000 else "✓"
        md.append(f"| `{cid}` | {s['n']} | {s['median']/1000:.1f}s | "
                  f"{s['p95']/1000:.1f}s | {s['max']/1000:.1f}s | {ko_lat} |")
    md.append("")

    # ── Kostenschätzung ──
    md.append("## Geschätzte Kosten für diesen Run\n")
    md.append("| Config | Kosten ($) für n Calls |")
    md.append("|---|---|")
    for cid in cfg_order:
        if cid not in cost:
            continue
        md.append(f"| `{cid}` | ${cost[cid]:.4f} |")
    md.append("")

    # ── Disagreements ──
    md.append("## Disagreements — Edikte mit Spreizung\n")
    if not disagreements:
        md.append("_Keine deutlichen Disagreements gefunden._\n")
    else:
        md.append("| Edikt-ID | Configs (name_exact, addr_exact) |")
        md.append("|---|---|")
        for d in disagreements:
            cells = ", ".join(f"{c[0]}: name={c[1]}/addr={c[2]}" for c in d["calls"])
            md.append(f"| `{d['edikt_id']}` | {cells} |")
        md.append("\n_Inspektion via `eval/runs/<label>/raw/<config_id>/<edikt_id>.json`._")
    md.append("")

    # ── Disclaimer + Methodik-Hinweis ──
    md.append("## Methodik-Hinweise\n")
    md.append("- Ground Truth: aus Notion-Properties (Status-Quo-Output von gpt-4o-mini), "
              "noch ohne Fritz-Stichprobe-Korrektur — Disclaimer aus 01-EVAL-CRITERIA.md §6.4 gilt.")
    md.append("- Cold-Start im NIM Free Tier: erste Calls eines Modells sind 30–80 s langsam, "
              "Warmup mit Edikt-ähnlichem Prompt vorher; trotzdem im Latenz-Resultat sichtbar.")
    md.append("- Vision (Call B) nur Status-Quo-gemessen — kein NIM-Vision-Modell im Free Tier.")
    md.append("")

    REPORT_OUT.write_text("\n".join(md), encoding="utf-8")
    print(f"[Report] geschrieben: {REPORT_OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
