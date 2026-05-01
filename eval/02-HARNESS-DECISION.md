# Phase 3 — Eval-Harness-Entscheidung

**Datum:** 2026-05-02
**Entscheidung:** Eigenes Python-Skript in `eval/`.

## Begründung (Top 5)

1. **Repo ist Python**, kein TS/Node — promptfoo wäre eine fremde Toolchain.
2. **Custom-Metriken aus Phase 1** (Levenshtein-Adresse, F1-Set-Vergleich Gläubiger, Halluzinations-Tagging gegen `null`-GT) sind in promptfoo nur als Custom-JS einbindbar — in Python schreibe ich sie direkt in 50–100 Zeilen.
3. **Reproduzierbarkeit**: JSONL-Eingabe + JSONL-Ausgabe, fixe Seeds. Keine versteckte Tool-State.
4. **AutoAgent** ist für Optimierungs-Loops eines einzelnen Modells gebaut, nicht für 5-Wege-Vergleich — würde gegen seine Kern-Stärke laufen.
5. **NIM ist OpenAI-kompatibel** → Provider-Switch = nur `base_url` + Modellname tauschen, kein SDK-Wechsel. Trivial in Python.

## Datei-Struktur

```
eval/
  00-ANALYSE.md           # Phase 0
  01-EVAL-CRITERIA.md     # Phase 1 (frozen)
  02-HARNESS-DECISION.md  # diese Datei
  data/
    eval-set.jsonl        # 40 PDFs mit GT-Output
    pdfs/                 # roh-PDFs (gitignored)
  runs/                   # Run-Logs (gitignored)
  sample_pdfs.py          # Drive → eval/data/pdfs/ + eval-set.jsonl
  models.py               # Provider-Konfigurationen
  metrics.py              # D1-D11 deterministische Metriken
  runner.py               # Haupt-Eval (5 Modelle × 40 PDFs)
  report.py               # 03-EVAL-REPORT.md generieren
```

## Bestätigte Modell-Konfigurationen für Eval

| ID | Provider | Modell | Endpunkt |
|---|---|---|---|
| `status_quo_a` | OpenAI | `gpt-4o-mini` | api.openai.com/v1 |
| `status_quo_b` | OpenAI | `gpt-4o` (Vision) | api.openai.com/v1 |
| `nim_qwen` | NIM | `qwen/qwen3-coder-480b-a35b-instruct` | integrate.api.nvidia.com/v1 |
| `nim_glm5` | NIM | `z-ai/glm5` | integrate.api.nvidia.com/v1 |
| `nim_deepseek_v32` | NIM | `deepseek-ai/deepseek-v3.2` | integrate.api.nvidia.com/v1 |
| `nim_deepseek_v4pro` | NIM | `deepseek-ai/deepseek-v4-pro` | integrate.api.nvidia.com/v1 |

**Vision-Caveat:** Keiner der NIM-Kandidaten ist vision-fähig im Free Tier (Stand 2026-05-02 — siehe Phase-0-Analyse). Call B (gescannte PDFs) bleibt vorerst auf OpenAI; wir messen den Status Quo trotzdem als Baseline. Falls das Eval-Ergebnis für Call A klar zu NIM rät, bleibt Call B als Hybrid bei OpenAI.

**Cold-Start-Caveat:** Free-Tier-NIM hat Cold-Starts ≥ 30 s beim ersten Call eines Modells. Runner führt vor jeder Messung einen Warmup-Call durch und verwirft den.
