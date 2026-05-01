# Phase 4 — Eval-Report

**Run:** `full-run-v1`
**Eingeschwungene Schwellen:** siehe `01-EVAL-CRITERIA.md` (gefroren).
**Eval-Set:** 36 Edikte × 4 Configs = 100 Calls

## Empfehlung

> **Bei Status Quo (gpt-4o-mini) bleiben.** Kein NIM-Kandidat erfüllt die Phase-1-Trigger-Regel. **ABER:** Auch der Status Quo verletzt den Adress-Knock-Out (84,6 % statt ≥ 90 %) — das war in Phase 1 §3 explizit als "deckt ein Pipeline-Problem auf" antizipiert. Die Eval hat nicht den Modell-Wechsel beantwortet, sondern ein bestehendes Adress-Problem in der Production-Pipeline aufgedeckt.

## Inhaltliche Auswertung

### Befund 1 — Status Quo unter selbst-gewählter Schwelle

`status_quo_text` (gpt-4o-mini) erreicht 84,6 % Adress-Genauigkeit — **gegen sich selbst** als Ground Truth. Eigentlich müsste der Wert ≈ 100 % sein, weil die GT aus dem gleichen Modell stammt. Mögliche Ursachen für die 15-%-Diskrepanz:

1. **`_clean_adresse()` in `main.py:_clean_adresse`**: Die Pipeline normalisiert die Adresse nach dem LLM-Call (Whitespace, Suffix-Cleanups), bevor sie in Notion landet. Beim Eval-Run vergleichen wir den rohen LLM-Output gegen die normalisierte Notion-Version → Mismatch.
2. **`GESCHUETZT_PHASEN`-Frozenset (`main.py`)**: Eingaben in geschützten Workflow-Phasen werden nicht überschrieben. Wenn Fritz/Betreuer manuell eine korrekte Adresse eintragen haben, weicht die GT vom LLM-Output ab.
3. **gpt-4o-mini-Version-Drift**: Gleiches Modell, andere Antworten je Update. Edikte aus Q1 wurden mit anderer Modell-Version analysiert als heutige Calls.

Bevor der Modell-Wechsel diskutiert wird, sollte das Adress-Pipeline-Problem geklärt werden.

### Befund 2 — NIM Qwen halluziniert Adressen (9,4 %)

`nim_qwen` füllt das Adress-Feld in 3 von 32 Fällen mit Werten, obwohl die GT `null` ist (kein Adress-Befund). Bei 90-prozentiger Name-Genauigkeit aber 9,4 % Halluzinations-Rate würde das in Production zu **3 % falsch zugestellten Briefen** führen — direkt das, was du in Frage 2 als "katastrophal" eingeordnet hast.

### Befund 3 — DeepSeek nicht messbar

`nim_deepseek_v32` und `nim_deepseek_v4pro` fielen beim Liveness-Check (30 s Timeout) durch — NIM Free Tier Stabilität für DeepSeek ist heute nicht ausreichend. Im Smoke v3 (30 Min früher) war V3.2 noch erreichbar mit 8 s, im Voll-Run dann tot. **Schlussfolgerung: DeepSeek im NIM Free Tier ist nicht produktionsfähig.**

### Befund 4 — GLM5 grenzwertig auf allen Achsen

- Latenz p95 = 138 s — **massiv über Knock-Out 30 s**. In Production völlig untauglich für ein 90-Min-Cron-Job.
- Genauigkeit: 93,5 % Name (knapp besser als Status Quo!), aber 76,9 % Adresse (schlechter).
- Keine Halluzinationen (0 %, also besser als Qwen).

GLM5 fällt rein wegen Latenz raus, nicht wegen Genauigkeit.

### Befund 5 — Vision-Status-Quo bleibt sicher

`status_quo_vision` (gpt-4o) trifft alle 4 Vision-Eval-Cases perfekt (100 %). Da NIM kein Vision-Modell im Free Tier hat, gibt es ohnehin keinen Wechsel-Pfad.

## Aktionsempfehlung — drei Schritte

1. **Adress-Pipeline-Problem untersuchen**: Vergleich `gpt-4o-mini`-Output ↔ Notion-`Zustell Adresse` ↔ `_clean_adresse()`-Output für 5–10 Fälle. Klären: ist das ein Eval-Artefakt oder ein echtes Pipeline-Problem?
2. **Bei Status Quo bleiben**: Migration auf NIM lohnt sich heute nicht — kein Kandidat ist klar besser, und Halluzinations-Risiko bei Qwen ist real.
3. **Re-Eval in 3-6 Monaten**: NIM-Modell-Landschaft ändert sich (DeepSeek V3.1 EOL nach 2 Wochen). Eval-Set + Harness liegen bereit; Re-Run via `python eval/runner.py` möglich.

## Hauptmatrix — Genauigkeit

| Config | n | JSON | Schema | Name | Adresse | PLZ | Gläubiger F1 | Forderung | Halluz. Name | Halluz. Adr. | Score | Knock-Outs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `status_quo_text` | 32 | 100.0% | 100.0% | 90.3% | 84.6% | 88.0% | 0.81 | 89.5% | 0.0% | 0.0% | **0.883** | 🔴 D5_addr<90% (84.6%) |
| `status_quo_vision` | 4 | 100.0% | 100.0% | 100.0% | 100.0% | 100.0% | 1.00 | 100.0% | 0.0% | 0.0% | **1.000** | ✓ |
| `nim_qwen` | 32 | 100.0% | 100.0% | 90.3% | 76.9% | 88.0% | 0.77 | 78.9% | 3.1% | 9.4% | **0.838** | 🔴 D5_addr<90% (76.9%), D9_halluc_name>3% (3.1%), D10_halluc_addr>3% (9.4%) |
| `nim_glm5` | 32 | 100.0% | 100.0% | 93.5% | 76.9% | 88.0% | 0.73 | 68.4% | 0.0% | 0.0% | **0.853** | 🔴 D5_addr<90% (76.9%) |

## Latenz-Stats (Cold-Start sichtbar)

| Config | n | Median | p95 | Max | über Knock-Out (30s)? |
|---|---|---|---|---|---|
| `status_quo_text` | 32 | 1.9s | 3.1s | 4.8s | ✓ |
| `status_quo_vision` | 4 | 10.4s | 11.4s | 11.4s | ✓ |
| `nim_qwen` | 32 | 2.2s | 4.3s | 14.3s | ✓ |
| `nim_glm5` | 32 | 27.7s | 138.0s | 149.9s | 🔴 ja |

## Geschätzte Kosten für diesen Run

| Config | Kosten ($) für n Calls |
|---|---|
| `status_quo_text` | $0.0000 |
| `status_quo_vision` | $0.0000 |
| `nim_qwen` | $0.0000 |
| `nim_glm5` | $0.0000 |

## Disagreements — Edikte mit Spreizung

| Edikt-ID | Configs (name_exact, addr_exact) |
|---|---|
| `edikt-001` | status_quo_text: name=False/addr=False, nim_qwen: name=False/addr=True, nim_glm5: name=False/addr=False |
| `edikt-012` | status_quo_text: name=False/addr=False, nim_qwen: name=True/addr=False, nim_glm5: name=True/addr=False |
| `edikt-014` | status_quo_text: name=True/addr=None, nim_qwen: name=False/addr=None, nim_glm5: name=False/addr=None |
| `edikt-019` | status_quo_text: name=True/addr=True, nim_qwen: name=True/addr=False, nim_glm5: name=True/addr=True |
| `edikt-024` | status_quo_text: name=True/addr=True, nim_qwen: name=True/addr=False, nim_glm5: name=True/addr=False |
| `edikt-029` | status_quo_text: name=True/addr=False, nim_qwen: name=True/addr=True, nim_glm5: name=True/addr=True |
| `edikt-030` | status_quo_text: name=False/addr=None, nim_qwen: name=False/addr=None, nim_glm5: name=True/addr=None |
| `edikt-032` | status_quo_text: name=True/addr=True, nim_qwen: name=True/addr=False, nim_glm5: name=True/addr=False |

_Inspektion via `eval/runs/<label>/raw/<config_id>/<edikt_id>.json`._

## Methodik-Hinweise

- Ground Truth: aus Notion-Properties (Status-Quo-Output von gpt-4o-mini), noch ohne Fritz-Stichprobe-Korrektur — Disclaimer aus 01-EVAL-CRITERIA.md §6.4 gilt.
- Cold-Start im NIM Free Tier: erste Calls eines Modells sind 30–80 s langsam, Warmup mit Edikt-ähnlichem Prompt vorher; trotzdem im Latenz-Resultat sichtbar.
- Vision (Call B) nur Status-Quo-gemessen — kein NIM-Vision-Modell im Free Tier.
