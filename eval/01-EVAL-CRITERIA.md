# Phase 1 — Erfolgskriterien für Modell-Wechsel

**Status:** Entwurf, wartet auf Freigabe (CHECKPOINT 1).
**Datum:** 2026-05-01
**Geltungsbereich:** Eval von OpenAI-Status-Quo gegen NIM-Kandidaten (DeepSeek V3.1, Qwen 3.5 Coder 480B, GLM-5).
**Wichtig:** Nach Freigabe sind die Schwellen **gefroren**. Neue Dimensionen dürfen ergänzt werden, aber bestehende Werte bleiben fix.

---

## 1. Scope der Eval

| Im Scope | NICHT im Scope |
|---|---|
| Call A (PDF-Text-Extraktion, `gpt-4o-mini`) | Call C (Geschlechtserkennung) — wird automatisch mitmigriert |
| Call B (Vision, `gpt-4o`) | Robustheit unter Network-Errors — bestehender Retry-Wrapper bleibt |
| | Latenz-Optimierung — nicht Hauptziel |

---

## 2. Bewertungs-Dimensionen

Jeder LLM-Output wird gegen die Ground Truth nach folgenden Dimensionen bewertet. Pro Dimension Beispiel-Berechnung am Ende dieses Dokuments.

### Deterministisch messbar

| ID | Dimension | Beschreibung | Berechnung |
|---|---|---|---|
| **D1** | **JSON-Validität** | Ist der Output ein valides JSON-Objekt mit allen 5 Feldern? | `valid_json_count / total_calls` |
| **D2** | **Schema-Konformität** | Sind alle Feldtypen korrekt (`gläubiger` ist Array, andere Strings/null)? | `schema_compliant / valid_json` |
| **D3** | **Eigentümer-Name-Genauigkeit** | Stimmt `eigentümer_name` exakt mit Ground Truth überein (nach Normalisierung: Whitespace, Case, Reihenfolge bei Mehrfach-Eigentümern)? | `exact_match_count / total` |
| **D4** | **Eigentümer-Name-Recall** | Ist der erwartete Name im Output (auch bei zusätzlichem Ballast wie Geburtsdatum)? | Substring-Match nach Normalisierung |
| **D5** | **Adress-Genauigkeit** | Stimmt `eigentümer_adresse` (Straße + Hausnummer) exakt? | Levenshtein ≤ 2 nach Normalisierung |
| **D6** | **PLZ/Ort-Genauigkeit** | Stimmt `eigentümer_plz_ort` exakt? | Exact Match nach Normalisierung |
| **D7** | **Gläubiger-F1** | Ist die Gläubiger-Liste korrekt? (Precision + Recall pro Eintrag, exakt-Match) | F1-Score über Set-Vergleich |
| **D8** | **Forderungsbetrag-Genauigkeit** | Stimmt der Betrag (numerischer Wert nach Parse)? | Exact Match nach Zahl-Extraktion |

### Halluzinations- / Vorsichts-Dimensionen

| ID | Dimension | Beschreibung |
|---|---|---|
| **D9** | **Halluzination-Rate (Name)** | Wie oft wird ein Name erfunden, der nicht im PDF steht? Ground Truth `null` → Output `not null` = Halluzination |
| **D10** | **Halluzination-Rate (Adresse)** | Wie oft wird eine Adresse erfunden? GT `null`/leer → Output gefüllt = Halluzination |
| **D11** | **Falsch-Positive Gläubiger** | Anzahl im Output gelisteter, in der GT NICHT vorhandener Gläubiger (oft Anwälte oder Hausverwaltungen, obwohl Prompt das ausschließt) |

### Operativ

| ID | Dimension | Beschreibung |
|---|---|---|
| **D12** | **Latenz p50 / p95** | Wallclock-Zeit von Request bis Response (Sekunden) |
| **D13** | **Cost / Call** | API-Kosten pro Call (€) |

---

## 3. Schwellenwerte (3-stufig)

Schwellen reflektieren Frage-2-Antwort: **gehärtet** — Adress- oder Namens-Fehler sind katastrophal, daher hohe Knock-Outs.

### Knock-Out (Modell ist sofort RAUS)

Verletzt ein Kandidat *eine einzige* dieser Schwellen → wird nicht weiter betrachtet, egal wie gut der Rest ist.

| Dimension | Knock-Out-Schwelle | Begründung |
|---|---|---|
| **D1** JSON-Validität | < **99 %** | Bei < 99 % JSON wird `try/except` zu oft greifen → Regex-Fallback zu oft → schlechtere Pipeline-Qualität |
| **D3** Name-Exact-Match | < **85 %** | Gehärtete Sicht (Frage 2). Auch der Status Quo dürfte hier nicht 100 % treffen (siehe Phase 0 Memory: Eigentümer-Doppel-Bug). Wenn Status Quo unter 85 % liegt, deckt die Eval ein bestehendes Pipeline-Problem auf. |
| **D5** Adresse-Genauigkeit | < **90 %** | Brief geht an die Adresse — gehärtete Sicht (Frage 2). 90 % ist die strengere von zwei diskutierten Schwellen. |
| **D9** Halluzination-Name | > **3 %** | Mehr als 3 % halluzinierte Namen = systematisches Risiko, Briefe an Phantasie-Personen. |
| **D10** Halluzination-Adresse | > **3 %** | Same, für Adresse. |
| **D12** Latenz p95 | > **30 s** | Würde GitHub-Actions-Run blockieren (Timeout-Risiko bei vielen Edikten). |

### Mindest-Anforderung für Wechsel

Ein Kandidat muss **alle Knock-Outs überleben UND in jeder dieser Dimensionen den Status Quo nicht mehr als marginal unterschreiten**:

| Dimension | Mindest gegenüber Status Quo |
|---|---|
| D1 JSON-Validität | ≥ Status-Quo − 0,5 pp |
| D3 Name-Exact-Match | ≥ Status-Quo − 1 pp |
| D5 Adresse-Genauigkeit | ≥ Status-Quo − 1 pp |
| D6 PLZ/Ort-Genauigkeit | ≥ Status-Quo − 2 pp |
| D7 Gläubiger-F1 | ≥ Status-Quo − 5 pp |
| D9, D10 Halluzinationsraten | ≤ Status-Quo + 1 pp |
| D11 Falsch-Positive Gläubiger | ≤ Status-Quo + 0,5 / Call (Mittel) |

### Wunsch-Niveau (klarer Sieg)

Wenn ein Kandidat all das **plus** mindestens *eines* davon erfüllt, ist es ein klarer Wechsel-Trigger:

- D3 Name-Exact-Match: ≥ Status-Quo + 3 pp
- D5 Adresse-Genauigkeit: ≥ Status-Quo + 3 pp
- D7 Gläubiger-F1: ≥ Status-Quo + 5 pp
- D9 + D10 Halluzinationsraten: BEIDE ≤ Status-Quo − 1 pp
- D13 Kosten: ≥ 50 % Einsparung (NIM Free = 100 % Einsparung, also automatisch erfüllt)

---

## 4. Gewichtung der Dimensionen

Da Briefe = Außenwirkung, **Genauigkeit dominiert** Latenz und Kosten:

```
Gesamt-Score = 0.35 · D3-Name
             + 0.30 · D5-Adresse
             + 0.10 · D6-PLZ
             + 0.10 · D7-Gläubiger
             + 0.05 · D8-Forderung
             + 0.10 · (1 − D9 − D10)        // Halluzinations-Penalty
```

JSON-Validität (D1) und Schema (D2) sind **Gates**, kein Gewichts-Faktor — wer sie nicht besteht, ist raus (Knock-Out).
Latenz (D12) und Kosten (D13) sind **Tiebreaker**: bei mehreren überlebenden Kandidaten gewinnt der schnellere/billigere.

---

## 5. Migrations-Triggerregel (in einem Satz)

> **Wechsel auf Kandidat X**, wenn X **keine** Knock-Out-Verletzung in D1, D3, D5, D9, D10, D12 zeigt **UND** alle Mindest-Anforderungen gegenüber Status Quo erfüllt **UND** mindestens **eine** Wunsch-Niveau-Bedingung (D3/D5/D7-Sieg oder Halluzinations-Verbesserung) übertrifft. Bei mehreren überlebenden Kandidaten gewinnt der mit dem höchsten gewichteten Gesamt-Score (Tiebreaker: Latenz p50, dann Kostengünstigkeit).

Wenn **kein** Kandidat dieses Bündel erfüllt → **bei Status Quo bleiben**, Eval-Artefakte bleiben für späteren Re-Test im Repo.

---

## 6. Eval-Set-Spezifikation

### Größe und Stratifikation

- **Zielgröße: 40 PDFs** (innerhalb 30–50-Range, gut für statistische Aussagen ohne Übermaß).
- **Aufteilung**:
  - 28 Text-PDFs (Call-A-Test) = 70 %
  - 8 gescannte PDFs (Call-B-Test) = 20 %
  - 4 Edge Cases = 10 % (Mehrfach-Eigentümer, ausländische Adressen, sehr lange Dokumente)

### Quelle

- **Google Drive**: Folder `Immo-in-Not Edikte-Downloads` (Folder-ID liefert Fritz in Phase 3).
- **Sampling-Methode**: zufälliger Stratified Sample mit fixem Random-Seed = `42` für Reproduzierbarkeit.
- **Speicherort**: `eval/data/pdfs/` (gitignored — siehe Phase 3 für `.gitignore`-Update).
- **Eval-Set-Index**: `eval/data/eval-set.jsonl` mit Schema:
  ```json
  {
    "id": "edikt-001",
    "pdf_filename": "Mustermann_Hauptstrasse_5.pdf",
    "modality": "text",  // oder "vision" oder "edge_case"
    "ground_truth": {
      "eigentümer_name": "...",
      "eigentümer_adresse": "...",
      "eigentümer_plz_ort": "...",
      "gläubiger": ["..."],
      "forderung_betrag": "..."
    },
    "ground_truth_source": "status_quo_uncorrected" | "manual_correction"
  }
  ```

### Ground-Truth-Erstellung (Frage-5-Antwort)

1. **Initial**: Notion-Status-Quo-Output für alle 40 PDFs als Ground Truth übernehmen (Feld `ground_truth_source = "status_quo_uncorrected"`).
2. **Stichprobenkorrektur durch Fritz**: 10 zufällig gewählte PDFs (= 25 % der Menge) öffnet Fritz, vergleicht mit Notion und korrigiert manuell. Diese 10 bekommen `ground_truth_source = "manual_correction"`.
3. **Fehlerrate ermitteln**: aus den 10 korrigierten extrapolieren wir, wie hoch der GT-Fehler in der Gesamtmenge ist.
4. **Disclaimer** im Eval-Report: "Status-Quo-Genauigkeit ist relativ zur GT mit erwarteter GT-Fehlerrate von ~X %."
5. **Korrekturschwelle**: Wenn die 10er-Stichprobe **> 30 % Fehlerrate** zeigt, eskalieren wir und gehen alle 40 manuell durch — sonst ist die Status-Quo-GT zu unzuverlässig.

---

## 7. Beispiel-Berechnung pro Dimension

### Beispiel D3 (Name-Exact-Match)

```
Ground Truth:  "Maria Mustermann"
Output A:      "Maria Mustermann"          → match (1)
Output B:      "Maria Mustermann, geb. 1.1.1980" → no exact (0), aber D4 = match
Output C:      "Marie Mustermann"          → no match (0)
Output D:      "Mustermann Maria"          → match nach Normalisierung (1)
```

Normalisierung: lowercase, Whitespace-Collapse, alphabetische Wort-Sortierung bei Mehrfach-Tokens, Geburts-Datum-Stripping.

### Beispiel D7 (Gläubiger-F1)

```
GT:     {"Erste Bank", "Raiffeisen NÖ"}
Output: {"Erste Bank", "Dr. Müller (RA)"}

Precision = 1/2 = 0.50
Recall    = 1/2 = 0.50
F1        = 0.50
```

`Dr. Müller (RA)` ist Anwalt → Falsch-Positiv (D11).

### Beispiel D9 (Halluzination Name)

```
GT:     null  (PDF war zu unklar, GT-Ersteller konnte keinen Namen finden)
Output: "Hans Müller"  → halluziniert
```

Halluzinations-Rate = (Anzahl GT-null mit Output-non-null) / (Anzahl GT-null).

---

## 8. LLM-as-Judge — wo?

**Nicht für die Hauptdimensionen.** D1–D11 sind alle deterministisch berechenbar, kein Judge nötig. Vorteil: keine Judge-Modell-Bias, voll reproduzierbar.

**Optional für Disagreement-Sichtung** (Phase 4): Bei den 5–10 Fällen, wo Modelle drastisch unterschiedliche Outputs lieferten, kann ein Judge (Claude Sonnet 4.6 oder GPT-4o) eine kurze Beurteilung "Welches Modell ist näher an der Wahrheit?" geben. Bidirektionale Side-by-Side-Bewertung gegen Position-Bias.

---

## 9. Reproduzierbarkeit

- `temperature=0` für alle Kandidaten (analog zum Status Quo).
- Random-Seed `42` für Sampling.
- Fester Eval-Set-JSONL mit Hash → bei späterem Re-Run identisch.
- Run-Logs in `eval/runs/<timestamp>/` mit Modell-Version, Token-Counts, Roh-Outputs.

---

## 10. Freigabe-Status (CHECKPOINT 1)

Mit Fritz interaktiv durchgegangen am 2026-05-01:

| Punkt | Entscheidung |
|---|---|
| Knock-Out-Schwellen Name/Adresse | **Verschärft** auf 85 % / 90 % (vorher 80 % / 85 %). Wenn Status Quo darunter fällt, decken wir ein bestehendes Pipeline-Problem auf. |
| Gewichtung | Name 35 % + Adresse 30 % bleibt (Name dominiert leicht, da ohne Name keine Anrede). |
| Latenz (D12) | Bleibt Tiebreaker, kein Score-Gewicht. GitHub-Actions-Runtime kostet keine echten Euros. |
| GT-Korrekturschwelle | 30 % Status-Quo-Fehler in der 10er-Stichprobe → alle 40 manuell. |
| D8 Forderungsbetrag | Wird gemessen mit 5 % Gewicht (geringes Risiko, billige Berechnung). |

**Datei-Status:** Mit dieser Freigabe **eingefroren**. Schwellen bleiben unverändert. Neue Dimensionen dürfen ergänzt werden, falls in Phase 4 ein Aspekt offensichtlich fehlt — bestehende Werte bleiben fix.
