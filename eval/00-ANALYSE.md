# Phase 0 — Codebase-Analyse für NIM-Evaluation

**Status:** READ-ONLY-Analyse, keine Production-Code-Änderungen.
**Datum:** 2026-05-01
**Scope:** Vorbereitung der Evaluation OpenAI vs. NVIDIA NIM (DeepSeek V3.1, Qwen 3.5 Coder 480B, GLM-5).

---

## 1. Tech-Stack-Snapshot

| Aspekt | Wert |
|---|---|
| Sprache | Python 3 (CPython) |
| Architektur | Single-File `main.py` (~4 970 Zeilen), Hilfs-Skripte für Notion-Cleanup |
| LLM-SDK | `openai>=1.30.0,<2.0.0` (offizieller Python-Client v1) |
| Client-Wrapper | Eigener: `_OpenAI` aus `openai`, optional importiert; Retry-Decorator `_openai_with_retry()` (`main.py:397`) — exponential 10s/30s/60s, retried 429/5xx/Timeout, propagiert 401/400 sofort |
| Andere Integrationen | `notion-client`, `pymupdf` (PDF-Text), `python-docx` (Briefe), `google-api-python-client` (Drive), `urllib` (HTTP, kein `requests`) |
| Eintrittspunkte | `main.py` (Voll-Run + Brief-Run via `BRIEF_ONLY=true`), `cleanup_*.py` und `dedup_tief.py` (manuelle Wartungsskripte, **kein** LLM-Call darin) |
| Deployment | GitHub Actions Cron (siehe `CLAUDE.md`); zwei Jobs: `full-run` (4× werktags), `brief-only` (alle 10 min) |

**Wichtig**: NIM bietet OpenAI-kompatibles `/v1/chat/completions`. Migration = nur `base_url`-Parameter + Modellname tauschen, keine SDK-Umstellung nötig.

---

## 2. LLM-Call-Inventar

Genau **3 OpenAI-Aufrufe** im Production-Code (`main.py`). Alle nutzen `client.chat.completions.create`. Andere `.py`-Dateien rufen kein LLM auf.

### Call A — PDF-Text-Extraktion (Standardfall)

| Feld | Wert |
|---|---|
| Datei:Zeile | `main.py:1127–1191` (`gutachten_extract_info_llm`) |
| Modell | `gpt-4o-mini` |
| Aufgabentyp | **Strukturierte Extraktion** (JSON) aus Freitext |
| Input | Erste 12 000 Zeichen des PDF-Volltexts (PyMuPDF-Output, Plain Text); System-Prompt mit Feld-Definitionen + Disambiguierungsregeln (Verpflichtete vs. Betreibende Partei, Anwälte ≠ Gläubiger, etc.) |
| Output-Schema | `response_format={"type":"json_object"}`, Felder: `eigentümer_name` (string), `eigentümer_adresse` (string), `eigentümer_plz_ort` (string), `gläubiger` (array of strings), `forderung_betrag` (string). Felder können `null` sein. |
| Hyperparameter | `temperature=0`, `max_tokens=400` |
| Frequenz | 1× pro neu importiertem Edikt mit lesbarem Gutachten-PDF (~ pro Edikt einmal, nie wiederholt; geschützt durch Notion-Flag `Gutachten analysiert?`) |
| Geschätzte Frequenz | **~ 80–150 Calls / Tag** in Spitzen (alle neuen Edikte aller 9 Bundesländer); typisch 30–80 / Tag |
| Fallback | Bei API-Fehler oder fehlendem Key → Regex-Parser `gutachten_extract_info()` (deterministisch, deckt nur Grundbuchauszug-Format zuverlässig ab) |

### Call B — Vision-Analyse (gescannte PDFs)

| Feld | Wert |
|---|---|
| Datei:Zeile | `main.py:2929–3030` (`gutachten_extract_info_vision`) |
| Modell | `gpt-4o` (Vision-fähig, **nicht** mini) |
| Aufgabentyp | **Multimodale strukturierte Extraktion** aus Bildern |
| Input | Erste **8 Seiten** des PDFs als JPEG (2,5× Zoom = ~190 DPI), base64-codiert mit `detail="high"` |
| Output-Schema | Identisch zu Call A (gleiche 5 Felder) |
| Hyperparameter | `temperature=0`, `max_tokens=500` |
| Frequenz | 1× pro Edikt mit gescanntem (Text-leerem) PDF, **hard-cap 20 Calls / Run** (`MAX_VISION = 20`, ~0,40 € / Run) — Code-Kommentar `main.py:3126` |
| Geschätzte Frequenz | 5–20 / Tag |

### Call C — Geschlechtserkennung Vornamen

| Feld | Wert |
|---|---|
| Datei:Zeile | `main.py:3584–3619` (`_geschlecht_via_gpt`) |
| Modell | `gpt-4o-mini` |
| Aufgabentyp | **Klassifikation** (3-Klassen: m / f / n) |
| Input | Ein Vorname als String, kurzer Prompt |
| Output | 1 Token: `m`, `f` oder `n` |
| Hyperparameter | `temperature=0`, `max_tokens=1` |
| Frequenz | 1× pro neuem Eigentümer-Vorname (in-memory Cache `_geschlecht_cache`), nur für Brief-Anrede; ~10–30 / Tag |
| Funktion in Pipeline | Reine Komfort-Funktion: bestimmt "Sehr geehrter Herr" vs. "Sehr geehrte Frau". Bei Fehler → Fallback "Sehr geehrte Damen und Herren" (`main.py:3636`). |

### Aggregierter Call-Mix

```
Volumen pro Tag (typisch):
  Call A (gpt-4o-mini, Text-Extraktion):  ~ 50  Calls
  Call B (gpt-4o, Vision-Extraktion):     ~ 10  Calls
  Call C (gpt-4o-mini, Klassifikation):   ~ 15  Calls
  ────────────────────────────────────────────────
  Σ ~ 75 Calls / Tag
```

→ NIM Free Tier liefert ~40 req/min (rate-limit, nicht Tageslimit). Selbst Spitzen passen rein, solange wir nicht parallelisieren. Aktueller Code läuft strikt seriell mit `time.sleep(0.5)` zwischen Vision-Calls.

---

## 3. Tests / Eval-Logik

**Ergebnis: keine vorhandene Test-Infrastruktur.**

- Keine Dateien `test_*.py` / `*_test.py` / `tests/`-Ordner.
- Keine `pytest`/`unittest`-Imports in `requirements.txt` oder im Code.
- Keine Snapshots, keine goldenen Datensätze.
- Manuelle "Tests" via `create_brief_template.py` (nur Template-Generierung, kein LLM).

→ **Eval-Set muss komplett neu gebaut werden.** Es gibt keine vorhandene Ground-Truth-Sammlung.

---

## 4. Logging-Status für LLM-Inputs/Outputs

### Was wird heute gespeichert?

| Quelle | Was | Wo |
|---|---|---|
| **Roh-PDFs (Input)** | Alle Gutachten-PDFs der "🟡 Gelb"-Einträge | Google Drive: `GOOGLE_DRIVE_FOLDER_ID` (`Immo-in-Not Edikte-Downloads`); Funktion `gdrive_sync_gelb_entries` (`main.py:768+`) |
| **Strukturierter LLM-Output** | Notion-Properties: `Verpflichtende Partei`, `Zustell Adresse`, `Zustell PLZ/Ort`, `Betreibende Partei`, `Notizen` (enthält `Forderung:` und `Gutachten-PDF:`-Link) | Notion-DB |
| **Roh-LLM-Output (JSON)** | **Wird nicht persistiert.** Nach `json.loads()` und Feld-Bereinigung verworfen. |
| **stdout-Logs** | Print-Statements wie `[Gutachten] 👤 Eigentümer: …`, `[Vision] 🔭 …` | GitHub Actions Run-Logs (90 Tage Retention) |

### Verfügbare echte Beispiele (letzte 30 Tage)

- **Drive-Ordner**: vermutlich 100–200+ PDFs (alle "Gelb"-Einträge der letzten Wochen). Genau: muss Fritz im Drive nachzählen oder via Service-Account-Skript zählen.
- **Notion**: alle Pages mit `Gutachten analysiert? = true` und nicht-leerem `Verpflichtende Partei` → das sind die "Status-Quo-Outputs" (gpt-4o-mini- bzw. gpt-4o-Output). Diese sind **nicht** Ground Truth, sondern OpenAI-Output, der in einigen Fällen falsch ist (siehe Memory: Eigentümer doppelt, falsche Gläubiger, etc.).

### Vorgehensweise zur Ground-Truth-Sammlung (Vorschlag für Phase 1)

1. **Stratifiziertes Sampling**: aus Drive 30–50 PDFs ziehen, gleichmäßig verteilt:
   - 70 % Text-PDFs (Call-A-Test-Set)
   - 20 % gescannte PDFs (Call-B-Test-Set)
   - 10 % Edge Cases: Mehrere Eigentümer, ausländische Adressen, sehr lange/kurze Dokumente
2. **Initiale Ground Truth = Status-Quo-Notion-Output**, dann **manuelle Korrektur durch Fritz** (PDF auf, Notion vergleichen, falsche Felder fixen). Erfahrungsgemäß sind 20–40 % manuell korrekturbedürftig.
3. **Persistierung als JSONL** in `eval/data/eval-set.jsonl` mit Schema: `{ "id": ..., "pdf_path": ..., "modality": "text|vision", "ground_truth": { eigentümer_name, eigentümer_adresse, eigentümer_plz_ort, gläubiger[], forderung_betrag } }`.
4. **Vornamen-Set für Call C** separat: 50 Vornamen mit bekanntem Geschlecht (10 m, 10 f, 10 ambiguous, 20 ausländisch) — synthetisch von Fritz oder aus Wikipedia herleitbar.

---

## 5. Datenfluss + Drittland-Status

### Datenfluss (Production heute)

```
edikte.justiz.gv.at (AT, EU)
  └─→ Python-Scraper auf GitHub Actions (US-Runner)
       ├─→ Notion-DB (US, AWS us-west-2)
       ├─→ Google Drive (US/EU Mix, Standort vom Service-Account-Project abhängig)
       ├─→ OpenAI API (US)              ← LLM-Calls A, B, C
       ├─→ SendGrid (US, E-Mail-Versand)
       └─→ Telegram (Cloudflare-Anycast, global)
```

**Konsequenz**: Edikt-PDFs (mit Eigentümer-Name + Zustelladresse, also personenbezogenen Daten) verlassen heute schon die EU und werden in den USA verarbeitet. NVIDIA NIM (US) ist **kein neuer Sprung in ein Drittland** — die Daten fließen ohnehin schon in US-Cloud-Dienste.

**Rechtsgrundlage** (laut Auftrag): Edikte sind aktiv von der österreichischen Justiz öffentlich gemacht (`edikte.justiz.gv.at`), Verarbeitung in US-Diensten ist hier vertretbar. **TODO Phase 5**: einzeilige Notiz in DSGVO-Doku ergänzen, dass NVIDIA als zusätzlicher Auftragsverarbeiter geführt wird.

### NIM-Spezifika

- Endpunkt: `https://integrate.api.nvidia.com/v1` (OpenAI-kompatibel)
- Free Tier: ~40 req/min, kein Tageslimit, kein Ablaufdatum
- Datennutzung NVIDIA: laut Free-Tier-AGB **werden Inputs zur Service-Verbesserung verwendet** (anders als OpenAI Enterprise, wo das opt-out ist). Das ist für *öffentliche Edikt-Daten* unbedenklich, wäre für nicht-öffentliche Kundendaten ein klares K.O.

---

## 6. Production-Risiko-Bewertung

### Kritischer Pfad

```
PDF-Download → LLM-Extraktion (Call A oder B) → Notion-Update → DOCX-Brief-Generierung → SendGrid an Betreuer
```

### Was bricht bei fehlerhaftem Modell-Output?

| Fehler | Auswirkung | Schweregrad |
|---|---|---|
| **Falscher `eigentümer_name`** | Brief mit falscher Anrede an falsche Person → erreicht möglicherweise gar niemanden | **🔴 HOCH**: Reputationsrisiko bei Immo-in-Not, mögliche Persönlichkeitsrechtsverletzung |
| **Falsche `eigentümer_adresse` / `plz_ort`** | Brief unzustellbar oder geht an Dritte | **🔴 HOCH**: Datenschutz + verlorenes Akquise-Lead |
| **Falsche `gläubiger`** | Erscheint nur in `Betreibende Partei`-Feld in Notion (interne Info) | 🟡 MITTEL: irreführende Lead-Bewertung, kein Außenwirkungs-Schaden |
| **Halluzinierte `forderung_betrag`** | Erscheint nur in `Notizen`-Feld | 🟢 NIEDRIG |
| **Ungültiges JSON / API-Fehler** | `try/except` greift, Eintrag bleibt ohne Eigentümer-Daten in Notion, kein Brief erstellt | 🟢 NIEDRIG (graceful degradation) |
| **Falsche Geschlechtserkennung (Call C)** | "Sehr geehrter Herr Maier" statt "Sehr geehrte Frau Maier"; Fallback "Damen und Herren" bei null | 🟡 MITTEL: kosmetisch, aber bei falschem Treffer peinlicher als bei Fallback |

### Schutzmechanismen heute

- **`GESCHUETZT_PHASEN`**-Frozenset: manuell editierte Notion-Einträge werden nicht überschrieben (siehe `CLAUDE.md`).
- **`Gutachten analysiert?`-Checkbox**: verhindert wiederholte Calls für dasselbe Edikt.
- **Regex-Fallback**: bei API-Fehler greift `gutachten_extract_info()` (deterministisch, aber deckt nur Grundbuchauszug-Format gut ab).
- **In-Memory-Cache** für Geschlechtserkennung.
- **Hard-Cap 20** Vision-Calls / Run.

### Konsequenz für Eval-Knockout-Kriterien (Vorschlag für Phase 1)

Die **Genauigkeit von `eigentümer_name` und `eigentümer_adresse`** ist das wichtigste Kriterium — beide gehen direkt in den verschickten Brief. JSON-Validität ist zweitwichtigstes Knock-Out (sonst greift Regex-Fallback und der ist schwächer als jedes der getesteten Modelle).

---

## 7. Kandidat-Modelle auf NIM (Stand 2026-05-01)

| Modell | NIM-Slug | Stärken (laut Hersteller) | Erwartete Eignung |
|---|---|---|---|
| **DeepSeek V3.1** | `deepseek-ai/deepseek-v3.1` (zu verifizieren) | Stark in strukturierter Extraktion, JSON-Mode | Hoher Kandidat für Call A |
| **Qwen 3.5 Coder 480B** | `qwen/qwen3-coder-480b-a35b` | Code & strukturierte Outputs | Hoher Kandidat für Call A |
| **GLM-5** | (Slug zu verifizieren) | Generalist | Backup-Kandidat |
| **Status Quo gpt-4o-mini** | OpenAI direkt | Baseline | Bleibt als Vergleich |
| **Status Quo gpt-4o (Vision)** | OpenAI direkt | Baseline für Call B | Bleibt — **kein** der Kandidaten ist explizit vision-fähig auf NIM-Free; Call B könnte beim Status Quo bleiben |

⚠️ **Offene Frage für Phase 3**: NIM-Free unterstützt vermutlich **kein Vision-Modell** in der Free-Liste — Call B bleibt evtl. zwingend bei OpenAI. Wird in Phase 3 beim API-Smoke-Test verifiziert.

---

## 8. Was als Nächstes passiert

→ Du bist am Zug, Fritz. Bitte **CHECKPOINT 0** durchgehen:

1. Stimmt das LLM-Call-Inventar (3 Calls, davon einer Vision)?
2. Stimmt deine Einschätzung des Risikos?
3. Welcher Drive-Ordner enthält die Eval-Kandidaten-PDFs? Pfadname / Folder-ID?
4. Sollen wir die Vornamen-Klassifikation (Call C) überhaupt mit-evaluieren oder rauslassen, weil sie unkritisch ist?
5. Bist du bereit, 30–50 PDFs durchzugehen und eine Ground Truth zu erstellen — oder sollen wir den Status-Quo-Output nehmen und in einer Stichprobe (z.B. 10 PDFs) die Korrektur-Quote schätzen?

Sobald du freigibst → ich starte Phase 1 (Erfolgskriterien festschreiben).
