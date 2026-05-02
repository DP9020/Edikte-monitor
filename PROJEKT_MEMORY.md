# PROJEKT-MEMORY: Edikte-Monitor
*Letzte Aktualisierung: 2026-03-11*

---

## 1. Projektname

**Edikte-Monitor** (intern: Immo-in-Not Edikte-Automation)

---

## 2. Projektziel

Automatische Überwachung österreichischer Zwangsversteigerungen auf [edikte.justiz.gv.at](https://edikte.justiz.gv.at), Filterung relevanter Immobilienobjekte, Anreicherung mit Eigentümer-/Gläubigerdaten aus Gerichtsgutachten (PDF), Verwaltung in Notion und automatischer Versand von personalisierten Anschreiben (DOCX per E-Mail + Telegram) an die zuständigen Betreuer.

**Geschäftskontext:** Immo-in-Not GmbH kauft Immobilien von Eigentümern in finanzieller Notlage vor der Zwangsversteigerung. Das Tool identifiziert und bearbeitet täglich neue Versteigerungstermine für ganz Österreich.

---

## 3. Aktueller Stand des Projekts

- ✅ Scraper läuft produktiv für alle 9 Bundesländer
- ✅ Notion-Datenbank wird vollautomatisch befüllt
- ✅ Gutachten-PDF-Analyse (Text-PDFs via PyMuPDF + LLM, gescannte PDFs via GPT-4o Vision)
- ✅ Automatische Brieferstellung (DOCX-Template) + E-Mail via SendGrid
- ✅ Telegram-Benachrichtigungen: Haupt-Chat (alle BL) + Benjamin (Wien/OÖ) + Christopher (NÖ/Burgenland)
- ✅ GitHub Actions Workflow mit zwei Jobs (full-run + brief-only) korrekt konfiguriert
- ⏳ Geplant aber noch nicht umgesetzt: Automatischer Download aller Edikt-Anhänge nach Google Drive

---

## 4. Architektur / Systemdesign

```
edikte.justiz.gv.at
        │  HTTP-Scraping (urllib, kein Browser)
        ▼
   main.py (Python 3.11)
        │
        ├── Scraping ──► Notion DB (notion-client)
        │                    │
        ├── PDF-Analyse ◄─── │ (PyMuPDF + OpenAI gpt-4o-mini / gpt-4o Vision)
        │                    │
        ├── Brief-Erstellung ► DOCX (python-docx) ► SendGrid E-Mail
        │                                         ► Telegram Dokument
        │
        └── Telegram-Notifications ► Haupt-Chat (alle BL)
                                   ► Benjamin    (Wien + OÖ)
                                   ► Christopher (NÖ + Burgenland)

GitHub Actions:
  full-run    → Mo-Fr 07:30 / 09:00 / 12:00 / 15:00 UTC  (BRIEF_ONLY=false)
  brief-only  → alle 10 Minuten                           (BRIEF_ONLY=true)
```

---

## 5. Zentrale Komponenten und Module

| Komponente | Beschreibung |
|---|---|
| **Scraper** | `fetch_results_for_state()` + `fetch_detail()` – scrapt edikte.at per HTTP |
| **Notion-Integration** | `notion_create_eintrag()`, `notion_load_all_ids()`, `notion_load_all_pages()`, `notion_status_sync()` |
| **Gutachten-Analyse** | `gutachten_extract_info()` (Text-PDF), `gutachten_extract_info_vision()` (Scan-PDF), `gutachten_extract_info_llm()` (LLM-Fallback) |
| **Brief-Workflow** | `notion_brief_erstellen()`, `_brief_fill_template()`, `_brief_anrede()`, `_brief_send_email()` |
| **Telegram** | `send_telegram()`, `send_telegram_document()`, `_telegram_send_raw()`, `_send_filtered()` |
| **Qualitätssicherung** | `notion_qualitaetscheck()`, `notion_reset_falsche_verpflichtende()`, `notion_archiviere_tote_urls()` |

---

## 6. Wichtige Dateien, Skripte und Funktionen

| Datei | Zweck |
|---|---|
| `main.py` | Gesamtes System (~3786 Zeilen) |
| `brief_vorlage.docx` | DOCX-Template mit Platzhaltern (ANREDE, NAME, ADRESSE, DATUM, KONTAKT_NAME, etc.) |
| `.github/workflows/run.yml` | GitHub Actions Workflow (2 Jobs) |
| `requirements.txt` | `notion-client`, `pymupdf`, `openai`, `python-docx`, `reportlab` |

**Kritische Funktionen:**

| Funktion | Zeile | Beschreibung |
|---|---|---|
| `main()` | ~3449 | Haupt-Orchestrierung, async |
| `fetch_results_for_state()` | ~3369 | Scraping pro Bundesland |
| `notion_create_eintrag()` | ~1522 | Neuen Eintrag in Notion anlegen |
| `notion_status_sync()` | ~2129 | Status/Relevanz → Workflow-Phase synchronisieren, gibt `int` zurück |
| `notion_brief_erstellen()` | ~3100 | Briefe erstellen + E-Mail + Telegram |
| `gutachten_enrich_notion_page()` | ~1235 | PDF herunterladen + analysieren + Notion befüllen |
| `gutachten_extract_info()` | ~834 | Text-PDF analysieren (Grundbuch-Parser + LLM-Fallback) |
| `gutachten_extract_info_vision()` | ~2340 | Gescannte PDFs via GPT-4o Vision |
| `_brief_anrede()` | ~2941 | Geschlechtsspezifische Anrede via GPT-4o-mini |
| `_send_filtered()` | ~3707 | Gefilterte Telegram-Nachricht direkt an Betreuer |
| `notion_archiviere_tote_urls()` | ~2637 | HTTP-404 Einträge archivieren |

---

## 7. GitHub Repository und wichtige Links

| | |
|---|---|
| **Repository** | https://github.com/DP9020/Edikte-monitor |
| **Hauptbranch** | `main` |
| **Edikte-Quelle** | https://edikte.justiz.gv.at |
| **GitHub Actions** | https://github.com/DP9020/Edikte-monitor/actions |
| **Secrets verwalten** | https://github.com/DP9020/Edikte-monitor/settings/secrets/actions |

---

## 8. Wichtige Agent-Logik und AI-Workflows

### Gutachten-Analyse Pipeline
1. PDF herunterladen → PyMuPDF Text extrahieren
2. Wenn Grundbuch-Format (Abschnitt B/C erkennbar) → `_gb_parse_owner()` + `_gb_parse_creditors()`
3. Sonst → Suche nach "Verpflichtete Partei"-Block im Text
4. Bei unvollständigem Ergebnis → `gutachten_extract_info_llm()` (GPT-4o-mini, ~0.002 €/Aufruf)
5. Bei gescanntem PDF (kein Text extrahierbar) → `gutachten_extract_info_vision()` (GPT-4o Vision)

### Brief-Anrede-Logik (`_brief_anrede()`)
1. Firma-Keywords (GmbH, AG, etc.) → „Sehr geehrte Damen und Herren,"
2. Mehrere Personen (`und`, `&`, `/`, `|`) → „Sehr geehrte Damen und Herren,"
3. Explizites „Herr"/„Hr." → „Sehr geehrter Herr [Nachname],"
4. Explizites „Frau"/„Fr." → „Sehr geehrte Frau [Nachname],"
5. Vorname via GPT-4o-mini → männlich/weiblich → entsprechende Anrede
6. Fallback → „Sehr geehrte Damen und Herren,"

### Workflow-Phasen in Notion
```
🆕 Neu eingelangt
  → 🔎 In Prüfung          (manuell oder Status 🟡 Gelb)
  → ✅ Relevant – Brief vorbereiten   (Für uns relevant? = Ja)
  → 📩 Brief versendet
  → ✅ Gekauft
  → ❌ Nicht relevant       (Für uns relevant? = Nein oder Status 🔴 Rot)
  → 🗄 Archiviert
```

### BRIEF_ONLY-Modus
- `BRIEF_ONLY=true`: nur `notion_status_sync()` + `notion_brief_erstellen()` → läuft in ~90 Sekunden
- `BRIEF_ONLY=false`: vollständiger Run mit Scraping, PDF-Analyse, allen Schritten → läuft ~10 Minuten

---

## 9. Wichtige Entscheidungen und deren Begründung

| Entscheidung | Begründung |
|---|---|
| Python urllib statt requests | Keine externe HTTP-Abhängigkeit nötig |
| Notion als Haupt-Datenbank | Team arbeitet bereits damit, kein Extra-Tool |
| Zwei GitHub Actions Jobs statt einem | Ternary-Ausdruck für BRIEF_ONLY war fehlerhaft (immer `true`), separate Jobs sind zuverlässig |
| `_telegram_send_raw()` direkt für Betreuer | `send_telegram(..., extra_chat_ids=[...])` lieferte HTTP 400 für Betreuer-Chat-IDs |
| GPT-4o Vision für gescannte PDFs | PyMuPDF kann keine Scans lesen; Vision-API einzige zuverlässige Option |
| `GESCHUETZT_PHASEN` als globales frozenset | War zuvor in 5 Funktionen separat definiert → Inkonsistenz-Risiko |
| Noch kein Google Drive | Feature für Anhang-Downloads geplant, aber noch nicht implementiert |
| Kein Selenium/Playwright | edikte.at liefert alle Daten per einfachem HTTP → kein Browser nötig |

---

## 10. Bekannte Probleme, Bugs, Risiken und Workarounds

| Status | Problem | Lösung / Workaround |
|---|---|---|
| ✅ Behoben | `_get_benjamin_chat_id()` war nicht definiert → NameError | Implementiert, liest `TELEGRAM_CHAT_ID_BENJAMIN` aus Env |
| ✅ Behoben | BRIEF_ONLY immer `true` (GitHub Actions Ternary-Bug) | Zwei separate Jobs im Workflow |
| ✅ Behoben | HTTP 400 beim Telegram-Senden an Betreuer via `extra_chat_ids` | Direkter `_telegram_send_raw()`-Aufruf |
| ✅ Behoben | `notion_brief_erstellen()` las `"Name"` statt `"Liegenschaftsadresse"` | Feldname korrigiert |
| ✅ Behoben | `GESCHUETZT_PHASEN` in 5 Funktionen dupliziert | Globales frozenset |
| ⚠️ Offen | `notion_load_all_ids()` nutzt `notion.search()` statt `data_sources.query()` | Bei sehr großen DBs ggf. unzuverlässig |
| ⚠️ Offen | Notion-Dateilimit 5 MB → Anhänge nicht in Notion speicherbar | Geplant: Google Drive |
| ℹ️ Info | Benjamin/Christopher bekommen nur Nachrichten bei **neu importierten** Objekten | Gewollt so – nur echte Neuzugänge |
| ⚠️ Risiko | Workflow-Datei nicht per Code pushbar (fehlende Bot-Permission) | Muss manuell auf GitHub bearbeitet werden |

---

## 11. Offene Aufgaben / Nächste Schritte

### Priorität 1 – Google Drive Integration
- **Trigger:** Checkbox „📥 Unterlagen herunterladen" in Notion
- **Aktion:** Alle Anhänge (PDF, Fotos, Grundrisse, Lagepläne) von der Edikt-Seite herunterladen
- **Ablage:** `Versteigerungen / Bundesland / YYYY-MM-DD – Aktenzeichen /`
- **Notion-Update:** Drive-Ordner-Link im Eintrag hinterlegen
- **Telegram:** Bestätigung nach erfolgreichem Download
- **Benötigt:** Google Service-Account → JSON-Key als GitHub Secret `GOOGLE_SERVICE_ACCOUNT_JSON`
- **Library:** `google-api-python-client` + `google-auth`

### Priorität 2 – Technische Verbesserungen
- `notion_load_all_ids()` auf `data_sources.query()` umstellen (aktuell: `notion.search()`)
- EXCLUDE_KEYWORDS reviewen (z.B. „lager" könnte Lagerflächen in Wohngebäuden falsch filtern)

---

## 12. Wichtige Annahmen und technische Rahmenbedingungen

### GitHub Secrets (alle erforderlich)

| Secret | Inhalt |
|---|---|
| `NOTION_TOKEN` | Notion Integration Token |
| `NOTION_DATABASE_ID` | ID der Notion-Datenbank |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot Token |
| `TELEGRAM_CHAT_ID` | Haupt-Chat-ID (Owner/du) |
| `TELEGRAM_CHAT_ID_BENJAMIN` | `8462725282` (Benjamin Pippan) |
| `TELEGRAM_CHAT_ID_CHRISTOPHER` | `8500953016` (Christopher Dovjak) |
| `OPENAI_API_KEY` | GPT-4o-mini + GPT-4o Vision |
| `SENDGRID_API_KEY` | E-Mail-Versand via SendGrid |
| `SMTP_USER` | Absender-E-Mail (muss in SendGrid verifiziert sein) |

### Notion-Datenbankfelder (kritische Felder)

| Feldname | Typ | Beschreibung |
|---|---|---|
| `Liegenschaftsadresse` | Title | Adresse der Immobilie (Primärtitel) |
| `Bundesland` | Select | Bundesland |
| `Status` | Select | 🔴 Rot / 🟡 Gelb / 🟢 Grün |
| `Für uns relevant?` | Select | Ja / Nein / Beobachten |
| `Workflow-Phase` | Select | Aktuelle Phase im Prozess |
| `Verpflichtende Partei` | Rich Text | Eigentümer (aus PDF extrahiert) |
| `Zustell Adresse` | Rich Text | Wohnadresse Eigentümer (für Briefversand) |
| `Betreibende Partei` | Rich Text | Gläubiger (aus PDF extrahiert) |
| `Link` | URL | URL zur Edikt-Detailseite |
| `Archiviert` | Checkbox | Archiviert ja/nein |
| `Brief erstellt am` | Date | Datum der Brieferstellung |
| `Gutachten analysiert?` | Checkbox | PDF wurde analysiert |
| `Liegenschafts PLZ` | Rich Text | PLZ der Immobilie |

### Betreuer-Zuordnung

| Betreuer | Bundesländer | E-Mail | Telegram-Chat-ID |
|---|---|---|---|
| Benjamin Pippan | Wien, Oberösterreich | office@benana.at | 8462725282 |
| Christopher Dovjak | Niederösterreich, Burgenland | christopher.dovjak@dp-im.at | 8500953016 |
| Friedrich Prause | Steiermark, Kärnten, Salzburg, Tirol, Vorarlberg | friedrich.prause@dp-im.at | – |

---

## 13. Was ein neues AI-System sofort wissen muss

1. **Alles ist in einer Datei:** `main.py` (~3786 Zeilen) – kein Modul-System
2. **Workflow-Datei nur manuell editierbar:** `.github/workflows/run.yml` kann nicht per Code gepusht werden – der Bot hat keine `workflows`-Permission → muss direkt auf GitHub bearbeitet werden
3. **Telegram an Betreuer:** IMMER über `_telegram_send_raw()` direkt senden – NICHT über `send_telegram(..., extra_chat_ids=[...])` → liefert HTTP 400
4. **`GESCHUETZT_PHASEN`** ist ein globales `frozenset` – niemals lokal in Funktionen neu definieren
5. **`notion_status_sync()`** gibt `int` zurück (updated_count) – kein Tupel
6. **`BRIEF_ONLY=true`** = nur Status-Sync + Brief-Erstellung, kein Scraping, kein PDF
7. **Betreuer bekommen Nachrichten** nur bei **neu importierten** Objekten in ihrem Bundesland – nicht bei bereits vorhandenen
8. **`_brief_anrede()`** erkennt ` | ` als Trenner bei mehreren Eigentümern (kommt von `_gb_parse_owner()`)
9. **Keine Notion-Dateianhänge** – 5 MB Limit macht das unbrauchbar für Gutachten
10. **Python 3.11**, `asyncio` nur für `main()`, alle anderen Funktionen synchron

---

## A. Executive Summary

1. **Was:** Automatischer Monitor für österreichische Zwangsversteigerungen (edikte.justiz.gv.at) für Immo-in-Not GmbH
2. **Wie:** Python 3.11 Script in GitHub Actions, läuft Mo-Fr 4x täglich (full) + alle 10 Min (brief-only)
3. **Daten:** Scraping → Notion-Datenbank → PDF-Analyse (PyMuPDF + GPT) → Brief-Erstellung (DOCX)
4. **Benachrichtigungen:** Telegram an 3 Empfänger je nach Bundesland + E-Mail via SendGrid
5. **Betreuer:** Benjamin (Wien/OÖ), Christopher (NÖ/Burgenland), Friedrich (Rest)
6. **Kritischer Fix:** Workflow hatte BRIEF_ONLY immer=true → voller Run lief nie → behoben durch 2 separate Jobs
7. **Kritischer Fix:** Telegram an Benjamin/Christopher schlug mit HTTP 400 fehl → direkter `_telegram_send_raw()`-Aufruf
8. **Nächstes Feature:** Google Drive Integration für automatischen Anhang-Download pro Immobilie
9. **Tech-Stack:** Python, Notion API, OpenAI API, Telegram Bot API, SendGrid, GitHub Actions
10. **Repo:** https://github.com/DP9020/Edikte-monitor

---

## B. Resume Prompt

```
Du arbeitest an einem produktiven Python-Automatisierungssystem namens "Edikte-Monitor"
für die Immo-in-Not GmbH (Österreich).

ZWECK: Das System scrapt täglich edikte.justiz.gv.at (österreichische
Zwangsversteigerungen), filtert relevante Wohnimmobilien, analysiert
Gerichtsgutachten (PDF) mit PyMuPDF + GPT-4o, verwaltet alles in Notion und
erstellt/versendet automatisch personalisierte Anschreiben an Eigentümer.

REPOSITORY: https://github.com/DP9020/Edikte-monitor
HAUPTDATEI: main.py (~3786 Zeilen, Python 3.11, ein einziges Modul)

WORKFLOW (GitHub Actions – 2 Jobs):
  - full-run:   Mo-Fr 07:30/09:00/12:00/15:00 UTC, BRIEF_ONLY=false
  - brief-only: alle 10 Min, BRIEF_ONLY=true

BETREUER-ZUORDNUNG:
  - Benjamin Pippan    → Wien, Oberösterreich     | TG: 8462725282 | office@benana.at
  - Christopher Dovjak → Niederösterreich, Burgenland | TG: 8500953016 | christopher.dovjak@dp-im.at
  - Friedrich Prause   → alle anderen BL          | friedrich.prause@dp-im.at

KRITISCHE REGELN:
1. Telegram an Betreuer IMMER über _telegram_send_raw() direkt senden –
   NICHT send_telegram(..., extra_chat_ids=[...]) → gibt HTTP 400
2. Workflow-Datei (.github/workflows/run.yml) NICHT per Code pushen –
   Bot hat keine workflows-Permission → muss manuell auf GitHub bearbeitet werden
3. GESCHUETZT_PHASEN ist globales frozenset – niemals lokal neu definieren
4. notion_status_sync() gibt int zurück (kein Tupel)
5. Notion-Dateianhänge funktionieren nicht (5 MB Limit)

NÄCHSTES GEPLANTES FEATURE: Google Drive Integration
  - Trigger: Notion-Checkbox "📥 Unterlagen herunterladen"
  - Aktion: Alle Anhänge (PDF, Fotos, Pläne) von Edikt-Seite downloaden
  - Ablage: Google Drive Ordner "Versteigerungen/Bundesland/Datum – Aktenzeichen/"
  - Drive-Link im Notion-Eintrag hinterlegen
  - Benötigt: Google Service-Account als GitHub Secret GOOGLE_SERVICE_ACCOUNT_JSON
  - Library: google-api-python-client + google-auth

Beginne mit der Weiterentwicklung. Stelle zuerst Fragen wenn unklar,
was als nächstes umzusetzen ist.
```

---

## C. Compact Handoff Memory

**PROJEKT: Edikte-Monitor** | Immo-in-Not GmbH | https://github.com/DP9020/Edikte-monitor

**Was es tut:** Scrapt täglich edikte.justiz.gv.at (österreichische Zwangsversteigerungen),
filtert Wohnimmobilien, analysiert PDFs mit GPT, verwaltet in Notion, erstellt personalisierte
Anschreiben (DOCX) und sendet sie per E-Mail + Telegram.

**Tech:** Python 3.11, eine Datei `main.py` (~3786 Zeilen), GitHub Actions, Notion API,
OpenAI API (gpt-4o-mini + gpt-4o Vision), Telegram Bot API, SendGrid, PyMuPDF, python-docx.

**GitHub Actions – 2 Jobs:**
- `full-run`: Mo-Fr 07:30/09:00/12:00/15:00 UTC – Scraping + PDF + Briefe + Telegram (BRIEF_ONLY=false)
- `brief-only`: alle 10 Min – nur Status-Sync + Briefe (BRIEF_ONLY=true)
- ⚠️ Workflow-Datei nur manuell auf GitHub editierbar (Bot hat keine workflows-Permission)

**Betreuer:**
- Benjamin Pippan    → Wien + OÖ          → TG: 8462725282, office@benana.at
- Christopher Dovjak → NÖ + Burgenland    → TG: 8500953016, christopher.dovjak@dp-im.at
- Friedrich Prause   → alle anderen BL    → friedrich.prause@dp-im.at

**Notion-Workflow-Phasen:**
🆕 Neu → 🔎 In Prüfung → ✅ Relevant – Brief vorbereiten → 📩 Brief versendet → ✅ Gekauft / ❌ Nicht relevant / 🗄 Archiviert

**Schlüssel-Konstanten:**
- `BUNDESLAENDER`: alle 9 BL mit Codes (Wien=0 bis Vorarlberg=8)
- `GESCHUETZT_PHASEN`: globales frozenset – schützt manuell bearbeitete Einträge
- `BENJAMIN_BUNDESLAENDER = {"Wien", "Oberösterreich"}`
- `CHRISTOPHER_BUNDESLAENDER = {"Niederösterreich", "Burgenland"}`
- `EXCLUDE_KEYWORDS`: filtert Landwirtschaft, Gewerbe, Lager etc.
- `KONTAKT_DATEN`: dict BL → {name, tel, email}

**Kritische Regeln:**
1. Telegram-Betreuer-Nachrichten: `_telegram_send_raw()` direkt – NICHT `extra_chat_ids` (→ HTTP 400)
2. `notion_status_sync()` → gibt `int` zurück
3. `GESCHUETZT_PHASEN` niemals lokal redefinieren
4. Notion-Dateianhänge: nicht nutzbar (5 MB Limit)

**GitHub Secrets erforderlich:**
`NOTION_TOKEN`, `NOTION_DATABASE_ID`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`,
`TELEGRAM_CHAT_ID_BENJAMIN`, `TELEGRAM_CHAT_ID_CHRISTOPHER`, `OPENAI_API_KEY`,
`SENDGRID_API_KEY`, `SMTP_USER`

**Behobene kritische Bugs:**
- BRIEF_ONLY war immer `true` (Workflow-Ternary-Bug) → 2 separate Jobs
- HTTP 400 bei Betreuer-Telegram → direkter `_telegram_send_raw()`-Aufruf
- `_get_benjamin_chat_id()` nicht definiert → implementiert
- `notion_brief_erstellen()` las falsches Notion-Feld → korrigiert
- `GESCHUETZT_PHASEN` 5x dupliziert → globales frozenset

**Nächstes Feature (noch nicht implementiert):** Google Drive Integration –
Checkbox in Notion triggert Download aller Edikt-Anhänge in strukturierten
Drive-Ordner pro Objekt. Benötigt: `GOOGLE_SERVICE_ACCOUNT_JSON` Secret,
`google-api-python-client` Library.
