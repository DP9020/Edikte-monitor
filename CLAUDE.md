# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Was macht die App

**Edikte-Monitor** ist ein automatischer Scraper für österreichische Zwangsversteigerungen. Er überwacht täglich `edikte.justiz.gv.at`, filtert relevante Wohnimmobilien, extrahiert Eigentümer- und Gläubigerdaten aus Gerichtsgutachten (PDF) und verwaltet alles in einer Notion-Datenbank. Für relevante Objekte werden automatisch personalisierte Anschreiben (DOCX) erstellt und per E-Mail an den zuständigen Betreuer versendet.

Betreiber: **Immo-in-Not GmbH** — kauft Immobilien von Eigentümern vor Zwangsversteigerung an.

## Commands

```bash
# Abhängigkeiten installieren
pip install -r requirements.txt

# Vollständiger Run (Scraping + PDF-Analyse + Briefe)
python main.py

# Nur Brief-Template neu erstellen und testen
python create_brief_template.py
```

### Umgebungsvariablen (alle erforderlich)

```bash
NOTION_TOKEN=...
NOTION_DATABASE_ID=...
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...           # Haupt-Chat (Friedrich)
TELEGRAM_CHAT_ID_BENJAMIN=8482923282
TELEGRAM_CHAT_ID_CHRISTOPHER=8500953016
OPENAI_API_KEY=...
SENDGRID_API_KEY=...
SMTP_USER=...                  # Absender-E-Mail (in SendGrid verifiziert)
BRIEF_ONLY=false               # "true" = nur Brief-Sync, kein Scraping
GOOGLE_SERVICE_ACCOUNT_KEY=... # Base64-codiertes JSON eines Google Service Accounts
GOOGLE_DRIVE_FOLDER_ID=...     # ID des Drive-Ordners "Immo-in-Not Edikte-Downloads"
```

### GitHub Actions (automatisch)

Zwei separate Jobs in `.github/workflows/run.yml`:
- **`full-run`**: Mo–Fr um 07:30, 09:00, 12:00, 15:00 UTC — vollständiges Scraping
- **`brief-only`**: Alle 10 Minuten — nur Status-Sync + Briefe (~30 Sek. Laufzeit)

## Architektur

### Single-File (`main.py`, ~3800 Zeilen)

Bewusste Entscheidung: vereinfacht GitHub Actions Deployment, keine Modul-Komplexität.

### Datenfluss (BRIEF_ONLY=false)

```
edikte.justiz.gv.at (alle 9 Bundesländer)
  → HTTP-Scraping (urllib, kein Browser/Selenium nötig)
  → Duplikat-Check gegen Notion (known_ids)
  → Neue Einträge in Notion anlegen
  → Status-Sync (Workflow-Phasen aktualisieren)
  → PDF-Download (Gutachten)
  → PDF-Analyse:
      Text-PDF  → PyMuPDF → Regex-Parser
      Scan-PDF  → GPT-4o Vision
      Fehler    → GPT-4o-mini Fallback
  → Notion-Update (Eigentümer, Gläubiger, Betrag)
  → DOCX-Brief erstellen (python-docx Template)
  → E-Mail via SendGrid an Betreuer
  → Telegram-Benachrichtigungen
```

### Betreuer-Zuordnung

| Betreuer | Bundesländer |
|----------|-------------|
| Benjamin Pippan | Wien, Oberösterreich |
| Christopher Dovjak | Niederösterreich, Burgenland |
| Friedrich Prause | Steiermark, Kärnten, Salzburg, Tirol, Vorarlberg |

### Wichtige Regeln

- **Telegram-Betreuer** immer über `_telegram_send_raw()` ansprechen — `send_telegram(..., extra_chat_ids=[...])` gibt HTTP 400
- **`GESCHUETZT_PHASEN`** (globales `frozenset`): Einträge in diesen Phasen werden nicht automatisch überschrieben
- **Nur Wohnimmobilien** werden importiert — `EXCLUDE_KEYWORDS` und `EXCLUDE_KATEGORIEN` filtern Gewerbe/Landwirtschaft heraus

### Brief-Template-Platzhalter (`brief_vorlage.docx`)

`{{EIGENTUEMER_NAME}}`, `{{ZUSTELL_ADRESSE}}`, `{{ZUSTELL_PLZ_ORT}}`, `{{DATUM}}`, `{{LIEGENSCHAFT_ADRESSE}}`, `{{LIEGENSCHAFT_PLZ_ORT}}`, `{{ANREDE}}`, `{{KONTAKT_NAME}}`, `{{KONTAKT_EMAIL}}`, `{{KONTAKT_TEL}}`

### Notion-Datenbankfelder (kritische)

`Liegenschaftsadresse` (Title), `Bundesland`, `Status`, `Workflow-Phase`, `Verpflichtende Partei`, `Zustell Adresse`, `Betreibende Partei`, `Link`, `Versteigerungstermin`, `Verkehrswert`, `Gutachten analysiert?`, `Brief erstellt am`, `Archiviert`
