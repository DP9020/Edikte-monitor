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
TELEGRAM_CHAT_ID=...                # Haupt-Chat (Friedrich)
TELEGRAM_CHAT_ID_BENJAMIN=8482923282
TELEGRAM_CHAT_ID_CHRISTOPHER=8500953016
OPENAI_API_KEY=...
SMTP_USER=...                       # Absender-E-Mail (auch Brevo-Sender-Identity)
BREVO_SMTP_KEY=...                  # primärer Mail-Pfad (smtp-relay.brevo.com:587)
BREVO_SMTP_LOGIN=...                # generierte Brevo-Login-ID, NICHT die Account-Mail
SENDGRID_API_KEY=...                # Legacy-Fallback (nach Free-Tier-Limit auf Brevo umgezogen)
BRIEF_ONLY=false                    # "true" = nur Brief-Sync, kein Scraping
GDRIVE_ONLY=false                   # "true" = nur Drive-Sync der gelben Einträge
WOCHENBERICHT=false                 # "true" = Wochenbericht-Modus (Mo)
GOOGLE_SERVICE_ACCOUNT_KEY=...      # Base64-codiertes JSON eines Google Service Accounts
GOOGLE_DRIVE_FOLDER_ID=...          # ID des Drive-Ordners "Immo-in-Not Edikte-Downloads"
NOTION_MIN_PAGES=500                # optional, Sanity-Check gegen vorzeitige Pagination-Abbrüche
```

### GitHub Actions (automatisch)

Workflows in `.github/workflows/`:
- **`run.yml` `full-run`**: Mo–Fr 05:30 / 07:00 / 10:00 / 13:00 UTC — Scraping + Gutachten-Analyse
- **`run.yml` `brief-only`**: alle 10 Min — Status-Sync + Brief-Erstellung (~30 Sek. Laufzeit)
- **`run.yml` `gdrive-sync`**: alle 30 Min — Drive-Upload für gelb markierte Einträge
- **`run.yml` `cleanup-duplikate`**: Di 03:00 UTC — Duplikat-Bereinigung (`--apply`)
- **`run.yml` `wochenbericht`**: Mo 07:30 UTC — wöchentlicher Statusbericht
- **`dedup-neu-eingelangt.yml`**: Mo/Mi/Fr 03:30 UTC — 🆕-Duplikate gegen bereits bearbeitete Zwillinge
- **`dedup-tief.yml`**: nur manuell (workflow_dispatch) — tiefe Bereinigung mit Union-Find

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
  → E-Mail via Brevo SMTP (Fallback: SendGrid) an Betreuer
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
