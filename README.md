# Edikte-Monitor

Automatischer Scraper für gerichtliche Versteigerungen auf [edikte.justiz.gv.at](https://edikte.justiz.gv.at).
Alle 9 österreichischen Bundesländer werden überwacht. Neue Einträge landen automatisch in Notion, relevante Objekte werden mit personalisierten Anschreiben (DOCX) per E-Mail an den zuständigen Betreuer geschickt; Statusmeldungen kommen via Telegram.

Betreiber: **Immo-in-Not GmbH** — kauft Immobilien von Eigentümern vor Zwangsversteigerung an.

## Was das System macht

1. **Scraping** – Liest alle aktuellen Versteigerungsedikte von edikte.justiz.gv.at
2. **Notion-Import** – Legt neue Immobilien automatisch in der Notion-Datenbank an
3. **Gutachten-Analyse** – Lädt das Gutachten-PDF herunter und extrahiert:
   - Eigentümername + Zustelladresse (Text-PDF: PyMuPDF + Regex; Scan-PDF: GPT-4o Vision)
   - Gläubiger / Bank + Forderungsbetrag
4. **URL-Anreicherung** – Findet fehlende Edikt-Links für manuell eingetragene Immobilien
5. **Brief-Erstellung** – Erzeugt personalisierte DOCX-Anschreiben aus `brief_vorlage.docx`
6. **E-Mail-Versand** – Sammel-E-Mail pro Betreuer via Brevo SMTP (Fallback: SendGrid)
7. **Drive-Upload** – Synchronisiert PDFs der gelb markierten Einträge in den Drive-Ordner
8. **Telegram-Benachrichtigung** – Status-Updates, Erfolge und Fehler-Alarme

## Automatisch befüllte Notion-Properties

| Property | Quelle |
|---|---|
| Liegenschaftsadresse | Edikt-Detailseite |
| Link | Edikt-URL |
| Art des Edikts | Edikt (Versteigerung / Entfall / Verschiebung) |
| Bundesland | Edikt |
| Versteigerungstermin | Edikt-Detailseite |
| Verkehrswert | Edikt-Detailseite |
| Fläche | Edikt-Detailseite |
| Liegenschafts PLZ | Edikt-Detailseite |
| Objektart | Edikt-Detailseite |
| Verpflichtende Partei | Gutachten-PDF (Grundbuch Sektion B) |
| Zustell Adresse | Gutachten-PDF (Grundbuch Sektion B) |
| Zustell PLZ/Ort | Gutachten-PDF (Grundbuch Sektion B) |
| Notizen | Gutachten-PDF (Gläubiger, Forderung, PDF-Link) |
| Gutachten analysiert? | wird nach PDF-Auswertung auf ✅ gesetzt |
| Brief erstellt am | nach DOCX-Erstellung + E-Mail-Versand |

## Workflows (GitHub Actions)

| Workflow | Schedule | Zweck |
|---|---|---|
| `run.yml` `full-run` | Mo–Fr 05:30 / 07:00 / 10:00 / 13:00 UTC | Scraping + Gutachten-Analyse + Brief |
| `run.yml` `brief-only` | alle 10 Min | Status-Sync + Brief-Erstellung (~30 s) |
| `run.yml` `gdrive-sync` | alle 30 Min | Drive-Upload für gelbe Einträge |
| `run.yml` `cleanup-duplikate` | Di 03:00 UTC | Duplikat-Bereinigung |
| `run.yml` `wochenbericht` | Mo 08:15 UTC | wöchentlicher Status-Report (eigene concurrency-group) |
| `dedup-neu-eingelangt.yml` | Mo/Mi/Fr 03:30 UTC | 🆕-Duplikate gegen bearbeitete Zwillinge |
| `dedup-tief.yml` | manuell | tiefe Union-Find-Bereinigung |

Manueller Start: **Actions → Edikte Monitor → Run workflow**, Modus auswählen.

## Erforderliche GitHub Secrets

| Secret | Beschreibung |
|---|---|
| `NOTION_TOKEN` | Notion Integration Token |
| `NOTION_DATABASE_ID` | ID der Notion-Zieldatenbank |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot Token |
| `TELEGRAM_CHAT_ID` | Telegram Chat/Channel ID (Friedrich) |
| `TELEGRAM_CHAT_ID_BENJAMIN` | Telegram-Chat von Benjamin |
| `TELEGRAM_CHAT_ID_CHRISTOPHER` | Telegram-Chat von Christopher |
| `OPENAI_API_KEY` | OpenAI API Key (gpt-4o-mini + Vision) |
| `BREVO_SMTP_KEY` | Brevo SMTP Master-Passwort |
| `BREVO_SMTP_LOGIN` | Brevo Login-ID (NICHT die Account-Mail) |
| `SMTP_USER` | Absender-E-Mail (auch Sender-Identity) |
| `SENDGRID_API_KEY` | Legacy-Fallback für E-Mail |
| `GOOGLE_SERVICE_ACCOUNT_KEY` | Base64-codiertes Service-Account-JSON |
| `GOOGLE_DRIVE_FOLDER_ID` | Drive-Ordner für Edikt-PDFs |

Komplette Variablen-Liste siehe `.env.example`.

## Ausgeschlossene Objektarten

Landwirtschaft, Forstwirtschaft, Gewerbeobjekte, Büros, Lager, Industrie – nur Wohnimmobilien werden importiert.
