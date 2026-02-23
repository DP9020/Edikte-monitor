# Edikte-Monitor

Automatischer Scraper für gerichtliche Versteigerungen auf [edikte.justiz.gv.at](https://edikte.justiz.gv.at).  
Alle 9 österreichischen Bundesländer werden überwacht. Neue Einträge landen automatisch in Notion, Benachrichtigungen kommen via Telegram.

## Was das System macht

1. **Scraping** – Liest alle aktuellen Versteigerungsedikte von edikte.justiz.gv.at
2. **Notion-Import** – Legt neue Immobilien automatisch in der Notion-Datenbank an
3. **Gutachten-Analyse** – Lädt das Gutachten-PDF herunter und extrahiert:
   - Eigentümername + Zustelladresse
   - Gläubiger / Bank + Forderungsbetrag
4. **URL-Anreicherung** – Findet fehlende Edikt-Links für manuell eingetragene Immobilien
5. **Telegram-Benachrichtigung** – Sendet eine Zusammenfassung aller neuen Einträge

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

## Workflow

Läuft automatisch **2× täglich** (06:30 und 16:30 Uhr) via GitHub Actions.  
Kann jederzeit manuell unter **Actions → Edikte Monitor → Run workflow** gestartet werden.

## Erforderliche GitHub Secrets

| Secret | Beschreibung |
|---|---|
| `NOTION_TOKEN` | Notion Integration Token |
| `NOTION_DATABASE_ID` | ID der Notion-Zieldatenbank |
| `TELEGRAM_BOT_TOKEN` | Telegram Bot Token |
| `TELEGRAM_CHAT_ID` | Telegram Chat/Channel ID |

## Ausgeschlossene Objektarten

Landwirtschaft, Forstwirtschaft, Gewerbeobjekte, Büros, Lager, Industrie – nur Wohnimmobilien werden importiert.
