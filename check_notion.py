"""
Notion Property Checker
Liest alle Properties der Datenbank aus und zeigt sie an.
"""

import os
from notion_client import Client

def env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Fehlende Umgebungsvariable: {name}")
    return value

def check_notion_properties():
    notion = Client(auth=env("NOTION_TOKEN"))
    db_id  = env("NOTION_DATABASE_ID")

    print(f"\n🔍 Verbinde mit Notion-Datenbank: {db_id}\n")

    db = notion.databases.retrieve(database_id=db_id)

    print(f"📋 Datenbank-Name: {db['title'][0]['text']['content'] if db.get('title') else 'Unbekannt'}")
    print(f"\n{'='*55}")
    print(f"{'Property Name':<35} {'Typ':<20}")
    print(f"{'='*55}")

    for name, prop in sorted(db["properties"].items()):
        print(f"{name:<35} {prop['type']:<20}")

    print(f"{'='*55}")
    print(f"\n✅ Gesamt: {len(db['properties'])} Properties gefunden\n")

    # Vergleich mit erwarteten Properties aus main.py
    expected = {
        "Name":                    "title",
        "Edikt-ID":                "rich_text",
        "Edikt-Link":              "url",
        "Art des Edikts":          "select",
        "Bundesland":              "select",
        "Neu eingelangt":          "checkbox",
        "Automatisch importiert?": "checkbox",
        "Workflow-Phase":          "select",
        "Import-Datum":            "date",
        "Gericht":                 "rich_text",
        "Beschreibung":            "rich_text",
        "Archiviert":              "checkbox",
    }

    print("🔎 Abgleich mit main.py:\n")
    all_ok = True
    for name, expected_type in expected.items():
        actual = db["properties"].get(name)
        if actual is None:
            print(f"  ❌ FEHLT:    '{name}' (erwartet: {expected_type})")
            all_ok = False
        elif actual["type"] != expected_type:
            print(f"  ⚠️  FALSCHER TYP: '{name}' → ist '{actual['type']}', erwartet '{expected_type}'")
            all_ok = False
        else:
            print(f"  ✅ OK:       '{name}' ({actual['type']})")

    if all_ok:
        print("\n🎉 Alles passt! Keine Anpassungen nötig.")
    else:
        print("\n⚠️  Einige Properties fehlen oder haben falschen Typ.")

if __name__ == "__main__":
    check_notion_properties()
