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

def clean_db_id(raw: str) -> str:
    """
    Bereinigt die Datenbank-ID:
    - Entfernt URL-Parameter wie ?v=...&pvs=...
    - Extrahiert nur die 32-stellige Hex-ID
    Beispiel:
      "5a18c99a02c84c9dbd56469d15a1a978?v=abc&pvs=13"  → "5a18c99a-02c8-4c9d-bd56-469d15a1a978"
      "https://notion.so/Titel-5a18c99a02c84c9dbd56469d15a1a978" → "5a18c99a..."
    """
    import re
    # Query-Parameter entfernen
    raw = raw.split("?")[0].strip()
    # Letzten Pfadteil nehmen (falls URL)
    raw = raw.rstrip("/").split("/")[-1]
    # Nur Hex-Zeichen und Bindestriche behalten
    raw = re.sub(r"[^0-9a-fA-F\-]", "", raw)
    # Bindestriche entfernen → 32 Hex-Zeichen
    clean = raw.replace("-", "")
    if len(clean) == 32:
        # Standard UUID-Format: 8-4-4-4-12
        return f"{clean[0:8]}-{clean[8:12]}-{clean[12:16]}-{clean[16:20]}-{clean[20:32]}"
    return raw  # Fallback: unverändert zurückgeben


def check_notion_properties():
    notion = Client(auth=env("NOTION_TOKEN"))
    raw_id = env("NOTION_DATABASE_ID")
    db_id  = clean_db_id(raw_id)

    print(f"\n🔍 Verbinde mit Notion-Datenbank...")
    print(f"   Rohe ID:     {raw_id[:40]}...")
    print(f"   Bereinigte ID: {db_id}\n")

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
