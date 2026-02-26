"""
Erstellt die Brief-Vorlage (brief_vorlage.docx) mit Platzhaltern.
Nur einmalig ausführen – danach die Datei ins Repo committen.
"""
from docx import Document
from docx.shared import Pt, Cm, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
import os

def set_font(run, name="Calibri", size=11, bold=False, color=None):
    run.font.name = name
    run.font.size = Pt(size)
    run.font.bold = bold
    if color:
        run.font.color.rgb = RGBColor(*color)

def add_paragraph(doc, text="", alignment=WD_ALIGN_PARAGRAPH.LEFT, space_before=0, space_after=6):
    p = doc.add_paragraph()
    p.alignment = alignment
    p.paragraph_format.space_before = Pt(space_before)
    p.paragraph_format.space_after = Pt(space_after)
    if text:
        run = p.add_run(text)
        set_font(run)
    return p

doc = Document()

# Seitenränder
section = doc.sections[0]
section.top_margin    = Cm(2.5)
section.bottom_margin = Cm(2.0)
section.left_margin   = Cm(2.5)
section.right_margin  = Cm(2.0)

# ── Absender-Block (oben rechts) ──────────────────────────────────────────────
p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
p.paragraph_format.space_after = Pt(0)
run = p.add_run("Immo-in-Not GmbH")
set_font(run, bold=True, size=11)

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
p.paragraph_format.space_after = Pt(0)
run = p.add_run("{{KONTAKT_NAME}}")
set_font(run, size=11)

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
p.paragraph_format.space_after = Pt(0)
run = p.add_run("{{KONTAKT_STRASSE}}")
set_font(run, size=11)

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
p.paragraph_format.space_after = Pt(0)
run = p.add_run("{{KONTAKT_PLZ_ORT}}")
set_font(run, size=11)

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
p.paragraph_format.space_after = Pt(0)
run = p.add_run("Tel: {{KONTAKT_TEL}}")
set_font(run, size=11)

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
p.paragraph_format.space_after = Pt(18)
run = p.add_run("E-Mail: {{KONTAKT_EMAIL}}")
set_font(run, size=11)

# ── Empfänger-Block ───────────────────────────────────────────────────────────
p = doc.add_paragraph()
p.paragraph_format.space_after = Pt(0)
run = p.add_run("{{EIGENTUEMER_NAME}}")
set_font(run, size=11)

p = doc.add_paragraph()
p.paragraph_format.space_after = Pt(0)
run = p.add_run("{{ZUSTELL_ADRESSE}}")
set_font(run, size=11)

p = doc.add_paragraph()
p.paragraph_format.space_after = Pt(18)
run = p.add_run("{{ZUSTELL_PLZ_ORT}}")
set_font(run, size=11)

# ── Datum ─────────────────────────────────────────────────────────────────────
p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.RIGHT
p.paragraph_format.space_after = Pt(18)
run = p.add_run("{{DATUM}}")
set_font(run, size=11)

# ── Betreff ───────────────────────────────────────────────────────────────────
p = doc.add_paragraph()
p.paragraph_format.space_after = Pt(12)
run = p.add_run("Betreff: Ihre Liegenschaft – {{LIEGENSCHAFT_ADRESSE}}")
set_font(run, bold=True, size=12)

# ── Anrede ────────────────────────────────────────────────────────────────────
p = doc.add_paragraph()
p.paragraph_format.space_after = Pt(12)
run = p.add_run("Sehr geehrte Damen und Herren,")
set_font(run, size=11)

# ── Brieftext ─────────────────────────────────────────────────────────────────
text1 = (
    "wir haben festgestellt, dass Ihre Liegenschaft in "
    "{{LIEGENSCHAFT_ADRESSE}} "
    "zum Verkauf ansteht. Als spezialisiertes Unternehmen im Bereich der "
    "Immobilienrettung bei finanziellen Engpässen möchten wir Ihnen eine "
    "diskrete und faire Lösung anbieten."
)
p = doc.add_paragraph()
p.paragraph_format.space_after = Pt(10)
run = p.add_run(text1)
set_font(run, size=11)

text2 = (
    "Immo-in-Not GmbH unterstützt Eigentümer in finanziell schwierigen "
    "Situationen mit einem transparenten Sale-and-Rent-Back-Modell: Sie "
    "erhalten sofortige Liquidität und können trotzdem in Ihrem Zuhause "
    "wohnen bleiben."
)
p = doc.add_paragraph()
p.paragraph_format.space_after = Pt(10)
run = p.add_run(text2)
set_font(run, size=11)

text3 = (
    "Gerne stehen wir Ihnen für ein unverbindliches Gespräch zur Verfügung. "
    "Bitte kontaktieren Sie uns telefonisch unter {{KONTAKT_TEL}} oder per "
    "E-Mail an {{KONTAKT_EMAIL}}."
)
p = doc.add_paragraph()
p.paragraph_format.space_after = Pt(18)
run = p.add_run(text3)
set_font(run, size=11)

# ── Grußformel ────────────────────────────────────────────────────────────────
p = doc.add_paragraph()
p.paragraph_format.space_after = Pt(24)
run = p.add_run("Mit freundlichen Grüßen,")
set_font(run, size=11)

# ── Unterschrift ──────────────────────────────────────────────────────────────
p = doc.add_paragraph()
p.paragraph_format.space_after = Pt(0)
run = p.add_run("{{KONTAKT_NAME}}")
set_font(run, bold=True, size=11)

p = doc.add_paragraph()
p.paragraph_format.space_after = Pt(0)
run = p.add_run("Immo-in-Not GmbH")
set_font(run, size=11)

output = "/home/user/webapp/brief_vorlage.docx"
doc.save(output)
print(f"✅ Vorlage gespeichert: {output}")
