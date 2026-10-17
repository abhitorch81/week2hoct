import io
from xml.sax.saxutils import escape
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer


def letter_pdf(text: str) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=LETTER, leftMargin=inch, rightMargin=inch, topMargin=.9 * inch, bottomMargin=.9 * inch,
                            title="Appeal letter")
    st = ParagraphStyle("b", parent=getSampleStyleSheet()["Normal"], fontName="Times-Roman", fontSize=11, leading=15)
    story = []
    for block in text.replace("\r", "").split("\n\n"):
        story.append(Paragraph(escape(block).replace("\n", "<br/>"), st))
        story.append(Spacer(1, 8))
    doc.build(story)
    return buf.getvalue()
