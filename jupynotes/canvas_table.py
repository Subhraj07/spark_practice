from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4, cm
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, Table, TableStyle
from reportlab.lib.enums import TA_JUSTIFY, TA_LEFT, TA_CENTER
from reportlab.lib import colors
import pandas as pd
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.piecharts import Pie
from reportlab.lib import colors


width, height = A4
styles = getSampleStyleSheet()
styleN = styles["BodyText"]
styleN.alignment = TA_LEFT
styleBH = styles["Normal"]
styleBH.alignment = TA_CENTER

def coord(x, y, unit=1):
    x, y = x * unit, height -  y * unit
    return x, y

# # Headers
# hdescrpcion = Paragraph('''<b>descrpcion</b>''', styleBH)
# hpartida = Paragraph('''<b>partida</b>''', styleBH)
# hcandidad = Paragraph('''<b>candidad</b>''', styleBH)
# hprecio_unitario = Paragraph('''<b>precio_unitario</b>''', styleBH)
# hprecio_total = Paragraph('''<b>precio_total</b>''', styleBH)
#
# # Texts
# descrpcion = Paragraph('long paragraph', styleN)
# partida = Paragraph('1', styleN)
# candidad = Paragraph('120', styleN)
# precio_unitario = Paragraph('$52.00', styleN)
# precio_total = Paragraph('$6240.00', styleN)

# data= [[hdescrpcion, hcandidad,hcandidad, hprecio_unitario, hprecio_total],
#        [partida, candidad, descrpcion, precio_unitario, precio_total]]

df = pd.read_csv('data/bank.csv',sep=';')
df = df[['age', 'job', 'marital', 'education']]
lista = [df.columns[:,].values.astype(str).tolist()] + df.values.tolist()

# PAGE_WIDTH = 7.1 * cm
# PAGE_HEIGHT = 1 * cm
# table = Table(lista, colWidths=[1.05 * cm, 2.7 * cm, 1 * cm,
#                                1* cm, 3 * cm])
table = Table(lista)
table.setStyle(TableStyle([
                        ('INNERGRID', (0,0), (-1,-1), 0.25, colors.black),
                        ('BOX', (0,0), (-1,-1), 0.25, colors.black),
                        ('BACKGROUND', (0, 0), (-1, 0), colors.gray),
                        ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
                       ]))

c = canvas.Canvas("pdfs/a.pdf", pagesize=A4)

c.setLineWidth(.3)
c.setFont('Helvetica', 12)

c.drawString(30, 750, 'OFFICIAL COMMUNIQUE')
c.drawString(30, 735, 'OF ACME INDUSTRIES')
c.drawString(500, 750, "12/12/2010")
c.line(480, 747, 580, 747)

c.drawString(275, 725, 'AMOUNT OWED:')
c.drawString(500, 725, "$1,000.00")
c.line(378, 723, 580, 723)

c.drawString(30, 703, 'RECEIVED BY:')
c.line(120, 700, 580, 700)
c.drawString(120, 703, "JOHN DOE")

table.wrapOn(c, width, height)
table.drawOn(c, *coord(0.2, 25, cm))
c.save()