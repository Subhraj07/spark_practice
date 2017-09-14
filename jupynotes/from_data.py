from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4,cm
from reportlab.platypus import *
from reportlab.lib import colors
import pandas as pd

PATH_OUT = "pdfs/"

elements = []
styles = getSampleStyleSheet()
doc = SimpleDocTemplate(PATH_OUT + 'out.pdf',
                        pagesize=A4,
                        #pagesize = landscape(A4),
                        leftMargin=cm,
                        rightMargin=cm,
                        topMargin=0.5 * cm,
                        bottomMargin=0.5 * cm)
elements.append(Paragraph("Report Title", styles['Title']))

# data = [[random.random() for i in range(1,4)] for j in range (1,8)]
# data = pd.read_csv('data/bank.csv',sep=';')
# df = pd.DataFrame (data)
df = pd.read_csv('data/bank.csv',sep=';')
df = df[['age', 'job', 'marital', 'education', 'default', 'balance', 'housing',
       'loan', 'contact', 'day', 'month', 'duration', 'campaign', 'pdays',
       'previous', 'poutcome', 'y']]
lista = [df.columns[:,].values.astype(str).tolist()] + df.values.tolist()

ts = [
    # ('VALIGN', (0, 0), (-1, -1), 'BOTTOM'),
    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
    ('LEFTPADDING', (0, 0), (-1, -1), 0),
    ('RIGHTPADDING', (0, 0), (-1, -1), 0),
    ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.black),
    ('BOX', (0, 0), (-1, -1), 0.25, colors.black),

]
# PAGE_WIDTH = 3.5 * cm
# PAGE_HEIGHT = 1 * cm
styles.wordWrap = 'CJK'
# table = Table(lista, style=ts,colWidths=PAGE_WIDTH,rowHeights=PAGE_HEIGHT)
table = Table(lista,style=ts)
elements.append(table)

doc.build(elements)