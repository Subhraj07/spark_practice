from reportlab.pdfgen.canvas import Canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import Paragraph, Frame, Spacer, Image, Table, TableStyle, SimpleDocTemplate,KeepTogether,PageBreak
from reportlab.graphics.charts.barcharts import VerticalBarChart,HorizontalBarChart,Line
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.textlabels import Label
from reportlab.graphics.charts.legends import Legend
import pandas as pd
import time
import numpy as np

data = pd.read_csv('data/Vendor Master.csv')
data['Created Date'] = pd.to_datetime(data['Created Date'])
data['Inactive Date'] = pd.to_datetime(data['Inactive Date'])

def inactivedatebeforcreateddate(data):

    inactive_data = data[data['Status'] == 'In Active']
    v_ina_dt = inactive_data[['Vendor ID', 'Vendor Name', 'Created Date', 'Inactive Date']]
    v_ina_dt = v_ina_dt[
        (v_ina_dt['Inactive Date'] < v_ina_dt['Created Date']) & (v_ina_dt['Inactive Date'] != '1990-01-01')]
    v_ina_dt = v_ina_dt[['Vendor ID', 'Vendor Name', 'Created Date', 'Inactive Date']]

    # if(v_ina_dt.count()>20):
    #     v_ina_dt.to_csv('./pdfs/Vendor_Inactive_Date.csv',index=False)
    #     return str.empty
    # else:
    rowheights = .2 * inch
    colwidths = (70, 180, 100, 100)
    lista = [v_ina_dt.columns[:, ].values.astype(str).tolist()] + v_ina_dt.values.tolist()
    t = Table(lista, colwidths, rowheights)
    GRID_STYLE = TableStyle(
            [('GRID', (0, 0), (-1, -1), 0.25, colors.black),
             ('ALIGN', (1, 1), (-1, -1), 'RIGHT')]
        )
    t.setStyle(GRID_STYLE)

    return t


def inactivevendorwithnoinactivedate(data):
    in_ved_no_indt = data[(data['Status'] == 'In Active') & (data['Inactive Date'] == '1990-01-01')]
    in_ved_no_indt = in_ved_no_indt[['Vendor ID', 'Vendor Name', 'Created Date', 'Inactive Date']]

    # if (in_ved_no_indt.count() > 20):
    #     in_ved_no_indt.to_csv('./pdfs/In_Ven_No_InDate.csv', index=False)
    #     return str.empty
    # else:
    rowheights = .2 * inch
    colwidths = (70, 180, 100, 100)
    lista = [in_ved_no_indt.columns[:, ].values.astype(str).tolist()] + in_ved_no_indt.values.tolist()
    t = Table(lista, colwidths, rowheights)
    GRID_STYLE = TableStyle(
            [('GRID', (0, 0), (-1, -1), 0.25, colors.black),
             ('ALIGN', (1, 1), (-1, -1), 'RIGHT')]
    )
    t.setStyle(GRID_STYLE)

    return t


def activestatusinactivedate(data):

    v_acts_in_dt = data[(data['Status'] == 'Active') & (data['Inactive Date'] != '1990-01-01')]
    v_acts_in_dt = v_acts_in_dt[['Vendor ID', 'Vendor Name', 'Created Date', 'Inactive Date']]

    # if (v_acts_in_dt.count() > 20):
    #     v_acts_in_dt.to_csv('./pdfs/Ven_Act_With_InDate.csv', index=False)
    #     return str.empty
    # else:
    rowheights = .2 * inch
    colwidths = (70, 180, 100, 100)
    lista = [v_acts_in_dt.columns[:, ].values.astype(str).tolist()] + v_acts_in_dt.values.tolist()
    t = Table(lista, colwidths, rowheights)
    GRID_STYLE = TableStyle(
            [('GRID', (0, 0), (-1, -1), 0.25, colors.black),
             ('ALIGN', (1, 1), (-1, -1), 'RIGHT')]
        )
    t.setStyle(GRID_STYLE)
    return t

# Export to csv
def vendorswithnoEINTIN(data):
    no_EINTIN = data[pd.isnull(data['EINTIN Number'])]
    no_EINTIN = no_EINTIN[['Vendor ID', 'Vendor Name', 'Created Date', 'Inactive Date']]

    # if (no_EINTIN.count() > 20):
    #     no_EINTIN.to_csv('./pdfs/Vendor_NoEINTIN.csv', index=False)
    #     return str.empty
    # else:
    colwidths = (70, 180, 100, 100)
    rowheights = .2 * inch

    lista = [no_EINTIN.columns[:, ].values.astype(str).tolist()] + no_EINTIN.values.tolist()
    t = Table(lista, colwidths, rowheights)
    GRID_STYLE = TableStyle(
            [('GRID', (0, 0), (-1, -1), 0.25, colors.black),
             ('ALIGN', (1, 1), (-1, -1), 'RIGHT')]
        )
    t.setStyle(GRID_STYLE)

    return t


# True Duplicate Vendors
def duplicatevendor(data):

    grouped_vendor = data.groupby(['Vendor Name']).count()
    grouped_vendor = grouped_vendor[grouped_vendor['Vendor ID'] > 1]
    ven_name = grouped_vendor.index.tolist()
    dup_vendor = data[data['Vendor Name'].isin(ven_name)]
    dup_vendor = dup_vendor[['Vendor ID', 'Vendor Name', 'Created Date', 'Inactive Date']]

    # if (dup_vendor.count() > 20):
    #     dup_vendor.to_csv('./pdfs/Duplicate_Vendor.csv', index=False)
    #     return str.empty
    # else:
    colwidths = 1.4 * inch
    rowheights = .2 * inch
    lista = [dup_vendor.columns[:, ].values.astype(str).tolist()] + dup_vendor.values.tolist()
    t = Table(lista, colwidths, rowheights)
    GRID_STYLE = TableStyle(
            [('GRID', (0, 0), (-1, -1), 0.25, colors.black),
             ('ALIGN', (1, 1), (-1, -1), 'RIGHT')]
        )
    t.setStyle(GRID_STYLE)

    return t


def vendorcreatedperquarter(vendor_data):
    vendor_data['Period'] = vendor_data['Created Date'].dt.to_period('M')
    vendor_data['Qtr'] = (vendor_data['Period']).dt.quarter
    vendor_data['Year'] = vendor_data['Created Date'].map(lambda x: 1 * x.year)
    vendor_data["Quarter"] = vendor_data["Year"].map(str) + "Q" + vendor_data["Qtr"].map(str)

    # raw_ven_data = vendor_data[['Vendor ID', 'Quarter']]

    act_raw_ven_data = vendor_data[vendor_data['Status'] == 'Active'][['Vendor ID', 'Quarter']]
    inact_raw_ven_data = vendor_data[vendor_data['Status'] == 'In Active'][['Vendor ID', 'Quarter']]

    group_data = vendor_data[['Vendor ID', 'Quarter']].groupby(['Quarter']).count()

    act_group_ven_data = act_raw_ven_data.groupby(['Quarter']).count()
    inact_group_ven_data = inact_raw_ven_data.groupby(['Quarter']).count()

    act_data = act_group_ven_data['Vendor ID'].tolist()
    inact_data = inact_group_ven_data['Vendor ID'].tolist()

    y_data = group_data.index.tolist()

    drawing = Drawing(200, 100)
    list_data = []
    list_data.append(act_data)
    list_data.append(inact_data)
    data = list_data

    lc = HorizontalBarChart()

    lc.x = 10
    lc.y = -150
    lc.height = 250
    lc.width = 450
    lc.data = data
    lc.categoryAxis.categoryNames = y_data
    lc.bars[0].fillColor = colors.lightblue
    lc.bars[1].fillColor = colors.lightgreen
    # lc.lines.symbol = makeMarker('Circle')

    # name1 = 'Active'
    # name2 = 'Inactive'
    #
    # swatches = Legend()
    # swatches.alignment = 'right'
    # swatches.x = 80
    # swatches.y = 160
    # swatches.deltax = 60
    # swatches.dxTextSpace = 10
    # swatches.columnMaximum = 4
    # items = [(colors.lightblue, name1), (colors.lightgreen, name2)]
    # swatches.colorNamePairs = items

    drawing.add(lc)

    return drawing


story = []

styles = getSampleStyleSheet()
styleN = styles['Normal']

a = Paragraph("<strong>Vendors Created Per Quarter</strong>", styleN)
b = Spacer(1, .25 * inch)
c = vendorcreatedperquarter(data)
story.append(KeepTogether([a, b, c]))

story.append(PageBreak())
story.append(Spacer(1, .5 * inch))

story.append(Paragraph("<strong>Inactive Vendors with Inactive dates post created date</strong>", styleN))
story.append(Spacer(1, .10 * inch))
story.append(Paragraph("The following are the list of vendors who are inactive per the Vendor master. However their inactive dates are before created dates.", styleN))
story.append(Spacer(1, .25 * inch))
story.append(inactivedatebeforcreateddate(data))
story.append(Spacer(1, .25 * inch))

story.append(PageBreak())
story.append(Spacer(1, .5 * inch))

story.append(Paragraph("<strong>Inactive Vendors with no inactive Date</strong>", styleN))
story.append(Spacer(1, .10 * inch))
story.append(Paragraph("The following lists Inactive Vendors who have no inavtive dates against them.", styleN))
story.append(Spacer(1, .25 * inch))
story.append(inactivevendorwithnoinactivedate(data))
story.append(Spacer(1, .25 * inch))

story.append(PageBreak())
story.append(Spacer(1, .5 * inch))

a = Paragraph("<strong>Active Vendors with Inactive Dates</strong>", styleN)
x = Spacer(1, .10 * inch)
y = Paragraph("The following lists Active vendors who have inactive dates coded in the system.", styleN)
b = Spacer(1, .25 * inch)
c = activestatusinactivedate(data)
story.append(KeepTogether([a,x,y, b, c]))
story.append(Spacer(1, .25 * inch))

story.append(PageBreak())
story.append(Spacer(1, .5 * inch))

story.append(Paragraph("<strong>vendors with no EIN TIN</strong>", styleN))
story.append(Spacer(1, .10 * inch))
story.append(Paragraph("The following lists Vendors who have no EIN TIN information.", styleN))
story.append(Spacer(1, .25 * inch))
story.append(vendorswithnoEINTIN(data))
story.append(Spacer(1, .25 * inch))

story.append(PageBreak())
story.append(Spacer(1, .5 * inch))

story.append(Paragraph("<strong>True Duplicate Vendors</strong>", styleN))
story.append(Spacer(1, .25 * inch))
story.append(duplicatevendor(data))


# Story.append(KeepTogether([question, answer1, answer2, answer3]))

doc = SimpleDocTemplate('./pdfs/audit.pdf', pagesize=letter, topMargin=0)
doc.build(story)
