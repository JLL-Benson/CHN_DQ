import pandas as pd
from openpyxl import load_workbook
import validation as vd
import sys
import getpass

company_colnames = ['Source ID', 'Company Name', 'Company Local Name', 'Billing Address line1 (Street/Road)', 'Billing Address line2 (Building Name)', 'Billing Address line3(Suite, Level, Floor, Unit)', 'Postal Code','District', 'City', 'State', 'Country', 'Company Type', 'Phone', 'Fax', 'Email', 'Website','Industry', 'Revenue', 'Employee', 'Full Address', 'dq_New']


# Source-Site-LoadRound
sourcename = 'CM-South-1'
# YYYYMMDDHH

backupfilepath = r'C:\Users\Benson.Chen\Desktop\CHN-DQ_CM-South-1_2018071717_BACKUP.xlsx'

checklist = pd.read_excel(r'C:\Users\Benson.Chen\Desktop\Client list-0417.xlsx', sheet_name='Check')

CHN_DQ_backupwriter = pd.ExcelWriter(r'C:\Users\Benson.Chen\Desktop\CHN-DQ_CM-South-1_2018071717_BACKUP.xlsx', engine='openpyxl')
backupbook = load_workbook(CHN_DQ_backupwriter.path)
CHN_DQ_backupwriter.book = backupbook
commonlist = pd.read_excel(backupfilepath, sheet_name='company_common_list', sort = False)

for index, company in commonlist.iterrows():
    name = company['Company Local Name']
    checks = checklist[checklist['Company Full Name'] == name]
    if checks.empty:
        continue
    else:
        checks = checks[pd.notnull(checks['Office Address'])]
        if checks.empty:
            continue
        commonlist.ix[commonlist['Source ID'] == company['Source ID'],'Billing Address line1 (Street/Road)'] = checks['Office Address'].iloc[0]

commonlist.to_excel(CHN_DQ_backupwriter, index=False, header=True, columns=company_colnames,
                                    sheet_name='company_addressenrich_list')

CHN_DQ_backupwriter.save()
CHN_DQ_backupwriter.close()