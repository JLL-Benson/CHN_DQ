# -*- coding: utf-8 -*-
"""
Created on Thu June 10th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pandas as pd
import numpy as np

path = r'C:\Users\Benson.Chen\JLL\TDIM-GZ - Documents\Capforce\ICG\From ICG'
writer = pd.ExcelWriter(r'C:\Users\Benson.Chen\Desktop\Capforce\icg-CF Data Load-20180627.xlsx', engine='xlsxwriter')
company_origin_list = pd.read_excel(path + '\icg-Company Check.xlsx', sheet_name='Company')
company_eve_list = pd.read_excel(path + '\icg-Company Check_Eve 20180612.xlsx', sheet_name='Company')
company_rannie_list = pd.read_excel(path + '\icg-Company Check_Rannie 20180625.xlsx', sheet_name='Company')
colnames = list(company_origin_list)
company_combine_list = pd.DataFrame(columns=colnames)

eve= 0
rannie = 0
origin = 0
for index, company in company_origin_list.iterrows():
    sourceid = company['Source ID']
    company_eve = company_eve_list[company_eve_list['Source ID'] == sourceid]
    company_rannie = company_rannie_list[company_rannie_list['Source ID'] == sourceid]
    if company_rannie.empty and company_eve.empty:
        company_combine_list.append(company)
    elif company_rannie.empty:
        if company_eve['Billing Address line1 (Street/Road)'].notnull().bool():
            company_combine_list = company_combine_list.append(company_eve, ignore_index=True)
        else:
            company_combine_list = company_combine_list.append(company, ignore_index=True)
    elif company_eve.empty:
        if company_rannie['Billing Address line1 (Street/Road)'].notnull().bool():
            company_combine_list = company_combine_list.append(company_rannie, ignore_index=True)
        else:
            company_combine_list = company_combine_list.append(company, ignore_index=True)
    elif (company_eve['Billing Address line1 (Street/Road)'].notnull().bool()) and (company_rannie['Billing Address line1 (Street/Road)'].notnull().bool()):
        if (company_eve['City'].to_string  == 'Beijing' or company_eve['City'].to_string  == '北京' or company_eve['City'].to_string  == '北京市'):
            company_combine_list = company_combine_list.append(company_rannie, ignore_index=True)
        else:
            company_combine_list = company_combine_list.append(company_eve, ignore_index=True)
    elif (company_eve['Billing Address line1 (Street/Road)'].notnull().bool()):
        company_combine_list = company_combine_list.append(company_eve, ignore_index=True)
    elif (company_rannie['Billing Address line1 (Street/Road)'].notnull().bool()):
        company_combine_list = company_combine_list.append(company_rannie, ignore_index=True)
    else:
        company_combine_list = company_combine_list.append(company, ignore_index=True)

print(len(company_origin_list), eve, rannie, origin)
# Concentrate address
company_combine_list['Full Address'] = company_combine_list['District'].apply(lambda x: x if x is not np.nan else '') + company_combine_list['Billing Address line1 (Street/Road)'].apply(lambda x: x if x is not np.nan else '') +company_combine_list['Billing Address line2 (Building Name)'].apply(lambda x: x if x is not np.nan else '') + company_combine_list['Billing Address line3(Suite, Level, Floor, Unit)'].apply(lambda x: x if x is not np.nan else '')
company_combine_list['Full Address'] = company_combine_list['Full Address'].apply(lambda  x: x.strip())
company_output_list = company_combine_list[company_combine_list['Full Address'] != '']


# Contact
contact_rannie_list = pd.read_excel(path + '\icg-Contact Check_Rannie 20180613.xlsx', sheet_name='Contact')

# Contacts in Company list, Email is not empty, Drop is not Y,
contact_output_list = contact_rannie_list[contact_rannie_list['Source Company ID'].isin(company_output_list['Source ID'])]
contact_output_list = contact_output_list[contact_output_list['Email'].notnull()]
contact_output_list = contact_output_list[contact_output_list['Drop'] != 'Y']
contact_output_list = contact_output_list[contact_output_list['E_Verified'] != 'N']
# Correct first name and last name
contact_nameswitch = contact_output_list['First Name2'].notnull()
contact_output_list.loc[contact_nameswitch,'First Name'] = contact_output_list.loc[contact_nameswitch,'First Name2']
contact_output_list.loc[contact_nameswitch,'Last Name'] = contact_output_list.loc[contact_nameswitch,'Last Name2']

company_output_list.to_excel(writer, index=False, header=True, columns= list(company_output_list), sheet_name='Company')
contact_output_list.to_excel(writer, index=False, header=True, columns= list(contact_output_list), sheet_name='Contact')
company_combine_list.to_excel(writer, index=False, header=True, columns= list(company_combine_list), sheet_name='Full Company')
company_eve_list.to_excel(writer, index=False, header=True, columns= list(company_eve_list), sheet_name='Eve Company')
company_rannie_list.to_excel(writer, index=False, header=True, columns= list(company_rannie_list), sheet_name='Rannie Company')
contact_rannie_list.to_excel(writer, index=False, header=True, columns= list(contact_rannie_list), sheet_name='Rannie Contact')
writer.save()