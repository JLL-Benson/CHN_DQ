import pandas as pd
import numpy as np

path = r'C:\Users\Benson.Chen\JLL\TDIM-GZ - Documents\Capforce\ICG\From ICG'

company_origin_list = pd.read_excel(path + '\icg-Company Check.xlsx', sheet_name='Company')
company_eve_list = pd.read_excel(path + '\icg-Company Check_Eve 20180612.xlsx', sheet_name='Company')
company_rannie_list = pd.read_excel(path + '\icg-Company Check_Rannie 20180625.xlsx', sheet_name='Company')
colnames = list(company_origin_list)
colnames.append('Eve')
colnames.append('Rannie')
company_eve_list['Name Check'] = False
company_eve_list['Source Check'] = None
company_rannie_list['Name Check'] = False
company_rannie_list['Source Check'] = None
company_combine_list = pd.DataFrame(columns=colnames)
company_origin_list['Eve'] = False
company_origin_list['Rannie'] = False

for index, company in company_origin_list.iterrows():
    company_origin_name = company['Company Name']
    print(company_origin_name)
    company_eve = company_eve_list[company_eve_list['Company Name'] == company_origin_name]
    company_rannie = company_rannie_list[company_rannie_list['Company Name'] == company_origin_name]
    if (not company_rannie.empty) and (not company_eve.empty):
        company_origin_list.loc[company_origin_list['Company Name'] == company_origin_name,'Eve'] = True
        company_origin_list.loc[company_origin_list['Company Name'] == company_origin_name,'Rannie'] = True
        company_rannie_list.loc[company_rannie_list['Company Name'] == company_origin_name,'Name Check'] = True
        company_rannie_list.loc[company_rannie_list['Company Name'] == company_origin_name, 'Source Check'] = company['Source ID']
        company_eve_list.loc[company_eve_list['Company Name'] == company_origin_name,'Name Check'] = True
        company_eve_list.loc[company_eve_list['Company Name'] == company_origin_name, 'Source Check'] = company['Source ID']
    elif company_rannie.empty:
        company_eve_list.loc[company_eve_list['Company Name'] == company_origin_name,'Name Check'] = True
        company_origin_list.loc[company_origin_list['Company Name'] == company_origin_name,'Eve'] = True
        company_eve_list.loc[company_eve_list['Company Name'] == company_origin_name, 'Source Check'] = company['Source ID']
    elif company_eve.empty:
        company_rannie_list.loc[company_rannie_list['Company Name'] == company_origin_name,'Name Check'] = True
        company_origin_list.loc[company_origin_list['Company Name'] == company_origin_name,'Rannie'] = True
        company_rannie_list.loc[company_rannie_list['Company Name'] == company_origin_name, 'Source Check'] = company['Source ID']


writer = pd.ExcelWriter(r'C:\Users\Benson.Chen\Desktop\Capforce\ICG\icg-Business Feedback Check-20180704.xlsx', engine='xlsxwriter')
company_origin_list.to_excel(writer, index=False, header=True, columns= list(company_origin_list), sheet_name='Origin')
company_eve_list.to_excel(writer, index=False, header=True, columns= list(company_eve_list), sheet_name='Eve')
company_rannie_list.to_excel(writer, index=False, header=True, columns= list(company_rannie_list), sheet_name='Rannie')
writer.save()