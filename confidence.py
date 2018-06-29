import pandas as pd
import numpy as np
import Levenshtein as lv
company_input_list = pd.read_excel(r'C:\Users\Benson.Chen\JLL\TDIM-GZ - Documents\Capforce\ICG\From ICG\icg-Company Check.xlsx',sheet_name='Company')
company_scrapy_list = pd.read_excel(r'C:\Users\Benson.Chen\Desktop\Capforce\ICG\QiChaCha\QiChaCha2018062223.xlsx',sheet_name='Sheet1')

# print(company_input_list.columns)
# print(company_scrapy_list.columns)
# for index, company in company_input_list.iterrows():
#     print(company)
#     break

print(lv.distance(u'宝钢集团', u'北京宝钢集团'))