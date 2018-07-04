# -*- coding: utf-8 -*-
"""
Created on Thu July 2nd 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pandas as pd
import numpy as np
import Levenshtein as lv
import sys
company_input_list = pd.read_excel(r'C:\Users\Benson.Chen\JLL\TDIM-GZ - Documents\Capforce\ICG\From ICG\icg-Company Check.xlsx',sheet_name='Company')
company_scrapy_list = pd.read_excel(r'C:\Users\Benson.Chen\Desktop\Capforce\ICG\QiChaCha\QiChaCha2018062223.xlsx',sheet_name='Sheet1')

# For english, split company name by space and calculate distance by matching words
def lvEN(com1, com2):
    list1 = com1.split(' ')
    #print(list1)
    list2 = com2.split(' ')
    #print(list2)
    if len(list1) == 0 or len(list2) == 0:
        return len(list1) + len(list2)
    d = [ list(range(len(list2) + 1)) for i in range(len(list1) + 1)]
    for i in range(1,len(list1)+1):
        for j in range(1,len(list2)+1):
            delete = d[i-1][j] + 1
            insert = d[i][j-1] + 1
            sub = d[i-1][j-1]
            if list1[i-1] != list2[j-1]:

                sub+=1
            d[i][j] = min(delete, insert, sub)
    return d[i][j]

# Check name contain Chinese
def hasCHN(str):
    for ch in str.encode().decode('utf-8'):
        if u'\u4e00' <= ch <= u'\u9fff':
            return True
    return False

def getConfidence(company_scrapy):
    company_search_key = company_scrapy['搜索词']
    company_response_name = company_scrapy['公司名称']
    if pd.isna(company_search_key) or pd.isna(company_response_name):
        return None
    elif hasCHN(company_search_key) and hasCHN(company_response_name):
        return lv.distance(company_search_key,company_response_name)
    elif not hasCHN(company_search_key) and not hasCHN(company_response_name):
        return lvEN(company_search_key, company_response_name)
    else:
        return None

company_scrapy_list['Confidence'] = company_scrapy_list.apply(getConfidence, axis=1)
company_scrapy_list.to_excel(r'C:\Users\Benson.Chen\Desktop\testconfidence.xlsx', sheet_name='Sheet1', index=False)

