import pandas as pd
#import pymssql
import requests
from bs4 import BeautifulSoup
import urllib
import re
import datetime
import pandas as pd
import numpy as np
import random
import time
from openpyxl import load_workbook
from run import rawfilepath
from run import backupfilepath
import validation as vd
import db

def tryc(x,y):
    return y,x

#print(tryc(1,2)[0])

col = ['col1','col2', 'ComName_temp']
test = pd.DataFrame([[1,'ab',3],[1,2,4], [1,2,3]],columns=col)
test2 = pd.DataFrame([[2,'ab',1],[2,2,3],[2,'ab',1]],columns=col)
# print(set(test['col1']).difference(set(test2['col1'])))
#print(test)

#test.ix[0,'col2'] = str(test.loc[test['col1'].dropna().duplicated(keep=False).index, 'col1'])
#print(len(test))
# print(test.loc[test['col1'].dropna().duplicated(keep=False).index, 'col1'])
#print(test.ix[1])
# company_raw_list = pd.read_excel(rawpath, sheet_name='Company', sort=False)

# for i,r in test.iterrows():
    #print(validation.format_space(r['col1'].lower()) )
    # print(type(r))
    # print(type(r['col1']))
    # test.ix[i,'col5'] = (r['col1'].strip().replace(' ',''))
# print(test['col5'])
# print(test.duplicated(subset=['col5'], keep=False))
# print(test[test['col1']])
# pd.read_excel(r'C:\Users\Benson.Chen\Desktop\a.xlsx')

# logs_columns = ['Source_ID', 'Entity_Type', 'Field', 'Action_Type', 'Log_From', 'Log_To']
# logs = pd.read_excel(r'C:\Users\Benson.Chen\Desktop\logs.xlsx', sheet_name='Sheet1', sort=False)
# # Source-Site-LoadRound
# sourcename = 'CM-East-1'
# # YYYYMMDDHH
# timestamp = '20180802'
# db.load_staging(logs, logs_columns, 'Logs', sourcename, timestamp)

#test3 = vd.dedup_comany_db(test2,test)
#print(test3)
