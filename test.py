import pandas as pd
import logging
import numpy as np
import re
import validation

def tryc(x,y):
    return y,x

#print(tryc(1,2)[0])

col = ['col1','col2', 'col3']
test = pd.DataFrame([['a ss ddd','ab',1], [' dvde dd   d', 'b', 2], ['c ', 'b', 0],['a','c',3], ['a', 'c', 'acdefg']],columns=col)
rawpath = r'C:\Users\Benson.Chen\JLL\TDIM-GZ - Documents\Capforce\CM - South\CHN-DQ_CM-South-1_2018071317_RAWC.xlsx'
# company_raw_list = pd.read_excel(rawpath, sheet_name='Company', sort=False)

# for i,r in test.iterrows():
    #print(validation.format_space(r['col1'].lower()) )
    # print(type(r))
    # print(type(r['col1']))
    # test.ix[i,'col5'] = (r['col1'].strip().replace(' ',''))
# print(test['col5'])
# print(test.duplicated(subset=['col5'], keep=False))
#print(test[test['col1']])
