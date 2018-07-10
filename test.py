import pandas as pd
import logging
import numpy as np
import re

def tryc(x,y):
    return y,x

#print(tryc(1,2)[0])

col = ['col1','col2', 'col3']
test = pd.DataFrame([['a','ab',1], ['a', 'b', 1], ['c', 'b', 0],['a','c',], ['a', 'c', 'acdefg']],columns=col)
test3 = test.ix[4,'col3']
test2 = test.ix[0]
print(test3)
if test2['col1'] in test3:
    print(test3.replace(test2['col2'],''))
    print(test3.replace(test2['col1'],''))
#print(test2)
test2['col1'] = 'd'

# for index, r in test.iterrows():
#     test.ix[index] = test2
#     print(test.ix[index])
#     print(index)
#print(test[test['col3'].notnull()].sort_values(by='col3')[0:2])
# test2 = test[test['col3'] == 3]
# print(test2.empty)
# print(pd.notnull(test2['col2']).bool())
#
# # l = ['aa', 'b', 'dvvvd']
# # for  cl in test.iloc[:,1:]:
# #     print(list(test[cl]))
# #print(test.ix[1,cl])
#
# suffix = [r'\.com$', r'\.cn$', r'\.cc$', r'\.uk$', r'\.fr$', r'\.hk$', r'\.tw$']
# email = 'ss   s@cc.co.m'
#logging.warning('test')
#print(test.duplicated(subset=['col1', 'col2'], keep=False))


