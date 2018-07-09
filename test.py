import pandas as pd
import logging
import numpy as np
import re
col = ['col1','col2', 'col3']
test = pd.DataFrame([['a','b',1], ['a', 'b', 2], ['c', 'b', 0]],columns=col)

test2 = test[test['col3'] == 3]
print(test2.empty)
print(pd.notnull(test2['col2']).bool())

# l = ['aa', 'b', 'dvvvd']
# for  cl in test.iloc[:,1:]:
#     print(list(test[cl]))
#print(test.ix[1,cl])

suffix = [r'\.com$', r'\.cn$', r'\.cc$', r'\.uk$', r'\.fr$', r'\.hk$', r'\.tw$']
email = 'ss   s@cc.co.m'
#logging.warning('test')
#print(test.duplicated(subset=['col1', 'col2'], keep=False))

