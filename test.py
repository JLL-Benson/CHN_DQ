import pandas as pd
import pymssql

def tryc(x,y):
    return y,x

#print(tryc(1,2)[0])

col = ['col1','col2', 'col3']
test = pd.DataFrame([[None,'ab',1]],columns=col)
test2 = [1, 2,3,1]
test.ix[0,'col2'] = str(test.loc[test['col1'].dropna().duplicated(keep=False).index, 'col1'])
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
#print(test[test['col1']])
# pd.read_excel(r'C:\Users\Benson.Chen\Desktop\a.xlsx')