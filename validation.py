# -*- coding: utf-8 -*-
"""
Created on Thu July 9th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pandas as pd
import numpy as np
import re
from openpyxl import load_workbook


path = r'C:\Users\Benson.Chen\JLL\TDIM-GZ - Documents\Capforce\Shared Files'
lastname_list = pd.read_excel(path + '\LastName.xlsx', sheet_name='Sheet2', sort=False)
geo_list = pd.read_excel(path + '\China City&District List.xlsx', sheet_name='district-full', sort=False)
null_list = [r'^\s*null\s*$', r'^\s*nan\s*$', r'^\s*n/*a\s*$', r'^\s*tbd\s*$', r'^\s*-\s*$', r'^\s*$']
company_common_suffix = ['股份', '有限', '责任', '公司', '集团', '企业', '控股', '实业']
company_common_func = ['银行', '置业', '房地产','地产', '开发', '银行', '投资', '基金', '证券', '资本', '物业', '服务', '管理', '资产']

# company_list = pd.read_excel(r'C:\Users\Benson.Chen\Desktop\icg-CF Data Load-20180704_dqreview.xlsx', sheet_name='Full Company', sort=False)
# contact_input_list = pd.read_excel(r'C:\Users\Benson.Chen\JLL\TDIM-GZ - Documents\Capforce\ICG\From ICG\icg-Contact Check_Rannie 20180613.xlsx', sheet_name='Contact', sort=False)

# Deduplicate company by name
def dedup_company(company_common_list, contact_common_list):
    company_common_list['ComName_temp'] = None
    company_common_list['vc_Deduplicate'] = None
    company_common_list['vc_Load'] = None
    company_common_list['vc_Master ID'] = None
    for index, company in company_common_list.iterrows():
        if pd.notna(company['Company_Local_Name']):
            company_common_list.ix[index, 'ComName_temp'] = extract_keyword(company['Company_Local_Name'])
        else:
            company_common_list.ix[index, 'ComName_temp'] = format_space(str(company['Company_Name']).strip().lower())
    company_common_list['vc_Deduplicate'] = company_common_list.duplicated(subset=['ComName_temp'], keep=False)
    company_common_list['vc_Deduplicate'] = company_common_list['vc_Deduplicate'].apply(lambda x: False if x else True)
    # Duplicate list needs review
    company_duplicate_list = company_common_list[company_common_list['vc_Deduplicate'] == False]
    company_duplicate_list['vc_Load'] = False
    # Full duplicate list
    company_duplicate_full = company_duplicate_list
    company_duplications = list(company_duplicate_list.groupby(['ComName_temp']).count().index)
    for dup in company_duplications:
        company_dup_group = company_duplicate_list[company_duplicate_list['ComName_temp'] == dup]
        company_masterid, company_common_list, company_dup_group = dedup_get_master(company_common_list, company_dup_group)

        if company_masterid == None:
            continue
        else:
            company_common_list, contact_common_list = dedup_fix(company_common_list, contact_common_list, company_dup_group)
            company_duplicate_list = company_duplicate_list[company_duplicate_list['ComName_temp'] != dup]

    return company_duplicate_list, company_duplicate_full, company_common_list, contact_common_list

# Deduplicate company with staging data
def dedup_comany_db (company_list, company_db_list):


    return 0

# Remove duplicate, fix contact source company id
def dedup_fix(company_common_list, contact_common_list, company_duplicate_list):
    company_remove_list = company_duplicate_list[company_duplicate_list['vc_Load'] == False]
    for index, company in company_remove_list.iterrows():
        sourceid = company['Source_ID']
        masterid = company['vc_Master ID']
        company_common_list = company_common_list[company_common_list['Source_ID'] != sourceid]
        contact_common_list.loc[contact_common_list['Source Company ID'] == sourceid, 'Source Company ID'] = masterid
    return company_common_list, contact_common_list

# Get master duplicate, if multiple duplicates
def dedup_get_master(company_common_list, company_dup_group):
    if company_dup_group.empty:
        return None
    master_address = company_dup_group.ix[company_dup_group['Billing_Address'].dropna().duplicated(keep=False).index, 'Billing_Address']
    master_city = company_dup_group.ix[company_dup_group['City'].dropna().duplicated(keep=False).index, 'City']
    if len(master_city) > 1:
        master_city = master_city.iloc[0]
    master_state = company_dup_group.ix[company_dup_group['State'].dropna().duplicated(keep=False).index, 'State']
    if len(master_state) > 1:
        master_state = master_state.iloc[0]
    master_country = company_dup_group.ix[company_dup_group['Country'].dropna().duplicated(keep=False).index, 'Country']
    if len(master_country) > 1:
        master_country = master_country.iloc[0]
    master_phone = company_dup_group.ix[company_dup_group['Phone'].dropna().duplicated(keep=False).index, 'Phone']
    master_email = company_dup_group.ix[company_dup_group['Email'].dropna().duplicated(keep=False).index, 'Email']
    master_website = company_dup_group.ix[company_dup_group['Website'].dropna().duplicated(keep=False).index, 'Website']

    # If multiple duplicates contain details, no master id
    if len(master_address) > 1 or len(master_phone) > 1 or len(master_email) > 1 or len(master_website) > 1:
        return None, company_common_list, company_dup_group
    else:
        company_masterid = list(company_dup_group['Source_ID'])[0]
        if not master_address.empty:
            company_common_list.ix[company_common_list['Source_ID'] == company_masterid, 'Billing_Address'] = list(master_address.values)[0]
        if not master_city.empty:
            company_common_list.ix[company_common_list['Source_ID'] == company_masterid, 'City'] = list(master_city.values)[0]
        if not master_state.empty:
            company_common_list.ix[company_common_list['Source_ID'] == company_masterid, 'State'] = list(master_state.values)[0]
        if not master_country.empty:
            company_common_list.ix[company_common_list['Source_ID'] == company_masterid, 'Country'] = list(master_country.values)[0]
        if not master_phone.empty:
            company_common_list.ix[company_common_list['Source_ID'] == company_masterid, 'Phone'] = list(master_phone.values)[0]
        if not master_website.empty:
            company_common_list.ix[company_common_list['Source_ID'] == company_masterid, 'Website'] = list(master_website.values)[0]
        if not master_email.empty:
            company_common_list.ix[company_common_list['Source_ID'] == company_masterid, 'Email'] = list(master_email.values)[0]
        company_dup_group.ix[company_dup_group['Source_ID'] == company_masterid, 'vc_Load'] = True
        company_dup_group.ix[company_dup_group['Source_ID'] != company_masterid, 'vc_Master ID'] = company_masterid
        return company_masterid, company_common_list, company_dup_group

# Enrich company address
def enrich_address(address):
    dcities = geo_list[geo_list['Level ID'] == 0]
    states = geo_list[geo_list['Level ID'] == 1]
    cities = geo_list[geo_list['Level ID'] == 2]
    districts = geo_list[geo_list['Level ID'] == 3]
    state = None
    city = None
    district = None
    street = None
    if address == None:
        return state, city, district, street
    # Find direct city
    for index, d in dcities:
        if d['Full Name'] in address or d['Name'] in address:
            state = d['Name']
            city = d['Name']
            address = address.replace(d['Full Name'], '')
            address = address.replace(d['Name'], '')
            break
    # Find state
    for index, s in states:
        if s['Full Name'] in address or s['Name'] in address:
            state = s['Name']
            address = address.replace(s['Full Name'], '')
            address = address.replace(s['Name'], '')
            break
    # Find city
    for index, c in cities:
        if c['Full Name'] in address or c['Name'] in address:
            city = c['Name']
            address = address.replace(c['Full Name'], '')
            address = address.replace(c['Name'], '')
            break
    # Find district
    for index, dis in districts:
        if dis['Full Name'] in address or dis['Name'] in address:
            district = dis['Name']
            address = address.replace(dis['Full Name'], '')
            address = address.replace(dis['Name'], '')
            break
    street = address
    return state, city, district, street
# TODO: Zipcode fill

# Enrich company and contact detail by business return
def enrich_business(company_scrapy_list, company_business_result, company_colnames):
    for index, company in company_business_result.iterrows():
        sourceid = company['Source_ID']
        company['Full Address'] = str(company['District']) + str(company['Billing_Address'])
        company['Full Address'] = format_space(company['Full Address']).strip()
        company_scrapy_list = company_scrapy_list[~(company_scrapy_list['Source_ID'] == sourceid)]
        company_scrapy_list = company_scrapy_list.append(company, ignore_index=True)

    return company_scrapy_list

# Enrich company detail by dq
def enrich_dq(company_dedup_list,company_dq_result):
    for index, company in company_dq_result.iterrows():
        sourceid = company['Integration_MDM_Ids__c']
        company_dedup_list.loc[company_dedup_list['Source_ID'] == sourceid, 'Company_Name'] = company['Name']
        company_dedup_list.loc[company_dedup_list['Source_ID'] == sourceid, 'Billing_Address'] = company['BillingStreet']
        company_dedup_list.loc[company_dedup_list['Source_ID'] == sourceid, 'Postal_Code'] = company['BillingPostalCode']
        company_dedup_list.loc[company_dedup_list['Source_ID'] == sourceid, 'City'] = company['BillingCity']
        company_dedup_list.loc[company_dedup_list['Source_ID'] == sourceid, 'State'] = company['BillingState']
        company_dedup_list.loc[company_dedup_list['Source_ID'] == sourceid, 'Country'] = company['BillingCountry']
        company_dedup_list.loc[company_dedup_list['Source_ID'] == sourceid, 'Website'] = company['Website']
        company_dedup_list.loc[company_dedup_list['Source_ID'] == sourceid, 'dq_New'] = False
    return company_dedup_list

# Enrich company detail by qichacha
def enrich_scrapy(company, scrapy):
    if scrapy['英文名'] != None:
        company['Company_Name'] = scrapy['英文名']
    company['Company_Local_Name'] = scrapy['公司名称']
    if scrapy['境外公司'] == True or scrapy['境外公司'] == 'True':
        company['Country'] = ''
    else:
        company['Country'] = 'China'
    if company['Billing_Address'] == None:
        state, company['City'], company['District'], company['Billing_Address'] = enrich_address(scrapy['地址'])
        if scrapy['所属地区'] is not None and len(scrapy['所属地区']) > 1:
            company['State'] = scrapy['所属地区']
        else:
            company['State'] = state
    # No district field in system for now
    company['Full Address'] = company['District'] + company['Billing_Address']
    company['Full Address'] = format_space(company['Full Address']).strip()
    company['Company_Type'] = scrapy['公司类型']
    company['Phone'] = scrapy['电话']
    company['Website'] = scrapy['网址']
    company['Email'] = scrapy['邮箱']
    company['Industry'] = scrapy['所属行业']
    company['Employee'] = scrapy['参保人数']

    return company

# Extract company keyword
def extract_keyword(company_name):
    company_keyword = str(company_name).strip().replace(' ','')
    dcities = geo_list[geo_list['Level ID'] == 0]
    states = geo_list[geo_list['Level ID'] == 1]
    cities = geo_list[geo_list['Level ID'] == 2]
    state = None
    city = None
    if company_keyword == None:
        return None
    # # Find direct city
    # for index, d in dcities.iterrows():
    #     if d['Full Name'] in company_keyword or d['Name'] in company_keyword:
    #         state = d['Name']
    #         city = d['Name']
    #         company_keyword = company_keyword.replace(d['Full Name'], '')
    #         company_keyword = company_keyword.replace(d['Name'], '')
    #         break
    # # Find state
    # for index, s in states.iterrows():
    #     if s['Full Name'] in company_keyword or s['Name'] in company_keyword:
    #         state = s['Name']
    #         company_keyword = company_keyword.replace(s['Full Name'], '')
    #         company_keyword = company_keyword.replace(s['Name'], '')
    #         break
    # # Find city
    # for index, c in cities.iterrows():
    #     if c['Full Name'] in company_keyword or c['Name'] in company_keyword:
    #         city = c['Name']
    #         company_keyword = company_keyword.replace(c['Full Name'], '')
    #         company_keyword = company_keyword.replace(c['Name'], '')
    #         break
    # # Find company function
    # for cf in company_common_func:
    #     if cf in company_keyword:
    #         company_keyword = company_keyword.replace(cf, '')
    # Find company suffix
    for cs in company_common_suffix:
        if cs in company_keyword:
            company_keyword = company_keyword.replace(cs, '')
    # Remove ()
    company_keyword = re.sub(r'\(.*\)', '', company_keyword)
    company_keyword = re.sub(r'（.*）', '', company_keyword)
    return company_keyword

# Keep only one space
def format_space(str):
    space = re.compile(r'\s\s+')
    str = space.subn('', str)
    return str[0].strip()

# Initial company
def init_list(raw_list, colnames, mode):
    for col in colnames:
        for i in null_list:
            if col not in list(raw_list) or pd.isnull(raw_list[col]).all():
                break
            else:
                raw_list[col] = raw_list[col].str.lower().replace(i, np.nan, regex=True)
                #raw_list[col] = raw_list[col].str.replace('nan', '')
    if mode == 'company':
        raw_list['dq_New'] = True
        raw_list['Company_Local_Name'] = raw_list.loc[pd.notnull(raw_list['Company_Local_Name']), 'Company_Local_Name'].apply(lambda x: x.replace(' ',''))
    # TODO: Null, '', space, 'N/A', '-' check
    return raw_list

# Merger duplicate company, no longer used
def merge_company(company_common_list, contact_common_list, company_dup_group, company_masterid):
    company_dup_group.ix[company_dup_group['Source_ID'] != company_masterid, 'vc_Load'] = False
    print(company_masterid)
    print(company_masterid.tolist())
    company_dup_group[company_dup_group['Source_ID'] != company_masterid, 'vc_Master ID'] = company_masterid.tolist()
    company_common_list, contact_common_list = dedup_fix(company_common_list, contact_common_list, company_dup_group)
    return company_common_list, contact_common_list

# Check company and contact cross
def validate_common(company_init_list, contact_raw_list):

    # Fill contact id
    #if contact_raw_list['Source_ID'].isnull().sum() == len(contact_raw_list):
    contact_raw_list['Source_ID'] = list(range(1, (len(contact_raw_list) + 1)))
    company_source_list = list(company_init_list['Source_ID'])
    contact_source_list = list(contact_raw_list['Source Company ID'].astype(str))
    common_source_list = list(set(company_source_list).intersection(set(contact_source_list)))
    company_common_list = company_init_list[company_init_list['Source_ID'].isin(common_source_list)]
    contact_common_list = contact_raw_list[contact_raw_list['Source Company ID'].isin(common_source_list)]
    return company_common_list, contact_common_list

# Validate company
def validate_company(company_dq_list, company_scrapy_result, company_colnames):
    company_scrapy_verify = pd.DataFrame(columns=list(company_colnames))
    for index, company in company_dq_list.iterrows():
        if company['dq_New'] == False:
            continue
        sourceid = company['Source_ID']
        scrapy_list = company_scrapy_result[company_scrapy_result['Source_ID'] == sourceid]
        scrapy_best = scrapy_list[scrapy_list['Confidence'] == 0]
        # If multiple best match, get first one with address
        # If no best match, return top 5 result
        if (len(scrapy_best) > 1):
            scrapy_best = scrapy_best[scrapy_best['地址'].notnull()].iloc[0]
        elif (len(scrapy_best) < 1):
            # scrapy_verify = scrapy_list[scrapy_list['地址'].notnull()].sort_values(by='Confidence')[0:5]
            # company_scrapy_verify = company_scrapy_verify.append(scrapy_verify)
            company_scrapy_verify = company_scrapy_verify.append(company)
            continue
        company_dq_list.iloc[index] = enrich_scrapy(company, scrapy_best)

    return company_dq_list, company_scrapy_verify

# Check contacts
def validate_contacts(contact_dedup_list, contact_colnames, company_load_list):
    contact_validate_list = pd.DataFrame(columns=contact_colnames)

    for index, contact in contact_dedup_list.iterrows():
        sourceid = contact['Source Company ID']
        company = company_load_list.loc[company_load_list['Source_ID'] == sourceid]
        contact['Reject_Reason'] = ''
        contact = validate_name(contact)
        contact = validate_email(contact,company)
        contact['Company_Name'] = list(company['Company_Name'])[0]
        contact['vc_Load'] = contact['vn_Name_Check'] and contact['ve_Email_Check']

        contact_validate_list = contact_validate_list.append(contact, ignore_index=True)

    # Deduplicate by name and email
    contact_validate_list['Fname_temp'] = contact_validate_list['First_Name'].apply(lambda x: x.lower())
    contact_validate_list['Lname_temp'] = contact_validate_list['Last_Name'].apply(lambda x: x.lower())
    # Switch True and False
    contact_validate_list['vc_Deduplicate'] = contact_validate_list.duplicated(subset=['Fname_temp', 'Lname_temp', 'Email'], keep=False)
    contact_validate_list['vc_Deduplicate'] = contact_validate_list['vc_Deduplicate'].apply(lambda x: False if x else True)
    contact_load = ((np.array(list(contact_validate_list['vc_Load']))) & (np.array(list(contact_validate_list['vc_Deduplicate']))))

    contact_validate_list['vc_Load'] = pd.Series(contact_load).values
    # TODO: no phone and email
    return contact_validate_list

# Validate email
def validate_email(contact, company):
    eformat = False
    esuffix = False
    epersonal = False
    edomain = False
    suffix = [r'\.com$', r'\.cn$', r'\.org$', r'\.net$', r'\.cc$', r'\.uk$', r'\.fr$', r'\.hk$', r'\.tw$', r'\.au$', r'\.jp$', r'\.sg$']
    personal = ['@gmail.com', '@hotmail.com', '@yahoo.com', '@sina.com', '@vip.sina.com', '@163.com', '@126.com', '@qq.com', '@vip.qq.com', '@139.com']

    if pd.notna(contact['Email']):
        # Lower and no space
        email = contact['Email'].lower().replace(' ','')
    else:
        echeck = eformat and esuffix and (epersonal or edomain)
        contact['ve_Email_Format'] = eformat
        contact['ve_Email_Suffix'] = esuffix
        contact['ve_Email_Domain'] = epersonal or edomain
        contact['ve_Email_Check'] = echeck
        contact['Reject_Reason'] = contact['Reject_Reason'] + 'No Email;  '
        return contact
    # TODO: Email format check
    # Email must contain @
    if('@' in email):
        eformat = True
    else:
        contact['Reject_Reason'] = contact['Reject_Reason'] + 'Email without @;  '

    # Email suffix check
    for s in suffix:
        if re.search(re.compile(s, re.I), email) != None:
            esuffix = True
            break
    if not esuffix:
        contact['Reject_Reason'] = contact['Reject_Reason'] + 'Email invalid suffix;  '

    # Email personal check
    for p in personal:
        if p in email:
            epersonal = True
            break

    # Email domain check
    if not company.empty:
        if pd.notna(company['Website']).bool():
            company_website = list(company['Website'])[0]
            domain = company_website.split('.')[1].split('.')[1].lower()
            if domain in email:
                edomain = True
            else:
                contact['Reject_Reason'] = contact['Reject_Reason'] + 'Email domain not match;  '
        elif pd.notna(company['Email']).bool():
            company_email = list(company['Email'])[0]
            domain = company_email.split('@')[1].split('.')[0].lower()
            if domain in email:
                edomain = True
            else:
                contact['Reject_Reason'] = contact['Reject_Reason'] + 'Email domain not match;  '
        else:
            contact['Reject_Reason'] = contact['Reject_Reason'] + 'Email domain not match;  '
        # else:
        #     companyname = contact['Company_Name'].split(' ',1)[0].lower()
        #     if companyname in contact['Email']:
        #         edomain = True
        #     else:
        #         contact['Reject_Reason'] = contact['Reject_Reason'] + 'Email domain not match;  '
    else:
        contact['Reject_Reason'] = contact['Reject_Reason'] + 'Company not exisits;  '

    # Email check
    echeck = eformat and esuffix and (epersonal or edomain)
    contact['ve_Email_Format'] = eformat
    contact['ve_Email_Suffix'] = esuffix
    contact['ve_Email_Domain'] = epersonal or edomain
    contact['ve_Email_Check'] = echeck
    return contact

# Validate name
def validate_name(contact):
    nfirst = True
    nlast = False
    nspace = False

    # Remove more than two space and starting/ending space, format Last_Name
    contact['Last_Name'] = format_space(contact['Last_Name'].lower().capitalize())
    contact['First_Name'] = format_space(contact['First_Name'])

    # Check First_Name and Last_Name misplace

    for lan in lastname_list.iloc[:,1:]:
        lastnames = list(lastname_list[lan])
        if contact['Last_Name'] in lastnames:
            contact['vn_Lastname_CN'] = lastname_list.ix[lastnames.index(contact['Last_Name']),'简体中文']
            nlast = True
            break
        elif contact['First_Name'] in lastnames:
            nfirst = False
            break
    if not (nlast or nfirst):
        contact['Reject_Reason'] = contact['Reject_Reason'] + 'First_Name and Last_Name misplace;  '

    # Check name contains space
    if ' ' in contact['First_Name'] or ' ' in contact['Last_Name']:
        contact['Reject_Reason'] = contact['Reject_Reason'] + 'name contains space;  '
    else:
        nspace = True

    # Name check
    ncheck = (nlast or nfirst) and nspace
    contact['vn_Name_Swap'] = (nlast or nfirst)
    contact['vn_Name_Space'] = nspace
    contact['vn_Name_Check'] = ncheck

    return contact

