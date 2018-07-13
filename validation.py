# -*- coding: utf-8 -*-
"""
Created on Thu July 9th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pandas as pd
import numpy as np
import re
from openpyxl import load_workbook


path = r'C:\Users\Benson.Chen\Desktop\Capforce\Shared Files'
lastname_list = pd.read_excel(path + '\LastName.xlsx', sheet_name='Sheet2', sort=False)
geo_list = pd.read_excel(path + '\China City&District List.xlsx', sheet_name='district-full', sort=False)

# company_list = pd.read_excel(r'C:\Users\Benson.Chen\Desktop\icg-CF Data Load-20180704_dqreview.xlsx', sheet_name='Full Company', sort=False)
# contact_input_list = pd.read_excel(r'C:\Users\Benson.Chen\JLL\TDIM-GZ - Documents\Capforce\ICG\From ICG\icg-Contact Check_Rannie 20180613.xlsx', sheet_name='Contact', sort=False)

# Check contacts
def validate_contacts(contact_dedup_list, contact_colnames, company_load_list):
    contact_validate_list = pd.DataFrame(columns=contact_colnames)

    for index, contact in contact_dedup_list.iterrows():
        sourceid = contact['Source Company ID']
        company = company_load_list.loc[company_load_list['Source ID'] == sourceid]
        contact['Reject Reason'] = ''
        contact = validate_name(contact)
        contact = validate_email(contact,company)
        contact['Company Name'] = list(company['Company Name'])[0]
        contact['vc_Load'] = contact['vn_Name_Check'] and contact['ve_Email_Check']

        contact_validate_list = contact_validate_list.append(contact, ignore_index=True)

    # Deduplicate by name and email
    contact_validate_list['Fname_temp'] = contact_validate_list['First Name'].apply(lambda x: x.lower())
    contact_validate_list['Lname_temp'] = contact_validate_list['Last Name'].apply(lambda x: x.lower())
    # Switch True and False
    contact_validate_list['vc_Deduplicate'] = contact_validate_list.duplicated(subset=['Fname_temp', 'Lname_temp', 'Email'], keep=False)
    contact_validate_list['vc_Deduplicate'] = contact_validate_list['vc_Deduplicate'].apply(lambda x: False if x else True)
    contact_load = ((np.array(list(contact_validate_list['vc_Load']))) & (np.array(list(contact_validate_list['vc_Deduplicate']))))

    contact_validate_list['vc_Load'] = pd.Series(contact_load).values
    # TODO: no phone and email
    return contact_validate_list

# Validate name
def validate_name(contact):
    nfirst = True
    nlast = False
    nspace = False

    # Remove more than two space and starting/ending space, format last name
    contact['Last Name'] = format_space(contact['Last Name'].lower().capitalize())
    contact['First Name'] = format_space(contact['First Name'])

    # Check first name and last name misplace

    for lan in lastname_list.iloc[:,1:]:
        lastnames = list(lastname_list[lan])
        if contact['Last Name'] in lastnames:
            contact['vn_Lastname_CN'] = lastname_list.ix[lastnames.index(contact['Last Name']),'简体中文']
            nlast = True
            break
        elif contact['First Name'] in lastnames:
            nfirst = False
            break
    if not (nlast or nfirst):
        contact['Reject Reason'] = contact['Reject Reason'] + 'first name and last name misplace;  '

    # Check name contains space
    if ' ' in contact['First Name'] or ' ' in contact['Last Name']:
        contact['Reject Reason'] = contact['Reject Reason'] + 'name contains space;  '
    else:
        nspace = True

    # Name check
    ncheck = (nlast or nfirst) and nspace
    contact['vn_Name_Swap'] = (nlast or nfirst)
    contact['vn_Name_Space'] = nspace
    contact['vn_Name_Check'] = ncheck

    return contact

# Validate email
def validate_email(contact, company):
    eformat = False
    esuffix = False
    epersonal = False
    edomain = False
    suffix = [r'\.com$', r'\.cn$', r'\.org$', r'\.net$', r'\.cc$', r'\.uk$', r'\.fr$', r'\.hk$', r'\.tw$', r'\.au$', r'\.jp$', r'\.sg$']
    personal = ['@gmail.com', '@hotmail.com', '@yahoo.com', '@sina.com', '@vip.sina.com', '@163.com', '@126.com', '@qq.com', '@vip.qq.com', '@139.com']

    if pd.notnull(contact['Email']):
        # Lower and no space
        email = contact['Email'].lower().replace(' ','')
    else:
        echeck = eformat and esuffix and (epersonal or edomain)
        contact['ve_Email_Format'] = eformat
        contact['ve_Email_Suffix'] = esuffix
        contact['ve_Email_Domain'] = epersonal or edomain
        contact['ve_Email_Check'] = echeck
        contact['Reject Reason'] = contact['Reject Reason'] + 'No Email;  '
        return contact
    # TODO: Email format check
    # Email must contain @
    if('@' in email):
        eformat = True
    else:
        contact['Reject Reason'] = contact['Reject Reason'] + 'Email without @;  '

    # Email suffix check
    for s in suffix:
        if re.search(re.compile(s, re.I), email) != None:
            esuffix = True
            break
    if not esuffix:
        contact['Reject Reason'] = contact['Reject Reason'] + 'Email invalid suffix;  '

    # Email personal check
    for p in personal:
        if p in email:
            epersonal = True
            break

    # Email domain check
    if not company.empty:
        if pd.notnull(company['Website']).bool():
            company_website = list(company['Website'])[0]
            domain = company_website.split('.')[1].split('.')[1].lower()
            if domain in email:
                edomain = True
            else:
                contact['Reject Reason'] = contact['Reject Reason'] + 'Email domain not match;  '
        elif pd.notnull(company['Email']).bool():
            company_email = list(company['Email'])[0]
            domain = company_email.split('@')[1].split('.')[0].lower()
            if domain in email:
                edomain = True
            else:
                contact['Reject Reason'] = contact['Reject Reason'] + 'Email domain not match;  '
        else:
            contact['Reject Reason'] = contact['Reject Reason'] + 'Email domain not match;  '
        # else:
        #     companyname = contact['Company Name'].split(' ',1)[0].lower()
        #     if companyname in contact['Email']:
        #         edomain = True
        #     else:
        #         contact['Reject Reason'] = contact['Reject Reason'] + 'Email domain not match;  '
    else:
        contact['Reject Reason'] = contact['Reject Reason'] + 'Company not exisits;  '

    # Email check
    echeck = eformat and esuffix and (epersonal or edomain)
    contact['ve_Email_Format'] = eformat
    contact['ve_Email_Suffix'] = esuffix
    contact['ve_Email_Domain'] = epersonal or edomain
    contact['ve_Email_Check'] = echeck
    return contact

# Initial company
def init_company(company_raw_list):
    company_raw_list['dq_New'] = True
    # TODO: Null, '', space, 'N/A', '-' check
    return company_raw_list

# Validate company
def validate_company(company_dq_list, company_scrapy_result, company_colnames):
    company_scrapy_verify = pd.DataFrame(columns=list(company_colnames))
    for index, company in company_dq_list.iterrows():
        if company['dq_New'] == False:
            continue
        sourceid = company['Source ID']
        scrapy_list = company_scrapy_result[company_scrapy_result['Source ID'] == sourceid]
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

# Enrich company and contact detail by business return
def enrich_business(company_scrapy_list, company_business_result, company_colnames):
    for index, company in company_business_result.iterrows():
        sourceid = company['Source ID']
        company['Full Address'] = str(company['District']) + str(company['Billing Address line1 (Street/Road)'])
        company['Full Address'] = format_space(company['Full Address']).strip()
        company_scrapy_list = company_scrapy_list[~(company_scrapy_list['Source ID'] == sourceid)]
        company_scrapy_list = company_scrapy_list.append(company, ignore_index=True)

    return company_scrapy_list

# Enrich company detail by qichacha
def enrich_scrapy(company, scrapy):
    if scrapy['英文名'] != None:
        company['Company Name'] = scrapy['英文名']
    company['Company Local Name'] = scrapy['公司名称']
    if scrapy['境外公司'] == True or scrapy['境外公司'] == 'True':
        company['Country'] = ''
    else:
        company['Country'] = 'China'
    if company['Billing Address line1 (Street/Road)'] == None:
        state, company['City'], company['District'], company['Billing Address line1 (Street/Road)'] = enrich_address(scrapy['地址'])
        if scrapy['所属地区'] is not None and len(scrapy['所属地区']) > 1:
            company['State'] = scrapy['所属地区']
        else:
            company['State'] = state
    # No district field in system for now
    company['Full Address'] = company['District'] + company['Billing Address line1 (Street/Road)']
    company['Full Address'] = format_space(company['Full Address']).strip()
    company['Company Type'] = scrapy['公司类型']
    company['Phone'] = scrapy['电话']
    company['Website'] = scrapy['网址']
    company['Email'] = scrapy['邮箱']
    company['Industry'] = scrapy['所属行业']
    company['Employee'] = scrapy['参保人数']

    return company

# Enrich company detail by dq
def enrich_dq(company_dedup_list,company_dq_result):
    for index, company in company_dq_result.iterrows():
        sourceid = company['Integration_MDM_Ids__c']
        company_dedup_list.loc[company_dedup_list['Source ID'] == sourceid, 'Company Name'] = company['Name']
        company_dedup_list.loc[company_dedup_list['Source ID'] == sourceid, 'Billing Address line1 (Street/Road)'] = company['BillingStreet']
        company_dedup_list.loc[company_dedup_list['Source ID'] == sourceid, 'Postal Code'] = company['BillingPostalCode']
        company_dedup_list.loc[company_dedup_list['Source ID'] == sourceid, 'City'] = company['BillingCity']
        company_dedup_list.loc[company_dedup_list['Source ID'] == sourceid, 'State'] = company['BillingState']
        company_dedup_list.loc[company_dedup_list['Source ID'] == sourceid, 'Country'] = company['BillingCountry']
        company_dedup_list.loc[company_dedup_list['Source ID'] == sourceid, 'Website'] = company['Website']
        company_dedup_list.loc[company_dedup_list['Source ID'] == sourceid, 'dq_New'] = False
    return company_dedup_list

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
# Deduplicate company by name
def dedup_company(company_common_list):
    company_common_list['ComName_temp'] = company_common_list['Company Name'].apply(lambda x: format_space(x.lower()))
    company_common_list['vc_Deduplicate'] = company_common_list.duplicated(subset=['ComName_temp'], keep=False)
    company_common_list['vc_Deduplicate'] = company_common_list['vc_Deduplicate'].apply(lambda x: False if x else True)
    company_duplicate_list = company_common_list[company_common_list['vc_Deduplicate'] == False]
    company_duplicate_list['vc_Load'] = False
    return company_duplicate_list

# Remove duplicate, fix contact source company id
def dedup_fix(company_raw_list, contact_raw_list, company_duplicate_list):
    company_remove_list = company_duplicate_list[company_duplicate_list['vc_Load'] == False]
    for index, company in company_remove_list.iterrows():
        sourceid = company['Source ID']
        masterid = company['vc_Master ID']
        company_raw_list = company_raw_list[company_raw_list['Source ID'] != sourceid]
        contact_raw_list.loc[contact_raw_list['Source Company ID'] == sourceid, 'Source Company ID'] = masterid
    return company_raw_list, contact_raw_list

def validate_common(company_init_list, contact_raw_list):

    # Fill contact id
    #if contact_raw_list['Source ID'].isnull().sum() == len(contact_raw_list):
    contact_raw_list['Source ID'] = list(range(1, (len(contact_raw_list) + 1)))

    company_source_list = list(company_init_list['Source ID'])
    contact_source_list = list(contact_raw_list['Source Company ID'])
    common_source_list = list(set(company_source_list).intersection(set(contact_source_list)))
    company_common_list = company_init_list[company_init_list['Source ID'].isin(common_source_list)]
    contact_common_list = contact_raw_list[contact_raw_list['Source Company ID'].isin(common_source_list)]
    return company_common_list, contact_common_list

# Keep only one space
def format_space(str):
    space = re.compile(r'\s\s+')
    str = space.subn('', str)
    return str[0].strip()



