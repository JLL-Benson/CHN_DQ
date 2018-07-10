# -*- coding: utf-8 -*-
"""
Created on Thu July 9th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pandas as pd
import numpy as np
import re

path = r'C:\Users\Benson.Chen\Desktop\Capforce\Shared Files'
lastname_list = pd.read_excel(path + '\LastName.xlsx', sheet_name='Sheet2', sort=False)
geo_list = pd.read_excel(path + '\China City&District List.xlsx', sheet_name='district-full', sort=False)

company_list = pd.read_excel(r'C:\Users\Benson.Chen\Desktop\icg-CF Data Load-20180704_dqreview.xlsx', sheet_name='Full Company', sort=False)
contact_input_list = pd.read_excel(r'C:\Users\Benson.Chen\JLL\TDIM-GZ - Documents\Capforce\ICG\From ICG\icg-Contact Check_Rannie 20180613.xlsx', sheet_name='Contact', sort=False)
contact_colnames = ['Company Name', 'First Name', 'Last Name', 'Email', 'Phone', 'Title', 'Source Company ID', 'vc_Load', 'Reject Reason', 'First Name2', 'Last Name2', 'Email2', 'vc_Deduplicate', 'vn_Lastname_CN', 'vn_Name_Swap', 'vn_Name_Space', 'vn_Name_Check', 've_Email_Format', 've_Email_Suffix', 've_Email_Domain', 've_Email_Check']
# Check contacts
def validate_contacts(contact_list, colnames):
    contact_output_list = pd.DataFrame(columns=colnames)
    for index, contact in contact_list.iterrows():
        contact['Reject Reason'] = ''
        contact_check = validate_name(contact)
        contact_check = validate_email(contact,company_list)
        contact_output_list = contact_output_list.append(contact, ignore_index=True)

    # Deduplicate by name and email
    contact_output_list['Fname_temp'] = contact_output_list['First Name'].apply(lambda x: x.upper())
    contact_output_list['Lname_temp'] = contact_output_list['Last Name'].apply(lambda x: x.upper())
    contact_output_list['vc_Deduplicate'] = contact_output_list.duplicated(subset=['Fname_temp', 'Lname_temp', 'Email'], keep=False)
    contact_output_list['vc_Deduplicate'] = contact_output_list['vc_Deduplicate'].apply(lambda x: False if x else True)
    contact_output_list['vc_Load'] = True

    return contact_output_list[colnames]

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
def validate_email(contact, company_list):
    eformat = False
    esuffix = False
    epersonal = False
    edomain = False
    suffix = [r'\.com$', r'\.cn$', r'\.org$', r'\.net$', r'\.cc$', r'\.uk$', r'\.fr$', r'\.hk$', r'\.tw$', r'\.au$', r'\.jp$', r'\.sg$']
    personal = ['@gmail.com', '@hotmail.com', '@yahoo.com', '@sina.com', '@vip.sina.com', '@163.com', '@126.com', '@qq.com', '@vip.qq.com', '@139.com']
    company = company_list[company_list['Source ID'] == contact['Source Company ID']]

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
        if p in contact['Email']:
            epersonal = True
            break

    # Email domain check
    if not company.empty:
        if pd.notnull(company['Website']).bool():
            domain = company['Website'].split('@')[1].rsplit('.',1)[0].lower()
            if domain in contact['Email']:
                edomain = True
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

# Validate company
def validate_company(company_list, company_scrapy_list):
    company_scrapy_verfy = pd.DataFrame(columns=list(company_scrapy_list))
    for index, company in company_list:
        sourceid = company['Source ID']
        scrapy_list = company_scrapy_list[company_scrapy_list['Source ID'] == sourceid]
        scrapy_best = scrapy_list[scrapy_list['Confidence'] == 0]
        # If multiple best match, get first one with address
        # If no best match, return top 5 result
        if (len(scrapy_best) > 1):
            scrapy_best = scrapy_best[scrapy_best['地址'].notnull()].iloc[0]
        elif (len(scrapy_best) < 1):
            scrapy_verfy = scrapy_list[scrapy_list['地址'].notnull()].sort_values(by='Confidence')[0:5]
            company_scrapy_verfy = company_scrapy_verfy.append(scrapy_verfy)
            continue
        company_list.iloc[index] = fill_company(company, scrapy_best)

    return company_list, company_scrapy_verfy

# Enrich company detail
def fill_company(company, scrapy):
    if scrapy['英文名'] != None:
        company['Company Name'] = scrapy['英文名']
    company['Company Local Name'] = scrapy['公司名称']
    if scrapy['境外公司'] == True or scrapy['境外公司'] == 'True':
        company['Country'] = ''
    else:
        company['Country'] = 'China'
    if company['Billing Address line1 (Street/Road)'] == None:
        state, company['City'], company['District'], company['Billing Address line1 (Street/Road)'] = format_address(scrapy['地址'])
        if scrapy['所属地区'] is not None and len(scrapy['所属地区']) > 1:
            company['State'] = scrapy['所属地区']
        else:
            company['State'] = state
    # No district field in system for now
    company['Full Address'] = company['District'] + company['Billing Address line1 (Street/Road)']
    company['Company Type'] = scrapy['公司类型']
    company['Phone'] = scrapy['电话']
    company['Website'] = scrapy['网址']
    company['Email'] = scrapy['邮箱']
    return company

# Keep only one space
def format_space(str):
    space = re.compile(r'\s\s+')
    str = space.subn('', str)
    return str[0].strip()

def format_address(address):
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



# TODO: Company Duplicate
contact_output = validate_contacts(contact_input_list, contact_colnames)
contact_output.to_excel(r'C:\Users\Benson.Chen\Desktop\test.xlsx', index=False, header=True, columns= contact_colnames, sheet_name='Contact')