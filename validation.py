# -*- coding: utf-8 -*-
"""
Created on Thu June 10th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pandas as pd
import numpy as np
import re

path = r'C:\Users\Benson.Chen\Desktop\Capforce\Shared Files'
lastname_list = pd.read_excel(path + '\LastName.xlsx', sheet_name='Sheet2', sort=False)
city_list = pd.read_excel(path + '\China City&District List.xlsx', sheet_name='district-full', sort=False)

company_list = pd.read_excel(r'C:\Users\Benson.Chen\Desktop\icg-CF Data Load-20180704_dqreview.xlsx', sheet_name='Full Company', sort=False)
contact_input_list = pd.read_excel(r'C:\Users\Benson.Chen\JLL\TDIM-GZ - Documents\Capforce\ICG\From ICG\icg-Contact Check_Rannie 20180613.xlsx', sheet_name='Contact', sort=False)
contact_colnames = ['Company Name', 'First Name', 'Last Name', 'Email', 'Phone', 'Title', 'Source Company ID', 'vc_Load', 'Reject Reason', 'First Name2', 'Last Name2', 'Email2', 'vc_Deduplicate', 'vn_Lastname_CN', 'vn_Name_Swap', 'vn_Name_Space', 'vn_Name_Check', 've_Email_Format', 've_Email_Suffix', 've_Email_Domain', 've_Email_Check']
# Check Contacts
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

def format_space(str):
    space = re.compile(r'\s\s+')
    str = space.subn('', str)
    return str[0].strip()


# TODO: Company Address, Duplicate
contact_output = validate_contacts(contact_input_list, contact_colnames)
contact_output.to_excel(r'C:\Users\Benson.Chen\Desktop\test.xlsx', index=False, header=True, columns= contact_colnames, sheet_name='Contact')