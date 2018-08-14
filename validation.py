# -*- coding: utf-8 -*-
"""
Created on Thu July 9th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pandas as pd
import numpy as np
import re
# from openpyxl import load_workbook


path = r'C:\Users\Benson.Chen\JLL\TDIM-GZ - Documents\Capforce\Shared Files'
lastname_list = pd.read_excel(path + '\LastName.xlsx', sheet_name='Sheet2', sort=False)
geo_list = pd.read_excel(path + '\China City&District List.xlsx', sheet_name='district-full', sort=False)
null_list = [r'^\s*null\s*$', r'^\s*nan\s*$', r'^\s*n/*a\s*$', r'^\s*tbd\s*$', r'^\s*-\s*$', r'^\s*$',  r'^\s*—\s*$']

company_common_suffix = ['分公司', '股份', '有限', '责任', '公司', '集团', '企业', '控股', '实业']
company_common_func = ['银行', '置业', '房地产', '地产', '开发', '银行', '投资', '基金', '证券', '资本', '物业', '服务', '管理', '资产']


# Deduplicate company by name
def dedup_company(company_common_list, contact_common_list):
    company_common_list['ComName_temp'] = None
    company_common_list['vc_Deduplicate'] = None
    # company_common_list['Load'] = None
    company_common_list['vc_Master_ID'] = None
    for index, company in company_common_list.iterrows():
        if pd.notna(company['Company_Name_CN']):
            company_common_list.ix[index, 'ComName_temp'] = extract_keyword(company['Company_Name_CN'])
        else:
            company_common_list.ix[index, 'ComName_temp'] = format_space(str(company['Company_Name']).strip().lower())
    company_common_list['vc_Deduplicate'] = company_common_list.duplicated(subset=['ComName_temp'], keep=False)
    company_common_list['vc_Deduplicate'] = company_common_list['vc_Deduplicate'].apply(lambda x: False if x else True)
    # Duplicate list needs review
    company_duplicate_list = company_common_list[company_common_list['vc_Deduplicate'] == False]
    company_duplicate_list['Load'] = False
    # Full duplicate list
    company_duplicate_full = company_duplicate_list
    company_duplications = list(company_duplicate_list.groupby(['ComName_temp']).count().index)
    for dup in company_duplications:
        company_dup_group = company_duplicate_list[company_duplicate_list['ComName_temp'] == dup]
        company_masterid, company_common_list, company_dup_group = dedup_get_master(company_common_list, company_dup_group)

        if company_masterid is None:
            continue
        else:
            company_common_list, contact_common_list = dedup_fix(company_common_list, contact_common_list, company_dup_group)
            company_duplicate_list = company_duplicate_list[company_duplicate_list['ComName_temp'] != dup]

    return company_duplicate_list, company_duplicate_full, company_common_list, contact_common_list


# Deduplicate company with staging data
def dedup_comany_db(company_dedup_list, company_db_return):
    if company_db_return.empty:
        return company_db_return
    company_combine_list = pd.concat([company_dedup_list, company_db_return], keys=['Input', 'Stage'])
    company_combine_list['Existing'] = company_combine_list.duplicated(subset=['ComName_temp'], keep=False)
    company_existing_list = company_combine_list.loc[company_combine_list['Existing'] == True]
    # Keep duplicate case not exists in staging data
    for tempname in company_existing_list['ComName_temp'].unique().tolist():
        company_existing_list.loc[company_existing_list['ComName_temp'] == tempname].loc['Stage']
        if company_existing_list.loc[company_existing_list['ComName_temp'] == tempname].loc['Stage'].empty:
            company_existing_list = company_existing_list.loc[~(company_existing_list['ComName_temp'] == tempname)]
    company_existing_list = company_existing_list.loc['Input']
    company_existing_list.loc['db_New'] = False
    company_existing_list.loc['Load'] = False
    company_dedup_list[company_dedup_list['Source_ID'].isin(company_existing_list['Source_ID'].tolist())].loc['db_New'] = False
    company_dedup_list[company_dedup_list['Source_ID'].isin(company_existing_list['Source_ID'].tolist())].loc['Load'] = False
    return company_dedup_list, company_existing_list


# Deduplicate company with staging data
def dedup_contact_db(contact_format_list, contact_db_return):
    if contact_db_return.empty:
        return contact_db_return
    contact_combine_list = pd.concat([contact_format_list, contact_db_return], keys=['Input', 'Stage'])
    contact_combine_list['Reject_Reason'] = ''
    contact_combine_list.loc[contact_combine_list.duplicated(subset=['Email'], keep=False), 'Reject_Reason'] = 'Email exists;   '
    contact_combine_list.loc[contact_combine_list.duplicated(subset=['Phone'], keep=False), 'Reject_Reason'] + 'Phone exists;   '
    contact_combine_list.loc[contact_combine_list.duplicated(subset=['Mobile'], keep=False), 'Reject_Reason'] + 'Mobile exists;   '
    contact_combine_list['Existing'] = contact_combine_list.duplicated(subset=['Email'], keep=False)
    contact_combine_list['Existing'] = contact_combine_list.duplicated(subset=['Email'], keep=False)
    contact_combine_list['Existing'] = contact_combine_list['Existing'] | contact_combine_list.duplicated(subset=['Phone'], keep=False)
    contact_combine_list['Existing'] = contact_combine_list['Existing'] | contact_combine_list.duplicated(subset=['Mobile'], keep=False)
    contact_dedup_list = contact_combine_list.loc['Input']
    contact_dedup_list.loc[contact_dedup_list['Existing'] == True, 'db_New'] = False

    return contact_dedup_list

# Remove duplicate, fix contact Source_Company_ID
def dedup_fix(company_common_list, contact_common_list, company_dup_group):
    company_remove_list = company_dup_group[company_dup_group['Load'] == False]

    for index, company in company_remove_list.iterrows():
        sourceid = company['Source_ID']
        masterid = company['vc_Master_ID']
        contact_common_list.loc[contact_common_list['Source_Company_ID'] == sourceid, 'Source_Company_ID'] = masterid
    company_common_list.drop(company_remove_list.index, inplace=True)
    return company_common_list, contact_common_list


# Get master duplicate, if multiple duplicates
def dedup_get_master(company_common_list, company_dup_group):
    if company_dup_group.empty:
        return None, company_common_list, company_dup_group
    master_group = company_dup_group.ix[company_dup_group['Parent_Name'].dropna().duplicated(keep=False).index, 'Parent_Name']
    master_name = company_dup_group.ix[company_dup_group['Company_Name'].dropna().duplicated(keep=False).index, 'Company_Name']
    master_name_cn = company_dup_group.ix[company_dup_group['Company_Name_CN'].dropna().duplicated(keep=False).index, 'Company_Name_CN']
    master_address = company_dup_group.ix[company_dup_group['Billing_Address'].dropna().duplicated(keep=False).index, 'Billing_Address']
    master_city = company_dup_group.ix[company_dup_group['City'].dropna().duplicated(keep=False).index, 'City']
    # if len(master_city) > 1:
    #     master_city = master_city.iloc[0]
    master_state = company_dup_group.ix[company_dup_group['State'].dropna().duplicated(keep=False).index, 'State']
    # if len(master_state) > 1:
    #     master_state = master_state.iloc[0]
    master_country = company_dup_group.ix[company_dup_group['Country'].dropna().duplicated(keep=False).index, 'Country']
    # if len(master_country) > 1:
    #     master_country = master_country.iloc[0]
    master_phone = company_dup_group.ix[company_dup_group['Phone'].dropna().duplicated(keep=False).index, 'Phone']
    master_email = company_dup_group.ix[company_dup_group['Email'].dropna().duplicated(keep=False).index, 'Email']
    master_website = company_dup_group.ix[company_dup_group['Website'].dropna().duplicated(keep=False).index, 'Website']

    # If multiple duplicates contain details, no master id
    if len(master_address) > 1 or len(master_phone) > 1 or len(master_email) > 1 or len(master_website) > 1:
        return None, company_common_list, company_dup_group
    else:
        company_masterindex = company_dup_group.index[0]
        company_masterid = company_dup_group['Source_ID'].values[0]
        if not master_group.empty:
            company_common_list.ix[company_masterindex, 'Parent_Name'] = list(master_group.values)[0]
        if not master_name.empty:
            company_common_list.ix[company_masterindex, 'Company_Name'] = list(master_name.values)[0]
        if not master_name_cn.empty:
            company_common_list.ix[company_masterindex, 'Company_Name_CN'] = list(master_name_cn.values)[0]
        if not master_address.empty:
            company_common_list.ix[company_masterindex, 'Billing_Address'] = list(master_address.values)[0]
        if not master_city.empty:
            company_common_list.ix[company_masterindex, 'City'] = list(master_city.values)[0]
        if not master_state.empty:
            company_common_list.ix[company_masterindex, 'State'] = list(master_state.values)[0]
        if not master_country.empty:
            company_common_list.ix[company_masterindex, 'Country'] = list(master_country.values)[0]
        if not master_phone.empty:
            company_common_list.ix[company_masterindex, 'Phone'] = list(master_phone.values)[0]
        if not master_website.empty:
            company_common_list.ix[company_masterindex, 'Website'] = list(master_website.values)[0]
        if not master_email.empty:
            company_common_list.ix[company_masterindex, 'Email'] = list(master_email.values)[0]

        company_dup_group.ix[company_masterindex, 'Load'] = True
        company_dup_group['vc_Master_ID'] = company_masterid
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
    if address is None:
        return state, city, district, street
    address = address.replace(' ', '')
    address = address.replace('中国', '')
    # Find direct city
    for index, d in dcities.iterrows():
        if d['Full Name'] in address or d['Name'] in address:
            state = d['Name']
            city = d['Name']
            address = address.replace(d['Full Name'], '')
            address = address.replace(d['Name'], '')
            break
    # Find state
    for index, s in states.iterrows():
        if s['Full Name'] in address or s['Name'] in address:
            state = s['Name']
            address = address.replace(s['Full Name'], '')
            address = address.replace(s['Name'], '')
            break
    # Find city
    for index, c in cities.iterrows():
        if c['Full Name'] in address or c['Name'] in address:
            city = c['Name']
            address = address.replace(c['Full Name'], '')
            address = address.replace(c['Name'], '')
            break
    # Find district
    for index, dis in districts.iterrows():
        if dis['Full Name'] in address or dis['Name'] in address:
            district = dis['Full Name']
            address = address.replace(dis['Full Name'], '')
            address = address.replace(dis['Name'], '')
            break
    zips = re.compile(r'\d{6}$')
    zipcode = zips.findall(address)
    address = zips.subn('', address)
    if len(zipcode) > 0:
        zipcode = zipcode[0]
    else:
        zipcode = None
    street = address[0].strip()
    return state, city, district, street, zipcode
# TODO: Zipcode fill


# Enrich company and contact detail by business return
def enrich_business(company_scrapy_list, company_business_return):
    for index, company in company_business_return.iterrows():
        company['Full Address'] = company['District'] + company['Billing_Address']
        company['Full Address'] = format_space(company['Full Address']).strip()
        company_scrapy_list = company_scrapy_list.loc[index] = company

    return company_scrapy_list


# Enrich company by best qichacha return
def enrich_company(company_dq_list, company_scrapy_result, company_colnames):
    company_scrapy_verify = pd.DataFrame(columns=list(company_colnames))
    for index, company in company_dq_list.iterrows():
        if company['db_New'] == False:
            continue
        sourceid = company['Source_ID']
        scrapy_list = company_scrapy_result[company_scrapy_result['Source_ID'] == sourceid]
        scrapy_best = scrapy_list[scrapy_list['Confidence'] == 0]
        # If multiple best match, get first one with address
        # If no best match, return top 5 result
        if len(scrapy_best) > 1:
            scrapy_best = scrapy_best[scrapy_best['地址'].notnull()].iloc[0].to_frame().transpose()
            company = enrich_scrapy(company, scrapy_best)
        elif len(scrapy_best) < 1:
            # scrapy_verify = scrapy_list[scrapy_list['地址'].notnull()].sort_values(by='Confidence')[0:5]
            # company_scrapy_verify = company_scrapy_verify.append(scrapy_verify)
            company = enrich_scrapy(company, scrapy_best)
            if pd.isna(company['Billing_Address']):
                company_scrapy_verify = company_scrapy_verify.append(company.to_frame().transpose())
        else:
            company = enrich_scrapy(company, scrapy_best)
        company_dq_list[company_dq_list['Source_ID'] == company['Source_ID']] = company.to_frame().transpose()
    company_dq_list = validate_company(company_dq_list)
    company_scrapy_verify = validate_company(company_scrapy_verify)
    return company_dq_list, company_scrapy_verify


# Enrich company detail by dq
def enrich_dq(company_dedup_list, company_dq_result):
    for index, company in company_dq_result.iterrows():
        company_dedup_list.loc[company_dedup_list['Source_ID'] == company['Source_ID']] = company
        company_dedup_list.loc[company_dedup_list['Source_ID'] == company['Source_ID'], 'db_New'] = False
    return company_dedup_list


# Enrich company detail by qichacha
def enrich_scrapy(company, scrapy):

    if scrapy.empty:
        state, city, district, company['Billing_Address'], zipcode = enrich_address(company['Billing_Address'])
        if pd.isna(company['State']):
            company['State'] = state
        if pd.isna(company['City']):
            company['City'] = city
        if pd.isna(company['District']):
            company['District'] = district
        if pd.isna(company['Postal_Code']):
            company['Postal_Code'] = zipcode
    else:
        if pd.notna(scrapy['英文名']).any():
            company['Company_Name'] = scrapy['英文名'].values[0]
        company['Company_Name_CN'] = scrapy['公司名称'].values[0]

        if scrapy['境外公司'] is True:
            company['Country'] = ''
        else:
            company['Country'] = 'China'

        if pd.isna(company['Billing_Address']):

            state, company['City'], company['District'], company['Billing_Address'], company['Postal_Code'] = enrich_address(scrapy['地址'].values[0])
            if pd.notna(scrapy['所属地区']).all():
                company['State'] = scrapy['所属地区'].values[0]
            else:
                company['State'] = state
        # Keep original address
        else:
            state, city, district, company['Billing_Address'], zipcode = enrich_address(company['Billing_Address'])
            if pd.isna(company['State']):
                company['State'] = state
            if pd.isna(company['City']):
                company['City'] = city
            if pd.isna(company['District']):
                company['District'] = district
            if pd.isna(company['Postal_Code']):
                company['Postal_Code'] = zipcode
        # company['Company_Type'] = scrapy['公司类型'].values[0]
        company['Phone'] = scrapy['电话'].values[0]
        company['Website'] = scrapy['网址'].values[0]
        company['Email'] = scrapy['邮箱'].values[0]
        company['Industry'] = scrapy['所属行业'].values[0]
        company['Employee'] = scrapy['参保人数'].values[0]
    # If district is found in list, combine district and street

    if pd.notna(company['District']):
        company['Full_Address'] = company['District'] + company['Billing_Address']
    else:
        company['Full_Address'] = company['Billing_Address']
    company['Full_Address'] = format_space(company['Full_Address']).strip()
    return company


# Extract company keyword
def extract_keyword(company_name):
    if type(company_name) != str:
        company_name = company_name.values[0]
    company_keyword = str(company_name).strip().replace(' ', '')
    # dcities = geo_list[geo_list['Level ID'] == 0]
    # states = geo_list[geo_list['Level ID'] == 1]
    # cities = geo_list[geo_list['Level ID'] == 2]
    # state = None
    # city = None
    if company_keyword is None:
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
    # Remove () and Chinese （）
    company_keyword = re.sub(r'\(.*\)', '', company_keyword)
    company_keyword = re.sub(r'（.*）', '', company_keyword)
    return company_keyword


# Keep only one space
def format_space(s):
    space = re.compile(r'\s\s+')
    s = space.subn('', s)
    return s[0].strip()


# Initial company, Null, '', space, 'N/A', '-' check
def init_list(raw_list, colnames, *args):
    for col in colnames:
        for i in null_list:
            if col not in list(raw_list) or pd.isnull(raw_list[col]).all():
                break
            else:
                if col in ['Source_ID', 'Source_Company_ID']:
                    continue
                else:
                    raw_list[col] = raw_list[col].astype(object).str.lower().replace(i, np.nan, regex=True)
                    raw_list[col] = raw_list[col].astype(object).str.title()

    if len(args) > 0:
        if args[0] == 'Company':
            raw_list['db_New'] = True
            raw_list['Load'] = True
            raw_list['Company_Name_CN'] = raw_list.loc[pd.notnull(raw_list['Company_Name_CN']), 'Company_Name_CN'].apply(lambda x: x.replace(' ', ''))
            # if len(args) > 2:
            #     raw_list['Source_ID'] = raw_list['Source_ID'].apply(lambda x: args[1] + '_' + args[2] + '_' + 'Company' + '_' + str(x))
        if args[0] == 'Contact':
            raw_list['db_New'] = True
            raw_list['Load'] = True
            raw_list['Source_ID'] = list(range(1, (len(raw_list) + 1)))
            raw_list['Source_ID'] = raw_list['Source_ID'].apply(lambda x: args[1] + '_' + args[2] + '_' + 'Contact' + '_' + str(x))
            # if len(args) > 2 and raw_list:
            #     raw_list['Source_Company_ID'] = raw_list['Source_Company_ID'].apply(lambda x: args[1] + '_' + args[2] + '_' + 'Company' + '_' + str(x))
    return raw_list


# Map state abbreviation
def map_state(company_db_list):
    states = geo_list[(geo_list['Level ID'] == 0) | (geo_list['Level ID'] == 1)]
    cities = geo_list[(geo_list['Level ID'] == 0) | (geo_list['Level ID'] == 2)]
    company_db_list['State_Abbr'] = None
    for index, company in company_db_list.iterrows():
        # Has state
        if pd.notna(company['State']):
            if not states[states['Name'] == company['State']].empty:
                company_db_list.loc[index, 'State_Abbr'] = states.loc[states['Name'] == company['State'], 'PingYin2']
            elif not states[states['Full Name'] == company['State']].empty:
                company_db_list.loc[index, 'State_Abbr'] = states.loc[states['Full Name'] == company['State'], 'PingYin2']
        # Only has city
        elif pd.notna(company['City']):
            if not cities[cities['Name'] == company['City']].empty:
                if (cities.loc[cities['Full Name'] == company['City'], 'Level ID'] == 0).any():
                    company_db_list.loc[index, 'State_Abbr'] = cities.loc[cities['Name'] == company['City'], 'PingYin2'].values[0]
                else:
                    company_db_list.loc[index, 'State_Abbr'] = states.loc[states['ID'] == cities.loc[cities['Name'] == company['City'], 'PID'].values[0], 'PingYin2'].values[0]

            elif not cities[cities['Full Name'] == company['City']].empty:
                if (cities.loc[cities['Full Name'] == company['City'], 'Level ID'] == 0).any():

                    company_db_list.loc[index, 'State_Abbr'] = cities.loc[cities['Full Name'] == company['City'], 'PingYin2'].values[0]
                else:
                    company_db_list.loc[index, 'State_Abbr'] = states.loc[states['ID'] == cities.loc[cities['Full Name'] == company['City'], 'PID'].values[0], 'PingYin2'].values[0]

    return company_db_list


# Merger duplicate company, no longer used
def merge_company(company_common_list, contact_common_list, company_dup_group, company_masterid):
    company_dup_group.ix[company_dup_group['Source_ID'] != company_masterid, 'Load'] = False
    print(company_masterid)
    print(company_masterid.tolist())
    company_dup_group[company_dup_group['Source_ID'] != company_masterid, 'vc_Master_ID'] = company_masterid.tolist()
    company_common_list, contact_common_list = dedup_fix(company_common_list, contact_common_list, company_dup_group)
    return company_common_list, contact_common_list


# Log of loading data
def staging_log(raw_list, load_list, mode, logs_columns):

    logs = pd.DataFrame(columns=logs_columns)
    raw_list['Source'] = 'Raw'
    load_list['Source'] = 'Load'
    # Deletion
    raw_source = list(raw_list['Source_ID'])
    load_source = list(load_list['Source_ID'])
    diff_source = list(set(raw_source).difference(set(load_source)))

    # Company deletion and different source_id merge
    if mode == 'Company':
        raw_list['ComName_temp'] = None
        for index, row in raw_list.iterrows():
            if pd.notna(row['Company_Name_CN']):
                raw_list.ix[index, 'ComName_temp'] = extract_keyword(row['Company_Name_CN'])
            else:
                raw_list.ix[index, 'ComName_temp'] = format_space(str(row['Company_Name']).strip().lower())

        for id in diff_source:
            row = raw_list[raw_list['Source_ID'] == id].iloc[0]
            if pd.notna(row['Company_Name_CN']):
                comname_temp = extract_keyword(row['Company_Name_CN'])
            else:
                comname_temp = format_space(str(row['Company_Name']).strip().lower())
            # No similar company name, log as delete
            if load_list[load_list['ComName_temp'] == comname_temp].empty:
                delta_logs = pd.DataFrame.from_dict(
                    {'Source_ID': [id], 'Entity_Type': [mode], 'Field': 'Source_ID', 'Action_Type': ['Delete'], 'Log_From': [id],
                     'Log_To': [None]})
            else:
                delta_logs = pd.DataFrame.from_dict(
                    {'Source_ID': [load_list.loc[load_list['ComName_temp'] == comname_temp, 'Source_ID'].values[0]], 'Entity_Type': [mode], 'Field': 'Source_ID', 'Action_Type': ['Merge'],
                     'Log_From': [id],
                     'Log_To': [load_list.loc[load_list['ComName_temp'] == comname_temp, 'Source_ID'].values[0]]})
        logs = logs.append(delta_logs, ignore_index=True)
        checklist = ['Parent_Name', 'Company_Name', 'Company_Name_CN', 'Billing_Address', 'Postal_Code', 'District', 'City', 'State', 'Country', 'Company_Type', 'Phone', 'Fax', 'Email', 'Website',
                     'Industry', 'Revenue', 'Employee', 'Full_Address']

    # Contact deletion
    # TODO: Contact deletion

    # Company & Contact, same source_id merge, modification, addition
    combine_list = raw_list.append(load_list[list(raw_list)])
    combine_list['Duplicate'] = combine_list.duplicated(subset=checklist, keep=False)
    combine_list = combine_list[combine_list['Duplicate'] == False]
    for id in combine_list['Source_ID'].unique().tolist():
        # Merge
        if len(combine_list[(combine_list['Source_ID'] == id) & (combine_list['Source'] == 'Raw')]) > 1:
            merge_logs = pd.DataFrame.from_dict({'Source_ID': [id], 'Entity_Type': [mode], 'Action_Type': ['Merge'], 'Log_From': [len(combine_list[(combine_list['Source_ID'] == id) & (combine_list['Source'] == 'Raw')])], 'Log_To': [1]})
            logs = logs.append(merge_logs, ignore_index=True)
        # Modification & Add
        load = combine_list[(combine_list['Source_ID'] == id) & (combine_list['Source'] == 'Load')]
        if load.empty:
            continue
        for col in checklist:
            # Addition
            if col not in list(raw_list):
                if pd.isna(load_list.loc[load_list['Source_ID'] == id, col]).all():
                    continue
                else:
                    add_logs = pd.DataFrame.from_dict({'Source_ID': [id], 'Entity_Type': [mode], 'Field': [col], 'Action_Type': ['Add'], 'Log_From': [None], 'Log_To': [load_list.loc[load_list['Source_ID'] == id, col].values[0]]})
                    logs = logs.append(add_logs, ignore_index=True)
            # Modification
            else:
                if pd.isna(combine_list.loc[(combine_list['Source_ID'] == id) & (combine_list['Source'] == 'Raw'), col]).all():
                    continue
                modify = True
                modify_from = None
                modify_to = None
                for raw in combine_list.loc[(combine_list['Source_ID'] == id) & (combine_list['Source'] == 'Raw'), col]:
                    if (str(load[col].values[0]).lower() == str(raw).lower()) or (pd.isna(load[col]).all()):
                        modify = False
                        break
                    else:
                        modify_from = raw
                        modify_to = load[col].values[0]
                if modify:
                    modify_logs = pd.DataFrame.from_dict({'Source_ID': [id], 'Entity_Type': [mode], 'Field': [col], 'Action_Type': ['Modify'], 'Log_From': [modify_from], 'Log_To': [modify_to]})
                    logs = logs.append(modify_logs, ignore_index=True)
    return logs


# Check company and contact across
def validate_common(company_init_list, contact_init_list):

    company_source_list = list(company_init_list['Source_ID'])
    contact_source_list = list(contact_init_list['Source_Company_ID'])
    common_source_list = list(set(company_source_list).intersection(set(contact_source_list)))
    # company doesn't have to have a contact
    company_common_list = company_init_list  # [company_init_list['Source_ID'].isin(common_source_list)]
    # contact must under a company
    contact_common_list = contact_init_list[contact_init_list['Source_Company_ID'].isin(common_source_list)]
    return company_common_list, contact_common_list


# Check company existing address
def validate_company(company_list):
    company_list['vc_Address'] = company_list['Billing_Address'].apply(lambda x: pd.notna(x))
    company_list['Load'] = company_list['vc_Address'] & company_list['db_New']
    return company_list

# validate contacts, check duplicate, check first name and last name misplacement, check email format
def validate_contacts(contact_dedup_list, contact_colnames, company_load_list):
    contact_validate_list = pd.DataFrame(columns=contact_colnames)

    for index, contact in contact_dedup_list.iterrows():
        sourceid = contact['Source_Company_ID']
        company = company_load_list.loc[company_load_list['Source_ID'] == sourceid]
        contact = validate_name(contact)
        contact = validate_email(contact, company)
        contact['Company_Name'] = list(company['Company_Name'])[0]
        if (pd.isna(contact['Mobile']) and pd.isna(contact['Phone']) and pd.isna(contact['Email'])):
            contact['Reject_Reason'] = contact['Reject_Reason'] + 'No communication method;  '

        contact['Load'] = contact['vn_Name_Check'] and (contact['ve_Email_Check'] or pd.notna(contact['Mobile']) or pd.notna(contact['Phone'])) and contact['db_New']

        contact_validate_list = contact_validate_list.append(contact, ignore_index=True)

    # Deduplicate by name and email
    contact_validate_list['Fname_temp'] = contact_validate_list['First_Name'].apply(lambda x: x.lower())
    contact_validate_list['Lname_temp'] = contact_validate_list['Last_Name'].apply(lambda x: x.lower())
    # TODO: keep only letters in email as Email_temp
    # Switch True and False
    contact_validate_list['vc_Deduplicate'] = contact_validate_list.duplicated(subset=['Fname_temp', 'Lname_temp', 'Email'], keep=False)
    contact_validate_list['vc_Deduplicate'] = contact_validate_list['vc_Deduplicate'].apply(lambda x: False if x else True)
    contact_validate_list['Load'] = contact_validate_list['Load'] & contact_validate_list['vc_Deduplicate']
    # TODO: no phone and email
    return contact_validate_list


# Validate email, valid suffix, cotains @, check personal, valid domain
def validate_email(contact, company):
    eformat = False
    esuffix = False
    epersonal = False
    edomain = False
    edup = False
    suffix = [r'\.com$', r'\.cn$', r'\.org$', r'\.net$', r'\.cc$', r'\.uk$', r'\.fr$', r'\.hk$', r'\.tw$', r'\.au$', r'\.jp$', r'\.sg$']
    personal = ['@gmail.com', '@hotmail.com', '@yahoo.com', '@sina.com', '@vip.sina.com', '@163.com', '@126.com', '@qq.com', '@vip.qq.com', '@139.com']

    if pd.notna(contact['Email']):
        # Lower and no space
        email = contact['Email'].lower().replace(' ', '')
    else:
        echeck = eformat and esuffix and (epersonal or edomain)
        contact['ve_Email_Format'] = eformat
        contact['ve_Email_Suffix'] = esuffix
        contact['ve_Email_Domain'] = epersonal or edomain
        contact['ve_Email_Check'] = echeck
        contact['Reject_Reason'] = contact['Reject_Reason'] + 'No Email;  '
        return contact
    # TODO: Email format check

    # Email Du
    # Email must contain @
    if '@' in email:
        eformat = True
    else:
        contact['Reject_Reason'] = contact['Reject_Reason'] + 'Email without @;  '

    # Email suffix check
    for s in suffix:
        if re.search(re.compile(s, re.I), email) is not None:
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
    domain = None
    if not company.empty:
        if pd.notna(company['Website']).bool():
            company_website = company['Website'].values[0]
            domain = company_website.split('.')[1]
        elif pd.notna(company['Email']).bool():
            company_email = company['Email'].values[0]
            domain = company_email.split('@')[1].split('.')[0]
            for p in personal:
                if p in company_email:
                    domain = None
                    break
        if domain is not None:
            if domain in email:
                edomain = True
            else:
                contact['Reject_Reason'] = contact['Reject_Reason'] + 'Email domain not match;  '
        else:
            edomain = True

    else:
        contact['Reject_Reason'] = contact['Reject_Reason'] + 'Company not exisits;  '

    # Email check
    echeck = eformat and esuffix and (epersonal or edomain)
    contact['ve_Email_Format'] = eformat
    contact['ve_Email_Suffix'] = esuffix
    contact['ve_Email_Domain'] = epersonal or edomain
    contact['ve_Email_Check'] = echeck
    return contact


# Validate name, check first name and last name misplacement,
def validate_name(contact):
    nfirst = True
    nlast = False
    nspace = False

    # Remove more than two space and starting/ending space, format Last_Name
    contact['Last_Name'] = format_space(contact['Last_Name'].lower().capitalize())
    contact['First_Name'] = format_space(contact['First_Name'])
    if pd.isna(contact['Reject_Reason']):
        contact['Reject_Reason'] = ''
    # Check First_Name and Last_Name misplace

    for lan in lastname_list.iloc[:, 1:]:
        lastnames = list(lastname_list[lan])
        if contact['Last_Name'] in lastnames:
            contact['vn_Lastname_CN'] = lastname_list.ix[lastnames.index(contact['Last_Name']), '简体中文']
            nlast = True
            break
        elif contact['First_Name'] in lastnames:
            nfirst = False
            break
    if not (nlast or nfirst):

        contact['Reject_Reason'] = contact['Reject_Reason'] + 'First_Name and Last_Name misplace;  '

    # Check name contains space
    if ' ' in contact['First_Name'] or ' ' in contact['Last_Name']:
        contact['Reject_Reason'] = contact['Reject_Reason'] + 'Name contains space;  '
    else:
        nspace = True

    # Name check
    ncheck = (nlast or nfirst) and nspace
    contact['vn_Name_Swap'] = (nlast or nfirst)
    contact['vn_Name_Space'] = nspace
    contact['vn_Name_Check'] = ncheck

    return contact
