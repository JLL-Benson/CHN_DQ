# -*- coding: utf-8 -*-
"""
Created on Thu June 12th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pandas as pd
from openpyxl import load_workbook
import validation as vd
import sys
import getpass
import dq
from supplyscrapy import qichacha
from confidence import getConfidence
sys.path.append(r'C:\Users\Benson.Chen\PycharmProjects\dq\jobs\live_sf\bau')
import bau_cf_accounts_lib as bau_accounts
import bau_cf_contacts as bau_contacts



# Source-Site-LoadRound
rawpath = r'C:\Users\Benson.Chen\Desktop\CM-North.xlsx'
sourcename = 'CM-North-1'
# YYYYMMDDHH
timestamp = '2018072217'
path = r'C:\Users\Benson.Chen\Desktop'
backupfilename = r'\CHN-DQ_' + sourcename + '_' + timestamp +'_BACKUP.xlsx'
backupfilepath = path + backupfilename
reviewfilename = r'\CHN-DQ_' + sourcename + '_' + timestamp +'_REVIEW.xlsx'
reviewfilepath = path + reviewfilename
#backupfilepath = r'C:\Users\Benson.Chen\Desktop\test_com.xlsx'

contact_colnames = ['Source ID', 'Company Name', 'First Name', 'Last Name', 'Email', 'Phone', 'Title', 'Source Company ID', 'vc_Load', 'Reject Reason', 'First Name2', 'Last Name2', 'Email2', 'vc_Deduplicate', 'vn_Lastname_CN', 'vn_Name_Swap', 'vn_Name_Space', 'vn_Name_Check', 've_Email_Format', 've_Email_Suffix', 've_Email_Domain', 've_Email_Check']
company_colnames = ['Source ID', 'Company Name', 'Company Local Name', 'Billing Address line1 (Street/Road)', 'Billing Address line2 (Building Name)', 'Billing Address line3(Suite, Level, Floor, Unit)', 'Postal Code','District', 'City', 'State', 'Country', 'Company Type', 'Phone', 'Fax', 'Email', 'Website','Industry', 'Revenue', 'Employee', 'Full Address', 'dq_New']
company_dup_colnames = ['Company Name','Company Name Local', 'Billing Address line1 (Street/Road)', 'City', 'State', 'Source ID', 'vc_Deduplicate', 'vc_Load', 'vc_Master ID']




def run(phrase):


    # Deduplicate company, find common companies and contacts
    if phrase == 'p1':
        CHN_DQ_reviewwriter = pd.ExcelWriter(reviewfilepath, engine='openpyxl')
        CHN_DQ_backupwriter = pd.ExcelWriter(backupfilepath, engine='openpyxl')
        company_raw_list = pd.read_excel(rawpath, sheet_name='Company', sort=False)
        contact_raw_list = pd.read_excel(rawpath, sheet_name='Contact', sort=False)
        company_init_list = vd.init_company(company_raw_list)
        company_common_list, contact_common_list = vd.validate_common(company_init_list, contact_raw_list)
        company_duplicate = vd.dedup_company(company_common_list)
        company_duplicate.to_excel(CHN_DQ_reviewwriter, index=False, header=True, columns= company_dup_colnames, sheet_name='1_Duplicate')
        company_common_list.to_excel(CHN_DQ_backupwriter, index=False, header=True, columns= company_colnames, sheet_name='company_common_list')
        contact_common_list.to_excel(CHN_DQ_backupwriter, index=False, header=True, columns= contact_colnames, sheet_name='contact_common_list')
        CHN_DQ_reviewwriter.save()
        CHN_DQ_reviewwriter.close()
        CHN_DQ_backupwriter.save()
        CHN_DQ_backupwriter.close()
        print('Check {}, {}, deduplicate company'.format(backupfilename, '1_Duplicate') )
    # Deduplicate company and clean relative contacts
    elif phrase == 'p2':
        CHN_DQ_backupwriter = pd.ExcelWriter(backupfilepath, engine='openpyxl')
        backupbook = load_workbook(CHN_DQ_backupwriter.path)
        CHN_DQ_backupwriter.book = backupbook
        company_common_list = pd.read_excel(backupfilepath, sheet_name='company_common_list', sort=False)
        contact_common_list = pd.read_excel(backupfilepath, sheet_name='contact_common_list', sort=False)
        company_duplicate_list = pd.read_excel(reviewfilepath, sheet_name='1_Duplicate', sort=False)
        company_dedup_list, contact_dedup_list = vd.dedup_fix(company_common_list, contact_common_list,
                                                              company_duplicate_list)
        company_dedup_list.to_excel(CHN_DQ_backupwriter, index=False, header=True, columns=company_colnames,
                                    sheet_name='company_dedup_list')
        contact_dedup_list.to_excel(CHN_DQ_backupwriter, index=False, header=True, columns=contact_colnames,
                                    sheet_name='contact_dedup_list')

        CHN_DQ_backupwriter.save()
        CHN_DQ_backupwriter.close()
        print('Run p3')
    # Run company dq process
    elif phrase == 'p3':
        if dq.is_appserver():
            dq.set_working_dir(dq.get_app_server_working_dir())
        else:
            dq.set_working_dir('C:/Users/{}/Scratch'.format(getpass.getuser()))

        mod_start_date = None
        mod_end_date = None

        job_reporter = dq.JobReporter(
        __file__, mode='TEST', start_date=mod_start_date,
        end_date=mod_end_date, environment='DEV',
        log_file=None, subject='Adhoc Salesforce Accounts Load',
        )

        sheetname = 'company_dedup_list'
        column_mapping = {
            'Source ID': 'Integration_MDM_Ids__c',
            'Company Name': 'Name',
            'Full Address': 'BillingStreet',
            'City': 'BillingCity',
            'State': 'BillingState',
            'Postal Code': 'BillingPostalCode',
            'Country': 'BillingCountry',
            'Phone': 'Phone',
            'Fax': 'Fax',
            'Website': 'Website',
            'Industry': 'Industry',
            'Revenue': 'AnnualRevenue',
            'Employee': 'NumberOfEmployee'
        }

        bau_accounts.precleanse_extract(
            backupfilepath, sheetname, sourcename, column_mapping, job_reporter
        )
        # End company dq process
    # Enrich company with company dq result, run company scrapy process and enrich
    elif phrase == 'p4':
        CHN_DQ_backupwriter = pd.ExcelWriter(backupfilepath, engine='openpyxl')
        CHN_DQ_reviewwriter = pd.ExcelWriter(reviewfilepath, engine='openpyxl')
        backupbook = load_workbook(CHN_DQ_backupwriter.path)
        reviewbook = load_workbook(CHN_DQ_reviewwriter.path)
        CHN_DQ_backupwriter.book = backupbook
        CHN_DQ_reviewwriter.book = reviewbook
        dqfilename = r'\CHN-DQ_' + sourcename + '_' + timestamp + '_BACKUP_ENRICHED.xlsx'
        dqfilepath = path + dqfilename
        company_dq_result = pd.read_excel(dqfilepath, sheet_name='Existing_Accounts', sort=False)
        company_dedup_list = pd.read_excel(backupfilepath, sheet_name='company_dedup_list', sort=False)
        company_dq_list = vd.enrich_dq(company_dedup_list, company_dq_result)
        # Run scrapy process
        company_scrapy_result = qichacha(company_dq_list[company_dq_list['dq_New'] != False], backupfilepath, sourcename, timestamp)
        company_scrapy_result['Confidence'] = company_scrapy_result.apply(getConfidence, axis=1)
        company_scrapy_list, company_scrapy_verify = vd.validate_company(company_dq_list, company_scrapy_result, company_colnames)
        company_scrapy_result.to_excel(CHN_DQ_backupwriter, index=False, header=True, columns=list(company_scrapy_result),
                                    sheet_name='company_scrapy_result')
        company_scrapy_list.to_excel(CHN_DQ_backupwriter, index=False, header=True, columns=company_colnames,
                                    sheet_name='company_scrapy_list')
        company_scrapy_verify.to_excel(CHN_DQ_reviewwriter, index=False, header=True, columns=company_colnames,
                                    sheet_name='2_Scrapy')
        CHN_DQ_backupwriter.save()
        CHN_DQ_backupwriter.close()
        CHN_DQ_reviewwriter.save()
        CHN_DQ_reviewwriter.close()
    # Enrich company with business return, validate contact
    elif phrase == 'p5':
        CHN_DQ_reviewwriter = pd.ExcelWriter(reviewfilepath, engine='openpyxl')
        reviewbook = load_workbook(CHN_DQ_reviewwriter.path)
        CHN_DQ_reviewwriter.book = reviewbook
        company_business_result = pd.read_excel(reviewfilepath, sheet_name='2_Scrapy', sort=False)
        company_scrapy_list = pd.read_excel(backupfilepath, sheet_name='company_scrapy_list', sort=False)
        contact_dedup_list = pd.read_excel(backupfilepath, sheet_name='contact_dedup_list', sort=False)
        company_load_list = vd.enrich_business(company_scrapy_list, company_business_result, company_colnames)

        contact_validate_list = vd.validate_contacts(contact_dedup_list, contact_colnames, company_load_list)
        contact_validate_list.to_excel(CHN_DQ_reviewwriter, index=False, header=True, columns=contact_colnames,
                                    sheet_name='3_Validate')

        company_load_list.to_excel(CHN_DQ_reviewwriter, index=False, header=True, columns=company_colnames,
                                   sheet_name='4_Company_Load')
        CHN_DQ_reviewwriter.save()
        CHN_DQ_reviewwriter.close()
    elif phrase == 'p6':
        CHN_DQ_reviewwriter = pd.ExcelWriter(reviewfilepath, engine='openpyxl')
        reviewbook = load_workbook(CHN_DQ_reviewwriter.path)
        CHN_DQ_reviewwriter.book = reviewbook
        contact_dedup_list = pd.read_excel(backupfilepath, sheet_name='contact_dedup_list', sort=False)
        contact_business_list = pd.read_excel(reviewfilepath, sheet_name='3_Validate', sort=False)
        droplist = list(contact_business_list.loc[contact_business_list['vc_Load'] == False,'Source ID'])
        contact_load_list = contact_dedup_list[~contact_dedup_list['Source ID'].isin(droplist)]
        contact_load_list.to_excel(CHN_DQ_reviewwriter, index=False, header=True, columns=contact_colnames,
                                   sheet_name='4_Contact_Load')
        CHN_DQ_reviewwriter.save()
        CHN_DQ_reviewwriter.close()
run('p6')









# contact_output = validate_contacts(contact_input_list, contact_colnames)
# contact_output.to_excel(r'C:\Users\Benson.Chen\Desktop\test.xlsx', index=False, header=True, columns= contact_colnames, sheet_name='Contact')