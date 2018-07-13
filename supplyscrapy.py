# -*- coding: utf-8 -*-
"""
Created on Thu June 10th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import requests
from bs4 import BeautifulSoup
import urllib
import re
import datetime
import pandas as pd
import numpy as np
import random
import time

# Replace Cookies with your own
# TODO: Multiple cookie
search_headers = {
        'Host': 'www.qichacha.com',
        #'Referer': 'http://www.qichacha.com/search?key=%E4%BB%B2%E9%87%8F%E8%81%94%E8%A1%8C',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Cookie':'UM_distinctid=163d94f3a16399-0da47b260a80f-737356c-e1000-163d94f3a197a; zg_did=%7B%22did%22%3A%20%22163d94f3a46245-0adc5aad4f380e-737356c-e1000-163d94f3a4d389%22%7D; _uab_collina=152835924223807635894595; PHPSESSID=ih1ucl9safclihopf3c997gdi6; CNZZDATA1254842228=728643443-1528356765-https%253A%252F%252Fwww.google.com.hk%252F%7C1531451309; Hm_lvt_3456bee468c83cc63fb5147f119f1075=1529723950,1529735916,1530168634,1531454260; hasShow=1; _umdata=85957DF9A4B3B3E8E8285445FCCF2E30F407CDC10EBABDA683652A36E0B34E63DA2686095C8088DDCD43AD3E795C914CBAAE924260BA7606432A5CCAFD53623D; acw_tc=AQAAAFZWsSVtVg0AqI+rbAp1RgmIntD5; zg_de1d1a35bfa24ce29bbf2c7eb17e6c4f=%7B%22sid%22%3A%201531454259635%2C%22updated%22%3A%201531454269526%2C%22info%22%3A%201531454259640%2C%22superProperty%22%3A%20%22%7B%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDomain%22%3A%20%22%22%2C%22cuid%22%3A%20%22a0a7d0d090beea056d84c394f3864499%22%7D; Hm_lpvt_3456bee468c83cc63fb5147f119f1075=1531454270',
        'Connection': 'keep-alive',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive'}

# Column count 32
columnname = ['ID','Source ID','搜索词','公司名称','公司ID','电话','网址','邮箱','地址','境外公司','注册资本','实缴资本','经营状态','成立日期','注册号','组织机构代码','纳税人识别号','统一社会信用代码','公司类型','所属行业','核准日期','登记机关','所属地区','英文名','曾用名','参保人数','人员规模','营业期限','经营范围','法律诉讼','自身风险','关联风险','经营风险']#,'财务信息_url','公司实力等级','纳税区间','销售净利润率','销售毛利率','企业年报_url','城镇职工基本养老保险人数','职工基本医疗保险人数','生育保险人数','失业保险人数','工伤保险人数']

def qichacha(company_input_list, path, sourcename, timestamp):

    company_count = len(company_input_list)
    company_progress = 0

    now = datetime.datetime.now()
    if timestamp == '':
        timestamp = str(now.strftime("%Y%m%d")) + str(now.hour)

    # Find existing file
    try:
        company_scrapy_result = pd.read_excel(
            path,
            sheet_name='company_srapy_list')
        # Remove breakpoint record
        company_keyword_break = \
        np.array(company_scrapy_result[company_scrapy_result['ID'] == 'breakpoint']['搜索词']).tolist()[0]
        company_progress = \
        np.array(company_scrapy_result[company_scrapy_result['ID'] == 'breakpoint']['Source ID']).tolist()[0]
        company_scrapy_result = company_scrapy_result[company_scrapy_result['搜索词'] != company_keyword_break]
        if company_input_list[company_input_list['Company Local Name'] == company_keyword_break].empty == False:
            company_input_break = np.array(
                company_input_list[company_input_list['Company Local Name'] == company_keyword_break].index).tolist()[0]
        else:
            company_input_break = np.array(
                company_input_list[company_input_list['Company Name'] == company_keyword_break].index).tolist()[0]
        company_input_list = company_input_list.drop(list(range(0, company_input_break)))
    # First time running
    except:
        company_scrapy_result = pd.DataFrame()  # columns = columnname)

    for index, row in company_input_list.iterrows():
        company_progress += 1
        if pd.notnull(row['Company Local Name']):
            company_keyword = row['Company Local Name']
        else:
            company_keyword = row['Company Name']
        company_sourceid = row['Source ID']

        # Search filter
        search_base = 'https://www.qichacha.com/search?key='
        # Keyword
        print(company_keyword)
        search_key = urllib.parse.quote(company_keyword)
        # Organization Type： 0:Company 1:Organization 3:HK Company 5:TW Company
        search_type = '&searchType='
        # Searching Index： 2:Company Name 4:Representative/Share holder  6:Management 8:Brand/Product 10:Connection(Address)
        search_index = '&index=2'
        # Province
        search_province = '&province='

        # Fuzzy search for keyword
        # time.sleep(random.randint(1, 2))
        search_url_keyword = search_base + search_key + '&ajaxflag=1' + search_index
        respond_keyword = requests.get(search_url_keyword, headers=search_headers)
        soup_keyword = BeautifulSoup(respond_keyword.text, 'lxml')
        company_info_list_flag = soup_keyword.find('span', attrs={'id': 'countOld'})

        # Company details
        if company_info_list_flag != None and company_info_list_flag.span.text.strip() != '0':
            try:
                search_companys = soup_keyword.find('table', attrs={'class': 'm_srchList'}).tbody.find_all('td')
                step = 0
                for company in search_companys:
                    if step % 3 == 1:
                        company_href = company.a['href']
                        search_url_company = 'https://www.qichacha.com' + company_href
                        # time.sleep(random.randint(0, 1))
                        respond_company = requests.get(search_url_company, headers=search_headers)
                        soup_company = BeautifulSoup(respond_company.text, 'lxml')
                        company_isforeign = False
                        if (soup_company.find('div', attrs={'class': 'row title'}).h1 == None):  # HongKong Company
                            soup_company.find('div', attrs={'class': 'row title'}).span.extract()
                            company_name = soup_company.find('div', attrs={'class': 'row title'}).text
                            company_isforeign = True
                        else:
                            company_name = soup_company.find('div', attrs={'class': 'row title'}).h1.text
                        company_id = re.findall(r'/firm_(.*).html', str(company_href))[0]
                        company_phone = ''
                        company_website = ''
                        company_email = ''
                        company_address = ''
                        if (soup_company.find('span', attrs={'class': "cdes"}).next_sibling.span != None):
                            cpmpany_phone = soup_company.find('span', attrs={'class': "cdes"}).next_sibling.span.text
                        if (soup_company.find('a', attrs={'onclick': "zhugeTrack('企业主页-企业头部-官网')"}) != None):
                            company_website = soup_company.find('a', attrs={'onclick': "zhugeTrack('企业主页-企业头部-官网')"})[
                                'href']
                        if (soup_company.find('a', attrs={'title': '发送邮件'}) != None):
                            company_email = soup_company.find('a', attrs={'title': '发送邮件'}).text
                        if (soup_company.find('a', attrs={'title': "查看地址"}) != None):
                            company_address = soup_company.find('a', attrs={'title': "查看地址"}).text
                        search_id = sourcename + timestamp + '_' + str(company_sourceid) + '_' + str(company_id)
                        #print('---------', company_name, '----------')
                        #print('---------', company_id, '----------')
                        if company_isforeign:
                            company_info_data = [search_id, company_sourceid, company_keyword, company_name, company_id,
                                                 company_phone, company_website, company_email, company_address, '', '',
                                                 '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                                                 '',
                                                 '',
                                                 '']
                            company_info_data = dict(zip(columnname, company_info_data))
                            company_scrapy_result = company_scrapy_result.append(company_info_data, ignore_index=True)
                            step += 1
                            continue
                        company_info_list = soup_company.find_all('table', attrs={'class': 'ntable'})[1].find_all('tr')
                        company_info_data = []
                        company_info_data.append(search_id)
                        company_info_data.append(company_sourceid)
                        company_info_data.append(company_keyword)
                        company_info_data.append(company_name)
                        company_info_data.append(company_id)
                        company_info_data.append(company_phone)
                        company_info_data.append(company_website)
                        company_info_data.append(company_email)
                        company_info_data.append(company_address)
                        company_info_data.append(company_isforeign)
                        for business_info in company_info_list[:-2]:
                            company_info_data.append(business_info.find_all('td')[1].text.replace('\n', '').strip())
                            company_info_data.append(business_info.find_all('td')[3].text.replace('\n', '').strip())
                        # Business scope
                        company_info_data.append(company_info_list[-1].find_all('td')[1].text.replace('\n', '').strip())

                        # Lawsuit count
                        company_lawsuit = soup_company.find('a', attrs={'id': 'susong_title'}).span.text
                        company_info_data.append(company_lawsuit)

                        # Risk
                        company_risk_info = soup_company.find('div', attrs={'class': 'risk-panel b-a'})
                        if (company_risk_info != None):
                            company_risk_details = company_risk_info.find_all('span', attrs={'class': 'text-danger'})
                            company_risk_operation = soup_company.find('a', attrs={'id': 'fengxian_title'}).span.text
                            company_info_data.append(company_risk_details[0].text)
                            company_info_data.append(company_risk_details[1].text)
                            company_info_data.append(company_risk_operation)

                        # # Finance
                        # company_name_encode = urllib.parse.quote(company_name)
                        # search_url_finance = 'http://www.qichacha.com/company_getinfos?unique=' + company_id + '&companyname=' + company_name_encode + '&tab=run'
                        # company_info_data.append(search_url_finance)
                        # time.sleep(random.randint(2, 4))
                        # respond_finance = requests.get(search_url_finance,headers = search_headers)
                        # soup_finance = BeautifulSoup(respond_finance.text,'lxml')
                        # finance_info_list_flag = soup_finance.find('section',attrs = {'id':'V3_cwzl'})
                        # if finance_info_list_flag:
                        #     finance_info_list = finance_info_list_flag.find_all('td')
                        #     company_info_data.append(finance_info_list[1].text)
                        #     company_info_data.append(finance_info_list[3].text)
                        #     company_info_data.append(finance_info_list[5].text)
                        #     company_info_data.append(finance_info_list[7].text)
                        # else:
                        #     company_info_data.append('')
                        #     company_info_data.append('')
                        #     company_info_data.append('')
                        #     company_info_data.append('')

                        # # Anual_report
                        # search_url_report = 'http://www.qichacha.com/company_getinfos?unique=' + company_id + '&companyname=' + company_name_encode + '&tab=report'
                        # company_info_data.append(search_url_report)
                        # time.sleep(random.randint(2, 4))
                        # respond_report = requests.get(search_url_report,headers = search_headers)
                        # soup_report = BeautifulSoup(respond_report.text,'lxml')
                        # report_info_list = soup_report.find('div',attrs = {'class':'tab-pane fade in active'})
                        # print(report_info_list)
                        # report_info_list = report_info_list.find_all('td')
                        # report_info_list_flag = 'N'
                        #
                        # for report in report_info_list:
                        #     if report.text == '城镇职工基本养老保险':
                        #         report_info_list_flag = 'Y'
                        #
                        # if report_info_list_flag == 'Y':
                        #     for report in report_info_list:
                        #         if report.text == '城镇职工基本养老保险':
                        #             company_info_data.append(report_info_list[report_info_list.index(report)+1].text)
                        #             print(report_info_list[report_info_list.index(report)+1].text)
                        #         if report.text == '职工基本医疗保险':
                        #             company_info_data.append(report_info_list[report_info_list.index(report)+1].text)
                        #         if report.text == '生育保险':
                        #             company_info_data.append(report_info_list[report_info_list.index(report)+1].text)
                        #         if report.text == '失业保险':
                        #             company_info_data.append(report_info_list[report_info_list.index(report)+1].text)
                        #         if report.text == '工伤保险':
                        #             company_info_data.append(report_info_list[report_info_list.index(report)+1].text)
                        # else:
                        #     company_info_data.append('')
                        #     company_info_data.append('')
                        #     company_info_data.append('')
                        #     company_info_data.append('')
                        #     company_info_data.append('')

                        company_info_data = dict(zip(columnname, company_info_data))
                        company_scrapy_result = company_scrapy_result.append(company_info_data, ignore_index=True)
                    step += 1

            except:  # Need verification, set ID as 'breakpoint'
                company_info_data = ['breakpoint', company_progress, company_keyword, '', '', '', '', '', '', '', '',
                                     '', '',
                                     '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
                company_info_data = dict(zip(columnname, company_info_data))
                company_scrapy_result = company_scrapy_result.append(company_info_data, ignore_index=True)
                print('Need Verification!')
                print('Progress: ' + company_progress / company_count * 100 + '%')
                break
        # Need verification, set ID as 'breakpoint'
        elif company_info_list_flag == None:
            company_info_data = ['breakpoint', company_progress, company_keyword, '', '', '', '', '', '', '', '', '',
                                 '',
                                 '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
            company_info_data = dict(zip(columnname, company_info_data))
            company_scrapy_result = company_scrapy_result.append(company_info_data, ignore_index=True)

            print('Need Verification!')
            print('Progress: ' + company_progress / company_count * 100 + '%')
            break
        # No result return
        elif company_info_list_flag.span.text.strip() == '0':
            search_id = sourcename + '_' + timestamp + '_' + str(company_sourceid)
            # Column count 32
            company_info_data = [search_id, company_sourceid, company_keyword, '', '', '', '', '', '', '', '', '', '',
                                 '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
            company_info_data = dict(zip(columnname, company_info_data))
            company_scrapy_result = company_scrapy_result.append(company_info_data, ignore_index=True)

    return company_scrapy_result

# print(timestamp)
# print()
# company_scrapy_result.to_excel(r'C:\Users\Benson.Chen\Desktop\Capforce\ICG\QiChaCha\QiChaCha'+timestamp+'.xlsx', index = False, header=True, columns= list(columnname), sheet_name='Qichacha')
#
#

