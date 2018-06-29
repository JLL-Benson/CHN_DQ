# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 10:50:38 2018

@author: patrick.xu
"""

import requests
from bs4 import BeautifulSoup
import urllib
import re
from pandas import DataFrame
import datetime
import pandas as pd
import random
import time

headers = {
        'Host': 'www.qichacha.com',
        #'Referer': 'http://www.qichacha.com/search?key=%E4%BB%B2%E9%87%8F%E8%81%94%E8%A1%8C',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
        'Cookie': 'UM_distinctid=162ffa8028a9ca-05fa0f79476db5-3f3c5501-144000-162ffa8028e25; zg_did=%7B%22did%22%3A%20%22162ffa80515a5c-041ed9ff52dd03-3f3c5501-144000-162ffa805167ad%22%7D; _uab_collina=152470808111614272876542; CNZZDATA1254842228=1699380818-1524703353-null%7C1526279775; hasShow=1; _umdata=C234BF9D3AFA6FE73718FD929BD062FF988C2F13E7912A36956B5E834DAB60D667A8A3FD63A7D1EBCD43AD3E795C914C7BFE704F08F93D62FD75994B171EE7BB; PHPSESSID=7d0h8ed98l7m00ue426u4lh8t7; Hm_lvt_3456bee468c83cc63fb5147f119f1075=1524707625,1526281295,1526283282; acw_tc=AQAAAFRYNhmPgwQAqI+rbFV8MaFT2kZ6; zg_de1d1a35bfa24ce29bbf2c7eb17e6c4f=%7B%22sid%22%3A%201526281295063%2C%22updated%22%3A%201526283331641%2C%22info%22%3A%201526281295067%2C%22superProperty%22%3A%20%22%7B%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDomain%22%3A%20%22www.google.com.hk%22%2C%22cuid%22%3A%20%228af23a84fe5a7ad651d92737b9c51017%22%7D; Hm_lpvt_3456bee468c83cc63fb5147f119f1075=1526283332',
        'Connection': 'keep-alive',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive'}

df_business_info = DataFrame(columns = ['搜索词','公司名称','公司ID','注册资本','实缴资本','经营状态','成立日期','注册号','组织机构代码','纳税人识别号','统一社会信用代码','公司类型','所属行业','核准日期','登记机关','所属地区','英文名','曾用名','经营方式','人员规模','营业期限','企业地址','经营范围','法律诉讼','自身风险','关联风险','经营风险','财务信息_url','公司实力等级','纳税区间','销售净利润率','销售毛利率','企业年报_url','城镇职工基本养老保险人数','职工基本医疗保险人数','生育保险人数','失业保险人数','工伤保险人数'])

company_input_list=pd.read_excel(r'C:\Users\patrick.xu\Desktop\new python\企查查\company\company_list.xlsx',sheetname='Sheet1')
for index, row in company_input_list.iterrows():  
    keyword = row['Customer_Name']
    print(keyword,'******************')
    company = urllib.parse.quote(keyword)
    #print(company)
    
    
    base_url = 'http://www.qichacha.com/search?key='
    
    url_search = base_url + company
    response_search = requests.get(url_search,headers =headers)
    #print(response_search.status_code)
    
    soup_search = BeautifulSoup(response_search.text,'lxml')
    exsit_flag = soup_search.find('table',attrs = {'class':'m_srchList'})
    if exsit_flag:
        
        href = soup_search.find('table',attrs = {'class':'m_srchList'}).tbody.find_all('td')[1].a['href']

        url = 'http://www.qichacha.com/' + href
         
        time.sleep(random.randint(2, 4))
        response = requests.get(url,headers = headers)
        #print(response.status_code)
        
        soup = BeautifulSoup(response.text,'lxml')
        #print(soup)
        
        company_name = soup.find('div',attrs = {'class':'row title'}).h1.text
        company_id = re.findall(r'/firm_(.*).html',str(href))[0]
        
        print('---------',company_name,'----------')
        print('---------',company_id,'----------')
        
        business_info_list = soup.find_all('table',attrs = {'class':'ntable'})[1].find_all('tr')
        business_info_data = []
        business_info_data.append(keyword)
        business_info_data.append(company_name)
        business_info_data.append(company_id)
        
        for business_info in business_info_list[:-2]:
            business_info_data.append(business_info.find_all('td')[1].text.replace('\n', '').strip())
            business_info_data.append(business_info.find_all('td')[3].text.replace('\n', '').strip())
        
        business_info_data.append(business_info_list[-2].find_all('td')[1].text.replace('查看地图', '').replace('附近公司', '').replace('\n', '').strip())
        business_info_data.append(business_info_list[-1].find_all('td')[1].text.replace('\n', '').strip())
        
        #lawsuit = soup.find_all('div',attrs = {'class':'company-nav-tab'})[1].a.span.text
        lawsuit = soup.find('a',attrs = {'id':'susong_title'}).span.text
        business_info_data.append(lawsuit)
        
        risk_info = soup.find('div',attrs = {'class':'risk-panel b-a'})
        risk_details = risk_info.find_all('span',attrs = {'class':'text-danger'})
        business_info_data.append(risk_details[0].text)
        business_info_data.append(risk_details[1].text)
        
        operational_risk = soup.find('a',attrs = {'id':'fengxian_title'}).span.text
        business_info_data.append(operational_risk)
        
        #######Finance
        company_name = urllib.parse.quote(company_name)
        url_finance = 'http://www.qichacha.com/company_getinfos?unique=' + company_id + '&companyname=' + company_name + '&tab=run'
        business_info_data.append(url_finance)
        
        time.sleep(random.randint(2, 4))
        response_finance = requests.get(url_finance,headers = headers)
        #print(response.status_code)
        soup_finance = BeautifulSoup(response_finance.text,'lxml')
        finance_info_list_flag = soup_finance.find('section',attrs = {'id':'V3_cwzl'})
        
        if finance_info_list_flag:
            finance_info_list = finance_info_list_flag.find_all('td')
            business_info_data.append(finance_info_list[1].text)
            print(finance_info_list[1].text)
            business_info_data.append(finance_info_list[3].text)
            business_info_data.append(finance_info_list[5].text)
            business_info_data.append(finance_info_list[7].text)
        else:
            business_info_data.append('')
            business_info_data.append('')
            business_info_data.append('')
            business_info_data.append('')
            
        #Anual_report
        url_report = 'http://www.qichacha.com/company_getinfos?unique=' + company_id + '&companyname=' + company_name + '&tab=report'
        #url_report = 'https://www.qichacha.com/company_getinfos?unique=ad8fe7bedfbdef6d35ad61736c0f53bd&companyname=%E5%8C%97%E4%BA%AC%E4%BB%B2%E9%87%8F%E8%81%94%E8%A1%8C%E7%89%A9%E4%B8%9A%E7%AE%A1%E7%90%86%E6%9C%8D%E5%8A%A1%E6%9C%89%E9%99%90%E5%85%AC%E5%8F%B8&tab=report'
        business_info_data.append(url_report)
        
        time.sleep(random.randint(2, 4))
        response_report = requests.get(url_report,headers = headers)
        soup_report = BeautifulSoup(response_report.text,'lxml')
        
        report_items_pre = soup_report.find('div',attrs = {'class':'tab-pane fade in active'})
        report_items = report_items_pre.find_all('td')
        
        flag = 'N'
        
        for report_item in report_items:
            if report_item.text == '城镇职工基本养老保险':
                flag = 'Y'
        
        if flag == 'Y':
            for report_item in report_items:
                if report_item.text == '城镇职工基本养老保险':
                    business_info_data.append(report_items[report_items.index(report_item)+1].text)
                    print(report_items[report_items.index(report_item)+1].text)
                if report_item.text == '职工基本医疗保险':
                    business_info_data.append(report_items[report_items.index(report_item)+1].text)
                if report_item.text == '生育保险':
                    business_info_data.append(report_items[report_items.index(report_item)+1].text) 
                if report_item.text == '失业保险':
                    business_info_data.append(report_items[report_items.index(report_item)+1].text)
                if report_item.text == '工伤保险':
                    business_info_data.append(report_items[report_items.index(report_item)+1].text)    
        else:
            business_info_data.append('')
            business_info_data.append('')
            business_info_data.append('')
            business_info_data.append('')
            business_info_data.append('')
        
        
    else:
        business_info_data = [keyword,'','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','','']
        
    df_business_info.loc[len(df_business_info)]=business_info_data

now = datetime.datetime.now()
timestamp = str(now.strftime("%Y%m%d"))+'_'+str(now.hour)+str(now.minute)+str(now.second)
#print(timestamp)

df_business_info.to_excel(r'C:\Users\patrick.xu\Desktop\new python\企查查\output'+timestamp+'.xlsx', index = False)



