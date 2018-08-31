# -*- coding: utf-8 -*-
"""
Created on Thu August 30th 2018

@author: Benson.Chen benson.chen@ap.jll.com
"""

import pymssql
import pandas as pd
import db_credentials as cre


server = cre.server
database = cre.database
user = cre.user
password = cre.password
tablenames = {'Company': '[CN_CCD].[COMPANY_FULL]', 'Contact':'[CN_CCD].[CONTACT_FULL]', 'Scrapy': '[CN_CCD].[SCRAPY_FULL]', 'Logs': '[CN_CCD].[STAGING_LOGS]', 'Summary': '[CN_CCD].[STAGING_SUMMARY]'}


def load_staging(load_list, colnames, table, sourcename, timestamp):
    conn = pymssql.connect(server, user, password, database)
    cur = conn.cursor()
    load_list = load_list[colnames]

    columns = 'STG_ID,' + ', '.join(colnames) + ', Source_Name, Source_Timestamp, Load_Timestamp'

    values = None
    for index, row in load_list.iterrows():
        row = map(lambda x: str(x).replace('\'', '\'\''), row)
        value = '\',N\''.join(row).replace('nan', '')
        value = '(N\'CNCM_{}_\' +  CONVERT(NVARCHAR(100), NEWID()), N\'{}\', N\'{}\', N\'{}\', GETDATE())'.format(table, value, sourcename, timestamp)
        if values is None:
            values = value
        else:
            values = values + ', ' + value
    query = 'INSERT INTO {}({}) VALUES {}'.format(tablenames[table], columns, values)

    # If error, delete all records related this load
    try:
        cur.execute(query)
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
        delete = 'DELETE * FROM {} WHERE SOURCE_NAME = {}'
        delete_query = map(lambda x: delete.format(x, sourcename), tablenames.values())
        delete_query = '; '.join(delete_query)
        cur.execute(delete_query)
        conn.commit()
        
    conn.close()
    return 0


def get_all(colnames, table):
    conn = pymssql.connect(server, user, password, database)

    columns = ', '.join(colnames)
    query = 'SELECT {} FROM {}'.format(columns, tablenames[table])

    result = pd.read_sql(query, conn)
    df = pd.DataFrame(result)
    conn.close()
    return df


def get_one(column, table, value, colnames):
    conn = pymssql.connect(server, user, password, database)

    columns = ', '.join(colnames)

    query = 'SELECT {} FROM {} WHERE {} = {}'.format(columns, tablenames[table], column, value)
    result = pd.read_sql(query, conn)
    df = pd.DataFrame(result)
    conn.close()
    return df

