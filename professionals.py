import snowflake.connector as sf
import pandas as pd
import matplotlib.pyplot as plt
from config import config
import numpy as np

# Connection String
conn = sf.connect(
    user=config.username,
    password=config.password,
    account=config.account
)

def test_connection(connect, query):
    cursor = connect.cursor()
    cursor.execute(query)
    cursor.close()

# try:
#     sql = """
#     SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."SPGSCOREVALUE"
#     LIMIT 5;
#     """
#     test_connection(conn, sql)
#     cursor = conn.cursor()
#     cursor.execute(sql)
#     for line in cursor:
#         print(line)
# except Exception as e:
#     print(e)

sql1 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQPROFESSIONAL";
""" 
#cursor = conn.cursor()
#cursor.execute(sql)
#data from SAMINDUSTRYNAME
sql2 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQPROTOPROFUNCTION";
"""
sql3 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQPROFUNCTION";
"""
sql4 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."CIQCOMPENSATION";
"""
sql5 = """
SELECT * FROM "MI_XPRESSCLOUD"."XPRESSFEED"."SP500";
"""
#cleaning CIQPROFESSIONAL dataset
df1 = pd.read_sql(sql1, conn)
df1 = df1.dropna()
df1 = df1.drop_duplicates()
#print(df1.head())
#cleaning CIQPROTOPROFUNCTION dataset
df2 = pd.read_sql(sql2, conn)
#cleaning CIQPROFUNCTION dataset
df3 = pd.read_sql(sql3, conn)
#cleaning CIQCOMPENSATION dataset
df4 = pd.read_sql(sql4, conn)
#cleaning SP500 dataset
df5 = pd.read_sql(sql5, conn)

df5 = pd.merge(df1, df2, how='left', on='PROID')
df6 = pd.merge(df5, df3, how='left', on='PROFUNCTIONID')
df7 = pd.merge(df6, df4, how='left', on='PROID')
df8 = df7[df7['COMPANYID'] == df500['COMPANYID']]
