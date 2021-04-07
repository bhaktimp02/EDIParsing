import cx_Oracle
from scripts.parsingv1 import dataParsing

# dsn_tns = cx_Oracle.makedsn('localhost', '1521', 'orcl')
# dns = cx_Oracle.makedsn('localhost','1521','orcl')
# #con = cx_Oracle.connect('sys', 'Niya.2012', dns, cx_Oracle.SYSDBA)


from sqlalchemy import create_engine
import pandas as pd

conn = create_engine('oracle+cx_oracle://username:password@localhost:1521/orcl?mode=2')

#df.to_sql('TEST', conn, if_exists='replace')
#con = cx_Oracle.connect('SYS/Niya.2012@localhost')

print("Successfully connected to Oracle Database")
df=dataParsing()
#print(df[1:])
#df.to_excel('output\EDI_850_3f.xlsx')
# df=pd.read_sql('SELECT * FROM STG_DOC_TRACK',conn)
# print(df)
df.to_sql('stg_doc_track', conn, if_exists='append', chunksize = 1000)
print('Data inserted to Oracle table')