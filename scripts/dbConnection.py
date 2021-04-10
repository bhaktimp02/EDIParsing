#from .edi.nike.comarsingv1 import dataParsing
import cx_Oracle
# dsn_tns = cx_Oracle.makedsn('localhost', '1521', 'orcl')
#dns = cx_Oracle.makedsn('edinkedevb2bdb03.ediaws.edi.nike.com','1521','dvdoctrc')
#con = cx_Oracle.connect('gis', 'atlanta7', dns, cx_Oracle)
import pdb as bp

from sqlalchemy import create_engine
import pandas as pd

try:
    conn = create_engine('oracle+cx_oracle://username:password@localhost:1521/orcl?mode=2')
except:
    conn = create_engine('oracle+cx_oracle://gis:atlanta7@edinkedevb2bdb03.ediaws.edi.nike.com:1521/dvdoctrc')

#df.to_sql('TEST', conn, if_exists='replace')
#con = cx_Oracle.connect('SYS/Niya.2012@localhost')

print("Successfully connected to Oracle Database")
def dbInsert(df):
    print(conn)
    print(df.head())
    print(df.columns)
    df.to_sql('stg_doc_track', conn, if_exists='append', chunksize = 1000,index=False, index_label='id')
    print('Data inserted to Oracle table')

