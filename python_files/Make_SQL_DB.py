import time
import pandas as pd
# from sqlalchemy import create_engine
import psycopg2

# https://towardsdatascience.com/upload-your-pandas-dataframe-to-your-database-10x-faster-eb6dc6609ddf


#Connection String
conn_string = 'postgres://user:password@host/discharge'

# Read in the data
data_folder = 'F:\\data\\fluvial\\nooksack\\uw_hydro\\historic\\'
data_file = 'historical_livneh_VIC_P3-NOOFE-streamflow-1.0.csv'
df = pd.read_csv(data_folder + data_file)
df.columns = [c.lower() for c in df.columns] #postgres doesn't like capitals or spaces

#perform COPY test and print result
sql = '''
COPY nook_test
FROM 'F:/data/fluvial/nooksack/uw_hydro/historic/historical_livneh_VIC_P3-NOOFE-streamflow-1.0.csv'
DELIMITER ',' CSV HEADER;
'''
table_create_sql = '''
CREATE TABLE IF NOT EXISTS nook_test (date              DATE,
                                      streamflow        FLOAT(3),
                                      model             VARCHAR(60),
                                      time_frame        VARCHAR(60))
'''

pg_conn = psycopg2.connect("dbname=discharge user=postgres password=password")
cur = pg_conn.cursor()
cur.execute(table_create_sql)
cur.execute('TRUNCATE TABLE nook_test') #Truncate the table in case you've already run the script before

start_time = time.time()
# df.to_csv('upload_test_data_from_copy.csv', index=False, header=False) #Name the .csv file reference in line 29 here
cur.execute(sql)
pg_conn.commit()
cur.close()
print("COPY duration: {} seconds".format(time.time() - start_time))
