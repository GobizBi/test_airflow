import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq
from time import gmtime, strftime, sleep
from datetime import datetime, timedelta
import pytz
from function.connection import  m5, sent_noti,convert_type_bigquery, hash_string
import os
from dotenv import load_dotenv
load_dotenv()
SERVICE_ACCOUNT_FILE = os.getenv('credential')
project_id = os.getenv('project_id')
ssh_key =  os.getenv('ssh_pkey')


credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
project_id = os.getenv('project_id')
client = bigquery.Client(credentials= credentials,project=project_id)


def m5_hashing(m5_db):
    def etl_m5(P_schema, P_table,T_schema, S_schema, S_table,conn):
        c = pd.read_sql("""select column_name as name, data_type as type from information_schema.columns 
                        where table_schema = '{}' and table_name = '{}'""".format(P_schema, P_table), con=conn)
        c = c.to_dict(orient='records')
        c = convert_type_bigquery(c)
        if 'id' not in [l.get('name') for l in c] or ('modified_at' not in [l.get('name') for l in c] and 'updated_at' not in [l.get('name') for l in c]):
            print("Start schema: " + T_schema + " table: "+ P_table +" at: " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
            data = pd.read_sql('select * from {}.{}'.format(P_schema, P_table), con=conn)
            data = data.replace({'0000-00-00':'1970-01-01'}, regex=True)
            data = data.replace({'0000-':'1970-'}, regex=True)
            data['hash'] = data.to_json(orient='records', lines=True).splitlines()
            data['hash'] = data['hash'].apply(lambda x : hash_string(x))
            pandas_gbq.to_gbq(data, T_schema + "." + P_table, project_id="gobiz-solution", if_exists="replace", credentials=credentials, table_schema=c)
            end = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            sleep(10)
            end = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            print("Finish schema: "+ T_schema + " table: " + P_table +" at: "+ end)
        else:
            print("Start schema: " + S_schema + " table: "+ S_table +" at: " + strftime("%Y-%m-%d %H:%M:%S", gmtime()))
            sql_bqr_id = """
            SELECT max(id) id  FROM gobiz-solution.{}.{}
            """.format(T_schema,P_table)

            sql_bqr_mdf = """
            SELECT max(modified_at) modified_at FROM gobiz-solution.{}.{}
            """.format(T_schema,P_table)

            sql_bqr_ud = """
            SELECT max(updated_at) updated_at FROM gobiz-solution.{}.{}
            """.format(T_schema,P_table)

            last_id = pd.read_gbq(sql_bqr_id,credentials =credentials ).id.values[0]
            modifield_time = 0
            updated_time = 0

            try:
                updated_time = str(pd.to_datetime(pd.read_gbq(sql_bqr_ud,credentials =credentials).updated_at.values[0]).date() - timedelta(days=2))
                print("updated_time: ", updated_time)
            except:
                modifield_time = str(pd.to_datetime(pd.read_gbq(sql_bqr_mdf,credentials =credentials ).modified_at.values[0]).date() - timedelta(days=2))

            if updated_time != 0:
                new_data = pd.read_sql(""" select * from {}.{} where id >= {} or updated_at >= '{}' """
                            .format(P_schema, P_table,last_id,updated_time), con=conn)
            else:
                new_data = pd.read_sql(""" select * from {}.{} where id >= {} or modified_at >= '{}' """
                            .format(P_schema, P_table,last_id,modifield_time), con=conn)
            new_data = new_data.replace({'0000-00-00':'1970-01-01'}, regex=True)
            new_data = new_data.replace({'0000-':'1970-'}, regex=True)
            if P_table == "transactions":
                new_data['transaction_time'] = pd.to_datetime(new_data['transaction_time'], format="%Y-%m-%d %H:%M:%S")
                new_data['resolved_time'] = pd.to_datetime(new_data['resolved_time'], format="%Y-%m-%d %H:%M:%S")
            new_data['hash'] = new_data.to_json(orient='records', lines=True).splitlines()
            new_data['hash'] = new_data['hash'].apply(lambda x : hash_string(x))   
            pandas_gbq.to_gbq(new_data, S_schema + "." + S_table, project_id="gobiz-solution", if_exists="replace", credentials=credentials, table_schema=c)
            update_columns = ''
            for column in new_data.columns:
                update_columns += 'T.'+column + '= S.' + column + ',' 
            update_columns = update_columns[:-1]
            sql = """merge {}.{} T
                using {}.{} S 
                on T.id = S.id 
                when matched and T.hash != S.hash then 
                update set {} 
                when not matched 
                then insert row""".format(T_schema, P_table, S_schema, S_table,update_columns)
            client.query(sql).result()
            print("da chay merge")
            end = strftime("%Y-%m-%d %H:%M:%S", gmtime())
            print("Finish schema: "+ S_schema + " table: " + S_table +" at: "+ end)
            

    tz_VN=pytz.timezone('Asia/Ho_Chi_Minh')
    for  P_table in m5_db.table_name:
            try:
                start_date = datetime.now(tz_VN)
                state = 'RUNNING'
                end_date = None
                error = None
                etl_m5('m5',P_table,"m5_production","snapshot_hashing","m5_"+P_table,m5())
                end_date = datetime.now(tz_VN)
                state = 'SUCCESS'
                data_to_bqr = {"dataset":'m5_production',"table_name":P_table,"start_date":start_date,"end_date":end_date,"state":state,"error":error}
                pandas_gbq.to_gbq(pd.DataFrame(data_to_bqr,index=[0]), "log_job.log_job", project_id="gobiz-solution", if_exists="append", credentials=credentials)
            except Exception as e:
                end_date = datetime.now(tz_VN)
                state = 'FAILED'
                error = e
                sent_noti("m5_production",P_table,error)
                ### UPDATE LOG to table_info 
                data_to_bqr = {"dataset":'m5_production',"table_name":P_table,"start_date":start_date,"end_date":end_date,"state":state,"error":error}
                pandas_gbq.to_gbq(pd.DataFrame(data_to_bqr,index=[0]), "log_job.log_job", project_id="gobiz-solution", if_exists="append", credentials=credentials)