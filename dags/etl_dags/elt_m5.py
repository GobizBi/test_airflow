from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from function.connection import m5
from function.m5_hashing import m5_hashing
import pandas as pd



# need = ['users', 'order_items', 'order_services', 'order_source_invoice', 'order_properties', 'complaint_sellers',
#        'order_details', 'order_freight_bill_logs','payment_request_invoice_codes', 'payment_requests','payment_request_transaction_codes',
#        'transactions', 'orders', 'partners', 'order_freight_bills', 'order_history_time', 'transaction_files',
#        'agencies', 'transaction_invoice_codes', 'account_purchasers', 'order_policies', 'services','complaint_seller_history_time','taobao_purchase_orders']

need = ['services']

sql = """select table_schema, table_name from information_schema.tables where table_schema != 'information_schema' and table_type = 'base table';"""
m5_db = pd.read_sql(sql, m5())
m5_db = m5_db[m5_db.table_name.isin(need)]
  

default_args = {
    'start_date': datetime(2022,4,1)
}


with DAG('elt_m5', schedule_interval = '0 0 * * 1-6', default_args = default_args, catchup=False) as dag:

    elt_m5 = PythonOperator(
        task_id = 'elt_m5',
        python_callable  = m5_hashing,
        op_kwargs  = {'m5_db':m5_db}
    )