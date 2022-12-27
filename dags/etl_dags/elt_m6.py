from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
# from function.connection import m6
# from function.m6_hashing import m6_hashing
from function.connection import m6
from function.m6_hashing import m6_hashing
import pandas as pd



# no_need = ['service_apply_settings', 'custom_delegations', 'flat_packages','customer_trackings', 'locations', 'box_sizes',
# 'bag_templates', 'agency_configs', 'shipping_domestic_address', 'service_mappings', 'agency_settings', 'partner_settings',
# 'shipping_fee_details', 'property_mappings', 'shipping_domestic_partners', 'partner_shipping_domestic_carriers',
# 'global_settings', 'shipping_domestic_locations', 'delivery_note_returns', 'warehouse_shipping_partners', 'package_process_shipping_fees',
# 'jobs', 'failed_jobs', 'product_groups', 'bag_template_agencies', 'settings', 'storage_fees',
# 'inventory_tracking_bills', 'user_identity', 'notice_stage_services', 'bag_areas', 'feature_apply_settings', 'last_mile_orders',
# 'process_shipping_fees', 'notice_stages', 'bag_wildcard_locations', 'migrations', 'shipping_domestic_carriers', 'fake_name_customers',
# 'shipping_return_partners', 'notice_stage_properties','lockings']
need = ['bags']
sql = """select table_schema, table_name from information_schema.tables 
where table_schema = 'm6' and table_type = 'base table';"""
m6_db = pd.read_sql(sql, con=m6())
m6_db = m6_db[m6_db.table_name.isin(need)]
  

default_args = {
    'start_date': datetime(2022,4,1)
}


with DAG('elt_m6', schedule_interval = '0 0 * * 1-6', default_args = default_args, catchup=False) as dag:

    elt_m6 = PythonOperator(
        task_id = 'elt_m6',
        python_callable  = m6_hashing,
        op_kwargs  = {'m6_db':m6_db}
    )