from airflow.models import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from m6_hashing_v2 import m6_hashing
from new_etl.airflow.dags.function_get_table_run_v2 import get_table_m6


default_args = {
    'start_date': datetime(2022,4,1)
}



with DAG('test_airflow', schedule_interval = '0 0 * * 1-6', default_args = default_args, catchup=False) as dag:

    elt_m6 = PythonOperator(
        task_id = 'elt_m6',
        python_callable  = m6_hashing,
        op_kwargs  = {'m6_db':get_table_m6()}
    )