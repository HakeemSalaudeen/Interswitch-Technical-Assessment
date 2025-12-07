from airflow.sdk import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from datetime import datetime, timedelta
from include.validate_data import _validate
from include.load_to_ch import _load_to_ch
from email_alert.email_alert import task_fail_alert, dag_success_alert


default_args = {
    'owner': 'abdulhakeem',
    'start_date': datetime(2025, 12, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=2), 
    'on_failure_callback': task_fail_alert,
}

with DAG(
    dag_id="weekend_trip_report",
    default_args=default_args,
    on_success_callback=dag_success_alert,
    schedule=None,
    catchup=False
) as dag:

    extract_data = SQLExecuteQueryOperator(
        task_id='extract_data',
        conn_id='postgres-db',
        sql="sql/weekend_trips.sql"
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=_validate
    )

    load = PythonOperator(
        task_id="load_clickhouse",
        python_callable=_load_to_ch
    )


    extract_data >> validate_data >> load