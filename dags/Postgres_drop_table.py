import uuid
import airflow
from datetime import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

dag_params = {
    'owner': 'airflow',
    'dag_id': 'Postgres_drop_table',
    'start_date': datetime.today(),
}

dag = DAG(
    'Postgres_drop_table',
    default_args=dag_params,
    schedule_interval="@once"
    )

drop_table = PostgresOperator(
    task_id='drop_table',
    postgres_conn_id='postgres_default',
    sql='DROP TABLE IF EXISTS smiles_flight',
    dag=dag
)


start = DummyOperator(
    task_id="start",
    dag=dag
)

start >> drop_table
