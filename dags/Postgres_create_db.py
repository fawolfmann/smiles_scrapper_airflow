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
    'dag_id': 'Postgres_create_db',
    'start_date': datetime.today(),
    
}

dag = DAG(
    'Postgres_create_db',
    schedule_interval="@once",
    default_args=dag_params
    )

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_default',
    sql='''
        CREATE TABLE smiles_flight (
            flight_id SERIAL PRIMARY KEY,
            scrpaed_date DATE NOT NULL,
            flight_url VARCHAR(255) NOT NULL,
            flight_date DATE NOT NULL,
            flight_org CHAR(3) NOT NULL,
            flight_dest CHAR(3) NOT NULL,
            flight_duration INTERVAL NOT NULL,
            flight_club_miles INTEGER NOT NULL,
            flight_miles INTEGER,
            flight_airline VARCHAR(255),
            flight_stop VARCHAR(255)
        );''',
        dag=dag
)


start = DummyOperator(
    task_id="start",
    dag=dag
)

start >> create_table
