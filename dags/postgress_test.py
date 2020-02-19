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
    'dag_id': 'Postgres_selectall_test',
    'start_date': datetime.today(),
    'schedule_interval': "@once"
}

dag = DAG(
    'Postgres_selectall_test',
    default_args=dag_params
    )

# PostgresHook
def get_date_from_db():
    request = "SELECT * FROM smiles_flight"
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    return sources


# PostgresOperator
"""select_all_op = PostgresOperator(
    task_id='select_all_op',
    postgres_conn_id='postgres_default',
    sql='SELECT * FROM smiles_flight',
    trigger_rule=TriggerRule.ALL_DONE,
    # parameters=(uuid.uuid4().int % 123456789, datetime.now(), uuid.uuid4().hex[:10]),
    dag=dag
)"""

start = DummyOperator(
    task_id="start",
    dag=dag
)


t2 = PythonOperator(
    task_id='select_all_hook',
    python_callable=get_date_from_db,
    dag=dag,
)

start >> t2