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
    'dag_id': 'Postgres_selectall_test',
    'start_date': airflow.utils.dates.days_ago(2)
    }

dag = DAG(
    'Postgres_selectall_test',
    schedule_interval="@once",
    default_args=dag_params
    )

# PostgresHook
def get_date_from_db():
    # request = "SELECT * FROM smiles_flight"
    request2 = '''SELECT min(flight_date) as min_date,
                max(flight_date) as max_date
                FROM smiles_flight
                WHERE smiles_flight.flight_org = 'EZE'
                AND smiles_flight.flight_dest = 'MAD'
                '''
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request2)
    sources = cursor.fetchall()
    cursor.close()
    connection.close()
    return sources


def select_min_max_date():
    conn = None
    try:  
        request = '''SELECT min(flight_date) as min_date,
                    max(flight_date) as max_date
                    FROM smiles_flight
                    WHERE smiles_flight.flight_org = \'{}\'
                    AND smiles_flight.flight_dest = \'{}\'
                    '''

        request = request.format('EZE', 'MAD')
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(request)
        sources = cursor.fetchall()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
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
    python_callable=select_min_max_date,
    dag=dag,
)

start >> t2