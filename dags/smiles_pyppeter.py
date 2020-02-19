from __future__ import print_function

from datetime import datetime
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pyppeteer_smiles import get_data_URL, transform_data
from _datetime import timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime.today()
}

dag = DAG(
    'smiles_pyppeteer',
    schedule_interval="@once",
    default_args=default_args
    )


start = DummyOperator(
    task_id="start",
    dag=dag
)

t1 = PythonOperator(
    task_id='get_html_data',
    python_callable=get_data_URL,
    op_kwargs={'URL': 'https://www.smiles.com.ar/emission?originAirportCode=COR&destinationAirportCode=EZE&departureDate=1582210800000&adults=1&children=0&infants=0&isFlexibleDateChecked=false&tripType=1&currencyCode=BRL'},
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

start >> t1 >> t2
