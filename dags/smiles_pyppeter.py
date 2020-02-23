from __future__ import print_function

from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators import DummyOperator, PythonOperator, \
    BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from pyppeteer_smiles import get_data_URL, insert_into_table, \
    read_scraped_date, write_scraped_date
import dateparser
import re
import numpy as np

AMOUNT_DAYS = 2

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'smiles_pyppeteer',
    schedule_interval="@once",
    default_args=default_args
    )

dag.doc_md = """
# DAG fetching data from smiles.com.ar
### procesing and dumping on postgresql
"""

start = DummyOperator(
    task_id="start",
    dag=dag
)
branches = []


def return_dates_branches(**kwargs):
    return branches


gen_url_branch = BranchPythonOperator(
    task_id='generate_url_dates',
    provide_context=True,
    python_callable=return_dates_branches,
    dag=dag
)


def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids=return_dates_branches())
    data = []
    print(raw_data)
    if raw_data is not None:
        np_array = np.array(raw_data)
        flat_list = [item for sublist in raw_data for item in sublist]
        for row in flat_list:
            row = list(row)
            # add À-ÿ for spanish accents
            date = '/'.join(list(re.compile("([A-ZÀ-ÿ]+)(\d+)([A-ZÀ-ÿ]+)").split(row[1]))[2:4])
            date = dateparser.parse(date, languages=['pt', 'es'], date_formats=['%d/%b']).strftime('%Y-%m-%d')
            row[1] = date
            row[4] = ':00' + row[4]
            row[5] = int(row[5].replace('.', ''))
            row[6] = int(row[6].replace('.', ''))
            row[8] = row[8].split(' ')[-1]
            row.insert(0, datetime.now().strftime('%Y-%m-%d'))
            data.append(tuple(row))
        return data
    else:
        print('No se recibio datos')


t2 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    depends_on_past=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    provide_context=True,
    dag=dag,
)

t2.doc_md = """
#### Task Documentation
Transform fetched data
@return a list of tuples
"""

# def gen_url_dates(**kwargs):
date_start = read_scraped_date() + timedelta(days=1)
date_end = date_start + timedelta(days=AMOUNT_DAYS)
date_generated = [date_start + timedelta(days=x) for x in range(0, (date_end-date_start).days)]

for i, date in enumerate(date_generated):
    date_ml = str(date.timestamp())[:8] + '00000'
    url_dated = 'https://www.smiles.com.ar/emission?originAirportCode=COR&destinationAirportCode=EZE&departureDate={}&adults=1&children=0&infants=0&isFlexibleDateChecked=false&tripType=1&currencyCode=BRL'.format(date_ml)

    get_data_op = PythonOperator(
        task_id='get_data_COR_EZE_{}'.format(i),
        python_callable=get_data_URL,
        op_kwargs={'URL': url_dated},
        trigger_rule=TriggerRule.ONE_SUCCESS,
        provide_context=True,
        dag=dag,
    )
    branches.append(get_data_op.task_id)
    get_data_op.set_upstream(gen_url_branch)
    get_data_op.set_downstream(t2)
    get_data_op.doc_md = """
    #### Task Documentation
    Fetch data from passed url
    return list of semi-parsed data
    """

insert_data = PythonOperator(
    task_id='insert_data',
    python_callable=insert_into_table,
    provide_context=True,
    dag=dag,
)


set_scraped_date = PythonOperator(
    task_id='set_date_variable',
    python_callable=write_scraped_date,
    dag=dag,
    op_kwargs={'amount_days': AMOUNT_DAYS},
)

insert_data.doc_md = """
#### Task Documentation
Insert parsed and transformed data into table
"""
t2.set_downstream(insert_data)
gen_url_branch.set_upstream(start)
set_scraped_date.set_upstream(insert_data)