from __future__ import print_function

from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators import DummyOperator, PythonOperator, \
    BranchPythonOperator
from airflow.operators.sensors import TimeDeltaSensor
from airflow.utils.trigger_rule import TriggerRule
from pyppeteer_smiles import get_data_URL, insert_into_table, \
    read_scraped_date, write_scraped_date
import dateparser
import re
import logging

AMOUNT_DAYS = 2


def create_dag(dag_id,
               schedule,
               start_date,
               delta_sensor,
               airpots_codes,
               default_args):

    dag = DAG(
        dag_id,
        schedule_interval=schedule,
        start_date=start_date,
        default_args=default_args
        )

    dag.doc_md = """
    # DAG fetching data from smiles.com.ar
    ### procesing and dumping on postgresql
    """

    start = TimeDeltaSensor(
        task_id='wait_to_start',
        delta=timedelta(minutes=delta_sensor),
        dag=dag)

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
        logging.info(raw_data)
        if raw_data is not None:
            flat_list = [item for sublist in raw_data for item in sublist]
            for row in flat_list:
                row = list(row)
                # add À-ÿ for spanish accents
                date = '/'.join(list(re.compile("([A-ZÀ-ÿ]+)(\d+)([A-ZÀ-ÿ]+)").split(row[1]))[2:4])
                date = dateparser.parse(date, languages=['pt', 'es'], date_formats=['%d/%b']).strftime('%Y-%m-%d')
                row[1] = date
                row[4] = '00:' + row[4]
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
    date_start = read_scraped_date(airpots_codes) + timedelta(days=1)
    date_end = date_start + timedelta(days=AMOUNT_DAYS)
    date_generated = [date_start + timedelta(days=x) for x in range(0, (date_end-date_start).days)]

    for i, date in enumerate(date_generated):
        date_ml = str(date.timestamp())[:8] + '00000'
        url_dated = """https://www.smiles.com.ar/emission?originAirportCode={}&destinationAirportCode={}&departureDate={}&adults=1&children=0&infants=0&isFlexibleDateChecked=false&tripType=3&currencyCode=BRL&segments=2&departureDate2={}&originAirportCode2={}&destinationAirportCode2={}""".format(airpots_codes[0][0], airpots_codes[1], date_ml,
                     date_ml, airpots_codes[0][1], airpots_codes[1])

        get_data_op = PythonOperator(
            task_id='get_data_{}and{}to{}_{}'.format(airpots_codes[0][0],
                                                     airpots_codes[0][1],
                                                     airpots_codes[1], i),
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

    return dag


airports_var = Variable.get("airport_codes", deserialize_json=True)
departures_list = airports_var["departures"]
arrivals_list = airports_var["arrivals"]

airport_combinations = [(dep, arr) for dep in departures_list for arr in arrivals_list]

# build a dag for each number in range(10)
for t, airport_code in enumerate(airport_combinations):
    # airport_code = str(airport_code).replace('\W+', '_')
    airport_code_str = re.sub('\W+', '_', str(airport_code))
    dag_id = 'scrap_{}'.format(airport_code_str[:-1])
    # start_date = airflow.utils.dates.days_ago(2)
    start_date = datetime(2020, 2, 23)
    schedule = '@daily'
    delta_sensor = 3*t
    default_args = {'owner': 'airflow',
                    'depends_on_past': False,
                    'retries': 3,
                    'retry_delay': timedelta(minutes=2),
                    }
    globals()[dag_id] = create_dag(dag_id, schedule, start_date,
                                   delta_sensor, airport_code, default_args)
