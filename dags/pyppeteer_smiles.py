import os 
import asyncio
from pyppeteer import launch
import pprint
from airflow.hooks.postgres_hook import PostgresHook
import subprocess
import datetime
from airflow.models import Variable
from _datetime import timedelta
import logging


async def get_browser():
    return await launch({"headless": True,
                        'args': ['--no-sandbox', '--disable-setuid-sandbox']})


async def get_page(browser, url):
    page = await browser.newPage()
    await page.goto(url, waitUntil='networkidle0')
    load_more = await page.querySelector('div.more__flights .btn-primary')
    if load_more is not None:
        print('have more')
        await load_more.tap()
    return page


async def extract_data(page):
    # Select tr with a th and td descendant from table
    cost_miles_club = await page.querySelectorAllEval(
        'div.miles .list-group-item.club span',
        '(elements => elements.map(e => e.textContent))')
    cost_miles = await page.querySelectorAllEval(
        'div.miles .list-group-item span',
        '(elements => elements.map(e => e.textContent))')
    date = await page.querySelectorAllEval(
        'div.calendar-day.selected',
        '(elements => elements.map(e => e.textContent))')
    departure = await page.querySelectorAllEval(
        'div.travel-details .travel-origin .travel-airport',
        '(elements => elements.map(e => e.textContent))')
    arrival = await page.querySelectorAllEval(
        'div.travel-details .travel-arrival .travel-airport',
        '(elements => elements.map(e => e.textContent))')
    duration = await page.querySelectorAllEval(
        'div.travel-details .travel-duration',
        '(elements => elements.map(e => e.textContent))')
    scale = await page.querySelectorAllEval(
        'div.travel-details .travel-stops',
        '(elements => elements.map(e => e.textContent))')
    airline = await page.querySelectorAllEval(
        'div.group-info-flights .company-thumb div',
        '(elements => elements.map(e => e.className))')

    if len(date) < len(cost_miles_club):
        for i in range(len(cost_miles_club) - len(date)):
            date.append(date[0])

    cost_miles_list = []
    for i in range(0, len(cost_miles), 4):
        cost_miles_list.append(cost_miles[i:i + 4][1])
    urls = []
    for i in range(len(cost_miles_club)):
        urls.append(page.url)

    result = list(zip(urls, date, departure, arrival, duration,
                      cost_miles_club, cost_miles_list, scale, airline))

    return result


async def extract(browser, url):
    page = await get_page(browser, url)
    raw_data = await extract_data(page)
    return raw_data


async def extract_all(url):
    browser = await get_browser()
    return await extract(browser, url)


def get_data_URL(**kwargs):
    URL = kwargs['URL']
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(extract_all(URL))
    logging.info(result)
    return result


# PostgresHook
def insert_into_table(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_data')
    print(data)
    if data is not None:
        request = '''INSERT INTO smiles_flight (scraped_date, flight_url,
                    flight_date, flight_org, flight_dest, flight_duration,
                    flight_club_miles, flight_miles, flight_airline,
                    flight_stop)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(request, data)
        conn.commit()
        cursor.close()
        conn.close()


def select_min_max_date(orig, dest):
    conn = None
    try:  
        request = '''SELECT min(flight_date) as min_date, 
        max(flight_date) as max_date 
        FROM smiles_flight 
        WHERE smiles_flight.flight_org = \'{}\' 
        AND smiles_flight.flight_dest = \'{}\' 
        '''

        request = request.format(orig, dest)
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


# select min and max date scraped and return the next date to scrap 
def read_scraped_date(airports):

    dates_var = Variable.get("dates", deserialize_json=True)
    start_date = datetime.datetime.strptime(dates_var["start_date"], "%Y-%m-%d")
    end_date = datetime.datetime.strptime(dates_var["end_date"], "%Y-%m-%d")
    sources = select_min_max_date(airports[0][0],  airports[1])

    if sources[0][0] is not None:
        min_date = datetime.datetime(sources[0][0].year,
                                     sources[0][0].month,
                                     sources[0][0].day)
        max_date = datetime.datetime(sources[0][1].year,
                                     sources[0][1].month,
                                     sources[0][1].day)
        if min_date >= start_date and max_date < end_date:
            return max_date + timedelta(days=1)
        elif max_date < start_date:
            return start_date
        elif max_date + timedelta(days=1) > end_date:
            return start_date
    else:
        return start_date + timedelta(days=2)
