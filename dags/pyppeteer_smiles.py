import os 
import asyncio
from pyppeteer import launch
import pprint
from airflow.hooks.postgres_hook import PostgresHook
import subprocess
import datetime
from airflow.models import Variable
from _datetime import timedelta

URL = "https://www.smiles.com.ar/emission?originAirportCode=COR&destinationAirportCode=EZE&departureDate=1582210800000&adults=1&children=0&infants=0&isFlexibleDateChecked=false&tripType=1&currencyCode=BRL"
URL1 = "https://www.smiles.com.ar/emission?originAirportCode=EZE&destinationAirportCode=MAD&departureDate=1583938800000&adults=1&children=0&infants=0&isFlexibleDateChecked=false&tripType=2&currencyCode=BRL"


async def get_browser():
    return await launch({"headless": True, 'args': ['--no-sandbox', '--disable-setuid-sandbox']})


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

    result = list(zip(urls, date, departure, arrival, duration, cost_miles_club, cost_miles_list, scale, airline))

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
    return result


# PostgresHook
def insert_into_table(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_data')
    print(data)
    if data is not None:
        request = '''INSERT INTO smiles_flight (scraped_date, flight_url, flight_date,
                    flight_org, flight_dest, flight_duration, flight_club_miles,
                    flight_miles, flight_airline, flight_stop)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'''
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(request, data)
        conn.commit()
        cursor.close()
        conn.close()
    # sources = cursor.fetchall()


def read_scraped_date():
    dates_var = Variable.get("dates_CBA_EZE", deserialize_json=True)
    date_list = dates_var["dates_scraped"]

    if len(date_list) != 0:
        date = datetime.datetime.strptime(date_list[-1], "%Y-%m-%d")
    else:
        date = datetime.datetime.now()
    return date


def write_scraped_date(amount_days):
    dates_var = Variable.get("dates_CBA_EZE", deserialize_json=True)
    date_list = dates_var["dates_scraped"]
    date = datetime.datetime.strptime(date_list[-1], "%Y-%m-%d")
    date_list.append((date+timedelta(days=amount_days)).strftime("%Y-%m-%d"))
    dates_var["dates_scraped"] = date_list
    Variable.set("dates_CBA_EZE", dates_var, serialize_json=True)
