import os 
import asyncio
from pyppeteer import launch
import pprint
import dateparser
import re


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
    # kwargs['ti'].xcom_push(key='raw_data', value=result)
    return result


def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='get_html_data')
    data = []
    try:
        for row in raw_data:
            row = list(row)
            date = '/'.join(list(re.compile("([A-Z]+)(\d+)([A-Z]+)").split(row[1]))[2:4])
            date = dateparser.parse(date, languages=['pt', 'es'], date_formats=['%d/%b']).strftime('%Y-%m-%d')
            row[1] = date
            row[4] = row[4] + ':00'
            row[5] = int(row[5].replace('.', ''))
            row[6] = int(row[6].replace('.', ''))
            row[8] = row[8].split(' ')[-1]
            data.append(row)
        kwargs['ti'].xcom_push(key='transformed_data', value=data)
    except TypeError:
        print('No se recibio datos')
