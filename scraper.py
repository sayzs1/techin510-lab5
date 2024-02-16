import re
import json
import datetime
from zoneinfo import ZoneInfo
import html
import requests
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

db_user = os.getenv('DB_USER')
db_pw = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')
conn_str = f'postgresql://{db_user}:{db_pw}@{db_host}:{db_port}/{db_name}'

def get_db_conn():
    conn = psycopg2.connect(conn_str)
    conn.autocommit = True
    return conn

URL = 'https://visitseattle.org/events/page/'
URL_LIST_FILE = './data/links.json'
URL_DETAIL_FILE = './data/data.json'

def list_links():
    res = requests.get(f'{URL}1/')
    last_page_no = int(re.findall(r'bpn-last-page-link"><a href=".+?/page/(\d+?)/.+" title="Navigate to last page">', res.text)[0])

    links = []
    for page_no in range(1, last_page_no + 1):
        res = requests.get(f'{URL}{page_no}/')
        links.extend(re.findall(r'<h3 class="event-title"><a href="(https://visitseattle.org/events/.+?/)" title=".+?">.+?</a></h3>', res.text))

    json.dump(links, open(URL_LIST_FILE, 'w'))

def get_detail_page():
    links = json.load(open(URL_LIST_FILE, 'r'))
    data = []
    err = []
    for link in links:
        try:
            row = {}
            res = requests.get(link)
            row['title'] = html.unescape(re.findall(r'<h1 class="page-title" itemprop="headline">(.+?)</h1>', res.text)[0])
            datetime_venue = re.findall(r'<h4><span>.*?(\d{1,2}/\d{1,2}/\d{4})</span> \| <span>(.+?)</span></h4>', res.text)[0]
            row['date'] = datetime.datetime.strptime(datetime_venue[0], '%m/%d/%Y').replace(tzinfo=ZoneInfo('America/Los_Angeles')).isoformat()
            row['venue'] = datetime_venue[1].strip() # remove leading/trailing whitespaces
            metas = re.findall(r'<a href=".+?" class="button big medium black category">(.+?)</a>', res.text)
            row['category'] = html.unescape(metas[0])
            row['location'] = metas[1]
            data.append(row)
        except IndexError as e:
            err.append(link)
            print(f'Error: {e}')
            print(f'Link: {link}')
    if len(err) > 0:
        for link in err:
            links.remove(link)
        json.dump(links, open(URL_LIST_FILE, 'w'))
    json.dump(data, open(URL_DETAIL_FILE, 'w'))

def get_geo_weather():
    urls = json.load(open(URL_LIST_FILE, 'r'))
    data = json.load(open(URL_DETAIL_FILE, 'r'))
    err_url = []
    err_data = []
    for url, row in zip(urls, data):
        res = requests.get(f"https://nominatim.openstreetmap.org/search.php?q={row['location']} seattle&format=jsonv2")
        if not res.json():
            res = requests.get(f"https://nominatim.openstreetmap.org/search.php?q={row['location']}&format=jsonv2")
        location = res.json()
        if not location:
            print(f'Location not found: {url}')
            err_url.append(url)
            err_data.append(row.copy())
            continue
        lat, lon = location[0]['lat'], location[0]['lon']
        row['lat'] = lat
        row['lon'] = lon
        res = requests.get(f"https://api.weather.gov/points/{lat},{lon}")
        weather_point = res.json()
        try:
            forecast_url = weather_point['properties']['forecast']
            forecast_url_grid = weather_point['properties']['forecastGridData']
            res = requests.get(forecast_url)
            row['condition'] = res.json()['properties']['periods'][0]['shortForecast']
            res = requests.get(forecast_url_grid)
            row["minTemperature"] = res.json()['properties']["minTemperature"]["values"][0]["value"]
            row["maxTemperature"] = res.json()['properties']["maxTemperature"]["values"][0]["value"]
            row["windChill"] = res.json()['properties']["windChill"]["values"][0]["value"]
        except Exception as e:
            print(f'Weather not found: {url}, {row["venue"]}')
            print(f'Error: {e}')
            err_url.append(url)
            err_data.append(row.copy())
            # continue
    if err_url:
        urls = [u for u in urls if u not in err_url]
        data = [d for d in data if d not in err_data]
        json.dump(urls, open(URL_LIST_FILE, 'w'), indent=2)
    json.dump(data, open(URL_DETAIL_FILE, 'w'), indent=2)

def insert_to_pg():
    q = '''
    CREATE TABLE IF NOT EXISTS events (
        url TEXT PRIMARY KEY,
        title TEXT,
        date TIMESTAMP WITH TIME ZONE,
        venue TEXT,
        category TEXT,
        location TEXT,
        latitude FLOAT,
        longitude FLOAT,
        condition TEXT,
        mintemperature FLOAT,
        maxtemperature FLOAT,
        windchill FLOAT
    );
    '''
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(q)
    
    urls = json.load(open(URL_LIST_FILE, 'r'))
    data = json.load(open(URL_DETAIL_FILE, 'r'))
    for url, row in zip(urls, data):
        q = '''
        INSERT INTO events (url, title, date, venue, category, location, latitude, longitude, condition, mintemperature, maxtemperature, windchill)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (url) DO NOTHING;
        '''
        cur.execute(q, (
            url, 
            row['title'], 
            row['date'], 
            row['venue'], 
            row['category'], 
            row['location'], 
            row['lat'], 
            row['lon'], 
            row['condition'], 
            row['minTemperature'], 
            row['maxTemperature'], 
            row['windChill']
            ))

if __name__ == '__main__':
    list_links()
    get_detail_page()
    get_geo_weather()
    insert_to_pg()