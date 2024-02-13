import re
import json
import datetime
from zoneinfo import ZoneInfo
import html
import requests
from db import get_db_conn

base_url = "https://nominatim.openstreetmap.org/search.php"

# Function to get latitude and longitude for a given location and region
def get_lat_long(location, region):
    query_params = {
        "q": f"{location}, {region}",
        "format": "jsonv2"
    }
    res = requests.get(base_url, params=query_params)
    data = res.json()

    if not data:
        query_params_location = {
            "q": f"{location}",
            "format": "jsonv2"
        }
        res_location = requests.get(base_url, params=query_params_location)
        data_location = res_location.json()

        if data_location:
            return data_location[0]["lat"], data_location[0]["lon"]

        query_params_region = {
            "q": f"{region}",
            "format": "jsonv2"
        }
        res_region = requests.get(base_url, params=query_params_region)
        data_region = res_region.json()

        if data_region:
            return data_region[0]["lat"], data_region[0]["lon"]

        return None, None

    return data[0]["lat"], data[0]["lon"]

# Function to get weather forecast for a given latitude and longitude
def get_weather_forecast(latitude, longitude):
    url = f"https://api.weather.gov/points/{latitude},{longitude}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        forecast_url = data['properties']['forecast']
        
        # Fetch the forecast data
        forecast_response = requests.get(forecast_url)
        if forecast_response.status_code == 200:
            forecast_data = forecast_response.json()
            return forecast_data['properties']['periods']
    
    return None

# Function to format date and time from the CSV file
def format_date_time(date, time):
    if "Ongoing" in date:
        return None  # Return None for "Ongoing" date
    elif "Now" in date:
        # If the date contains "Now," use the current date and set the time
        current_date = datetime.now()
        return current_date.replace(hour=int(time[:2]), minute=int(time[3:5]))
    else:
        # Use the regular date and time format
        return datetime.datetime.strptime(f'{date} {time}', '%m/%d/%Y %I:%M%p')

URL = 'https://visitseattle.org/events/page/'
URL_LIST_FILE = './data/links.json'
URL_DETAIL_FILE = './data/data.json'

def list_links():
    res = requests.get(URL + '1/')
    last_page_no = int(re.findall(r'bpn-last-page-link"><a href=".+?/page/(\d+?)/.+" title="Navigate to last page">', res.text)[0])

    links = []
    for page_no in range(1, last_page_no + 1):
        res = requests.get(URL + str(page_no) + '/')
        links.extend(re.findall(r'<h3 class="event-title"><a href="(https://visitseattle.org/events/.+?/)" title=".+?">.+?</a></h3>', res.text))

    json.dump(links, open(URL_LIST_FILE, 'w'))

def get_detail_page():
    links = json.load(open(URL_LIST_FILE, 'r'))
    data = []
    for link in links:
        try:
            row = {}
            res = requests.get(link)
            row['title'] = html.unescape(re.findall(r'<h1 class="page-title" itemprop="headline">(.+?)</h1>', res.text)[0])
            datetime_venue = re.findall(r'<h4><span>.*?(\d{1,2}/\d{1,2}/\d{4})</span> \| <span>(.+?)</span></h4>', res.text)[0]
            row['date'] = format_date_time(datetime_venue[0], datetime_venue[1])
            row['venue'] = datetime_venue[1].strip() # remove leading/trailing whitespaces
            metas = re.findall(r'<a href=".+?" class="button big medium black category">(.+?)</a>', res.text)
            row['category'] = html.unescape(metas[0])
            row['location'] = metas[1]

            # New: Retrieve Geolocation
            latitude, longitude = get_lat_long(row['location'], 'Seattle')  # You can adjust the region as needed
            row['latitude'] = latitude
            row['longitude'] = longitude

            data.append(row)
        except IndexError as e:
            print(f'Error: {e}')
            print(f'Link: {link}')
    json.dump(data, open(URL_DETAIL_FILE, 'w'))

def get_weather_info():
    data = json.load(open(URL_DETAIL_FILE, 'r'))
    for event in data:
        if event['latitude'] and event['longitude']:
            weather_forecast = get_weather_forecast(event['latitude'], event['longitude'])
            if weather_forecast:
                event['weather_forecast'] = weather_forecast

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
        longitude FLOAT
    );
    '''
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(q)

    urls = json.load(open(URL_LIST_FILE, 'r'))
    data = json.load(open(URL_DETAIL_FILE, 'r'))
    for url, row in zip(urls, data):
        q = '''
        INSERT INTO events (url, title, date, venue, category, location, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (url) DO NOTHING;
        '''
        cur.execute(q, (url, row['title'], row['date'], row['venue'], row['category'], row['location'], row['latitude'], row['longitude']))

if __name__ == '__main__':
    list_links()
    get_detail_page()
    get_weather_info()
    insert_to_pg()
