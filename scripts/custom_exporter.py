import time, os, requests
from prometheus_client import start_http_server, Gauge, Counter, Histogram
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Metrics
weather_temp, weather_hum, weather_press, weather_wind, weather_clouds = [
    Gauge(f'external_weather_{n}', d, ['city']) for n, d in [
        ('temperature_celsius', 'Temperature in Celsius'),
        ('humidity_percent', 'Humidity percentage'),
        ('pressure_hpa', 'Atmospheric pressure in hPa'),
        ('wind_speed_ms', 'Wind speed in m/s'),
        ('clouds_percent', 'Cloud coverage percentage')
    ]
]
currency_usd, currency_eur, currency_rub = [
    Gauge(f'external_currency_{r}_kzt', f'{r.upper()} to KZT exchange rate') 
    for r in ['usd', 'eur', 'rub']
]
github_stars = Gauge('external_github_stars', 'Number of GitHub stars', ['repo'])
github_forks = Gauge('external_github_forks', 'Number of GitHub forks', ['repo'])
api_total = Counter('external_api_requests_total', 'Total API requests', ['api', 'status'])
api_duration = Histogram('external_api_request_duration_seconds', 'API request duration', ['api'])
api_errors = Counter('external_api_errors_total', 'Total API errors', ['api'])

OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY') or exit("OPENWEATHER_API_KEY required")
CITIES = ['Almaty', 'Moscow', 'Toronto']
GITHUB_REPOS = [{'owner': 'prometheus', 'repo': 'prometheus'}, {'owner': 'grafana', 'repo': 'grafana'}]

def fetch_api(url, api_name, **kwargs):
    try:
        with api_duration.labels(api=api_name).time():
            r = requests.get(url, timeout=5, **kwargs)
            r.raise_for_status()
            return r.json()
    except Exception as e:
        logger.error(f"{api_name} error: {e}")
        api_errors.labels(api=api_name).inc()
        return None

def get_weather_data(city):
    data = fetch_api('http://api.openweathermap.org/data/2.5/weather', 'weather',
                     params={'q': city, 'appid': OPENWEATHER_API_KEY, 'units': 'metric'})
    if data:
        m = data['main']
        w = data.get('wind', {})
        return {k: m.get(k, w.get(k, data.get('clouds', {}).get('all', 0))) 
                for k in ['temp', 'humidity', 'pressure', 'speed', 'clouds']}
    return None

def get_exchange_rates():
    data = fetch_api('https://api.exchangerate-api.com/v4/latest/USD', 'exchange')
    if data and 'rates' in data:
        r = data['rates']
        if all(k in r for k in ['KZT', 'EUR', 'RUB']):
            return {'usd_kzt': r['KZT'], 'eur_kzt': r['KZT']/r['EUR'], 'rub_kzt': r['KZT']/r['RUB']}
    return None

def get_github_stats(owner, repo):
    data = fetch_api(f'https://api.github.com/repos/{owner}/{repo}', 'github')
    return {'stars': data.get('stargazers_count', 0), 'forks': data.get('forks_count', 0)} if data else None

def collect_metrics():
    logger.info("Starting metric collection...")
    for city in CITIES:
        w = get_weather_data(city)
        if w:
            weather_temp.labels(city=city).set(w['temp'])
            weather_hum.labels(city=city).set(w['humidity'])
            weather_press.labels(city=city).set(w['pressure'])
            weather_wind.labels(city=city).set(w['speed'])
            weather_clouds.labels(city=city).set(w['clouds'])
            api_total.labels(api='weather', status='success').inc()
        else:
            api_total.labels(api='weather', status='error').inc()
    
    r = get_exchange_rates()
    if r:
        currency_usd.set(r['usd_kzt'])
        currency_eur.set(r['eur_kzt'])
        currency_rub.set(r['rub_kzt'])
        api_total.labels(api='exchange', status='success').inc()
    else:
        api_total.labels(api='exchange', status='error').inc()
    
    for repo_info in GITHUB_REPOS:
        s = get_github_stats(repo_info['owner'], repo_info['repo'])
        if s:
            name = f"{repo_info['owner']}/{repo_info['repo']}"
            github_stars.labels(repo=name).set(s['stars'])
            github_forks.labels(repo=name).set(s['forks'])
            api_total.labels(api='github', status='success').inc()
        else:
            api_total.labels(api='github', status='error').inc()
    logger.info("Metric collection completed")

if __name__ == '__main__':
    logger.info("Starting Custom Prometheus Exporter on port 8000...")
    start_http_server(8000)
    logger.info("Exporter started at http://0.0.0.0:8000/metrics")
    collect_metrics()
    while True:
        time.sleep(20)
        collect_metrics()