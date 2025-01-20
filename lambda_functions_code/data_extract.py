import boto3
from bs4 import BeautifulSoup
from datetime import date , datetime
import json
import omdb
import os
import random
import requests
from selenium import webdriver 
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from tempfile import mkdtemp
import time

def handler(event, context):

    url = 'https://www.imdb.com/search/title/?groups=top_1000&count=250'
    #headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:82.0) Gecko/20100101 Firefox/82.0'}
    user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    ]

    # Instantiate ChromeOptions
    chrome_options = webdriver.ChromeOptions()

    # Activate headless mode and set user 
    chrome_options.binary_location = "/opt/chrome/chrome-linux64/chrome"
    chrome_options.add_argument(f"user-agent={random.choice(user_agents)}")
    # chrome_options.add_argument(f'--user-agent={headers}')
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-tools")
    chrome_options.add_argument("--no-zygote")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument(f"--user-data-dir={mkdtemp()}")
    chrome_options.add_argument(f"--data-path={mkdtemp()}")
    chrome_options.add_argument(f"--disk-cache-dir={mkdtemp()}")
    chrome_options.add_argument("--remote-debugging-pipe")
    chrome_options.add_argument("--verbose")
    chrome_options.add_argument("--log-path=/tmp")

    service = Service(
        executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver",
        service_log_path="/tmp/chromedriver.log")

    # Instantiate a webdriver instance
    driver = webdriver.Chrome(service = service, options=chrome_options)

    # Wait
    #driver.implicitly_wait(30)
    wait = WebDriverWait(driver, 20)

    # Visit the target website with Chrome web driver
    driver.get(url) 

    #loading webpage elements

    load_more_clicks_needed = 4
    current_clicks = 0

    while current_clicks < load_more_clicks_needed:
        try:
            load_more_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.ipc-see-more__button'))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_more_button)
            time.sleep(1)
            load_more_button.click()
            current_clicks += 1
            time.sleep(random.uniform(4, 6))  

        except TimeoutException:
            print(f"Cannot find 'Load More' button after {current_clicks + 1} clicks. Stopping.")
            break

    # load webpage
    soup = BeautifulSoup(driver.page_source, "html")

    h3_tag = soup.find_all('h3', class_= 'ipc-title__text')

    movie_names = []
    for i in h3_tag:
        movie_names.append(i.text[3:].lstrip('.').strip())

    # omdb credentials 
    api_key = os.environ.get('omdb_api_key')
    omdb.set_default('apikey', api_key)

    movies_info = [] 
    for movie in movie_names:
        m = omdb.title(movie, tomatoes = True)
        movies_info.append(m)    

    movies_raw =  movies_info

    #putting raw netflix data in s3 bucket

    client = boto3.client("s3")

    filename = 'movies_raw' + str(datetime.now()) + '.json'
    client.put_object(
        Bucket = 'movies-etl-project-at',
        Key = 'raw_data/to_process/' + filename,
        Body= json.dumps(movies_raw)
        )
