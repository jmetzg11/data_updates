from urls import *
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from pymongo.mongo_client import MongoClient
import certifi
import datetime
from dotenv import load_dotenv
import os
import time
import random
load_dotenv()
uri = os.getenv('MONGO_URI')

logging.basicConfig(filename='error.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

chrome_options = Options()
chrome_options.add_argument('--headless')

def scrap_data(url, driver, documents, sector_name, ticker):
    driver.get(url)
    price = driver.find_element(By.XPATH, '//*[@id="quote-header-info"]/div[3]/div[1]/div[1]/fin-streamer[1]').text
    price = price.replace(',', '')
    change = driver.find_element(By.XPATH, '//*[@id="quote-header-info"]/div[3]/div[1]/div[1]/fin-streamer[3]/span').text[1:-2] 
    documents[sector_name][ticker] = float(price)
    documents[sector_name]['change_average'].append(float(change))

def get_change_average(documents, sector_name):
    changes = documents[sector_name]['change_average']
    total_sum = sum(changes)
    total_count = len(changes)
    documents[sector_name]['change_average'] = round(total_sum/total_count, 2)

def update_database(sector_names, documents, client):
    client = MongoClient(uri, tlsCAFile=certifi.where())
    db = client['stocks']
    for sector in sector_names:
        collection = db[sector]
        document = documents[sector]
        try:
            result = collection.insert_one(document)
            print(f'inserted {sector}', result.inserted_id)
        except Exception as e: 
            logging.error(f'error with {sector}, {str(e)}')

def get_s_and_p(s_p, driver, db, collection_name):
    driver.get(s_p)
    price = driver.find_element(By.XPATH, '//*[@id="quote-header-info"]/div[3]/div[1]/div[1]/fin-streamer[1]').text
    price = price.replace(',', '')
    change = driver.find_element(By.XPATH, '//*[@id="quote-header-info"]/div[3]/div[1]/div[1]/fin-streamer[3]/span').text[1:-2] 
    document = {'date': str(datetime.datetime.now().date()), 'price': price, 'change': change}

    collection = db[collection_name]
    try:
        result = collection.insert_one(document)
        print(f'inserted {collection_name}', result.inserted_id)
    except Exception as e: 
        logging.error(f'error with {collection_name}, {str(e)}')

   

def get_data(driver, sectors, sector_names, documents):
    driver = webdriver.Chrome(options=chrome_options)
    for sector, sector_name in zip(sectors, sector_names):
        print(f'{sector_name}------------')
        for ticker in sector:
            time.sleep(random.random()*2)
            print(ticker)
            try:
                scrap_data(sector[ticker], driver, documents, sector_name, ticker)
            except Exception as e:
                logging.error(f'error with {ticker}, {str(e)}')
                documents[sector_name][f'{ticker}_price'] = 0
                documents[sector_name]['change_average'].append(0)
        get_change_average(documents, sector_name)


    client = MongoClient(uri, tlsCAFile=certifi.where())
    db = client['stocks']

    update_database(sector_names, documents, client)

    get_s_and_p(s_p, driver, db, 's_p')
    
    driver.quit()
    client.close()

sectors = [technology, health, finance, consumer_discretionary, industrial, cosumer_staples,
           energy, utility, real_estate, commodities]
sector_names = ['technology', 'health', 'finance', 'consumer_discretionary', 'industrial', 'cosumer_staples',
                'energy', 'utility', 'real_estate', 'commodities']
date = str(datetime.datetime.now().date())
documents = {}
for sector_name in sector_names:
    documents[sector_name] = {}
    documents[sector_name]['date'] = date
    documents[sector_name]['change_average'] = []

driver = webdriver.Chrome(options=chrome_options)


get_data(driver, sectors, sector_names, documents)
