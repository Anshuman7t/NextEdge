# athletics_scraper.py
import time
import random
from datetime import datetime
from dateutil.relativedelta import relativedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import os
from urllib.parse import urljoin
from common_utils import db_params, log_error, insert_player
import psycopg2
import logging

logger = logging.getLogger(__name__)

federations = [
    'AFG', 'ALB', 'ALG', 'ASA', 'AND', 'ANG', 'AIA', 'ANT', 'ARG', 'ARM', 'ARU', 'ART', 'AUS', 'AUT', 'ANA', 'AZE', 'BAH', 'BRN',
    'BAN', 'BAR', 'BLR', 'BEL', 'BIZ', 'BEN', 'BER', 'BHU', 'BOL', 'BIH', 'BOT', 'BRA', 'IVB', 'BRU', 'BUL', 'BUR', 'BDI',
    'CPV', 'CAM', 'CMR', 'CAN', 'CAY', 'CAF', 'CHA', 'CHI', 'TPE', 'COL', 'DMA', 'COM', 'CGO', 'COK', 'CRC', 'CIV', 'CRO',
    'CUB', 'CYP', 'CZE', 'COD', 'DEN', 'DJI', 'DOM', 'PRK', 'ECU', 'EGY', 'ESA', 'GEQ', 'ERI', 'EST', 'SWZ', 'ETH', 'FIJ',
    'FIN', 'FRA', 'PYF', 'GAB', 'GEO', 'GER', 'GHA', 'GIB', 'GBR', 'GRE', 'GRN', 'GUM', 'GUA', 'GUI', 'GBS', 'GUY', 'HAI',
    'HON', 'HKG', 'HUN', 'ISL', 'IND', 'INA', 'INT', 'IRQ', 'IRL', 'IRI', 'ISR', 'ITA', 'JAM', 'JPN', 'JOR', 'KAZ', 'KEN',
    'KIR', 'KOR', 'KOS', 'KUW', 'KGZ', 'LAO', 'LAT', 'LBN', 'LES', 'LBR', 'LBA', 'LIE', 'LTU', 'LUX', 'MAC', 'MAD', 'MAW',
    'MAS', 'MDV', 'MLI', 'MLT', 'MHL', 'MTN', 'MRI', 'MEX', 'FSM', 'MDA', 'MON', 'MGL', 'MNE', 'MNT', 'MAR', 'MOZ', 'MYA',
    'NAM', 'NRU', 'NEP', 'NED', 'NZL', 'NCA', 'NIG', 'NGR', 'NFI', 'NMI', 'NOR', 'OMA', 'PAK', 'PLW', 'PLE', 'PAN', 'PNG',
    'PAR', 'PER', 'PHI', 'POL', 'POR', 'CHN', 'PUR', 'QAT', 'ROT', 'EOR', 'MKD', 'YEM', 'ROU', 'RUS', 'RWA', 'SKN', 'LCA',
    'VIN', 'SAM', 'SMR', 'STP', 'KSA', 'SEN', 'SRB', 'SEY', 'SLE', 'SGP', 'SVK', 'SLO', 'SOL', 'SOM', 'RSA', 'SSD', 'ESP',
    'SRI', 'SUD', 'SUR', 'SWE', 'SUI', 'SYR', 'TJK', 'TAN', 'THA', 'GAM', 'TLS', 'TOG', 'TGA', 'TTO', 'TUN', 'TUR', 'TKM',
    'TKS', 'TUV', 'UGA', 'UKR', 'UND', 'UAE', 'USA', 'URU', 'UZB', 'VAN', 'VEN', 'VIE', 'ISV', 'ZAM', 'ZIM'
]

def calculate_age(dob_str):
    try:
        dob = datetime.strptime(dob_str, '%d %b %Y').date()
        return relativedelta(datetime.now().date(), dob).years
    except Exception:
        return None

def fetch_page(url):
    options = Options()
    options.add_argument("--headless")
    service = Service()
    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'table.AthleteSearch_results__3W7HB'))
        )
        html = driver.page_source
        return BeautifulSoup(html, "html.parser")
    except Exception as e:
        log_error(str(e), url)
        return None
    finally:
        driver.quit()

def get_athletes(federation, base_url):
    athletes = []
    url = f"{base_url}/athletes?countryCode={federation}"
    soup = fetch_page(url)
    if not soup:
        return athletes

    rows = soup.select('table.AthleteSearch_results__3W7HB tbody tr')
    for row in rows:
        try:
            cols = row.find_all("td")
            if len(cols) < 5:
                continue
            name_elem = cols[0].find("a")
            name = name_elem.text.strip()
            athlete_url = urljoin(url, name_elem['href'])
            position = cols[1].text.strip()
            dob = cols[4].text.strip()
            age = calculate_age(dob)
            athlete_data = {
                "name": name,
                "age": age,
                "weight": None,
                "height": None,
                "sport": "Athletics",
                "country": federation,
                "position": position,
                "team": federation,
                "source": "worldathletics.org",
                "player_url": athlete_url
            }
            athletes.append(athlete_data)
        except Exception as e:
            log_error(str(e), url)
    return athletes

def scrape_all_athletics():
    base_url = "https://worldathletics.org"
    for federation in federations:
        logger.info(f"Scraping athletes for federation {federation}")
        athletes = get_athletes(federation, base_url)
        for athlete in athletes:
            insert_player(athlete)
            time.sleep(0.5)
        time.sleep(1)