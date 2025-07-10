# Final working rugby_scraper.py with all teams auto-extracted and country field filled

import os
import time
import re
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import psycopg2
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def init_db():
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rugby_players (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER,
                weight TEXT,
                height TEXT,
                sport TEXT DEFAULT 'Rugby',
                country TEXT,
                position TEXT,
                team TEXT,
                source TEXT DEFAULT 'all.rugby',
                player_url TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
    finally:
        cur.close()
        conn.close()

def setup_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def extract_from_bio(driver):
    try:
        container = driver.find_element(By.CSS_SELECTOR, 'div.pas')
        bio_section = container.find_element(By.CSS_SELECTOR, 'div.bio')
        bio_text = bio_section.text.strip()

        height_match = re.search(r'[Ss]tanding at ([0-9.]+) ?m', bio_text)
        height = f"{height_match.group(1)} m" if height_match else None

        weight_match = re.search(r'[Ww]eighing in at (\d+) ?kg', bio_text)
        weight = f"{weight_match.group(1)} kg" if weight_match else None

        team_match = re.search(r'currently plays for (.+?) in', bio_text)
        team = team_match.group(1).strip() if team_match else None

        country_match = re.search(r'([A-Z][a-z]+) rugby player', bio_text)
        country = country_match.group(1).strip() if country_match else None

        return {
            'height': height,
            'weight': weight,
            'team': team,
            'country': country
        }
    except Exception as e:
        logger.warning(f"Failed bio extraction: {e}")
        return {}

def insert_player(data):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO rugby_players (name, age, weight, height, country, position, team, source, player_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_url) DO UPDATE SET
                age = EXCLUDED.age,
                weight = EXCLUDED.weight,
                height = EXCLUDED.height,
                country = EXCLUDED.country,
                position = EXCLUDED.position,
                team = EXCLUDED.team,
                updated_at = CURRENT_TIMESTAMP;
        """, (
            data['name'], data['age'], data['weight'], data['height'], data['country'],
            data['position'], data['team'], data['source'], data['player_url']
        ))
        conn.commit()
        logger.info(f"Saved: {data['name']}")
    except Exception as e:
        logger.error(f"DB Error: {e}")
    finally:
        cur.close()
        conn.close()

def get_all_team_urls(driver):
    driver.get("https://all.rugby/players/")
    time.sleep(3)

    # Scroll to bottom to ensure JS renders all
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(3)

    team_urls = []

    # Collect from national teams
    nation_links = driver.find_elements(By.XPATH, '//div[contains(@class, "bloc dra")]/a')
    for link in nation_links:
        href = link.get_attribute("href")
        if href:
            team_urls.append(href)

    # Collect from club and tournament teams
    club_links = driver.find_elements(By.XPATH, '//div[contains(@class, "bloc clbb")]/a')
    for link in club_links:
        href = link.get_attribute("href")
        if href:
            team_urls.append(href)

    logger.info(f"Found {len(team_urls)} team URLs")
    return list(set(team_urls))

def get_player_urls_from_team(driver, team_url):
    try:
        driver.get(team_url)
        time.sleep(3)
        links = driver.find_elements(By.CSS_SELECTOR, 'a[href^="/player/"]')
        player_urls = [link.get_attribute("href") for link in links if '/player/' in link.get_attribute('href')]
        logger.info(f"Found {len(player_urls)} players in team {team_url}")
        return player_urls
    except Exception as e:
        logger.error(f"Failed to fetch team players from {team_url}: {e}")
        return []

def scrape_player(driver, url):
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.pas')))

        h1_text = driver.find_element(By.CSS_SELECTOR, "h1.inbl").text.strip()
        match = re.match(r"(.+?)\s+(\d+)\s+years,\s+(.+)", h1_text)
        if match:
            name = match.group(1).strip()
            age = int(match.group(2))
            position = match.group(3).strip()
        else:
            name = h1_text
            age = None
            position = None

        bio = extract_from_bio(driver)
        return {
            'name': name,
            'age': age,
            'weight': bio.get('weight'),
            'height': bio.get('height'),
            'country': bio.get('country'),
            'position': position,
            'team': bio.get('team'),
            'source': 'all.rugby',
            'player_url': url
        }
    except Exception as e:
        logger.warning(f"Failed to scrape {url}: {e}")
        return None

def scrape_all():
    init_db()
    driver = setup_driver()
    team_urls = get_all_team_urls(driver)
    for team_url in team_urls:
        player_urls = get_player_urls_from_team(driver, team_url)
        for player_url in player_urls:
            data = scrape_player(driver, player_url)
            if data:
                insert_player(data)
    driver.quit()

if __name__ == '__main__':
    scrape_all()