import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import psycopg2
import time
import random
from urllib.parse import urljoin
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI()

# Database configuration
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Federation list
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

# Initialize database tables
def init_db():
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        logger.debug("Attempting to create 'athletes' table.")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS athletes (
                id SERIAL PRIMARY KEY,
                name TEXT,
                age INTEGER,
                weight TEXT,
                height TEXT,
                sport TEXT DEFAULT 'Athletics',
                country TEXT,
                position TEXT,
                team TEXT,
                source TEXT,
                athlete_url TEXT UNIQUE
            );
        """)
        logger.debug("Attempting to create 'logs' table.")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                error_message TEXT,
                player_url CHARACTER VARYING(255),
                timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logger.debug("Attempting to create 'fetched_urls' table.")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fetched_urls (
                id SERIAL PRIMARY KEY,
                url CHARACTER VARYING(255) UNIQUE,
                fetch_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                status TEXT,
                error_message TEXT
            );
        """)
        conn.commit()
        logger.info("Database tables initialized successfully.")
    except psycopg2.Error as e:
        logger.error(f"Database initialization failed: {e}", exc_info=True)
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# Log error to database
def log_error(error_message, player_url=None):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        logger.debug(f"Attempting to log error: {error_message}, URL: {player_url}")
        cur.execute("""
            INSERT INTO logs (error_message, player_url)
            VALUES (%s, %s);
        """, (str(error_message), player_url))
        conn.commit()
        logger.info("Error logged successfully.")
    except psycopg2.Error as e:
        logger.error(f"Failed to log error to database: {e}", exc_info=True)
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# Log fetched URL to database
def log_fetched_url(url, status, error_message=None):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        logger.debug(f"Attempting to log fetched URL: {url}, Status: {status}")
        cur.execute("""
            INSERT INTO fetched_urls (url, status, error_message)
            VALUES (%s, %s, %s)
            ON CONFLICT (url) DO UPDATE SET
                fetch_timestamp = DEFAULT,
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message;
        """, (url, status, error_message))
        conn.commit()
        logger.info(f"Fetched URL logged: {url}, Status: {status}")
    except psycopg2.Error as e:
        logger.error(f"Failed to log fetched URL {url}: {e}", exc_info=True)
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# Fetch page content with selenium and wait for data
def fetch_page(url, max_retries=3):
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    # Explicit path to chromedriver.exe for Windows
    chromedriver_path = r"D:\projects_source_code\PYTHON\nextedge\chrome.exe"
    try:
        service = Service(chromedriver_path)
    except Exception as e:
        logger.error(f"Failed to initialize Service with path {chromedriver_path}: {e}")
        # Fallback to Selenium Manager if manual path fails
        service = Service()
    for attempt in range(max_retries):
        try:
            logger.debug(f"Fetching page: {url}, Attempt {attempt + 1}/{max_retries}")
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.get(url)
            # Wait up to 10 seconds for the athlete table to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'table.AthleteSearch_results__3W7HB'))
            )
            time.sleep(1)  # Additional wait for content stability
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            logger.debug(f"Page fetched successfully: {url}")
            log_fetched_url(url, 'success')
            driver.quit()
            return soup
        except Exception as e:
            log_error(f"Attempt {attempt + 1}/{max_retries} - Error fetching {url}: {e}", url)
            log_fetched_url(url, 'failed', str(e))
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Max retries reached for {url}", exc_info=True)
                if 'driver' in locals():
                    driver.quit()
                return None

# Calculate age from DOB
def calculate_age(dob_str):
    if not dob_str:
        return None
    try:
        dob = datetime.strptime(dob_str, '%d %b %Y').date()  # Adjusted format to match "10 NOV 1999"
        today = datetime.now().date()
        age = relativedelta(today, dob).years
        return age
    except ValueError:
        logger.warning(f"Invalid DOB format: {dob_str}")
        return None

# Extract athletes from a federation page
def get_athletes(federation, base_url):
    logger.debug(f"Extracting athletes for federation: {federation}")
    athletes = []
    url = f"{base_url}/athletes?countryCode={federation}"
    soup = fetch_page(url)
    if not soup:
        logger.warning(f"No data retrieved for {url}")
        return athletes
    # Log HTML content for debugging
    with open(f"debug_{federation}.html", "w", encoding="utf-8") as f:
        f.write(str(soup))
    logger.debug(f"Saved HTML content to debug_{federation}.html for inspection")
    tables = soup.find_all('table')
    logger.debug(f"Found {len(tables)} tables on page")
    for table in tables:
        logger.debug(f"Table class: {table.get('class', 'No class')}")
    athlete_rows = soup.select('table.AthleteSearch_results__3W7HB tbody tr')
    if not athlete_rows:
        logger.warning(f"No rows found with selector 'table.AthleteSearch_results__3W7HB tbody tr'. Checking all tables...")
        for table in tables:
            rows = table.find_all('tr')
            if rows:
                logger.debug(f"Found {len(rows)} rows in table with class {table.get('class', 'No class')}")
                athlete_rows.extend(rows)
    logger.debug(f"Found {len(athlete_rows)} athlete rows for {federation}")
    for row in athlete_rows:
        try:
            cols = row.find_all('td')
            if len(cols) < 5:  # Ensure we have all columns: NAME, DISCIPLINE, SEX, hidden, DOB
                logger.warning(f"Skipping row with insufficient columns: {cols}")
                continue
            name_elem = cols[0].select_one('a[href*="athletes/"]')
            name = name_elem.text.strip() if name_elem else cols[0].text.strip()
            athlete_url = urljoin(url, name_elem['href']) if name_elem else None
            disciplines = cols[1].text.strip() if len(cols) > 1 else None
            sex = cols[2].text.strip() if len(cols) > 2 else None
            dob = cols[4].text.strip() if len(cols) > 4 else None  # DOB is in the 5th column, index 4

            athlete_data = {
                'name': name,
                'age': calculate_age(dob),
                'weight': None,
                'height': None,
                'sport': 'Athletics',
                'country': federation,
                'position': disciplines,  # Store as comma-separated string
                'team': federation,  # Using country as team
                'source': 'worldathletics.org',
                'athlete_url': athlete_url
            }
            athletes.append(athlete_data)
        except AttributeError as e:
            logger.warning(f"Error parsing athlete data from {url}: {e}", exc_info=True)
            continue
    logger.info(f"Extracted {len(athletes)} athletes for {federation}")
    return athletes

# Insert athlete data into the database with retry
def insert_athlete(data, max_retries=3):
    for attempt in range(max_retries):
        try:
            logger.debug(f"Attempting to insert athlete: {data['name']}, URL: {data['athlete_url']}, Attempt {attempt + 1}/{max_retries}")
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO athletes (name, age, weight, height, sport, country, position, team, source, athlete_url)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (athlete_url) DO NOTHING;
            """, (
                data['name'], data['age'], data['weight'], data['height'], data['sport'],
                data['country'], data['position'], data['team'], data['source'], data['athlete_url']
            ))
            conn.commit()
            logger.info(f"Successfully inserted athlete: {data['name']}")
            return True
        except psycopg2.Error as e:
            log_error(f"Database error inserting {data['name']}: {e}", data['athlete_url'])
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait before retry
            else:
                return False
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()

# Process a single federation
def process_federation(federation, base_url):
    logger.info(f"Processing federation: {federation}")
    athletes = get_athletes(federation, base_url)
    for athlete in athletes:
        if insert_athlete(athlete):
            global processed
            processed += 1
    time.sleep(random.uniform(1, 3))  # Random delay to avoid rate limiting

# Start scraping
@app.post("/start-scraping/")
async def start_scraping():
    init_db()
    global processed
    processed = 0
    total_athletes = 1000000  # Estimated total

    logger.info("Starting scraping process.")
    base_url = 'https://worldathletics.org'
    with ThreadPoolExecutor(max_workers=1) as executor:  # Single worker for debugging
        executor.map(lambda f: process_federation(f, base_url), federations)

    logger.info(f"Scraping completed. Processed {processed} athletes.")
    return {"message": f"Scraping completed for {processed} out of {total_athletes} athletes"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)