import os
import json
import time
import urllib.parse
import re
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from apscheduler.schedulers.background import BackgroundScheduler

# Importing external basketball scrapers
from basketafrica import process_country as process_basket_africa_country, african_countries
from basketoceania import process_country as process_basket_oceania_country, oceania_countries
from basketeuro import process_country as process_basket_euro_country, europe_countries
from basketasia import process_country as process_basket_asia_country, asia_countries
from basketusa import process_country as process_basket_usa_country, us_teams
from basketlatin import process_country as process_basket_latin_country, latin_countries

# Importing the separate ultimate_rugby_scraper file
# from ultimate_rugby_scraper import scrape_ultimate_rugby_players
from ultimate import scrape_ultimate_rugby_players

# Importing other scrapers
from rugbypass import scrape_all_pages
from athletics_scraper import scrape_all_athletics

import psycopg2
import logging
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
import math
import threading

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(title="Sport Player API", version="1.0.0")
scheduler = BackgroundScheduler()

# Add CORS middleware to allow frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Global variables for scraping status
scraping_status = {
    'is_running': False,
    'processed': 0,
    'current_sport': '',
    'current_item': '',
    'total_items': 0,
    'start_time': None,
    'errors': []
}

# Pydantic models for API responses
class Player(BaseModel):
    id: int
    name: str
    age: Optional[int] = None
    weight: Optional[str] = None
    height: Optional[str] = None
    sport: str = "Unknown" # Changed default for API response to "Unknown" if DB value is NULL
    country: Optional[str] = None
    position: Optional[str] = None
    team: Optional[str] = None
    source: Optional[str] = None
    player_url: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class PlayerListResponse(BaseModel):
    players: List[Player]
    total_count: int
    page: int
    per_page: int
    total_pages: int
    has_next: bool
    has_prev: bool

class FilterOptions(BaseModel):
    sports: List[str]
    countries: List[str]
    teams: List[str]
    positions: List[str]
    sources: List[str]

class ScrapingStatus(BaseModel):
    is_running: bool
    processed: int
    current_sport: str
    current_item: str
    total_items: int
    start_time: Optional[datetime] = None
    errors: List[str]

# Database connection helper
def get_db_connection():
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")

# Initialize database tables
def init_db():
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                age INTEGER,
                weight TEXT,
                height TEXT,
                sport TEXT, -- Removed DEFAULT 'Rugby' to allow NULL
                country TEXT,
                position TEXT,
                team TEXT,
                source TEXT, -- Removed DEFAULT 'all.rugby'
                player_url TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create logs table for error tracking
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rugby_logs ( 
                id SERIAL PRIMARY KEY,
                error_message TEXT,
                player_url TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        conn.commit()
        logger.info("Database tables initialized successfully.")
    except psycopg2.Error as e:
        logger.error(f"Database initialization failed: {e}")
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
        cur.execute("""
            INSERT INTO rugby_logs (error_message, player_url)
            VALUES (%s, %s);
        """, (str(error_message), player_url))
        conn.commit()
        
        # Also add to global status
        global scraping_status
        scraping_status['errors'].append(f"{datetime.now()}: {error_message}")
        # Keep only last 10 errors
        if len(scraping_status['errors']) > 10:
            scraping_status['errors'] = scraping_status['errors'][-10:]
            
    except psycopg2.Error as e:
        logger.error(f"Failed to log error to database: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# Selenium setup (for all.rugby and ultimate.rugby) - Kept as is from previous turn
def setup_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument(
        '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )
    options.add_argument("--log-level=3")
    options.add_argument("--silent")
    options.add_argument("--disable-logging")
    
    try:
        service = Service(ChromeDriverManager().install())
        service.log_path = os.devnull
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        driver.set_page_load_timeout(60)
        driver.implicitly_wait(10)
        logger.info("WebDriver initialized successfully")
        return driver
    except Exception as e:
        logger.error(f"Error initializing WebDriver: {e}")
        raise

# Extract player info from bio section (for all.rugby) - Kept as is from previous turn
def extract_from_bio_all_rugby(driver):
    try:
        container = driver.find_element(By.CSS_SELECTOR, 'div.pas')
        bio_section = container.find_element(By.CSS_SELECTOR, 'div.bio')
        bio_text = bio_section.text.strip()

        height_match = re.search(r'[Ss]tanding at ([0-9.]+) ?m', bio_text)
        if height_match:
            height_m = float(height_match.group(1))
            total_inches = int(round(height_m * 39.3701)) 
            feet = total_inches // 12
            inches = total_inches % 12
            height = f"{feet}'{inches}\""
        else:
            height = None

        weight_match = re.search(r'[Ww]eighing in at (\d+)', bio_text)
        weight = weight_match.group(1) if weight_match else None # Store just the number

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
        logger.warning(f"Failed bio extraction from all.rugby: {e}")
        return {}

# Get all team URLs from main page (for all.rugby) - Kept as is from previous turn
def get_all_rugby_team_urls(driver):
    try:
        driver.get("https://all.rugby/players/")
        time.sleep(3)

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)

        team_urls = []
        nation_links = driver.find_elements(By.XPATH, '//div[contains(@class, "bloc dra")]/a')
        for link in nation_links:
            href = link.get_attribute("href")
            if href:
                team_urls.append(href)

        club_links = driver.find_elements(By.XPATH, '//div[contains(@class, "bloc clbb")]/a')
        for link in club_links:
            href = link.get_attribute("href")
            if href:
                team_urls.append(href)

        unique_urls = list(set(team_urls))
        logger.info(f"Found {len(unique_urls)} all.rugby team URLs")
        return unique_urls
    except Exception as e:
        logger.error(f"Failed to get all.rugby team URLs: {e}")
        log_error(f"Failed to get all.rugby team URLs: {e}")
        return []

# Get player URLs from team page (for all.rugby) - Kept as is from previous turn
def get_player_urls_from_all_rugby_team(driver, team_url):
    try:
        driver.get(team_url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href^="/player/"]')))
        links = driver.find_elements(By.CSS_SELECTOR, 'a[href^="/player/"]')
        player_urls = [link.get_attribute("href") for link in links if '/player/' in link.get_attribute('href')]
        logger.info(f"Found {len(player_urls)} players in all.rugby team {team_url}")
        return player_urls
    except Exception as e:
        logger.error(f"Failed to fetch all.rugby team players from {team_url}: {e}")
        log_error(f"Failed to fetch all.rugby team players from {team_url}: {e}")
        return []

# Scrape individual player data (for all.rugby) - Kept as is from previous turn
def scrape_all_rugby_player(driver, url):
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

        bio = extract_from_bio_all_rugby(driver) # Use specific bio extractor
        return {
            'name': name,
            'age': age,
            'weight': bio.get('weight'),
            'height': bio.get('height'),
            'sport': 'Rugby', # Explicitly set sport for all.rugby
            'country': bio.get('country'),
            'position': position,
            'team': bio.get('team'),
            'source': 'all.rugby',
            'player_url': url
        }
    except Exception as e:
        logger.warning(f"Failed to scrape all.rugby player {url}: {e}")
        log_error(f"Failed to scrape all.rugby player {url}: {e}", url)
        return None

# Insert player data into database
def insert_player(data: Dict[str, Any]):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        # Explicitly handle sport and source to allow NULL in DB if not provided,
        # or use a generic 'Unknown' for display.
        # For DB insertion, pass None if no sport/source is specifically set by the scraper.
        sport_to_insert = data.get('sport') # Will be None if key doesn't exist
        source_to_insert = data.get('source') # Will be None if key doesn't exist
        
        cur.execute("""
            INSERT INTO players (name, age, weight, height, sport, country, position, team, source, player_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_url) DO UPDATE SET
                name = EXCLUDED.name,
                age = COALESCE(EXCLUDED.age, players.age),
                weight = COALESCE(EXCLUDED.weight, players.weight),
                height = COALESCE(EXCLUDED.height, players.height),
                sport = EXCLUDED.sport, -- This will update to 'Basketball'/'Athletics' or 'Rugby' or NULL as provided
                country = COALESCE(EXCLUDED.country, players.country),
                position = COALESCE(EXCLUDED.position, players.position),
                team = COALESCE(EXCLUDED.team, players.team),
                source = EXCLUDED.source,
                updated_at = CURRENT_TIMESTAMP;
        """, (
            data['name'], data.get('age'), data.get('weight'), data.get('height'),
            sport_to_insert, data.get('country'), data.get('position'), data.get('team'),
            source_to_insert, data['player_url']
        ))
        conn.commit()
        logger.info(f"Saved: {data['name']} (Sport: {sport_to_insert}, Source: {source_to_insert})")
        return True
    except Exception as e:
        logger.error(f"DB Error: {e}", exc_info=True)
        logger.debug(f"Failed Data: {json.dumps(data, indent=2)}")
        log_error(f"DB Error inserting {data.get('name', 'Unknown')}: {e}", data.get('player_url'))
        return False
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# --- MODIFIED Sport Fixing Function ---
def fix_sport_columns_after_scrape():
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        # Define all known sources and their sports
        basketball_sources = [
            'basketball.afrobasket.com',
            'basketball.australiabasket.com',
            'basketball.eurobasket.com',
            'basketball.asia-basket.com',
            'basketball.usbasket.com',
            'basketball.latinbasket.com'
        ]
        rugby_sources = [
            'all.rugby',
            'ultimaterugby.com',
            'rugbypass.com' # Uncomment if you enable rugbypass scraper
        ]
        athletics_sources = [
            'worldathletics.org' # Uncomment if you enable worldathletics scraper
        ]

        # Update Basketball sports
        cur.execute("""
            UPDATE players
            SET sport = 'Basketball'
            WHERE source IN %s AND (sport IS NULL OR sport != 'Basketball');
        """, (tuple(basketball_sources),))
        logger.info(f"✔️ Fixed sport column for basketball websites. Rows updated: {cur.rowcount}")

        # Update Rugby sports
        cur.execute("""
            UPDATE players
            SET sport = 'Rugby'
            WHERE source IN %s AND (sport IS NULL OR sport != 'Rugby');
        """, (tuple(rugby_sources),))
        logger.info(f"✔️ Fixed sport column for rugby websites. Rows updated: {cur.rowcount}")

        # Update Athletics sports
        cur.execute("""
            UPDATE players
            SET sport = 'Athletics'
            WHERE source IN %s AND (sport IS NULL OR sport != 'Athletics');
        """, (tuple(athletics_sources),))
        logger.info(f"✔️ Fixed sport column for athletics websites. Rows updated: {cur.rowcount}")

        conn.commit()
    except Exception as e:
        logger.error(f"❌ Failed to fix sport column: {e}", exc_info=True)
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


# Background scraping function
def scrape_all_background():
    global scraping_status
    
    scraping_status['is_running'] = True
    scraping_status['processed'] = 0
    scraping_status['current_sport'] = 'Initializing...'
    scraping_status['current_item'] = ''
    scraping_status['total_items'] = 0
    scraping_status['start_time'] = datetime.now()
    scraping_status['errors'] = []
    
    driver = None # Initialize driver here, to be shared by Selenium-based scrapers
    try:
        init_db() # Ensure DB is initialized

        # Setup Selenium driver once for all Selenium-based scrapers
        driver = setup_driver()
        
        # === BasketLatin ===
        # if scraping_status['is_running']:
        #     scraping_status['current_sport'] = 'Basketball'
        #     logger.info("➡️ Running BasketLatin scraper...")
        #     # process_basket_latin_country is assumed to construct player_data correctly including 'sport': 'Basketball'
        #     for country in latin_countries:
        #         if not scraping_status['is_running']: break
        #         scraping_status['current_item'] = f"{country} (Men - LatinBasket)"
        #         process_basket_latin_country(country, 'men', 'https://www.latinbasket.com', 'basketball.latinbasket.com', scraping_status)
        #         if not scraping_status['is_running']: break
        #         scraping_status['current_item'] = f"{country} (Women - LatinBasket)"
        #         process_basket_latin_country(country, 'women', 'https://www.latinbasket.com', 'basketball.latinbasket.com', scraping_status)
        #     logger.info("✅ BasketLatin scraper completed.")

        # === BasketAsia ===
        # if scraping_status['is_running']:
        #     scraping_status['current_sport'] = 'Basketball'
        #     logger.info("➡️ Running BasketAsia scraper...")
        #     for country in asia_countries:
        #         if not scraping_status['is_running']: break
        #         scraping_status['current_item'] = f"{country} (Men - AsiaBasket)"
        #         process_basket_asia_country(country, 'men', 'https://www.asia-basket.com', 'basketball.asia-basket.com', scraping_status)
        #         if not scraping_status['is_running']: break
        #         scraping_status['current_item'] = f"{country} (Women - AsiaBasket)"
        #         process_basket_asia_country(country, 'women', 'https://www.asia-basket.com', 'basketball.asia-basket.com', scraping_status)
        #     logger.info("✅ BasketAsia scraper completed.")

        # === BasketEuro ===
        # if scraping_status['is_running']:
        #     scraping_status['current_sport'] = 'Basketball'
        #     logger.info("➡️ Running BasketEuro scraper...")
        #     for country in europe_countries:
        #         if not scraping_status['is_running']: break
        #         scraping_status['current_item'] = f"{country} (Men - EuroBasket)"
        #         process_basket_euro_country(country, 'men', 'https://www.eurobasket.com', 'basketball.eurobasket.com', scraping_status)
        #         if not scraping_status['is_running']: break
        #         scraping_status['current_item'] = f"{country} (Women - EuroBasket)"
        #         process_basket_euro_country(country, 'women', 'https://www.eurobasket.com', 'basketball.eurobasket.com', scraping_status)
        #     logger.info("✅ BasketEuro scraper completed.")

        # === BasketOceania ===
        # if scraping_status['is_running']:
        #     scraping_status['current_sport'] = 'Basketball'
        #     logger.info("➡️ Running BasketOceania scraper...")
        #     for country in oceania_countries:
        #         if not scraping_status['is_running']: break
        #         scraping_status['current_item'] = f"{country} (Men - OceaniaBasket)"
        #         process_basket_oceania_country(country, 'men', 'https://www.australiabasket.com', 'basketball.australiabasket.com', scraping_status)
        #         if not scraping_status['is_running']: break
        #         scraping_status['current_item'] = f"{country} (Women - OceaniaBasket)"
        #         process_basket_oceania_country(country, 'women', 'https://www.australiabasket.com', 'basketball.australiabasket.com', scraping_status)
        #     logger.info("✅ BasketOceania scraper completed.")

        # === BasketAfrica ===
        # if scraping_status['is_running']:
        #     scraping_status['current_sport'] = 'Basketball'
        #     logger.info("➡️ Running BasketAfrica scraper...")
        #     for country in african_countries:
        #         if not scraping_status['is_running']: break
        #         scraping_status['current_item'] = f"{country} (Men - AfroBasket)"
        #         process_basket_africa_country(country, 'men', 'https://www.afrobasket.com', 'basketball.afrobasket.com', scraping_status)
        #         if not scraping_status['is_running']: break
        #         scraping_status['current_item'] = f"{country} (Women - AfroBasket)"
        #         process_basket_africa_country(country, 'women', 'https://www.afrobasket.com', 'basketball.afrobasket.com', scraping_status)
        #     logger.info("✅ BasketAfrica scraper completed.")

        # --- WorldAthletics ---
        # if scraping_status['is_running']:
        #     scraping_status['current_sport'] = 'Athletics'
        #     logger.info("➡️ Running WorldAthletics scraper...")
        #     scrape_all_athletics()
        #     logger.info("✅ WorldAthletics scraper completed.")

        # === UltimateRugby.com ===
        # if scraping_status['is_running']:
        #     scraping_status['current_sport'] = 'Rugby'
        #     scraping_status['current_item'] = 'Initializing UltimateRugby Scraper...'
        #     logger.info("➡️ Running UltimateRugby scraper...")
            
        #     try:
        #         ultimate_players = scrape_ultimate_rugby_players(driver)
        #         scraping_status['total_items'] = len(ultimate_players)
                
        #         for i, player_data in enumerate(ultimate_players):
        #             if not scraping_status['is_running']:
        #                 logger.info("UltimateRugby scraping stopped by user.")
        #                 break

        #             scraping_status['current_item'] = f"UltimateRugby Player {i+1}/{len(ultimate_players)}"
        #             if insert_player(player_data):
        #                 scraping_status['processed'] += 1
        #             time.sleep(1)  # Rate limit
        #     except Exception as e:
        #         logger.error(f"❌ UltimateRugby scraping failed: {e}")
        #         log_error(f"UltimateRugby scraping failed: {e}")
            
        #     logger.info("✅ UltimateRugby scraper completed.")

        # === All.Rugby (using Selenium, kept in this file) ===
        if scraping_status['is_running']:
            scraping_status['current_sport'] = 'Rugby'
            scraping_status['current_item'] = 'Initializing All.Rugby Scraper...'
            logger.info("➡️ Running All.Rugby scraper...")
            
            # The driver is already initialized at the start of scrape_all_background
            team_urls = get_all_rugby_team_urls(driver) # Uses the shared driver
            scraping_status['total_items'] = len(team_urls)
            
            for i, team_url in enumerate(team_urls):
                if not scraping_status['is_running']: 
                    logger.info("All.Rugby scraping stopped by user.")
                    break
                    
                scraping_status['current_item'] = f"Team {i+1}/{len(team_urls)} (All.Rugby): {team_url}"
                logger.info(f"Processing team {i+1}/{len(team_urls)} (All.Rugby): {team_url}")
                
                player_urls = get_player_urls_from_all_rugby_team(driver, team_url) # Uses the shared driver
                
                for j, player_url in enumerate(player_urls):
                    if not scraping_status['is_running']: 
                        logger.info("All.Rugby player scraping stopped by user.")
                        break
                        
                    scraping_status['current_item'] = f"Player {j+1}/{len(player_urls)} from {team_url} (All.Rugby)"
                    data = scrape_all_rugby_player(driver, player_url) # Uses the shared driver
                    if data:
                        # scrape_all_rugby_player already explicitly sets 'sport': 'Rugby' and 'source': 'all.rugby'
                        if insert_player(data): 
                            scraping_status['processed'] += 1
                    time.sleep(1) # Rate limiting
                
                time.sleep(2) # Rate limiting between teams
            
            logger.info("✅ All.Rugby scraper completed.")

        # === BasketUSA ===
        if scraping_status['is_running']:
            scraping_status['current_sport'] = 'Basketball'
            logger.info("➡️ Running BasketUSA scraper...")
            for team in us_teams:
                if not scraping_status['is_running']: break
                scraping_status['current_item'] = f"{team} (Men - USABasket)"
                process_basket_usa_country(team, 'men', 'https://www.usbasket.com', 'basketball.usbasket.com', scraping_status)
                if not scraping_status['is_running']: break
                scraping_status['current_item'] = f"{team} (Women - USABasket)"
                process_basket_usa_country(team, 'women', 'https://www.usbasket.com', 'basketball.usbasket.com', scraping_status)
            logger.info("✅ BasketUSA scraper completed.")

        # --- RugbyPass ---
        if scraping_status['is_running']:
            scraping_status['current_sport'] = 'Rugby'
            logger.info("➡️ Running RugbyPass scraper...")
            scrape_all_pages() 
            logger.info("✅ RugbyPass scraper completed.")


        # --- Final sport column fixes as a safety net ---
        fix_sport_columns_after_scrape() # Call this once after all scraping is done
        
    except Exception as e:
        logger.error(f"Global scraping error: {e}", exc_info=True)
        log_error(f"Global scraping error: {e}")
    finally:
        if driver:
            driver.quit() # Ensure shared driver is closed
        scraping_status['is_running'] = False
        scraping_status['current_sport'] = 'Completed'
        scraping_status['current_item'] = ''
        scraping_status['total_items'] = 0


# ==================== API ENDPOINTS ====================

@app.on_event("startup")
def startup_event():
    print("✅ Server startup: Running initial DB setup and starting background scheduler")
    init_db() # Ensure DB is initialized before scheduling/scraping
    
    # Schedule periodic scraping every 5 days
    scheduler.add_job(scrape_all_background, 'interval', days=5, id='full_scrape_job', max_instances=1) # max_instances to prevent overlapping runs
    scheduler.start()
    
    # Run the first scrape on startup in a separate thread
    threading.Thread(target=scrape_all_background, daemon=True).start()

@app.get("/", summary="API Health Check")
async def root():
    """Health check endpoint"""
    return {"message": "Sport Player API is running", "version": "1.0.0"}

@app.get("/players", response_model=PlayerListResponse, summary="Get Players with Pagination, Search, and Filters")
async def get_players(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Players per page"),
    search: Optional[str] = Query(None, description="Search by player name"),
    country: Optional[str] = Query(None, description="Filter by country"),
    team: Optional[str] = Query(None, description="Filter by team"),
    position: Optional[str] = Query(None, description="Filter by position"),
    source: Optional[str] = Query(None, description="Filter by source"),
    sport: Optional[str] = Query(None, description="Filter by sport"),
    min_age: Optional[int] = Query(None, ge=0, description="Minimum age"),
    max_age: Optional[int] = Query(None, ge=0, description="Maximum age"),
    sort_by: Optional[str] = Query("name", description="Sort by field (name, age, country, team, position, sport)"),
    sort_order: Optional[str] = Query("asc", regex="^(asc|desc)$", description="Sort order")
):
    """Get players with advanced filtering, searching, and pagination."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        where_conditions = []
        params = []
        
        if search:
            where_conditions.append("name ILIKE %s")
            params.append(f"%{search}%")
        
        if country:
            where_conditions.append("country = %s")
            params.append(country)
        
        if team:
            where_conditions.append("team = %s")
            params.append(team)
        
        if position:
            where_conditions.append("position = %s")
            params.append(position)
        
        if source:
            where_conditions.append("source = %s")
            params.append(source)

        if sport:
            where_conditions.append("sport = %s")
            params.append(sport)
        
        if min_age is not None:
            where_conditions.append("age >= %s")
            params.append(min_age)
        
        if max_age is not None:
            where_conditions.append("age <= %s")
            params.append(max_age)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        valid_sort_fields = ["name", "age", "country", "team", "position", "id", "sport"]
        if sort_by not in valid_sort_fields:
            sort_by = "name"
        
        count_query = f"SELECT COUNT(*) FROM players {where_clause}"
        cur.execute(count_query, params)
        total_count = cur.fetchone()[0]
        
        total_pages = math.ceil(total_count / per_page)
        offset = (page - 1) * per_page
        
        query = f"""
            SELECT id, name, age, weight, height, sport, country, position, team, source, player_url, created_at, updated_at
            FROM players 
            {where_clause}
            ORDER BY {sort_by} {sort_order.upper()}
            LIMIT %s OFFSET %s
        """
        
        cur.execute(query, params + [per_page, offset])
        players_data = cur.fetchall()
        
        players = []
        for row in players_data:
            player = Player(
                id=row[0],
                name=row[1],
                age=row[2],
                weight=row[3],
                height=row[4],
                sport=row[5] if row[5] else "Unknown", # Return "Unknown" for NULL sports
                country=row[6],
                position=row[7],
                team=row[8],
                source=row[9],
                player_url=row[10],
                created_at=row[11],
                updated_at=row[12]
            )
            players.append(player)
        
        response = PlayerListResponse(
            players=players,
            total_count=total_count,
            page=page,
            per_page=per_page,
            total_pages=total_pages,
            has_next=page < total_pages,
            has_prev=page > 1
        )
        
        return response
        
    except psycopg2.Error as e:
        logger.error(f"Database error in get_players: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.get("/players/{player_id}", response_model=Player, summary="Get Player by ID")
async def get_player(player_id: int):
    """Get a specific player by their ID"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, name, age, weight, height, sport, country, position, team, source, player_url, created_at, updated_at
            FROM players WHERE id = %s
        """, (player_id,))
        
        player_data = cur.fetchone()
        if not player_data:
            raise HTTPException(status_code=404, detail="Player not found")
        
        player = Player(
            id=player_data[0],
            name=player_data[1],
            age=player_data[2],
            weight=player_data[3],
            height=player_data[4],
            sport=player_data[5] if player_data[5] else "Unknown", # Return "Unknown" for NULL sports
            country=player_data[6],
            position=player_data[7],
            team=player_data[8],
            source=player_data[9],
            player_url=player_data[10],
            created_at=player_data[11],
            updated_at=player_data[12]
        )
        
        return player
        
    except psycopg2.Error as e:
        logger.error(f"Database error in get_player: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.get("/filter-options", response_model=FilterOptions, summary="Get Filter Options")
async def get_filter_options():
    """Get available filter options for countries, teams, positions, sports, and sources"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("SELECT DISTINCT sport FROM players WHERE sport IS NOT NULL AND sport != '' ORDER BY sport")
        sports = [row[0] for row in cur.fetchall()]

        cur.execute("SELECT DISTINCT country FROM players WHERE country IS NOT NULL AND country != '' ORDER BY country")
        countries = [row[0] for row in cur.fetchall()]

        cur.execute("SELECT DISTINCT team FROM players WHERE team IS NOT NULL AND team != '' ORDER BY team")
        teams = [row[0] for row in cur.fetchall()]

        cur.execute("SELECT DISTINCT position FROM players WHERE position IS NOT NULL AND position != '' ORDER BY position")
        positions = [row[0] for row in cur.fetchall()]

        cur.execute("SELECT DISTINCT source FROM players WHERE source IS NOT NULL AND source != '' ORDER BY source")
        sources = [row[0] for row in cur.fetchall()]

        return FilterOptions(
            sports=sports,
            countries=countries,
            teams=teams,
            positions=positions,
            sources=sources
        )

    except psycopg2.Error as e:
        logger.error(f"Database error in get_filter_options: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.get("/stats", summary="Get Database Statistics")
async def get_stats():
    """Get database statistics like total players, countries, teams, etc."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Total players
        cur.execute("SELECT COUNT(*) FROM players")
        total_players = cur.fetchone()[0]
        
        # Total countries
        cur.execute("SELECT COUNT(DISTINCT country) FROM players WHERE country IS NOT NULL")
        total_countries = cur.fetchone()[0]
        
        # Total teams
        cur.execute("SELECT COUNT(DISTINCT team) FROM players WHERE team IS NOT NULL")
        total_teams = cur.fetchone()[0]
        
        # Total positions
        cur.execute("SELECT COUNT(DISTINCT position) FROM players WHERE position IS NOT NULL")
        total_positions = cur.fetchone()[0]
        
        # Average age
        cur.execute("SELECT AVG(age) FROM players WHERE age IS NOT NULL")
        avg_age = cur.fetchone()[0]
        avg_age = round(float(avg_age), 1) if avg_age else 0
        
        # Players by country (top 10)
        cur.execute("""
            SELECT country, COUNT(*) as count 
            FROM players 
            WHERE country IS NOT NULL 
            GROUP BY country 
            ORDER BY count DESC 
            LIMIT 10
        """)
        top_countries = [{"country": row[0], "count": row[1]} for row in cur.fetchall()]
        
        return {
            "total_players": total_players,
            "total_countries": total_countries,
            "total_teams": total_teams,
            "total_positions": total_positions,
            "average_age": avg_age,
            "top_countries": top_countries
        }
        
    except psycopg2.Error as e:
        logger.error(f"Database error in get_stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.post("/start-scraping", summary="Start Scraping Process")
async def start_scraping(background_tasks: BackgroundTasks):
    """Start the web scraping process to collect player data"""
    global scraping_status
    
    if scraping_status['is_running']:
        raise HTTPException(status_code=400, detail="Scraping is already running")
    
    background_tasks.add_task(scrape_all_background)
    
    return {"message": "Scraping process started in background"}

@app.post("/stop-scraping", summary="Stop Scraping Process")
async def stop_scraping():
    """Stop the current scraping process"""
    global scraping_status
    
    if not scraping_status['is_running']:
        raise HTTPException(status_code=400, detail="No scraping process is currently running")
    
    scraping_status['is_running'] = False
    
    return {"message": "Scraping process stopped"}

@app.get("/scraping-status", response_model=ScrapingStatus, summary="Get Scraping Status")
async def get_scraping_status():
    """Get the current status of the scraping process"""
    return ScrapingStatus(**scraping_status)

@app.get("/logs", summary="Get Error Logs")
async def get_logs(limit: int = Query(100, ge=1, le=1000, description="Number of logs to retrieve")):
    """Get recent error logs from the database"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT id, error_message, player_url, timestamp
            FROM rugby_logs
            ORDER BY timestamp DESC
            LIMIT %s
        """, (limit,))
        
        logs = []
        for row in cur.fetchall():
            logs.append({
                "id": row[0],
                "error_message": row[1],
                "player_url": row[2],
                "timestamp": row[3]
            })
        
        return {"logs": logs}
        
    except psycopg2.Error as e:
        logger.error(f"Database error in get_logs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)