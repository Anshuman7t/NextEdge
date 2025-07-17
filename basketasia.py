import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup
import psycopg2
import time
from urllib.parse import urljoin
import logging

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

# Country lists - Add more Asian countries here
asia_countries = [
    'Afghanistan', 'Bahrain', 'Bangladesh', 'Bhutan', 'Brunei', 'Cambodia', 
    'China', 'Hong Kong', 'India', 'Indonesia', 'Iran', 'Iraq', 'Japan', 
    'Jordan', 'Kazakhstan', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Lebanon', 
    'Macao', 'Malaysia', 'Maldives', 'Mongolia', 'Myanmar', 'Nepal', 
    'North Korea', 'Oman', 'Pakistan', 'Palestine', 'Philippines', 
    'Qatar', 'Saudi Arabia', 'Singapore', 'South Korea', 'Sri Lanka', 
    'Syria', 'Taiwan', 'Tajikistan', 'Thailand', 'Turkmenistan', 
    'UAE', 'Uzbekistan', 'Vietnam', 'Yemen'
]
europe_countries = []  # Disable Europe for now

# Initialize database tables
def init_db():
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        logger.debug("Attempting to create 'players' table.")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id SERIAL PRIMARY KEY,
                name TEXT,
                age INTEGER,
                weight TEXT,
                height TEXT,
                sport TEXT DEFAULT 'Basketball',
                country TEXT,
                position TEXT,
                team TEXT,
                source TEXT,
                player_url TEXT UNIQUE
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
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'players');")
        if cur.fetchone()[0]:
            logger.info("Players table verified.")
        else:
            logger.error("Players table creation failed verification.")
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

# Fetch page content with retry logic
def fetch_page(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            logger.debug(f"Fetching page: {url}, Attempt {attempt + 1}/{max_retries}")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            logger.debug(f"Page fetched successfully: {url}")
            log_fetched_url(url, 'success')
            return soup
        except requests.RequestException as e:
            log_error(f"Attempt {attempt + 1}/{max_retries} - Error fetching {url}: {e}", url)
            log_fetched_url(url, 'failed', str(e))
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
            else:
                logger.error(f"Max retries reached for {url}", exc_info=True)
                return None

# Convert CM to feet and inches
def cm_to_feet_inches(cm):
    if not cm or not cm.strip().isdigit():
        return None
    cm = float(cm.strip())
    total_feet = cm * 0.0328084
    feet = int(total_feet)
    inches = round((total_feet - feet) * 12)
    return f"{feet}'{inches}''"

def slugify_country(country):
    return country.strip().replace(" ", "-")

# Extract teams from a country page
def get_teams(country, gender, base_url, website):
    logger.debug(f"Extracting teams for {country}, gender: {gender}, base_url: {base_url}")
    teams = []
    suffix = '?women=1' if gender == 'women' else ''
    url = f"{base_url}/{slugify_country(country)}/basketball-Teams.aspx{suffix}"
    soup = fetch_page(url)
    if not soup:
        logger.warning(f"No data retrieved for {url}")
        return teams
    team_elements = soup.select('a[href*="team/"]')  # Adjust if team links are in a different format
    logger.debug(f"Found {len(team_elements)} team elements for {country}")
    for team in team_elements:
        team_url = urljoin(url, team['href'])
        team_name = team.text.strip()
        teams.append({'name': team_name, 'url': team_url})
    logger.info(f"Extracted {len(teams)} teams for {country}")
    return teams

# Extract player data from a team roster page
def get_roster(team_url, team_name, website, gender):
    logger.debug(f"Extracting roster from {team_url}")
    players = []
    soup = fetch_page(team_url)
    if not soup:
        logger.warning(f"No data retrieved for roster at {team_url}")
        return players
    roster = soup.select('table#trRoster tbody tr')
    logger.debug(f"Found {len(roster)} player rows in roster")
    for row in roster:
        try:
            cols = row.find_all('td')
            if len(cols) < 6:  # Minimum columns needed: Name, CM, Pos, Age, Nat
                logger.warning(f"Skipping row with insufficient columns: {cols}")
                continue
            
            # Fix the name extraction to avoid duplication
            name_elem = cols[2].select_one('a[href*="player/"]')
            if name_elem:
                name = name_elem.text.strip()
                player_url = urljoin(team_url, name_elem['href'])
            else:
                name = cols[2].text.strip()
                player_url = None
            
            height_elem = cols[3].find('font', size="3")
            height_cm = height_elem.text.strip() if height_elem else None
            height = cm_to_feet_inches(height_cm) if height_cm else None
            position = cols[4].find('font', size="3").text.strip() if cols[4].find('font', size="3") else None
            age = int(cols[5].text.strip()) if cols[5].text.strip().isdigit() else None
            country_elem = cols[6].select_one('img')
            country = country_elem['alt'].replace('ian', '') if country_elem else None

            player_data = {
                'name': name,
                'age': age,
                'height': height,
                'sport': 'Basketball',  # Default sport
                'position': position,
                'country': country,
                'team': team_name,
                'weight': None,
                'source': website,
                'player_url': player_url
            }
            players.append(player_data)
        except AttributeError as e:
            logger.warning(f"Error parsing player data from {team_url}: {e}", exc_info=True)
            continue
    logger.info(f"Extracted {len(players)} players from {team_url}")
    return players

# Extract additional player details from individual player page
def get_player_details(player_url):
    logger.debug(f"Extracting details from {player_url}")
    soup = fetch_page(player_url)
    if not soup:
        logger.warning(f"No data retrieved for player details at {player_url}")
        return None
    weight_elem = soup.select_one('div.player-details.mobile p:contains("Weight:")')
    weight = weight_elem.text.strip().split('Weight: ')[1].split('kg')[0].strip() if weight_elem else None
    logger.debug(f"Extracted weight: {weight}kg from {player_url}")
    return weight

# Insert player data into the database
def insert_player(data):
    try:
        logger.debug(f"Attempting to insert players: {data['name']}, URL: {data['player_url']}")
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO players (name, age, weight, height, sport, country, position, team, source, player_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_url) DO NOTHING;
        """, (
            data['name'], data['age'], data['weight'], data['height'], data['sport'], data['country'],
            data['position'], data['team'], data['source'], data['player_url']
        ))
        conn.commit()
        logger.info(f"Successfully inserted player: {data['name']}")
        return True
    except psycopg2.Error as e:
        log_error(f"Database error inserting {data['name']}: {e}", data['player_url'])
        return False
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# Process a single country
def process_country(country, gender, base_url, website, scraping_status):
    logger.info(f"Processing country: {country}, gender: {gender}")
    teams = get_teams(country, gender, base_url, website)
    for team in teams:
        logger.info(f"Processing team: {team['name']}")
        players = get_roster(team['url'], team['name'], website, gender)
        for player in players:
            if player['player_url']:
                player['weight'] = get_player_details(player['player_url'])
            if insert_player(player):
                scraping_status['processed'] += 1
        time.sleep(1)  # Avoid rate limiting

# Start scraping
@app.post("/start-scraping/")
async def start_scraping():
    init_db()
    global processed
    processed = 0
    total_players = 9400000

    logger.info("Starting scraping process.")
    with ThreadPoolExecutor(max_workers=1) as executor:  # Single worker for debugging
        # Process Asia (men and women)
        for country in asia_countries:
            for gender in ['men', 'women']:
                base_url = 'https://basketball.asia-basket.com'
                process_country(country, gender, base_url, 'basketball.asia-basket.com')

    logger.info(f"Scraping completed. Processed {processed} players.")
    return {"message": f"Scraping completed for {processed} out of {total_players} players"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)