import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from concurrent.futures import ThreadPoolExecutor
from common_utils import insert_player, log_error
import requests
from bs4 import BeautifulSoup
import psycopg2
import time
from urllib.parse import urljoin
import logging
import re

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
# Initialize processed count
processed = 0 # Initialize global processed count here

us_teams = ['NBA', 'G-League', 'NCAA1', 'NCAA2', 'NCAA3', 'NAIA', 'JUCO', 'USCAA',
            'High Schools', 'ABA', 'Big3', 'EBA', 'ECBL', 'ESL', 'FBA', 'MBL', 'NBL-US',
            'OTE', 'PBA', 'SCBL', 'SEBL', 'TBA', 'TBL', 'TBT', 'UBA', 'V League', 'BSL', 'NBLCanada',
            'CEBL', 'U Sports', 'CCAA']

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
def cm_to_feet_inches(cm_text):
    if not cm_text:
        return None
    
    # Extract numeric value from text like "201cm" or "6'7"
    cm_value_match = re.search(r'(\d+)\s*cm', cm_text, re.IGNORECASE)
    if cm_value_match:
        cm = float(cm_value_match.group(1))
        total_feet = cm * 0.0328084
        feet = int(total_feet)
        inches = round((total_feet - feet) * 12)
        return f"{feet}'{inches}''"
    
    # If already in feet/inches format, return as is
    if "'" in cm_text:
        return cm_text.replace('"', "''") # Ensure consistent double quotes for inches
    
    return None

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
    team_elements = soup.select('a[href*="team/"]')
    logger.debug(f"Found {len(team_elements)} team elements for {country}")
    for team in team_elements:
        team_url = urljoin(url, team['href'])
        team_name = team.text.strip()
        teams.append({'name': team_name, 'url': team_url})
    logger.info(f"Extracted {len(teams)} teams for {country}")
    return teams

# Function to extract detailed player info from their individual page (especially FAQ)
def extract_player_page_details(player_url):
    logger.debug(f"Extracting detailed info from player page: {player_url}")
    soup = fetch_page(player_url)
    if not soup:
        return {'age': None, 'weight': None, 'position_detail': None, 'height_detail': None}

    detailed_data = {'age': None, 'weight': None, 'position_detail': None, 'height_detail': None}

    # Extract from FAQ section questions and answers
    faq_section = soup.find('div', id='faq-section') 
    if not faq_section:
        faq_section = soup.find('div', class_=re.compile(r'faq|questions', re.IGNORECASE))
    if not faq_section:
        faq_heading = soup.find(['h2', 'h3', 'h4'], text=re.compile(r'frequently asked questions', re.IGNORECASE))
        if faq_heading:
            faq_section = faq_heading.find_next_sibling()
            if not faq_section: 
                faq_section = faq_heading.find_parent('div')

    if faq_section:
        questions = faq_section.find_all(['h3', 'h4'])
        for question_elem in questions:
            question_text = question_elem.get_text().strip().lower()
            answer_elem = question_elem.find_next_sibling(['p', 'div'])
            if not answer_elem:
                answer_elem = question_elem.find_next(['p', 'div'], class_=re.compile(r'answer|content', re.IGNORECASE))

            if answer_elem:
                answer_text = answer_elem.get_text().strip()

                # Extract Age
                if 'age' in question_text or 'old' in question_text:
                    age_match = re.search(r'(\d+)\s*years?\s*old', answer_text, re.IGNORECASE)
                    if age_match:
                        detailed_data['age'] = int(age_match.group(1))
                        logger.debug(f"Found age {detailed_data['age']} from FAQ for {player_url}")

                # Extract Weight
                elif 'weight' in question_text:
                    weight_kg_match = re.search(r'(\d+)\s*kg', answer_text, re.IGNORECASE)
                    if weight_kg_match:
                        detailed_data['weight'] = weight_kg_match.group(1) # Store just the number
                        logger.debug(f"Found weight {detailed_data['weight']} (number only) from FAQ for {player_url}")
                    else:
                        weight_lbs_match = re.search(r'(\d+)\s*lbs', answer_text, re.IGNORECASE)
                        if weight_lbs_match:
                            detailed_data['weight'] = weight_lbs_match.group(1) # Store just the number
                            logger.debug(f"Found weight {detailed_data['weight']} (lbs, number only) from FAQ for {player_url}")

                # Extract Height (for refinement if roster height is basic)
                elif 'how tall' in question_text:
                    height_match = re.search(r'(\d+cm)\s*/\s*(\d+\'?\d*"?\s*(?:tall)?)', answer_text, re.IGNORECASE)
                    if height_match:
                        detailed_data['height_detail'] = height_match.group(2).replace('tall', '').strip()
                    else:
                        height_ft_in_match = re.search(r'(\d+\'?\d*"?)\s*(?:tall)?', answer_text, re.IGNORECASE)
                        if height_ft_in_match:
                            detailed_data['height_detail'] = height_ft_in_match.group(1).strip()
                    logger.debug(f"Found detailed height {detailed_data['height_detail']} from FAQ for {player_url}")

                # Extract Position (for refinement if roster position is basic)
                elif 'position' in question_text:
                    position_patterns = [
                        r'([A-Za-z\s]+)\s+position',
                        r'plays?\s+(?:as\s+)?(?:a\s+)?([A-Za-z\s]+)',
                        r'is\s+(?:a\s+)?([A-Za-z\s]+)'
                    ]
                    for pattern in position_patterns:
                        pos_match = re.search(pattern, answer_text, re.IGNORECASE)
                        if pos_match:
                            position = pos_match.group(1).strip()
                            position_map = {
                                'forward': 'Forward', 'g': 'Guard', 'pg': 'Point Guard', 'sg': 'Shooting Guard',
                                'guard': 'Guard', 'center': 'Center', 'c': 'Center',
                                'centre': 'Center', 'point guard': 'Point Guard',
                                'shooting guard': 'Shooting Guard', 'small forward': 'Small Forward',
                                'power forward': 'Power Forward', 'f': 'Forward', 'sf': 'Small Forward', 'pf': 'Power Forward',
                                'g/f': 'Swingman', 'f/g': 'Swingman' # Added for G/F to Swingman
                            }
                            detailed_data['position_detail'] = position_map.get(position.lower(), position.title())
                            logger.debug(f"Found detailed position {detailed_data['position_detail']} from FAQ for {player_url}")
                            break

    # Fallback/additional check for weight outside FAQ if needed (less reliable)
    if not detailed_data['weight']:
        weight_elem = soup.select_one('div.player-details.mobile p:contains("Weight:")')
        if weight_elem:
            weight_text = weight_elem.text.strip()
            weight_match = re.search(r'Weight:\s*(\d+)\s*(?:kg|lbs)', weight_text, re.IGNORECASE) # Match kg or lbs but don't capture
            if weight_match:
                detailed_data['weight'] = weight_match.group(1) # Store just the number
                logger.debug(f"Found weight {detailed_data['weight']} from general player details for {player_url}")

    return detailed_data

# Extract player data from a team roster page
def get_roster(team_url, team_name, website, gender):
    logger.debug(f"Extracting roster from {team_url}")
    players = []
    soup = fetch_page(team_url)
    if not soup:
        logger.warning(f"No data retrieved for roster at {team_url}")
        return players
    
    roster_rows = soup.select('table#trRoster tbody tr')
    logger.debug(f"Found {len(roster_rows)} player rows in roster for {team_name}")
    
    for row in roster_rows:
        try:
            cols = row.find_all('td')
            if len(cols) < 6:
                logger.warning(f"Skipping row with insufficient columns in roster for {team_name}: {len(cols)} columns found.")
                continue

            # --- FIX FOR DUPLICATE NAME (Revised) ---
            name_link_elem = cols[2].select_one('a[href*="player/"]')
            name = None
            player_url = None
            if name_link_elem:
                full_name_text = name_link_elem.text.strip()
                # A more robust check: if the first half is identical to the second half
                half_len = len(full_name_text) // 2
                if half_len > 0 and full_name_text[:half_len] == full_name_text[half_len:]:
                    name = full_name_text[:half_len]
                    logger.debug(f"Cleaned duplicate name from '{full_name_text}' to '{name}'")
                else:
                    name = full_name_text
                player_url = urljoin(team_url, name_link_elem['href'])
            else:
                name = cols[2].text.strip() # Fallback if no link, but less ideal

            if not name:
                logger.warning(f"Skipping row with no name found in roster for {team_name}.")
                continue

            # Extract Height from roster (often in CM)
            height_roster_cm_elem = cols[3].find('font', size="3")
            height_roster = None
            if height_roster_cm_elem:
                height_roster_text = height_roster_cm_elem.text.strip()
                # Attempt to convert CM to feet/inches if it's purely CM, otherwise keep as is
                if 'cm' in height_roster_text.lower() and re.search(r'^\d+cm$', height_roster_text, re.IGNORECASE):
                    height_roster = cm_to_feet_inches(height_roster_text.replace('cm', ''))
                else:
                    height_roster = height_roster_text
            
            # Extract Position from roster (often abbreviated)
            position_roster_elem = cols[4].find('font', size="3")
            position_roster = position_roster_elem.text.strip() if position_roster_elem else None
            
            # Extract Age from roster
            age_roster = None
            age_text = cols[5].text.strip()
            if age_text.isdigit():
                age_roster = int(age_text)

            # Extract Country from roster
            country_elem = cols[6].select_one('img') # Assuming country flag image with alt text
            country_roster = None
            if country_elem and 'alt' in country_elem.attrs:
                country_raw = country_elem['alt'].strip()
                nationality_map = {
                    'canadian': 'Canada', 'american': 'USA', 'british': 'UK', 'french': 'France',
                    'german': 'Germany', 'italian': 'Italy', 'spanish': 'Spain', 'australian': 'Australia',
                    'cameroonian': 'Cameroon', 'brazilian': 'Brazil', 'argentine': 'Argentina',
                    'nigerian': 'Nigeria', 'usa-nigeria': 'Usa-Nigeria', # Added for your example
                    'us': 'USA' # Added for generic 'US' if it appears
                }
                country_roster = nationality_map.get(country_raw.lower(), country_raw.title()) # Default to title case

            player_data = {
                'name': name,
                'age': age_roster, # Initial age from roster
                'weight': None,    # Will be populated from player page
                'height': height_roster, # Initial height from roster
                'sport': 'Basketball', # Default sport
                'position': position_roster, # Initial position from roster
                'country': country_roster,
                'team': team_name,
                'source': website,
                'player_url': player_url
            }
            players.append(player_data)
        except IndexError as e:
            logger.warning(f"IndexError when parsing roster row for {team_name}: {e}. Row content: {row.get_text()}", exc_info=True)
            continue
        except AttributeError as e:
            logger.warning(f"AttributeError when parsing roster row for {team_name}: {e}. Likely missing element. Row content: {row.get_text()}", exc_info=True)
            continue
        except Exception as e:
            logger.error(f"Unexpected error parsing roster row for {team_name}: {e}. Row content: {row.get_text()}", exc_info=True)
            continue
            
    logger.info(f"Extracted {len(players)} players from {team_url}")
    return players

# Insert player data into the database
def insert_player(data):
    try:
        # Check if player_url is valid before attempting insertion
        if not data.get('player_url'):
            logger.warning(f"Skipping insertion due to missing player_url for {data.get('name')}")
            log_error(f"Missing player_url for player: {data.get('name')}", None)
            return False

        logger.debug(f"Attempting to insert/update player: {data['name']}, URL: {data['player_url']}")
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO players (name, age, weight, height, sport, country, position, team, source, player_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_url) DO UPDATE SET
                name = EXCLUDED.name,
                age = COALESCE(EXCLUDED.age, players.age), -- Update only if new age is not null
                weight = COALESCE(EXCLUDED.weight, players.weight), -- Update only if new weight is not null
                height = COALESCE(EXCLUDED.height, players.height), -- Update only if new height is not null
                sport = COALESCE(EXCLUDED.sport, players.sport), -- Update only if new sport is not null
                country = COALESCE(EXCLUDED.country, players.country),
                position = COALESCE(EXCLUDED.position, players.position), -- Update only if new position is not null
                team = EXCLUDED.team,
                source = EXCLUDED.source;
        """, (
            data['name'], data['age'], data['weight'], data['height'], data['sport'], data['country'],
            data['position'], data['team'], data['source'], data['player_url']
        ))
        conn.commit()
        logger.info(f"Successfully inserted/updated player: {data['name']}")
        return True
    except psycopg2.Error as e:
        log_error(f"Database error inserting/updating {data.get('name', 'N/A')}: {e}", data.get('player_url'))
        logger.error(f"Database error for {data.get('name', 'N/A')}: {e}", exc_info=True)
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
        players_from_roster = get_roster(team['url'], team['name'], website, gender)
        
        for player_data in players_from_roster:
            if player_data['player_url']:
                detailed_player_info = extract_player_page_details(player_data['player_url'])
                
                # Merge the data, preferring detailed info if available and not already set
                player_data['age'] = player_data['age'] or detailed_player_info['age']
                player_data['weight'] = player_data['weight'] or detailed_player_info['weight']
                
                # Prioritize detailed height if it's more specific or the roster one was basic CM
                if detailed_player_info['height_detail'] and (not player_data['height'] or 'cm' in player_data['height'].lower()):
                    player_data['height'] = detailed_player_info['height_detail']
                
                # Prioritize detailed position if it's more descriptive or the roster one was abbreviated
                if detailed_player_info['position_detail'] and (not player_data['position'] or len(player_data['position']) < 3): # Check for length < 3 (e.g., G, F, C)
                    player_data['position'] = detailed_player_info['position_detail']
                
                # Further refine position abbreviations (including G/F to Swingman)
                if player_data['position']:
                    position_map = {
                        'g': 'Guard', 'pg': 'Point Guard', 'sg': 'Shooting Guard',
                        'f': 'Forward', 'sf': 'Small Forward', 'pf': 'Power Forward',
                        'c': 'Center',
                        'g/f': 'Swingman', 'f/g': 'Swingman' # Added for G/F and F/G to Swingman
                    }
                    # Apply mapping if the current position is a key in the map (case-insensitive)
                    player_data['position'] = position_map.get(player_data['position'].lower(), player_data['position'])

            if insert_player(player_data):
                scraping_status['processed'] += 1
                logger.info(f"Processed player {processed}: {player_data['name']}")
            time.sleep(1)

# Start scraping
@app.post("/start-scraping/")
async def start_scraping():
    init_db()
    global processed
    processed = 0
    total_players = 9400000

    logger.info("Starting scraping process.")
    for country in us_teams:
        for gender in ['men', 'women']:
            base_url = 'https://basketball.usbasket.com'
            try:
                process_country(country, gender, base_url, 'basketball.usbasket.com')
            except Exception as e:
                logger.error(f"Error processing {country} {gender}: {e}", exc_info=True)
                continue

    logger.info(f"Scraping completed. Processed {processed} players.")
    return {"message": f"Scraping completed for {processed} players. Note: Total players count is an estimate."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)