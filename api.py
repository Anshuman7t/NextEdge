import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup
import psycopg2
import time
from urllib.parse import urljoin
import logging
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
import math

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

load_dotenv()

app = FastAPI(title="Basketball Player API", version="1.0.0")

# Add CORS middleware to allow frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this properly for production
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

# Country lists (testing with Taiwan only)
asia_countries = ['Taiwan']  # Limit to one country for debugging
europe_countries = []  # Disable Europe for now

# Pydantic models for API responses
class Player(BaseModel):
    id: int
    name: str
    age: Optional[int] = None
    weight: Optional[str] = None
    height: Optional[str] = None
    sport: str = "Basketball"
    country: Optional[str] = None
    position: Optional[str] = None
    team: Optional[str] = None
    source: Optional[str] = None
    player_url: Optional[str] = None

class PlayerListResponse(BaseModel):
    players: List[Player]
    total_count: int
    page: int
    per_page: int
    total_pages: int
    has_next: bool
    has_prev: bool

class FilterOptions(BaseModel):
    countries: List[str]
    teams: List[str]
    positions: List[str]
    sources: List[str]

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

# Extract teams from a country page
def get_teams(country, gender, base_url, website):
    logger.debug(f"Extracting teams for {country}, gender: {gender}, base_url: {base_url}")
    teams = []
    suffix = '?women=1' if gender == 'women' else ''
    url = f"{base_url}/{country}/basketball-Teams.aspx{suffix}"
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
            name_elem = cols[2].select_one('a[href*="player/"]')
            name = name_elem.text.strip() if name_elem else cols[2].text.strip()
            player_url = urljoin(team_url, name_elem['href']) if name_elem else None
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
        logger.debug(f"Attempting to insert player: {data['name']}, URL: {data['player_url']}")
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO players (name, age, weight, height, country, position, team, source, player_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_url) DO NOTHING;
        """, (
            data['name'], data['age'], data['weight'], data['height'], data['country'],
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
def process_country(country, gender, base_url, website):
    logger.info(f"Processing country: {country}, gender: {gender}")
    teams = get_teams(country, gender, base_url, website)
    for team in teams:
        logger.info(f"Processing team: {team['name']}")
        players = get_roster(team['url'], team['name'], website, gender)
        for player in players:
            if player['player_url']:
                player['weight'] = get_player_details(player['player_url'])
            if insert_player(player):
                global processed
                processed += 1
        time.sleep(1)  # Avoid rate limiting

# ==================== NEW API ENDPOINTS ====================

@app.get("/", summary="API Health Check")
async def root():
    """Health check endpoint"""
    return {"message": "Basketball Player API is running", "version": "1.0.0"}

@app.get("/players", response_model=PlayerListResponse, summary="Get Players with Pagination, Search, and Filters")
async def get_players(
    page: int = Query(1, ge=1, description="Page number"),
    per_page: int = Query(20, ge=1, le=100, description="Players per page"),
    search: Optional[str] = Query(None, description="Search by player name"),
    country: Optional[str] = Query(None, description="Filter by country"),
    team: Optional[str] = Query(None, description="Filter by team"),
    position: Optional[str] = Query(None, description="Filter by position"),
    source: Optional[str] = Query(None, description="Filter by source"),
    min_age: Optional[int] = Query(None, ge=0, description="Minimum age"),
    max_age: Optional[int] = Query(None, ge=0, description="Maximum age"),
    sort_by: Optional[str] = Query("name", description="Sort by field (name, age, country, team, position)"),
    sort_order: Optional[str] = Query("asc", regex="^(asc|desc)$", description="Sort order")
):
    """
    Get players with advanced filtering, searching, and pagination.
    
    - **page**: Page number (starts from 1)
    - **per_page**: Number of players per page (max 100)
    - **search**: Search players by name (case-insensitive)
    - **country**: Filter by specific country
    - **team**: Filter by specific team
    - **position**: Filter by specific position
    - **source**: Filter by data source
    - **min_age**: Minimum age filter
    - **max_age**: Maximum age filter
    - **sort_by**: Sort by field (name, age, country, team, position)
    - **sort_order**: Sort order (asc or desc)
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Build the WHERE clause
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
        
        if min_age is not None:
            where_conditions.append("age >= %s")
            params.append(min_age)
        
        if max_age is not None:
            where_conditions.append("age <= %s")
            params.append(max_age)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        # Validate sort_by field
        valid_sort_fields = ["name", "age", "country", "team", "position", "id"]
        if sort_by not in valid_sort_fields:
            sort_by = "name"
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM players {where_clause}"
        cur.execute(count_query, params)
        total_count = cur.fetchone()[0]
        
        # Calculate pagination
        total_pages = math.ceil(total_count / per_page)
        offset = (page - 1) * per_page
        
        # Get players with pagination
        query = f"""
            SELECT id, name, age, weight, height, sport, country, position, team, source, player_url
            FROM players 
            {where_clause}
            ORDER BY {sort_by} {sort_order.upper()}
            LIMIT %s OFFSET %s
        """
        
        cur.execute(query, params + [per_page, offset])
        players_data = cur.fetchall()
        
        # Convert to Player objects
        players = []
        for row in players_data:
            player = Player(
                id=row[0],
                name=row[1],
                age=row[2],
                weight=row[3],
                height=row[4],
                sport=row[5],
                country=row[6],
                position=row[7],
                team=row[8],
                source=row[9],
                player_url=row[10]
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
        logger.error(f"Database error in get_players: {e}")
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
            SELECT id, name, age, weight, height, sport, country, position, team, source, player_url
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
            sport=player_data[5],
            country=player_data[6],
            position=player_data[7],
            team=player_data[8],
            source=player_data[9],
            player_url=player_data[10]
        )
        
        return player
        
    except psycopg2.Error as e:
        logger.error(f"Database error in get_player: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

@app.get("/filter-options", response_model=FilterOptions, summary="Get Filter Options")
async def get_filter_options():
    """Get available filter options for countries, teams, positions, and sources"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get unique countries
        cur.execute("SELECT DISTINCT country FROM players WHERE country IS NOT NULL ORDER BY country")
        countries = [row[0] for row in cur.fetchall()]
        
        # Get unique teams
        cur.execute("SELECT DISTINCT team FROM players WHERE team IS NOT NULL ORDER BY team")
        teams = [row[0] for row in cur.fetchall()]
        
        # Get unique positions
        cur.execute("SELECT DISTINCT position FROM players WHERE position IS NOT NULL ORDER BY position")
        positions = [row[0] for row in cur.fetchall()]
        
        # Get unique sources
        cur.execute("SELECT DISTINCT source FROM players WHERE source IS NOT NULL ORDER BY source")
        sources = [row[0] for row in cur.fetchall()]
        
        return FilterOptions(
            countries=countries,
            teams=teams,
            positions=positions,
            sources=sources
        )
        
    except psycopg2.Error as e:
        logger.error(f"Database error in get_filter_options: {e}")
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
        
        return {
            "total_players": total_players,
            "total_countries": total_countries,
            "total_teams": total_teams,
            "total_positions": total_positions,
            "average_age": avg_age
        }
        
    except psycopg2.Error as e:
        logger.error(f"Database error in get_stats: {e}")
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

# ==================== EXISTING SCRAPING ENDPOINT ====================

@app.post("/start-scraping/", summary="Start Scraping Process")
async def start_scraping():
    """Start the web scraping process to collect player data"""
    init_db()
    global processed
    processed = 0
    total_players = 9400000

    logger.info("Starting scraping process.")
    with ThreadPoolExecutor(max_workers=1) as executor:  # Single worker for debugging
        # Process Asia (men and women)
        for gender in ['men', 'women']:
            base_url = 'https://basketball.asia-basket.com'
            executor.map(lambda c: process_country(c, gender, base_url, 'basketball.asia-basket.com'), asia_countries)

        # Process Europe (women only) - disabled for now
        # base_url = 'https://www.eurobasket.com'
        # executor.map(lambda c: process_country(c, 'women', base_url, 'eurobasket.com'), europe_countries)

    logger.info(f"Scraping completed. Processed {processed} players.")
    return {"message": f"Scraping completed for {processed} out of {total_players} players"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)