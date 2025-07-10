import os
import time
import re
import logging
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import psycopg2
from dotenv import load_dotenv

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

def setup_driver():
    options = Options()
    # options.add_argument('--headless')  # Uncomment for headless mode
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(10)
    
    # Execute script to avoid detection
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def accept_popups(driver):
    """Handle privacy/cookie consent popup"""
    print("Checking for privacy popup...")
    
    popup_selectors = [
        "button[id*='accept']",
        "button[class*='accept']",
        "button[id*='consent']",
        "button[class*='consent']",
        "button[id*='agree']",
        "button[class*='agree']",
        "button[id*='cookie']",
        "button[class*='cookie']",
        "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')]",
        "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'agree')]",
    ]
    
    for selector in popup_selectors:
        try:
            if selector.startswith("//"):
                elements = WebDriverWait(driver, 3).until(
                    EC.presence_of_all_elements_located((By.XPATH, selector))
                )
            else:
                elements = WebDriverWait(driver, 3).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector))
                )
            
            for element in elements:
                if element.is_displayed() and element.is_enabled():
                    print(f"Found privacy popup button: {element.text.strip()}")
                    driver.execute_script("arguments[0].click();", element)
                    print("✓ Privacy popup accepted!")
                    time.sleep(3)
                    return True
                    
        except TimeoutException:
            continue
    
    print("No privacy popup found")
    return False

def init_db():
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rugbypass_players (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            weight TEXT,
            height TEXT,
            position TEXT,
            country TEXT,
            team TEXT,
            source TEXT DEFAULT 'rugbypass.com',
            player_url TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def insert_player(player):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        details = player.get('detailed_bio', {}).get('extracted_details', {})
        
        # Extract age as integer
        age = None
        if details.get('age'):
            try:
                age = int(details['age'])
            except:
                pass
        
        cur.execute("""
            INSERT INTO rugbypass_players (name, age, weight, height, position, country, team, source, player_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_url) DO UPDATE SET
                age = EXCLUDED.age,
                weight = EXCLUDED.weight,
                height = EXCLUDED.height,
                position = EXCLUDED.position,
                country = EXCLUDED.country,
                team = EXCLUDED.team,
                updated_at = CURRENT_TIMESTAMP;
        """, (
            player['name'],
            age,
            details.get('weight'),
            details.get('height'),
            player.get('position'),
            details.get('country') or details.get('nationality') or 'Unknown',
            player.get('team'),
            'rugbypass.com',
            player.get('player_link')
        ))
        conn.commit()
        print(f"✓ Saved: {player['name']}")
    except Exception as e:
        print(f"❌ DB Error for {player['name']}: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def construct_player_url(player_name):
    """Construct player URL from player name"""
    try:
        # Convert name to URL format
        url_name = re.sub(r'[^\w\s-]', '', player_name.lower())
        url_name = re.sub(r'\s+', '-', url_name.strip())
        url_name = re.sub(r'-+', '-', url_name)
        
        return f"https://www.rugbypass.com/players/{url_name}/"
    except Exception as e:
        print(f"Error constructing URL for {player_name}: {e}")
        return None

def scrape_players_from_page(driver):
    """Extract player data from current page"""
    try:
        # Wait for the player list container to be visible
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "list-players"))
        )
        
        # Get the main container
        player_container = driver.find_element(By.ID, "list-players")
        print("Found main player container")
        
        players_found = []
        
        # Look for player profile links directly
        player_link_selectors = [
            "a[href*='/players/']",
            "a[href*='player']",
            ".player-name a",
            ".name a",
            "h4 a",
            "h3 a"
        ]
        
        for selector in player_link_selectors:
            try:
                player_links = player_container.find_elements(By.CSS_SELECTOR, selector)
                print(f"Found {len(player_links)} player links with selector: {selector}")
                
                if player_links:
                    for link in player_links:
                        try:
                            href = link.get_attribute('href')
                            name = link.text.strip()
                            
                            if href and name and '/players/' in href:
                                # Extract additional info from the surrounding element
                                player_data = extract_player_info_from_link(link)
                                if player_data:
                                    players_found.append(player_data)
                                    print(f"Found player link: {name} -> {href}")
                        except Exception as e:
                            continue
                    
                    if players_found:
                        print(f"Successfully found {len(players_found)} players with direct links")
                        break
                        
            except Exception as e:
                print(f"Error with selector {selector}: {e}")
                continue
        
        # If no direct links found, try text parsing approach
        if not players_found:
            print("No direct player links found, trying text parsing approach...")
            players_found = parse_player_text_with_links(player_container)
        
        return players_found
        
    except Exception as e:
        print(f"Error in scrape_players_from_page: {e}")
        return []

def extract_player_info_from_link(link_element):
    """Extract player information from a player link element"""
    try:
        href = link_element.get_attribute('href')
        name = link_element.text.strip()
        
        if not href or not name or '/players/' not in href:
            return None
        
        player_data = {
            'name': name,
            'player_link': href,
            'team': '',
            'position': '',
            'raw_text': ''
        }
        
        # Try to get additional info from parent elements
        try:
            parent = link_element.find_element(By.XPATH, "..")
            for i in range(3):  # Check up to 3 levels up
                try:
                    parent_text = parent.text.strip()
                    if parent_text and len(parent_text) > len(name):
                        lines = [line.strip() for line in parent_text.split('\n') if line.strip()]
                        
                        # Usually format is: Name, Team, Position
                        for i, line in enumerate(lines):
                            if name.lower() in line.lower():
                                if i + 1 < len(lines):
                                    player_data['team'] = lines[i + 1]
                                if i + 2 < len(lines):
                                    player_data['position'] = lines[i + 2]
                                break
                        
                        player_data['raw_text'] = parent_text
                        break
                    
                    parent = parent.find_element(By.XPATH, "..")
                except:
                    break
                    
        except Exception as e:
            pass
        
        return player_data
        
    except Exception as e:
        return None

def parse_player_text_with_links(container):
    """Parse player data from container text and construct player links"""
    print("Parsing player data from text content and constructing links...")
    
    try:
        text_content = container.text
        print(f"DEBUG: Container text length: {len(text_content)}")
        
        if not text_content or len(text_content) < 10:
            print("Container text is empty or too short")
            return []
        
        lines = [line.strip() for line in text_content.split('\n') if line.strip()]
        print(f"DEBUG: Found {len(lines)} non-empty lines")
        
        players = []
        i = 0
        
        while i < len(lines) - 2:
            try:
                potential_name = lines[i]
                potential_team = lines[i + 1] if i + 1 < len(lines) else ""
                potential_position = lines[i + 2] if i + 2 < len(lines) else ""
                
                # Skip header lines or navigation
                skip_patterns = ["name", "team", "position", "current squad", "filter", "tournaments", "teams", "positions"]
                if any(pattern in potential_name.lower() for pattern in skip_patterns):
                    i += 1
                    continue
                
                # Check if this looks like a player entry
                if (len(potential_name) > 1 and 
                    len(potential_name) < 50 and
                    not potential_name.isdigit() and
                    ("'" in potential_name or len(potential_name.split()) >= 2)):
                    
                    # Construct player URL from name
                    player_url = construct_player_url(potential_name)
                    
                    player_info = {
                        "name": potential_name,
                        "team": potential_team,
                        "position": potential_position,
                        "player_link": player_url,
                        "raw_text": f"{potential_name}\n{potential_team}\n{potential_position}"
                    }
                    
                    players.append(player_info)
                    print(f"Found player: {potential_name} -> {player_url}")
                    
                    i += 3
                else:
                    i += 1
                    
            except Exception as e:
                i += 1
                continue
        
        print(f"Parsed {len(players)} players from text with constructed URLs")
        return players
        
    except Exception as e:
        print(f"Error in parse_player_text_with_links: {e}")
        return []

def scrape_player_bio(driver, url):
    """Scrape detailed player bio from bio page"""
    try:
        print(f"Scraping bio from: {url}")
        
        original_window = driver.current_window_handle
        
        # Open bio page in new tab
        driver.execute_script("window.open(arguments[0], '_blank');", url)
        time.sleep(2)
        
        # Switch to new tab
        if len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])
        
        # Wait for bio page to load
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(2)
        
        # Check if page loaded successfully
        if "404" in driver.title.lower() or "not found" in driver.title.lower():
            print(f"Player page not found")
            return {'extracted_details': {}}
        
        extracted_details = {}
        
        # Try to find player details
        try:
            # Look for player-details container
            player_details = driver.find_element(By.CSS_SELECTOR, "div.player-details")
            
            # Find all detail sections
            detail_sections = player_details.find_elements(By.CSS_SELECTOR, "div.detail")
            
            for detail_section in detail_sections:
                try:
                    # Get the h3 heading
                    h3_element = detail_section.find_element(By.TAG_NAME, "h3")
                    heading = h3_element.text.strip().lower()
                    
                    # Get the content
                    content_element = detail_section.find_element(By.TAG_NAME, "p")
                    content = content_element.text.strip()
                    
                    # Map common rugby player details
                    if 'age' in heading:
                        match = re.search(r'(\d+)', content)
                        if match:
                            extracted_details['age'] = match.group(1)
                    elif 'height' in heading:
                        extracted_details['height'] = content
                    elif 'weight' in heading:
                        extracted_details['weight'] = content
                    elif 'nationality' in heading or 'country' in heading:
                        extracted_details['country'] = content
                    elif 'position' in heading:
                        extracted_details['position'] = content
                    elif 'team' in heading or 'club' in heading:
                        extracted_details['team'] = content
                        
                except Exception as e:
                    continue
                    
        except Exception as e:
            print(f"Error finding player details: {e}")
        
        return {'extracted_details': extracted_details}
        
    except Exception as e:
        print(f"Error scraping bio: {e}")
        return {'extracted_details': {}}
    
    finally:
        # Always try to close the bio tab and return to original window
        try:
            if len(driver.window_handles) > 1:
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
                time.sleep(1)
        except Exception as e:
            pass

def scrape_all_pages():
    """Main scraping function with pagination"""
    base_url = "https://www.rugbypass.com/players/"
    driver = setup_driver()
    all_players = []
    page = 1
    max_pages = 10  # Safety limit

    try:
        # Load first page
        driver.get(base_url)
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        print(f"✓ Page loaded: {driver.title}")
        
        # Handle privacy popup
        accept_popups(driver)
        time.sleep(5)
        
        while page <= max_pages:
            print(f"\n🔄 Scraping page {page}...")
            
            # Scrape players from current page
            players = scrape_players_from_page(driver)
            
            if not players:
                print(f"⚠️ No players found on page {page}, stopping.")
                break
            
            print(f"Found {len(players)} players on page {page}")
            
            # For each player, scrape their detailed bio and save to DB
            for i, player in enumerate(players):
                try:
                    player_name = player.get('name', 'Unknown')
                    player_link = player.get('player_link')
                    
                    if player_link:
                        print(f"Scraping bio {i+1}/{len(players)}: {player_name}")
                        
                        # Scrape detailed bio
                        detailed_bio = scrape_player_bio(driver, player_link)
                        player['detailed_bio'] = detailed_bio
                        
                        # Save to database immediately
                        insert_player(player)
                        
                        all_players.append(player)
                        
                        # Small delay between bio scrapes
                        time.sleep(1)
                    else:
                        print(f"Skipping {player_name} - no link")
                        
                except Exception as e:
                    print(f"Error processing player {player.get('name', 'Unknown')}: {e}")
                    continue
            
            print(f"✅ Page {page} complete. Total players so far: {len(all_players)}")
            
            # Try to navigate to next page
            try:
                next_page_url = f"{base_url}?p={page + 1}"
                driver.get(next_page_url)
                time.sleep(3)
                
                # Check if next page has players
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.ID, "list-players"))
                    )
                    page += 1
                except TimeoutException:
                    print("No more pages found")
                    break
                    
            except Exception as e:
                print(f"Error navigating to next page: {e}")
                break

    except Exception as e:
        print(f"Error in scrape_all_pages: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        driver.quit()

    return all_players

def main():
    print("=== Starting RugbyPass Player Scraper ===")
    init_db()
    print("✓ Database initialized")
    
    players = scrape_all_pages()
    print(f"\n=== Scraping Complete ===")
    print(f"Total players processed: {len(players)}")

if __name__ == '__main__':
    main()