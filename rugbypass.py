import os
import time
import re
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
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
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(10)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def accept_popups(driver):
    popup_selectors = [
        "button[id*='accept']", "button[class*='accept']", "button[id*='consent']",
        "button[class*='consent']", "button[id*='agree']", "button[class*='agree']",
        "//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')]"
    ]
    
    for selector in popup_selectors:
        try:
            if selector.startswith("//"):
                elements = WebDriverWait(driver, 3).until(EC.presence_of_all_elements_located((By.XPATH, selector)))
            else:
                elements = WebDriverWait(driver, 3).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, selector)))
            
            for element in elements:
                if element.is_displayed() and element.is_enabled():
                    driver.execute_script("arguments[0].click();", element)
                    time.sleep(3)
                    return True
        except TimeoutException:
            continue
    return False

def init_db():
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            weight TEXT,
            height TEXT,
            sport TEXT DEFAULT 'Rugby',
            country TEXT,
            position TEXT,
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

def cm_to_feet_inches(cm_str):
    """Convert centimeters to feet'inches" format"""
    if not cm_str:
        return None
    try:
        cm_match = re.search(r'(\d+)', cm_str)
        if cm_match:
            cm = int(cm_match.group(1))
            inches = cm / 2.54
            feet = int(inches // 12)
            remaining_inches = int(inches % 12)
            return f"{feet}'{remaining_inches}\""
    except:
        pass
    return None

def extract_weight_kg(weight_str):
    """Extract weight number without 'kg'"""
    if not weight_str:
        return None
    try:
        weight_match = re.search(r'(\d+)', weight_str)
        if weight_match:
            return weight_match.group(1)
    except:
        pass
    return None

def insert_player(player):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        details = player.get('detailed_bio', {})
        
        age = None
        if details.get('age'):
            try:
                age = int(re.search(r'\d+', str(details['age'])).group())
            except:
                pass
        
        # Convert weight and height
        weight = extract_weight_kg(details.get('weight'))
        height = cm_to_feet_inches(details.get('height'))
        
        cur.execute("""
            INSERT INTO players (name, age, weight, height, position, country, team, source, player_url)
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
            weight,
            height,
            details.get('position') or player.get('position'),
            details.get('country') or 'Unknown',
            details.get('team'),
            'rugbypass.com',
            player.get('player_link')
        ))
        conn.commit()
        print(f"✓ Saved: {player['name']} (Age: {age}, Country: {details.get('country')}, Weight: {weight}, Height: {height})")
    except Exception as e:
        print(f"❌ DB Error for {player['name']}: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def construct_player_url(player_name):
    try:
        url_name = re.sub(r'[^\w\s-]', '', player_name.lower())
        url_name = re.sub(r'\s+', '-', url_name.strip())
        url_name = re.sub(r'-+', '-', url_name)
        return f"https://www.rugbypass.com/players/{url_name}/"
    except Exception as e:
        print(f"Error constructing URL for {player_name}: {e}")
        return None

def scrape_players_from_page(driver):
    try:
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "list-players")))
        player_container = driver.find_element(By.ID, "list-players")
        
        # Try direct player links first
        player_links = player_container.find_elements(By.CSS_SELECTOR, "a[href*='/players/']")
        
        if player_links:
            players_found = []
            for link in player_links:
                try:
                    href = link.get_attribute('href')
                    name = re.sub(r"^[\"']+|[\"']+$", "", link.get_attribute("innerText").strip().split('\n')[0])
                    if href and name and '/players/' in href:
                        player_data = {
                            'name': name,
                            'player_link': href,
                            'team': '',
                            'position': ''
                        }
                        players_found.append(player_data)
                except:
                    continue
            if players_found:
                return players_found
        
        # Fallback: parse text content
        return parse_player_text_with_links(player_container)
        
    except Exception as e:
        print(f"Error in scrape_players_from_page: {e}")
        return []

def parse_player_text_with_links(container):
    try:
        text_content = container.text
        if not text_content or len(text_content) < 10:
            return []
        
        lines = [line.strip() for line in text_content.split('\n') if line.strip()]
        players = []
        i = 0
        
        while i < len(lines) - 2:
            potential_name = lines[i]
            potential_team = lines[i + 1] if i + 1 < len(lines) else ""
            potential_position = lines[i + 2] if i + 2 < len(lines) else ""
            
            skip_patterns = ["name", "team", "position", "current squad", "filter", "tournaments", "teams", "positions"]
            if any(pattern in potential_name.lower() for pattern in skip_patterns):
                i += 1
                continue
            
            if (len(potential_name) > 1 and len(potential_name) < 50 and
                not potential_name.isdigit() and
                ("'" in potential_name or len(potential_name.split()) >= 2)):
                
                player_url = construct_player_url(potential_name)
                players.append({
                    "name": potential_name,
                    "team": potential_team,
                    "position": potential_position,
                    "player_link": player_url
                })
                i += 3
            else:
                i += 1
        
        return players
        
    except Exception as e:
        print(f"Error in parse_player_text_with_links: {e}")
        return []

def scrape_player_bio(driver, url):
    try:
        original_window = driver.current_window_handle
        driver.execute_script("window.open(arguments[0], '_blank');", url)
        time.sleep(2)
        
        if len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])
        
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(2)
        
        if "404" in driver.title.lower() or "not found" in driver.title.lower():
            return {}
        
        extracted_details = {}
        
        # Strategy 1: Extract country from page source and visible text
        try:
            # Get the page source and visible text
            page_source = driver.page_source.lower()
            body_text = driver.find_element(By.TAG_NAME, "body").text
            
            # List of rugby nations to search for
            country_names = ['england', 'wales', 'scotland', 'ireland', 'france', 'italy', 'argentina', 
                           'australia', 'new zealand', 'south africa', 'japan', 'tonga', 'samoa', 
                           'fiji', 'georgia', 'romania', 'uruguay', 'canada', 'usa', 'portugal', 
                           'chile', 'brazil', 'hong kong', 'spain', 'russia', 'germany', 'belgium',
                           'netherlands', 'poland', 'czech republic', 'ukraine', 'kenya', 'namibia',
                           'zimbabwe', 'madagascar', 'tunisia', 'morocco', 'senegal', 'ivory coast']
            
            # First try to find country from visible text
            for line in body_text.split('\n'):
                line = line.strip()
                if line.lower() in country_names:
                    extracted_details['country'] = line.title()
                    print(f"Found country from visible text: {line.title()}")
                    break
            
            # If not found in visible text, try to find in page source
            if 'country' not in extracted_details:
                for country in country_names:
                    # Look for country in various contexts in the HTML
                    patterns = [
                        f'alt="{country}"',
                        f'title="{country}"',
                        f'>{country}<',
                        f'data-country="{country}"',
                        f'country">{country}',
                        f'team-logo.*{country}',
                        f'{country}.*team-logo'
                    ]
                    
                    for pattern in patterns:
                        if re.search(pattern, page_source, re.IGNORECASE):
                            extracted_details['country'] = country.title()
                            print(f"Found country from page source: {country.title()}")
                            break
                    
                    if 'country' in extracted_details:
                        break
            
            # Try to find country from any element that might contain it
            if 'country' not in extracted_details:
                try:
                    # Try various selectors that might contain country info
                    selectors = [
                        "img[alt*='flag']",
                        "img[src*='flag']",
                        "div.team-logo",
                        "div.country",
                        "span.country",
                        "div[class*='team']",
                        "div[class*='country']",
                        "img[alt]",
                        "img[title]"
                    ]
                    
                    for selector in selectors:
                        try:
                            elements = driver.find_elements(By.CSS_SELECTOR, selector)
                            for element in elements:
                                # Check alt, title, and text content
                                for attr in ['alt', 'title', 'text']:
                                    try:
                                        if attr == 'text':
                                            text = element.text.strip()
                                        else:
                                            text = element.get_attribute(attr)
                                        
                                        if text and text.lower() in country_names:
                                            extracted_details['country'] = text.title()
                                            print(f"Found country from element {selector} {attr}: {text.title()}")
                                            break
                                    except:
                                        continue
                                
                                if 'country' in extracted_details:
                                    break
                            
                            if 'country' in extracted_details:
                                break
                        except:
                            continue
                except:
                    pass
                        
        except Exception as e:
            print(f"Could not extract country: {e}")
        
        # Strategy 2: Extract other details from player-details
        try:
            player_details = driver.find_element(By.CSS_SELECTOR, "div.player-details")
            detail_sections = player_details.find_elements(By.CSS_SELECTOR, "div.detail")
            
            for detail_section in detail_sections:
                try:
                    h3_element = detail_section.find_element(By.TAG_NAME, "h3")
                    heading = h3_element.text.strip().lower()
                    content_element = detail_section.find_element(By.TAG_NAME, "p")
                    content = content_element.text.strip()
                    
                    if 'age' in heading:
                        match = re.search(r'(\d+)', content)
                        if match:
                            extracted_details['age'] = match.group(1)
                    elif 'height' in heading:
                        extracted_details['height'] = content
                    elif 'weight' in heading:
                        extracted_details['weight'] = content
                    elif 'position' in heading:
                        extracted_details['position'] = content
                    # elif 'team' in heading or 'club' in heading:
                    #     extracted_details['team'] = content
                except:
                    continue
        except Exception as e:
            print(f"Could not extract player details: {e}")
        
        # Strategy 3: Fallback text extraction
        if not any(key in extracted_details for key in ['age', 'height', 'weight', 'position']):
            try:
                body_text = driver.find_element(By.TAG_NAME, "body").text.lower()
                details_patterns = {
                    'age': r'age[:\s]+(\d+)',
                    'height': r'height[:\s]+([^\n]+)',
                    'weight': r'weight[:\s]+([^\n]+)',
                    'position': r'position[:\s]+([^\n]+)'
                    # 'team': r'team[:\s]+([^\n]+)'
                }
                
                for key, pattern in details_patterns.items():
                    if key not in extracted_details:
                        match = re.search(pattern, body_text)
                        if match:
                            extracted_details[key] = match.group(1).strip()
            except:
                pass
        
        return extracted_details
        
    except Exception as e:
        print(f"Error scraping bio: {e}")
        return {}
    finally:
        try:
            if len(driver.window_handles) > 1:
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
        except:
            pass

def scrape_all_pages():
    base_url = "https://www.rugbypass.com/players/"
    driver = setup_driver()
    all_players = []
    max_pages = 5

    try:
        driver.get(base_url)
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        accept_popups(driver)
        time.sleep(5)
        
        for page in range(1, max_pages + 1):
            print(f"\n🔄 Scraping page {page}...")
            
            players = scrape_players_from_page(driver)
            if not players:
                print(f"⚠️ No players found on page {page}, stopping.")
                break
            
            print(f"Found {len(players)} players on page {page}")
            
            for i, player in enumerate(players):
                try:
                    player_name = player.get('name', 'Unknown')
                    player_link = player.get('player_link')
                    
                    if player_link:
                        print(f"Scraping bio {i+1}/{len(players)}: {player_name}")
                        detailed_bio = scrape_player_bio(driver, player_link)
                        player['detailed_bio'] = detailed_bio
                        insert_player(player)
                        all_players.append(player)
                        time.sleep(2)
                    else:
                        print(f"Skipping {player_name} - no link")
                except Exception as e:
                    print(f"Error processing player {player.get('name', 'Unknown')}: {e}")
                    continue
            
            # Navigate to next page
            if page < max_pages:
                try:
                    next_page_url = f"{base_url}?p={page + 1}"
                    driver.get(next_page_url)
                    time.sleep(3)
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "list-players")))
                except TimeoutException:
                    print("No more pages found")
                    break

    except Exception as e:
        print(f"Error in scrape_all_pages: {e}")
    finally:
        driver.quit()

    return all_players

def main():
    print("=== Starting Enhanced RugbyPass Player Scraper ===")
    init_db()
    print("✓ Database initialized")
    
    players = scrape_all_pages()
    print(f"\n=== Scraping Complete ===")
    print(f"Total players processed: {len(players)}")
    
    players_with_country = [p for p in players if p.get('detailed_bio', {}).get('country')]
    players_with_age = [p for p in players if p.get('detailed_bio', {}).get('age')]
    players_with_weight = [p for p in players if p.get('detailed_bio', {}).get('weight')]
    players_with_height = [p for p in players if p.get('detailed_bio', {}).get('height')]
    
    print(f"Players with country: {len(players_with_country)}")
    print(f"Players with age: {len(players_with_age)}")
    print(f"Players with weight: {len(players_with_weight)}")
    print(f"Players with height: {len(players_with_height)}")

if __name__ == '__main__':
    main()