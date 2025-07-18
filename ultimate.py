import os
import json
import time
import urllib.parse
import re
from datetime import datetime
from typing import List, Dict
from common_utils import insert_player, log_error # Assuming common_utils has insert_player and log_error
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager


class UltimateRugbyPlayerScraper:
    def __init__(self, base_url="https://www.ultimaterugby.com/team", output_dir="ultimate_rugby_data", delay=2):
        """
        Initialize the Ultimate Rugby Player scraper
        
        Args:
            base_url (str): The base URL to scrape (default: https://www.ultimaterugby.com/team)
            output_dir (str): Directory to save scraped data
            delay (int): Delay between requests in seconds
        """
        self.base_url = base_url
        self.output_dir = output_dir
        self.delay = delay
        self.scraped_players = set()
        self.all_player_data = []

        # Create output directories
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Setup Selenium WebDriver with improved options for better stability
        chrome_options = Options()
        
        # Comment out headless mode for debugging - uncomment after testing
        # chrome_options.add_argument("--headless")
        
        # Basic stability options
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--start-maximized")
        
        # SSL and certificate handling
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--ignore-ssl-errors")
        chrome_options.add_argument("--ignore-certificate-errors-spki-list")
        chrome_options.add_argument("--ignore-certificate-errors-revocation-list")
        chrome_options.add_argument("--allow-running-insecure-content")
        chrome_options.add_argument("--disable-web-security")
        
        # Popup and notification blocking
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-extensions")
        
        # Performance and stability improvements
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-features=TranslateUI,VizDisplayCompositor")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        
        # Memory and resource optimization
        chrome_options.add_argument("--memory-pressure-off")
        chrome_options.add_argument("--max_old_space_size=4096")
        chrome_options.add_argument("--disable-plugins")
        
        # Set realistic user agent
        chrome_options.add_argument(
            '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Disable image loading for faster performance (optional)
        chrome_options.add_experimental_option("prefs", {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_settings.popups": 0,
        })
        
        # Additional experimental options
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # Initialize WebDriver with better error handling
        try:
            self.driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()),
                options=chrome_options
            )
            # Increase timeouts for better stability
            self.driver.set_page_load_timeout(60)  # Increased from 30
            self.driver.implicitly_wait(15)  # Increased from 10
            
            # Execute script to avoid detection
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
        except Exception as e:
            print(f"Error initializing WebDriver: {e}")
            raise

    def wait_and_click(self, element, timeout=10):
        """Wait for element to be clickable and click it"""
        try:
            WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable(element)
            )
            self.driver.execute_script("arguments[0].click();", element)
            time.sleep(self.delay)
            return True
        except Exception as e:
            print(f"Error clicking element: {e}")
            return False

    def get_teams_from_main_page(self):
        """Extract all team links from the main page with retry mechanism"""
        print("Loading main page...")
        
        # Retry mechanism for loading the page
        max_retries = 3
        for attempt in range(max_retries):
            try:
                print(f"Attempt {attempt + 1} to load main page...")
                self.driver.get(self.base_url)
                
                # Wait longer for initial page load
                WebDriverWait(self.driver, 30).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                print(f"Page loaded successfully. Title: {self.driver.title}")
                time.sleep(5)
                break
                
            except Exception as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    print("All attempts failed. Exiting...")
                    return []
                print("Retrying in 10 seconds...")
                time.sleep(10)

        teams = []
        
        try:
            print("Main page loaded, searching for teams...")
            
            # Debug: Save page source for inspection
            try:
                with open(os.path.join(self.output_dir, "main_page_debug.html"), 'w', encoding='utf-8') as f:
                    f.write(self.driver.page_source)
                print("Main page source saved for debugging")
            except Exception as e:
                print(f"Could not save debug page: {e}")
            
            # Strategy 1: Look for team containers in tables
            try:
                tables = self.driver.find_elements(By.TAG_NAME, "table")
                print(f"Found {len(tables)} tables on main page")
                
                for table in tables:
                    rows = table.find_elements(By.TAG_NAME, "tr")
                    for row in rows:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        for cell in cells:
                            # Look for div.row elements
                            row_divs = cell.find_elements(By.CSS_SELECTOR, "div.row")
                            for row_div in row_divs:
                                # Look for team links: div > a
                                team_links = row_div.find_elements(By.CSS_SELECTOR, "div a")
                                for link in team_links:
                                    try:
                                        team_url = link.get_attribute("href")
                                        team_name = link.text.strip()
                                        
                                        if (team_url and team_name and 
                                            len(team_name) > 1 and
                                            '/team/' in team_url and
                                            not any(t["url"] == team_url for t in teams)):
                                            
                                            teams.append({
                                                "name": team_name,
                                                "url": team_url
                                            })
                                            print(f"Found team: {team_name}")
                                            
                                    except Exception as e:
                                        continue
            except Exception as e:
                print(f"Error with table strategy: {e}")
            
            # Strategy 2: Look for all div.row elements anywhere on page
            if not teams:
                try:
                    print("Trying direct div.row search...")
                    row_divs = self.driver.find_elements(By.CSS_SELECTOR, "div.row")
                    print(f"Found {len(row_divs)} div.row elements")
                    
                    for row_div in row_divs:
                        team_links = row_div.find_elements(By.CSS_SELECTOR, "div a, a")
                        for link in team_links:
                            try:
                                team_url = link.get_attribute("href")
                                team_name = link.text.strip()
                                
                                if (team_url and team_name and 
                                    len(team_name) > 1 and
                                    ('/team/' in team_url or 'ultimaterugby.com' in team_url) and
                                    not any(t["url"] == team_url for t in teams)):
                                    
                                    teams.append({
                                        "name": team_name,
                                        "url": team_url
                                    })
                                    print(f"Found team: {team_name}")
                                    
                            except Exception as e:
                                continue
                except Exception as e:
                    print(f"Error with div.row strategy: {e}")
            
            # Strategy 3: Look for any team-related links
            if not teams:
                print("Trying general link search...")
                all_links = self.driver.find_elements(By.TAG_NAME, "a")
                print(f"Found {len(all_links)} total links")
                
                for link in all_links:
                    try:
                        href = link.get_attribute("href")
                        text = link.text.strip()
                        
                        if (href and text and 
                            ('/team/' in href or '/teams/' in href) and 
                            len(text) > 2 and
                            not any(t["url"] == href for t in teams)):
                            
                            teams.append({
                                "name": text,
                                "url": href
                            })
                            print(f"Found team: {text}")
                            
                    except Exception as e:
                        continue
            
            print(f"Total teams found: {len(teams)}")
            
            # If still no teams found, print some debug info
            if not teams:
                print("DEBUG: No teams found. Checking page content...")
                print(f"Current URL: {self.driver.current_url}")
                print(f"Page title: {self.driver.title}")
                
                # Print first 1000 characters of page source
                page_source = self.driver.page_source
                print(f"Page source preview: {page_source[:1000]}...")
            
            return teams
            
        except Exception as e:
            print(f"Error getting teams from main page: {e}")
            return []

    def navigate_to_squad_page(self, team_url):
        """Navigate to team page and click on squad navigation"""
        print(f"Loading team page: {team_url}")
        self.driver.get(team_url)
        time.sleep(5)

        try:
            # Wait for the page to load
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            print("Team page loaded, looking for squad navigation...")
            
            # Find the squad navigation: nav.navbar-secondary > ul.page-nav > li (2nd element)
            try:
                nav_element = self.driver.find_element(By.CSS_SELECTOR, "nav.navbar-secondary")
                ul_element = nav_element.find_element(By.CSS_SELECTOR, "ul.page-nav")
                li_elements = ul_element.find_elements(By.TAG_NAME, "li")
                
                print(f"Found {len(li_elements)} navigation items")
                
                if len(li_elements) >= 2:
                    # Click on the 2nd li element (index 1) which should be squad
                    squad_nav = li_elements[1]
                    squad_link = squad_nav.find_element(By.TAG_NAME, "a")
                    
                    print(f"Clicking on squad navigation: {squad_link.text}")
                    self.wait_and_click(squad_link)
                    
                    # Wait for squad page to load
                    time.sleep(5)
                    return True
                else:
                    print("Not enough navigation items found")
                    return False
                    
            except Exception as e:
                print(f"Error finding squad navigation: {e}")
                return False
                
        except Exception as e:
            print(f"Error navigating to squad page: {e}")
            return False

    def get_players_from_squad_page(self):
        """Extract all player information from squad page"""
        print("Extracting players from squad page...")
        
        players = []
        
        try:
            # Wait for squad page to load
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Find all flipper elements: div.flipper
            flipper_elements = self.driver.find_elements(By.CSS_SELECTOR, "div.flipper")
            print(f"Found {len(flipper_elements)} flipper elements")
            
            for i, flipper in enumerate(flipper_elements):
                try:
                    player_data = {}
                    
                    # Get player name from front: div.front > h4
                    try:
                        front_div = flipper.find_element(By.CSS_SELECTOR, "div.front")
                        player_name_elem = front_div.find_element(By.TAG_NAME, "h4")
                        player_data["name"] = player_name_elem.text.strip()
                        print(f"Found player: {player_data['name']}")
                    except Exception as e:
                        print(f"Could not find player name in flipper {i}: {e}")
                        continue
                    
                    # Get bio link from back: div.back > a
                    try:
                        back_div = flipper.find_element(By.CSS_SELECTOR, "div.back")
                        bio_link_elem = back_div.find_element(By.TAG_NAME, "a")
                        player_data["bio_url"] = bio_link_elem.get_attribute("href")
                        print(f"Found bio URL for {player_data['name']}: {player_data['bio_url']}")
                    except Exception as e:
                        print(f"Could not find bio link for {player_data['name']}: {e}")
                        player_data["bio_url"] = None
                    
                    if player_data["name"]:
                        players.append(player_data)
                        
                except Exception as e:
                    print(f"Error processing flipper element {i}: {e}")
                    continue
            
            print(f"Total players found: {len(players)}")
            return players
            
        except Exception as e:
            print(f"Error getting players from squad page: {e}")
            return []

    def scrape_player_bio(self, player_data):
        """Scrape individual player bio details"""
        if not player_data.get("bio_url"):
            print(f"No bio URL for player: {player_data['name']}")
            return player_data
            
        bio_url = player_data["bio_url"]
        
        if bio_url in self.scraped_players:
            print(f"Player bio already scraped: {bio_url}")
            return player_data
            
        print(f"Scraping player bio: {player_data['name']} - {bio_url}")
        self.driver.get(bio_url)
        time.sleep(3)

        try:
            # Wait for the profile page to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Extract profile details: div.profile-detail
            try:
                profile_detail = self.driver.find_element(By.CSS_SELECTOR, "div.profile-detail")
                
                # Extract name from h1
                try:
                    name_elem = profile_detail.find_element(By.TAG_NAME, "h1")
                    player_data["full_name"] = name_elem.text.strip()
                    print(f"Full name: {player_data['full_name']}")
                except Exception as e:
                    print(f"Could not find full name: {e}")
                
                # Extract details from div.detail > span
                try:
                    detail_div = profile_detail.find_element(By.CSS_SELECTOR, "div.detail")
                    detail_spans = detail_div.find_elements(By.TAG_NAME, "span")
                    
                    player_data["details"] = {}
                    for span in detail_spans:
                        span_text = span.text.strip()
                        if span_text and ':' in span_text:
                            key, value = span_text.split(':', 1)
                            player_data["details"][key.strip()] = value.strip()
                        elif span_text:
                            player_data["details"][f"info_{len(player_data['details'])}"] = span_text
                    
                    print(f"Details extracted: {len(player_data['details'])} fields")
                    
                except Exception as e:
                    print(f"Could not find detail spans: {e}")
                
                # Extract other details from p elements
                try:
                    p_elements = profile_detail.find_elements(By.TAG_NAME, "p")
                    player_data["additional_info"] = []
                    
                    for p in p_elements:
                        p_text = p.text.strip()
                        if p_text:
                            player_data["additional_info"].append(p_text)
                    
                    print(f"Additional info: {len(player_data['additional_info'])} paragraphs")
                    
                except Exception as e:
                    print(f"Could not find p elements: {e}")
                    
            except Exception as e:
                print(f"Could not find profile-detail div: {e}")
            
            # Add timestamp
            player_data["scraped_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            self.scraped_players.add(bio_url)
            
            return player_data
            
        except Exception as e:
            print(f"Error scraping player bio: {e}")
            return player_data

    def save_data(self, data, filename):
        """Save data to JSON file"""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"Data saved to: {filepath}")

    def scrape_all_ultimate_rugby_data(self):
        """Main method to scrape all Ultimate Rugby player data"""
        try:
            print("Starting Ultimate Rugby data scraping...")
            
            # Step 1: Get all teams
            teams = self.get_teams_from_main_page()
            if not teams:
                print("No teams found. Exiting.")
                return
            
            self.save_data(teams, "teams_list.json")
            
            # Step 2: Process each team
            for i, team in enumerate(teams, 1):
                print(f"\n--- Processing team {i}/{len(teams)}: {team['name']} ---")
                
                try:
                    # Navigate to squad page
                    if not self.navigate_to_squad_page(team['url']):
                        print(f"Could not navigate to squad page for team: {team['name']}")
                        continue
                    
                    # Get players from squad page
                    players = self.get_players_from_squad_page()
                    
                    if not players:
                        print(f"No players found for team: {team['name']}")
                        continue
                    
                    # Step 3: Process each player's bio
                    team_player_data = []
                    for j, player in enumerate(players, 1):
                        print(f"\n  Processing player {j}/{len(players)}: {player['name']}")
                        
                        try:
                            # Add team information
                            player['team'] = team['name']
                            player['team_url'] = team['url']
                            
                            # Scrape player bio
                            player_with_bio = self.scrape_player_bio(player)
                            
                            team_player_data.append(player_with_bio)
                            self.all_player_data.append(player_with_bio)
                            
                            # --- Start: Extract and map data for DB insertion ---
                            name = player_with_bio.get("full_name") or player_with_bio.get("name")
                            team_name = player_with_bio.get("team")
                            player_url = player_with_bio.get("bio_url")

                            # Defaults
                            age = None
                            height = None
                            weight = None
                            position = None
                            country = None # Ultimate Rugby does not consistently provide country on player bio page

                            details = player_with_bio.get("details", {})

                            # Parse Age from 'info_0' (Date of Birth)
                            dob_str = details.get('info_0')
                            if dob_str:
                                try:
                                    # Ensure the date format matches the scraped data, e.g., "23rd Apr 2008" or "6th Apr 2008"
                                    # The example JSON uses "DDth Mon YYYY" or "DD Mon YYYY"
                                    # We need to handle the "th", "st", "nd", "rd" suffixes if they appear.
                                    # Simpler approach: remove "st", "nd", "rd", "th" before parsing.
                                    dob_str_cleaned = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', dob_str)
                                    dob = datetime.strptime(dob_str_cleaned, "%d %b %Y")
                                    today = datetime.today()
                                    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
                                except ValueError:
                                    print(f"Could not parse DOB for {name}: {dob_str}")
                                    pass # Age remains None

                            # Parse Height and Weight from 'info_1'
                            height_weight_str = details.get('info_1')
                            if height_weight_str:
                                # Regex to find patterns like "1.88m/95kg" or "1.88m/95" or "0.00m/kg"
                                # Capture height (group 1) and optional weight (group 4)
                                match = re.match(r'(\d+(\.\d+)?)m/(?:(\d+)?(?:kg)?)?', height_weight_str.strip())
                                if match:
                                    try:
                                        meters = float(match.group(1))
                                        total_inches = round(meters * 39.3701)
                                        feet = total_inches // 12
                                        inches = total_inches % 12
                                        
                                        # Set height to None if it's "0'0""
                                        if feet == 0 and inches == 0:
                                            height = None
                                        else:
                                            height = f"{feet}'{inches}\""

                                        weight_val = match.group(3) # Group 3 captures the number before optional 'kg'
                                        if weight_val: # Only set weight if a numerical value was captured
                                            try:
                                                # Attempt conversion to int to ensure it's a valid number
                                                weight_int = int(weight_val)
                                                if weight_int > 0: # Only consider positive weights
                                                    weight = str(weight_int) # Store as string as per schema
                                                else:
                                                    weight = None # Handle 0 or negative weights as None
                                            except ValueError:
                                                weight = None # Not a valid number
                                        else:
                                            weight = None # No numerical weight captured
                                    except ValueError:
                                        print(f"Could not parse height/weight for {name}: {height_weight_str}") # Debug print
                                        pass # Height/Weight remain None

                            # Parse Position from 'info_2'
                            position_str = details.get('info_2')
                            if position_str:
                                position = position_str.strip()


                            db_data = {
                                "name": name,
                                "age": age,
                                "weight": weight,
                                "height": height,
                                "sport": "Rugby", # Explicitly set sport for Ultimate Rugby
                                "country": country, # Will remain None as it's not scraped
                                "position": position,
                                "team": team_name,
                                "source": "ultimaterugby.com",
                                "player_url": player_url,
                            }
                            # --- End: Extract and map data for DB insertion ---

                            # Save directly to DB
                            # from scraper_api import scraping_status  # So you can update counts
                            # Ensure insert_player is correctly imported or defined within this context
                            # For local testing, ensure common_utils has insert_player
                            if insert_player(db_data):
                                # If you have a global scraping_status accessible here:
                                # scraping_status['processed'] += 1
                                pass # Placeholder if scraping_status is not directly available

                        except Exception as e:
                            print(f"Error processing player {player['name']}: {e}")
                            log_error(f"Error processing player {player['name']}: {e}", player.get('bio_url'))
                            continue
                    
                    # Save team data
                    if team_player_data:
                        team_filename = f"team_{re.sub(r'[^a-zA-Z0-9_]', '_', team['name'])}.json"
                        self.save_data(team_player_data, team_filename)
                    
                except Exception as e:
                    print(f"Error processing team {team['name']}: {e}")
                    log_error(f"Error processing team {team['name']}: {e}", team.get('url'))
                    continue
            
            # Final save
            self.save_data(self.all_player_data, "all_players_final.json")
            print(f"\n=== Scraping completed! Total players scraped: {len(self.all_player_data)} ===")
            
        except Exception as e:
            print(f"Critical error in main scraping process: {e}")
            log_error(f"Critical error in Ultimate Rugby main scraping process: {e}")
        finally:
            self.close()

    def test_connection(self):
        """Test if we can connect to the website"""
        print("Testing connection to Ultimate Rugby website...")
        try:
            self.driver.get(self.base_url)
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            print(f"✓ Successfully connected to: {self.driver.current_url}")
            print(f"✓ Page title: {self.driver.title}")
            return True
        except Exception as e:
            print(f"✗ Connection test failed: {e}")
            return False

    def close(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()


def main():
    """Main function to run the scraper"""
    scraper = UltimateRugbyPlayerScraper(
        base_url="https://www.ultimaterugby.com/team",
        output_dir="ultimate_rugby_data",
        delay=3  # Increased delay to be more respectful
    )
    
    # Test connection first
    if scraper.test_connection():
        print("Connection test passed. Starting full scraping...")
        scraper.scrape_all_ultimate_rugby_data()
    else:
        print("Connection test failed. Please check your internet connection and try again.")
        scraper.close()

def scrape_ultimate_rugby_players(shared_driver=None) -> List[Dict]:
    """
    Public interface to run the scraper and return player dicts in standard format.
    If shared_driver is passed, it will use that Selenium instance.
    """
    scraper = UltimateRugbyPlayerScraper()
    if shared_driver:
        scraper.driver = shared_driver
    if not scraper.test_connection():
        print("❌ Cannot connect to Ultimate Rugby")
        return []

    # Call the main scraping logic which now includes DB insertion
    scraper.scrape_all_ultimate_rugby_data()

    # The loop below becomes redundant if scrape_all_ultimate_rugby_data already inserts to DB.
    # If the intent of `scrape_ultimate_rugby_players` is only to return the list,
    # then the `insert_player` calls inside `scrape_all_ultimate_rugby_data`
    # would still happen. If the goal is to *only* insert players here,
    # then remove the `insert_player` calls from `scrape_all_ultimate_rugby_data`
    # and retain this loop.
    # For now, let's assume `scrape_all_ultimate_rugby_data` is the primary inserter.
    # You might want to refine if `scrape_ultimate_rugby_players` is used elsewhere
    # to simply return data without DB interaction.

    # This loop is currently redundant if scraping_status and insert_player are managed
    # within scrape_all_ultimate_rugby_data.
    # If `scrape_ultimate_rugby_players` is meant to *only* format and return,
    # then the DB insertion should NOT be in scrape_all_ultimate_rugby_data.
    # For now, I'm assuming `scrape_all_ultimate_rugby_data` is the canonical
    # place for DB insertion for Ultimate Rugby players.
    final_player_data_for_return = []
    for player in scraper.all_player_data:
        name = player.get("full_name") or player.get("name")
        team = player.get("team")
        player_url = player.get("bio_url")

        age = None
        height = None
        weight = None
        position = None
        country = None # Country will remain None unless specifically scraped or inferred

        details = player.get("details", {})

        # Re-parse or just use the already parsed values from player_with_bio if they were stored there.
        # It's better to process once and store thoroughly.
        # Given the provided structure, re-parsing from `details` for `return` consistency:
        
        # Parse Age
        dob_str = details.get('info_0')
        if dob_str:
            try:
                dob_str_cleaned = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', dob_str)
                dob = datetime.strptime(dob_str_cleaned, "%d %b %Y")
                today = datetime.today()
                age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
            except ValueError:
                pass

        # Parse Height and Weight
        height_weight_str = details.get('info_1')
        if height_weight_str:
            match = re.match(r'(\d+(\.\d+)?)m/((\d+)?kg)?', height_weight_str.replace("kg", "").strip())
            if match:
                try:
                    meters = float(match.group(1))
                    total_inches = round(meters * 39.3701)
                    feet = total_inches // 12
                    inches = total_inches % 12
                    
                    if feet == 0 and inches == 0:
                        height = None
                    else:
                        height = f"{feet}'{inches}\""

                    weight_val = match.group(4) if match.group(4) else None
                    if weight_val == '':
                        weight = None
                    elif weight_val:
                        weight = weight_val
                except ValueError:
                    pass
        
        # Parse Position
        position = details.get("info_2")

        data = {
            "name": name,
            "age": age,
            "weight": weight,
            "height": height,
            "sport": "Rugby",
            "country": country,
            "position": position,
            "team": team,
            "source": "ultimaterugby.com",
            "player_url": player_url,
        }
        final_player_data_for_return.append(data)
        
        # The `insert_player` call here is redundant if `scrape_all_ultimate_rugby_data`
        # is already doing it and this function is primarily called by `scraper_api.py`
        # where `scrape_all_ultimate_rugby_data` is used to trigger insertions.
        # Keeping it commented to avoid duplicate insertions if scrape_all_ultimate_rugby_data inserts.
        # from scraper_api import scraping_status # To update counts
        # if insert_player(data):
        #    scraping_status['processed'] += 1

    scraper.close()
    return final_player_data_for_return # This ensures the function returns formatted data

if __name__ == '__main__':
        main()