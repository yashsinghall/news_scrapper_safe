# ==============================================================================
# --- GLOBAL USER SETTINGS ---
#
# How many articles to get from each source (e.g., 25)
# This is a 'max' value. If a feed only has 20 articles, it will get 20.
MAX_ARTICLES_PER_SOURCE = 1
#
# --- NEW: PROXY CONFIGURATION ---
# Set 'use_proxies' to True to route all requests (Requests & Selenium)
# through the 'proxy_url'.
#
# This is the "at any cost" solution for IP bans.
#
# 'proxy_url' should be in the format: http://username:password@proxy.example.com:8080
# This single URL can be a static proxy or a gateway for a rotating proxy service.
#
PROXY_SETTINGS = {
    "use_proxies": False,
    "proxy_url": None  # e.g., "http://user:pass@proxy.service.com:8080"
}
# ==============================================================================

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import trafilatura # <-- We will use this for text
# import schedule # <-- REMOVED: No longer needed for CI
import time # <-- Still needed for politeness delays
import logging
import sqlite3
from datetime import datetime
import random # <-- For User-Agent rotation
import os # <-- REMOVED: No longer needed for CI path check

# --- UPDATED: Import Selenium ---
try:
    from selenium import webdriver
    # from selenium.webdriver.chrome.service import Service as ChromeService # <-- REMOVED
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.common.exceptions import WebDriverException
    # --- REMOVED: webdriver_manager is no longer needed ---
    SELENIUM_AVAILABLE = True
except ImportError:
    logging.critical("Selenium not installed. Run 'pip install selenium'. Selenium-dependent sources will fail.")
    SELENIUM_AVAILABLE = False
# ---------------------------

# --- Robust Session and Header Management ---

def create_robust_session():
    """
    Creates a requests.Session with automatic retries on server errors (5xx).
    """
    logging.info("Creating new robust session with 3 retries on 5xx errors.")
    session = requests.Session()
    # Define a retry strategy: 3 retries, 1s/2s/4s backoff, retry on 5xx errors
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["HEAD", "GET"] # Only retry on safe methods
    )
    # Mount the strategy to all http and https requests
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

# Base headers to look like a modern browser
BASE_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1', # Do Not Track
    'Upgrade-Insecure-Requests': '1',
}

# A list of browser user-agents to rotate through
BROWSER_USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
]

# Specific user-agents for different "personas"
GOOGLEBOT_USER_AGENT = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
FEEDFETCHER_USER_AGENT = 'Mozilla/5.0 (compatible; FeedFetcher-Google; +http://www.google.com/feedfetcher.html)'

def get_headers(header_type):
    """
    Returns a complete header dictionary for a given "persona".
    """
    headers = BASE_HEADERS.copy()
    
    # Remove 'requests_' prefix to get the core type
    core_type = header_type.replace('requests_', '')

    if core_type == 'browser':
        headers['User-Agent'] = random.choice(BROWSER_USER_AGENTS)
    elif core_type == 'googlebot':
        headers['User-Agent'] = GOOGLEBOT_USER_AGENT
    elif core_type == 'feedfetcher':
        # Feedfetcher is simpler, so we'll remove some browser-specific headers
        headers = {'User-Agent': FEEDFETCHER_USER_AGENT}
    return headers

# --- UPDATED: Selenium WebDriver Setup ---
def create_selenium_driver():
    """
    Initializes and returns a headless Selenium Chrome WebDriver.
    
    --- REVERTED FIX ---
    We now rely *only* on Selenium's built-in SeleniumManager.
    The GitHub Action workflow is responsible for installing Chrome
    and its matching driver onto the system PATH.
    """
    if not SELENIUM_AVAILABLE:
        logging.error("Cannot create Selenium driver, library not found.")
        return None
        
    logging.info("Initializing headless Selenium Chrome driver (using SeleniumManager)...")
    
    try:
        options = ChromeOptions()
        options.add_argument("--headless=new") # Use "new" headless mode
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox") # Required for running as root in CI
        options.add_argument("--disable-dev-shm-usage") # Required for CI
        options.add_argument(f"user-agent={random.choice(BROWSER_USER_AGENTS)}") # Use random agent
        
        # --- NEW: Add proxy to Selenium ---
        if PROXY_SETTINGS["use_proxies"] and PROXY_SETTINGS["proxy_url"]:
            logging.info(f"Configuring Selenium driver to use proxy.")
            options.add_argument(f"--proxy-server={PROXY_SETTINGS['proxy_url']}")
        # ----------------------------------

        # --- THIS IS THE FIX ---
        # We no longer check for OS paths or use webdriver-manager.
        # We let SeleniumManager find the driver on the PATH,
        # which the GitHub Action (`browser-actions/setup-chrome`) will provide.
        driver = webdriver.Chrome(options=options)
        # ------------------------------------
        
        # --- TIMEOUT FIX: Increase timeout to 60 seconds ---
        driver.set_page_load_timeout(60) # 60 second timeout
        logging.info("Selenium driver initialized successfully.")
        return driver
    except WebDriverException as e:
        # Catch a more specific error
        logging.critical(f"Failed to initialize Selenium driver. This can happen if Chrome updates. Error: {e}")
        return None
    except Exception as e:
        logging.critical(f"An unexpected error occurred during Selenium initialization: {e}")
        return None

# --- UPDATED: Central Source Configuration ---
# Strategies now include 'requests_browser', 'requests_googlebot', and 'selenium_browser'
#
SOURCE_CONFIG = [
    {
        'name': 'BBC',
        'rss_url': 'http://feeds.bbci.co.uk/news/world/rss.xml',
        'rss_headers_type': 'feedfetcher',
        'article_strategies': ['requests_browser'], # Works fine, keep it fast
        'article_url_contains': None,
        'referer': 'https://www.bbc.com/news',
    },
    {
        'name': 'Times of India',
        'rss_url': 'https://timesofindia.indiatimes.com/rssfeeds/296589292.cms',
        'rss_headers_type': 'feedfetcher',
        'article_strategies': ['selenium_browser'], # Was blocked, use Selenium
        'article_url_contains': '.cms',
        'referer': 'https://timesofindia.indiatimes.com/',
    },
    {
        'name': 'The Guardian',
        'rss_url': 'https://www.theguardian.com/world/rss',
        'rss_headers_type': 'feedfetcher',
        'article_strategies': ['requests_browser'], # Works fine, keep it fast
        'article_url_contains': None,
        'referer': 'https://www.theguardian.com/',
    },
    {
        'name': 'The Hindu',
        'rss_url': 'https://www.thehindu.com/news/national/feeder/default.rss',
        'rss_headers_type': 'browser',
        'article_strategies': ['selenium_browser'], # Was blocked, use Selenium
        'article_url_contains': None,
        'referer': 'https://www.thehindu.com/',
    },
    {
        'name': 'Reuters',
        'rss_url': 'https://feeds.reuters.com/reuters/worldNews',
        'rss_headers_type': 'feedfetcher',
        'article_strategies': ['requests_browser'], # Works fine, keep it fast
        'article_url_contains': None,
        'referer': 'https://www.reuters.com/',
    }
]
# -----------------------------------------------

# Configure logging
logging.basicConfig(filename='news_scraper.log', 
                    filemode='a', 
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize SQLite DB and table
db_path = 'news_articles.db'
logging.info(f"Initializing database connection at: {db_path}")
conn = sqlite3.connect(db_path, check_same_thread=False) # check_same_thread=False is safer with Selenium
cursor = conn.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS news (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT,
    title TEXT,
    url TEXT UNIQUE,
    summary TEXT,
    image_url TEXT,
    scraped_at TIMESTAMP
)
''')
conn.commit()

def save_article(source, title, url, summary, image_url):
    """
    Saves a single article to the SQLite database.
    Prevents duplicates based on the 'url' column.
    Cleans data before saving.
    """
    try:
        # --- MORE ROBUST CLEANING ---
        # Clean up title
        title = " ".join(title.replace('\n', ' ').replace('\r', ' ').split()).strip()
        
        # Clean up summary
        if summary:
            # Split by lines, strip each line, remove empty lines, join with two newlines
            summary_lines = [line.strip() for line in summary.splitlines() if line.strip()]
            summary = "\n\n".join(summary_lines)
        
        if not summary:
            summary = "No content available"
            
        if not image_url:
            image_url = "No image available"
        # ----------------------------

        cursor.execute('''
            INSERT INTO news (source, title, url, summary, image_url, scraped_at) 
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (source, title, url, summary, image_url, datetime.now()))
        conn.commit()
        logging.info(f"Saved article: {title} from {source}")
    except sqlite3.IntegrityError:
        # This is expected if the article URL is already in the DB
        logging.info(f"Duplicate article skipped: {title} from {source}")
    except Exception as e:
        logging.error(f"Error saving article: {e}")

# --- RE-ARCHITECTED: Generic Scraper Function with Strategy Loop ---
def scrape_source(session, selenium_driver, source_config, proxies_dict):
    """
    A generic function that scrapes any source based on its config.
    It will try every strategy in `article_strategies` to get the full text
    before falling back to the RSS summary.
    """
    name = source_config['name']
    rss_url = source_config['rss_url']
    
    articles_saved = []
    logging.info(f"Starting scrape for {name} RSS feed: {rss_url}")
    
    try:
        # 1. Get RSS Feed
        rss_headers = get_headers(source_config['rss_headers_type'])
        # --- UPDATED: Pass proxies to session.get ---
        response = session.get(rss_url, headers=rss_headers, timeout=15, proxies=proxies_dict) 
        response.raise_for_status() # Will raise an error for 4xx/5xx
        
        soup = BeautifulSoup(response.content, 'xml')
        items = soup.find_all('item')
        # --- UPDATED: Use new MAX_ARTICLES_PER_SOURCE variable ---
        logging.info(f"Found {len(items)} articles in {name} RSS feed. Processing up to {MAX_ARTICLES_PER_SOURCE}.")

        # 2. Process each article
        for item in items[:MAX_ARTICLES_PER_SOURCE]: # <-- Use new limit
            try:
                if not item.link:
                    continue
                
                article_url = item.link.text.strip()
                
                # Check for URL filter
                if source_config['article_url_contains']:
                    if source_config['article_url_contains'] not in article_url:
                        logging.warning(f"[{name}] Skipping non-article link: {article_url}")
                        continue
                
                rss_title = item.title.text if item.title else "Title not found"
                
                # --- NEW MULTI-STRATEGY LOGIC ---
                summary = None
                raw_html = None
                final_title = rss_title # Default to RSS title
                image_url = "No image available" # Default image
                
                strategies = source_config['article_strategies']
                
                for i, strategy in enumerate(strategies):
                    logging.info(f"[{name}] Article: {article_url}")
                    logging.info(f"[{name}] Attempt {i+1}/{len(strategies)}: Trying with '{strategy}' strategy...")
                    
                    try:
                        # --- STRATEGY ROUTER ---
                        if strategy.startswith('requests_'):
                            # 3. Download Article Page with REQUESTS
                            header_type = strategy.replace('requests_', '')
                            article_headers = get_headers(header_type)
                            article_headers['Referer'] = source_config['referer']
                            
                            # --- UPDATED: Pass proxies to session.get ---
                            page_response = session.get(article_url, headers=article_headers, timeout=10, proxies=proxies_dict)
                            page_response.raise_for_status()
                            raw_html = page_response.text
                        
                        elif strategy == 'selenium_browser':
                            # 3. Download Article Page with SELENIUM
                            # Selenium driver is already configured with proxy, if set
                            if not selenium_driver:
                                logging.error(f"[{name}] Selenium strategy selected but driver is not available. Skipping.")
                                continue # Try next strategy
                            
                            selenium_driver.get(article_url)
                            # --- TIMEOUT FIX: Increase sleep to 5 seconds ---
                            time.sleep(5) 
                            raw_html = selenium_driver.page_source
                        
                        else:
                            logging.error(f"[{name}] Unknown strategy: {strategy}. Skipping.")
                            continue
                        # --- END STRATEGY ROUTER ---

                        # 4. Extract Content
                        if not raw_html:
                            logging.warning(f"[{name}] FAILED with '{strategy}' (HTML was empty).")
                            continue # Try next strategy

                        # --- NEW: Added try/except around trafilatura ---
                        temp_summary = None
                        try:
                            temp_summary = trafilatura.extract(raw_html, include_comments=False, include_tables=False)
                        except Exception as e:
                            logging.error(f"[{name}] trafilatura failed to parse HTML: {e}")
                        
                        if temp_summary and len(temp_summary) > 100:
                            logging.info(f"[{name}] Success with '{strategy}'. Found content.")
                            summary = temp_summary
                            
                            # Since we have good HTML, parse metadata
                            soup = BeautifulSoup(raw_html, 'html.parser')
                            page_title = soup.find('title')
                            if page_title:
                                final_title = page_title.text
                            
                            og_image = soup.find('meta', property='og:image')
                            if og_image:
                                image_url = og_image['content']
                            
                            break # <-- Success! Exit the strategy loop.
                        else:
                            logging.warning(f"[{name}] FAILED with '{strategy}' (content was empty or too short).")
                    
                    except Exception as e:
                        # Catching errors from requests OR selenium
                        logging.error(f"[{name}] Request failed for strategy '{strategy}': {e}")
                    
                    # Wait a moment before trying the next strategy
                    if i < len(strategies) - 1:
                        time.sleep(random.uniform(0.5, 1.0))
                
                # --- END OF STRATEGY LOOP ---

                # 5. Fallback Logic
                # This block only runs if ALL strategies in the loop failed
                if not summary:
                    logging.error(f"[{name}] All scrape strategies failed for {article_url}. Falling back to RSS description.")
                    if item.description:
                        # Use BeautifulSoup to strip any HTML from the RSS description
                        summary_soup = BeautifulSoup(item.description.text, 'html.parser')
                        summary = summary_soup.get_text().strip()
                    else:
                        summary = "No content available"

                # 6. Save
                save_article(name, final_title, article_url, summary, image_url)
                articles_saved.append(final_title)
                
                time.sleep(random.uniform(0.5, 1.5)) # Politeness delay between *articles*

            except Exception as e:
                # This catches errors inside the article loop (e.g., a single bad article)
                logging.error(f"[{name}] Article-level Error: {e} for url {article_url}")

    except requests.RequestException as e:
        # This will catch connection errors, timeouts, and 4xx/5xx errors for the RSS feed
        logging.error(f"Failed to fetch {name} RSS feed: {e}")
    except Exception as e:
        # This catches errors parsing the RSS feed itself
        logging.error(f"Failed to parse {name} RSS feed: {e}")
        
    return articles_saved

# --- REFACTORED: scrape_all() ---
def scrape_all():
    """
    Runs all scraping jobs defined in SOURCE_CONFIG.
    --- NEW ---
    Creates and destroys its own Selenium driver for each run
    to prevent "invalid session id" errors.
    """
    logging.info("--- Starting new scraping job ---")
    
    session = create_robust_session() # Create one session for the whole job
    driver = None # Initialize driver as None
    
    # --- NEW: Create proxy dictionary from settings ---
    proxies_dict = None
    if PROXY_SETTINGS["use_proxies"] and PROXY_SETTINGS["proxy_url"]:
        logging.info(f"Proxy is ENABLED. Routing requests through: {PROXY_SETTINGS['proxy_url']}")
        proxies_dict = {
            "http": PROXY_SETTINGS["proxy_url"],
            "https": PROXY_SETTINGS["proxy_url"]
        }
    else:
        logging.info("Proxy is DISABLED.")
    # --------------------------------------------------
    
    all_counts = {}
    total_saved = 0 
    
    # This is the robust "at any cost" logic.
    # We wrap the *entire* job in a try/finally block
    # to ensure the Selenium driver is ALWAYS shut down,
    # even if the script crashes. This prevents "stale" drivers.
    try:
        # --- NEW: Create driver inside the job ---
        logging.info("Attempting to initialize Selenium driver for this run...")
        driver = create_selenium_driver()
        if not driver:
            logging.warning("Selenium driver failed to start. Sites requiring Selenium will fail.")
        # ---------------------------------------

        for source in SOURCE_CONFIG:
            # We wrap each source scrape in its own try/except
            # so that if one source (e.g. BBC) fails completely,
            # it doesn't stop the others (e.g. TOI) from running.
            try:
                # --- UPDATED: Pass proxies_dict to scrape_source ---
                articles_saved = scrape_source(session, driver, source, proxies_dict) # Pass the (possibly None) driver
                count = len(articles_saved)
                all_counts[source['name']] = count
                total_saved += count # Add to total
            except Exception as e:
                logging.critical(f"--- CRITICAL: Scrape job for {source['name']} failed entirely. --- {e}")
                all_counts[source['name']] = 0
        
        # Create a dynamic log message
        log_summary = ", ".join(f"{count} {name}" for name, count in all_counts.items())
        log_message = f"Scraped: {log_summary} articles. (Total saved: {total_saved})"
        
        logging.info(log_message)
        print(log_message)
    
    except Exception as e:
        logging.critical(f"--- CRITICAL: The entire scrape_all job failed. --- {e}")
        
    finally:
        # --- NEW: Quit the driver at the end of the job ---
        # This is the most important part for stability.
        # This block will run NO MATTER WHAT, even if the
        # script crashes, preventing "invalid session id" errors.
        if driver:
            logging.info("Shutting down Selenium driver for this run.")
            driver.quit()
        logging.info("--- Scraping job finished ---")


# --- main() function with cleanup ---
def main():
    """
    Main function to run the scraper immediately and then schedule it.
    Includes robust error handling and DB connection closing.
    
    --- UPDATED FOR GITHUB ACTIONS ---
    This function no longer schedules or loops. It runs 
    scrape_all() exactly once and then exits.
    """
    global conn # Make connection global to be accessible in finally
    
    try:
        logging.info("--- Scraper service started (CI Mode: Run Once) ---")
        
        print("Running single scrape for CI...")
        scrape_all() # Run once
        
        print("Scrape finished.")
            
    except Exception as e:
        logging.critical(f"A critical error occurred in the main function: {e}")
    finally:
        if conn:
            conn.close() # Ensure database connection is closed on exit
            logging.info("--- Scraper service stopped and database connection closed. ---")
            print("Scraper stopped and database connection closed.")

if __name__ == '__main__':
    main()
