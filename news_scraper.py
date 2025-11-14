# ==============================================================================
# --- GLOBAL USER SETTINGS ---
#
# How many articles to get from each source (e.g., 25)
# This is a 'max' value. If a feed only has 20 articles, it will get 20.
MAX_ARTICLES_PER_SOURCE = 5
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
import os # <-- os is still needed for os.kill

# --- NEW: Imports for Parallelism ---
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, ALL_COMPLETED
import concurrent.futures # <-- Added for wait()
import threading
# ------------------------------------

# --- UPDATED: Import Selenium ---
try:
    from selenium import webdriver
    # from selenium.webdriver.chrome.service import Service as ChromeService # <-- REMOVED
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.common.exceptions import WebDriverException
    # --- NEW: Imports for Explicit Waits ---
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException
    # ---------------------------------------
    SELENIUM_AVAILABLE = True
except ImportError:
    logging.critical("Selenium not installed. Run 'pip install selenium'. Selenium-dependent sources will fail.")
    SELENIUM_AVAILABLE = False
# ---------------------------

# --- Robust Session and Header Management ---

def create_robust_session():
    """
    Creates a requests.Session with automatic retries on server errors (5xx)
    AND connection/read errors.
    """
    logging.info("Creating new robust session with 3 retries on 5xx/connection/read errors.")
    session = requests.Session()
    
    # --- FIX 3: Make retry strategy more robust ---
    # We explicitly add 'connect' and 'read' to the retry strategy.
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504], # Retry on server errors
        allowed_methods=["HEAD", "GET"], # Only retry on safe methods
        connect=3, # Retry on connection errors
        read=3 # Retry on read errors
    )
    # ---------------------------------------------
    
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
        
        # --- FIX 2: Increase page load timeout to 60 seconds ---
        # Give heavy pages a better chance to load on slow CI machines
        driver.set_page_load_timeout(60) 
        # -----------------------------------------------------
        
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

# --- NEW: Create a lock for thread-safe database writes ---
db_lock = threading.Lock()
# ---------------------------------------------------------

def save_article(source, title, url, summary, image_url):
    """
    Saves a single article to the SQLite database.
    Prevents duplicates based on the 'url' column.
    Cleans data before saving.
    
    --- NEW: This function is now thread-safe ---
    --- MODIFIED: Returns True on success, False on skip/error ---
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

        # --- NEW: Acquire lock before writing to DB ---
        with db_lock:
            cursor.execute('''
                INSERT INTO news (source, title, url, summary, image_url, scraped_at) 
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (source, title, url, summary, image_url, datetime.now()))
            conn.commit()
        # --- Lock is automatically released here ---
            
        logging.info(f"Saved article: {title} from {source}")
        return True # <-- Return True on successful save
    except sqlite3.IntegrityError:
        # This is expected if the article URL is already in the DB
        logging.info(f"Duplicate article skipped: {title} from {source}")
        return False # <-- Return False on duplicate
    except Exception as e:
        logging.error(f"Error saving article: {e}")
        return False # <-- Return False on error

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
                            
                            # --- MODIFIED: Use Explicit Wait instead of time.sleep() ---
                            # This is *much* faster and more reliable.
                            # We wait up to 10s for *any* <p> tag to appear.
                            # This indicates the main content has likely started loading.
                            try:
                                WebDriverWait(selenium_driver, 10).until(
                                    EC.presence_of_element_located((By.TAG_NAME, "p"))
                                )
                                logging.info(f"[{name}] Page content loaded.")
                            except TimeoutException:
                                # If no <p> tags appear, the page is likely broken
                                logging.warning(f"[{name}] Page timed out (10s). No <p> tags found. Proceeding anyway.")
                            # -----------------------------------------------------------
                            
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
                        
                        # --- MODIFIED: Check for word count >= 50 ---
                        word_count = 0
                        if temp_summary:
                            word_count = len(temp_summary.split())
                            
                        if temp_summary and word_count >= 50:
                            logging.info(f"[{name}] Success with '{strategy}'. Found content ({word_count} words).")
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
                            # --- MODIFIED: Improved logging for failure ---
                            if not temp_summary:
                                logging.warning(f"[{name}] FAILED with '{strategy}' (content was empty).")
                            else:
                                logging.warning(f"[{name}] FAILED with '{strategy}' (content was too short: {word_count} words).")
                            # ---------------------------------------------
                    
                    except Exception as e:
                        # Catching errors from requests OR selenium
                        # --- MODIFIED: Log the specific URL in this error ---
                        logging.error(f"[{name}] Request failed for strategy '{strategy}' on URL {article_url}: {e}")
                        # ----------------------------------------------------
                    
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
                # --- MODIFIED: Check return value before appending ---
                was_saved = save_article(name, final_title, article_url, summary, image_url)
                if was_saved:
                    articles_saved.append(final_title)
                # ----------------------------------------------------
                
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

# --- NEW: Wrapper function to run in each thread ---
# This function manages the lifecycle of a Selenium driver *within its own thread*
def scrape_source_wrapper(source, session, proxies_dict):
    """
    A wrapper function to be run in a separate thread.
    It creates and destroys its own Selenium driver if needed.
    """
    driver = None # Initialize driver as None for *this source*
    name = source['name']
    
    # This try/finally block manages the driver's lifecycle for this *one source*
    try:
        # Check if this source *needs* Selenium
        uses_selenium = any(s == 'selenium_browser' for s in source['article_strategies'])
        
        if uses_selenium:
            logging.info(f"[{name}] (Thread) requires Selenium. Initializing driver...")
            driver = create_selenium_driver() # <--- Create driver
            if not driver:
                logging.warning(f"[{name}] (Thread) Selenium driver failed to start. This source will likely fail.")

        # This inner try/except catches errors *during* the scrape
        try:
            # Pass the (possibly None) driver to the scrape function
            articles_saved = scrape_source(session, driver, source, proxies_dict)
            count = len(articles_saved)
            return (name, count) # Return results
        except Exception as e:
            logging.critical(f"--- CRITICAL: (Thread) Scrape job for {name} failed entirely. --- {e}")
            return (name, 0)
    
    except Exception as e:
        # This catches a failure in *driver creation* itself
        logging.critical(f"--- CRITICAL: (Thread) Driver creation failed for {name}. --- {e}")
        return (name, 0)

    finally:
        # --- MODIFIED: "Surgical Kill" for the driver ---
        # This is the robust way to clean up a stuck driver.
        if driver:
            logging.info(f"[{name}] (Thread) Finished. Attempting to shut down its Selenium driver.")
            pid_to_kill = None
            try:
                # Get the Process ID (PID) of the chromedriver service
                pid_to_kill = driver.service.process.pid
            except Exception:
                pass # If we can't get it, we can't kill it.
            
            try:
                # Try the clean quit first
                driver.quit()
                logging.info(f"[{name}] (Thread) driver.quit() successful.")
            except Exception as e:
                # If driver.quit() fails (e.g., hangs or errors)
                logging.warning(f"[{name}] (Thread) driver.quit() failed: {e}. Attempting surgical kill.")
                if pid_to_kill:
                    try:
                        os.kill(pid_to_kill, 9) # 9 = SIGKILL
                        logging.info(f"[{name}] (Thread) Successfully killed stuck driver process PID {pid_to_kill}.")
                    except Exception as e_kill:
                        logging.error(f"[{name}] (Thread) Failed to kill process PID {pid_to_kill}: {e_kill}")
                else:
                    logging.error(f"[{name}] (Thread) driver.quit() failed, but PID was not found. A zombie process may remain.")
            
# --- REFACTORED: scrape_all() ---
def scrape_all():
    """
    Runs all scraping jobs defined in SOURCE_CONFIG *in parallel*
    using a ThreadPoolExecutor, with a 5-minute global timeout.
    """
    logging.info("--- Starting new scraping job (Parallel Mode) ---")
    
    session = create_robust_session() # Create one session for all threads
    
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
    
    # --- MODIFIED: Manually manage executor to force shutdown ---
    executor = ThreadPoolExecutor(max_workers=len(SOURCE_CONFIG))
    
    try:
        # Submit all scrape tasks to the pool
        future_to_source = {
            executor.submit(scrape_source_wrapper, source, session, proxies_dict): source['name']
            for source in SOURCE_CONFIG
        }
        
        futures = future_to_source.keys()
        
        # --- NEW: Wait for all futures with a 5-minute (300s) global timeout ---
        total_timeout = 300 # 5 minutes
        logging.info(f"--- All jobs submitted. Waiting for completion with a {total_timeout}s timeout... ---")
        
        try:
            # wait() blocks until all futures complete OR the timeout is hit
            done, not_done = concurrent.futures.wait(futures, timeout=total_timeout, return_when=ALL_COMPLETED)
        except Exception as e:
            logging.critical(f"--- 'wait()' command failed: {e} ---")
            # Fallback: check futures manually
            done = [f for f in futures if f.done() and not f.cancelled()]
            not_done = [f for f in futures if not f.done()]
        # --------------------------------------------------------------------

        # --- Process results from 'done' (completed) futures ---
        logging.info("--- Timeout or completion hit. Processing results... ---")
        for future in done:
            source_name = future_to_source[future]
            try:
                # Get the result from the finished task (won't block)
                name, count = future.result()
                all_counts[name] = count
                total_saved += count
                logging.info(f"--- (Thread) Finished job for {name}, saved {count} articles. ---")
            except Exception as e:
                # Catch any unexpected errors from *within* the thread
                logging.critical(f"--- CRITICAL: (Thread) {source_name} job's result() failed: {e} ---")
                all_counts[source_name] = 0

        # --- Cancel and log 'not_done' (timed out) futures ---
        if not_done:
            logging.warning(f"--- GLOBAL TIMEOUT. {len(not_done)} tasks did not complete. ---")
            for future in not_done:
                source_name = future_to_source[future]
                logging.warning(f"--- Cancelling incomplete task: {source_name} ---")
                future.cancel() # Attempt to cancel the running thread
                all_counts[source_name] = 0 # Mark as 0 saved
    
    except Exception as e:
        logging.critical(f"--- CRITICAL: ThreadPoolExecutor task submission failed: {e} ---")
    finally:
        # --- NEW: Manually shut down the executor *without* waiting ---
        # This prevents the main thread from hanging on a stuck child thread (e.g., driver.quit())
        logging.info("--- Main thread finished. Forcing executor shutdown... ---")
        executor.shutdown(wait=False)
    # -----------------------------------------------------------
            
    # Create a dynamic log message
    log_summary = ", ".join(f"{count} {name}" for name, count in all_counts.items())
    log_message = f"Scraped: {log_summary} articles. (Total saved: {total_saved})"
    
    logging.info(log_message)
    print(log_message)
    
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
        
        # --- REMOVED: Force process exit ---
        # We no longer need os._exit(0) because the "Surgical Kill"
        # in the wrapper's finally block will clean up zombie threads,
        # allowing the script to exit cleanly on its own.
        logging.info("--- Main thread exiting. ---")

if __name__ == '__main__':
    main()
