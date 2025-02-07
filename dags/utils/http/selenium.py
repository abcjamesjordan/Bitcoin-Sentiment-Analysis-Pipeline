from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from time import sleep
import logging

def fetch_with_selenium(url: str, wait_time: int = 5) -> tuple[str, int]:
    """
    Fetch content using Selenium for JavaScript-heavy sites.
    
    Args:
        url (str): The URL to fetch content from
        wait_time (int): Time to wait for dynamic content to load in seconds
        
    Returns:
        tuple[str, int]: Tuple containing (page_content, status_code)
        
    Raises:
        WebDriverException: If browser automation fails
        TimeoutException: If page load times out
    """
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.binary_location = '/usr/bin/google-chrome'  # Specify Chrome binary location
    # Additional recommended options for stability
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    
    try:
        with webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options) as driver:
            driver.set_page_load_timeout(30)  # Set page load timeout
            logging.info(f"Fetching {url} with Selenium")
            driver.get(url)
            
            # Wait for dynamic content to load
            sleep(wait_time)
            content = driver.page_source
            
            if not content:
                raise ValueError("No content retrieved")
                
            return content, 200
            
    except Exception as e:
        logging.error(f"Selenium fetch failed for {url}: {str(e)}")
        raise