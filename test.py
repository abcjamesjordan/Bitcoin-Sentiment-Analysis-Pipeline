# from airflow.decorators import dag, task
# from airflow.models import Variable
from google.cloud import bigquery
from trafilatura import fetch_url, extract
# from trafilatura import get_crawl_delay
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
import logging
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

# client = bigquery.Client()

# SOURCE = 'news-api-421321.articles.raw'
# query = f"""
# SELECT  url
# FROM    `{SOURCE}`
# LIMIT   10
# """
# query_job = client.query(query)
# results = query_job.result()
# results_list = [row.url for row in results]

# print(results_list)

urls = [
    # 'https://www.denverpost.com/2025/02/05/denver-ice-raid-cedar-run-apartments/',
    # 'https://nypost.com/2025/02/05/us-news/emmarae-gervasi-raped-assaulted-during-kidnap-sources/',
    'https://financialpost.com/globe-newswire/purpose-investments-files-preliminary-prospectus-for-the-worlds-first-ripple-xrp-etf',
    # 'https://www.forbes.com/sites/digital-assets/2025/02/05/this-is-a-big-deal-bitcoin-and-crypto-now-braced-for-a-huge-us-price-earthquake/'
]

DELAY_BETWEEN_REQUESTS = 10  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

def fetch_with_requests(url: str) -> tuple[bytes | None, int | None]:
    """
    Custom fetch function using requests with built-in retry mechanism.
    
    Args:
        url: The URL to fetch
        
    Returns:
        tuple: (content_bytes, status_code) or (None, status_code) if failed
    """
    # Configure retry strategy
    retry_strategy = Retry(
        total=5,  # increased retries
        backoff_factor=2,  # more conservative backoff
        status_forcelist=[429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 408, 409],
        allowed_methods=["HEAD", "GET", "OPTIONS"]  # explicitly specify allowed methods
    )
    
    # Create session with retry strategy
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    try:
        response = session.get(
            url,
            headers=HEADERS,
            timeout=(10, 30),  # (connect timeout, read timeout)
            verify=True,
            allow_redirects=True
        )
        
        return response.content, response.status_code
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error for {url}: {str(e)}")
        return None, getattr(e.response, 'status_code', None)
    finally:
        session.close()

processed_data = []

for url in urls:
    for attempt in range(MAX_RETRIES):
        try:
            if attempt > 0:
                sleep(RETRY_DELAY)
                logging.info(f"Retry attempt {attempt + 1} for URL: {url}")
            
            sleep(DELAY_BETWEEN_REQUESTS)
            
            # Fetch content using requests
            content, status_code = fetch_with_requests(url)
            
            if content is not None:
                result = extract(content)
                processed_data.append({
                    "url": url,
                    "text": result or "No content extracted.",
                    "status_code": status_code,
                    "success": bool(result),
                    "retry_count": attempt + 1
                })
                
                logging.info(
                    f"Processed URL: {url} | "
                    f"Status: {status_code} | "
                    f"Content extracted: {bool(result)} | "
                    f"Attempt: {attempt + 1}/{MAX_RETRIES}"
                )
                break  # Success, exit retry loop
                
            elif attempt == MAX_RETRIES - 1:  # Last attempt
                processed_data.append({
                    "url": url,
                    "text": "URL could not be fetched after max retries.",
                    "status_code": status_code,
                    "success": False,
                    "retry_count": attempt + 1
                })
            
        except Exception as e:
            if attempt == MAX_RETRIES - 1:  # Last attempt
                logging.error(
                    f"Failed to process URL: {url} | "
                    f"Error: {str(e)} | "
                    f"Final attempt: {attempt + 1}/{MAX_RETRIES}"
                )
                processed_data.append({
                    "url": url,
                    "text": f"Error processing URL: {str(e)}",
                    "status_code": status_code if 'status_code' in locals() else None,
                    "success": False,
                    "retry_count": attempt + 1
                })
            else:
                logging.warning(
                    f"Attempt {attempt + 1} failed for URL: {url} | "
                    f"Error: {str(e)}"
                )


logging.info(processed_data)





# import requests

# def get_bitcoin_price():
#     try:
#         # URL for Coinbase API to get Bitcoin price in USD
#         url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
        
#         # Send GET request to the API
#         response = requests.get(url)
        
#         # Raise an exception for bad status codes
#         response.raise_for_status()
        
#         # Parse the JSON response
#         data = response.json()
        
#         # Get the price from the data
#         price = data['data']['amount']
        
#         return price
#     except requests.RequestException as e:
#         print(f"An error occurred while fetching the Bitcoin price: {e}")
#         return None

# # Fetch and print the Bitcoin price
# bitcoin_price = get_bitcoin_price()
# if bitcoin_price:
#     print(f"The latest Bitcoin price in USD is: ${bitcoin_price}")
# else:
#     print("Failed to retrieve Bitcoin price.")