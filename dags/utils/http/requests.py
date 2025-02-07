import logging
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Tuple, Optional

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'DNT': '1',  # Do Not Track
    'Sec-Ch-Ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Linux"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
}

def fetch_with_requests(url: str, headers: dict = HEADERS) -> Tuple[Optional[bytes], Optional[int]]:
    """
    Custom fetch function using requests with built-in retry mechanism.
    
    Args:
        url: The URL to fetch
        
    Returns:
        tuple: (content_bytes, status_code) or (None, status_code) if failed
    """
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 408, 409],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    try:
        # Add cookies consent for news sites
        session.cookies.set('euconsent-v2', 'TRUE', domain='.nypost.com')
        session.cookies.set('cookie_consent', 'accepted', domain='.nypost.com')
        
        response = session.get(
            url,
            headers=headers,
            timeout=(10, 30),
            verify=True,
            allow_redirects=True
        )
        
        # Check if we got a valid response with content
        if response.status_code == 200 and len(response.content) > 1000:
            return response.content, response.status_code
        else:
            logging.warning(f"Response too short or invalid for {url}: {len(response.content)} bytes")
            return None, response.status_code
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error for {url}: {str(e)}")
        return None, getattr(e.response, 'status_code', None)
    finally:
        session.close()