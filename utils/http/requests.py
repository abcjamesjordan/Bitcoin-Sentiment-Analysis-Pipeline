import requests

def fetch_with_requests(url: str, headers: dict = None) -> tuple[str, int]:
    """
    Fetch URL content using requests library.
    
    Args:
        url: URL to fetch
        headers: Optional headers dictionary
        
    Returns:
        tuple: (content, status_code)
    """
    if headers is None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
    
    response = requests.get(url, headers=headers)
    return response.text, response.status_code 