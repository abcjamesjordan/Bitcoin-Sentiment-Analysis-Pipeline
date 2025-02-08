from time import sleep
import logging
from urllib.parse import urlparse
from trafilatura import fetch_url, extract

from .http.requests import fetch_with_requests
from .http.selenium import fetch_with_selenium

def is_valid_url(url: str) -> bool:
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except:
        return False

def process_urls(urls: list[str], domain_strategies: dict) -> dict:
    """Process URLs using domain-specific strategies."""
    DEFAULT_STRATEGIES = ['requests', 'trafilatura', 'selenium']
    new_successful_strategies = {}
    domains_to_remove = set()
    processed_data = []
    
    for url in urls:
        domain = urlparse(url).netloc
        using_domain_strategy = domain in domain_strategies
        strategies = domain_strategies.get(domain, DEFAULT_STRATEGIES)
        if isinstance(strategies, str):
            strategies = [strategies]
        
        DELAY_BETWEEN_REQUESTS = 5
        MAX_RETRIES = 3
        RETRY_DELAY = 5
        
        success = False
        for attempt in range(MAX_RETRIES):
            try:
                if attempt > 0:
                    sleep(RETRY_DELAY * (attempt + 1))
                    logging.info(f"Retry attempt {attempt + 1} for URL: {url}")
                
                sleep(DELAY_BETWEEN_REQUESTS)
                
                content = None
                status_code = None
                scraping_method = None
                failed_strategies = []
                
                logging.info(f"URL: {url} - Attempting strategies in order: {strategies}")
                
                for strategy in strategies:
                    logging.info(f"URL: {url} - Trying strategy: {strategy}")
                    try:
                        if strategy == 'selenium':
                            content, status_code = fetch_with_selenium(url)
                            scraping_method = 'selenium'
                        elif strategy == 'requests':
                            content, status_code = fetch_with_requests(url)
                            scraping_method = 'requests'
                        elif strategy == 'trafilatura':
                            content = fetch_url(url)
                            status_code = 200 if content else None
                            scraping_method = 'trafilatura'
                        
                        if content and len(content.strip()) > 25:
                            result = extract(content)
                            
                            if result and len(result.strip()) >= 25:
                                success = True
                                logging.info(f"URL: {url} - Strategy '{strategy}' succeeded with valid content")
                                processed_data.append({
                                    "url": url,
                                    "text": result,
                                    "status_code": status_code,
                                    "success": True,
                                    "retry_count": attempt + 1,
                                    "scraping_method": scraping_method
                                })
                                
                                if not using_domain_strategy:
                                    new_successful_strategies[domain] = scraping_method
                                
                                break
                    
                    except Exception as method_error:
                        failed_strategies.append({
                            'strategy': strategy,
                            'error': str(method_error),
                            'status_code': status_code if 'status_code' in locals() else None
                        })
                        logging.warning(
                            f"Strategy '{strategy}' failed for {url}:\n"
                            f"  Error: {str(method_error)}\n"
                            f"  Status Code: {status_code if 'status_code' in locals() else 'N/A'}\n"
                            f"  Attempt: {attempt + 1}/{MAX_RETRIES}"
                        )
                        continue
                
                if success:
                    break
                
                if not success and using_domain_strategy:
                    domains_to_remove.add(domain)
                    logging.warning(f"Domain strategy failed for {domain}, will remove from domain_strategies")
                
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    failed_strategies_detail = '\n'.join([
                        f"  - {f['strategy']}: {f['error']} (Status: {f['status_code'] or 'N/A'})"
                        for f in failed_strategies
                    ])
                    logging.error(
                        f"Failed to process URL: {url}\n"
                        f"Error: {str(e)}\n"
                        f"Failed strategies:\n{failed_strategies_detail}\n"
                        f"Final attempt: {attempt + 1}/{MAX_RETRIES}"
                    )
                    processed_data.append({
                        "url": url,
                        "text": f"Error processing URL: {str(e)}",
                        "status_code": failed_strategies[0]['status_code'] if failed_strategies else None,
                        "success": False,
                        "retry_count": attempt + 1,
                        "scraping_method": failed_strategies[0]['strategy'] if failed_strategies else None
                    })
                    
                    if using_domain_strategy:
                        domains_to_remove.add(domain)
                        logging.warning(f"Domain strategy failed for {domain} after all retries, will remove from domain_strategies")
    
    logging.info("Processed data results:")
    for item in processed_data:
        logging.info(f"""
        URL: {item['url']}
        Status Code: {item['status_code']}
        Success: {item['success']}
        Retry Count: {item['retry_count']}
        Text Length: {len(item['text']) if item['text'] else 0} chars
        Scraping Method: {item['scraping_method']}
        """)
    
    return {
        "processed_data": processed_data,
        "new_strategies": new_successful_strategies,
        "strategies_to_remove": list(domains_to_remove)
    } 