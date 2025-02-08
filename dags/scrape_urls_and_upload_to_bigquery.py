# Airflow imports
from airflow import DAG
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

# Python standard library imports
from datetime import datetime, timedelta
import logging
from time import sleep
from urllib.parse import urlparse

# Third-party imports
from google.cloud import bigquery
import pandas as pd
from trafilatura import fetch_url, extract

# Local imports
from utils.http.requests import fetch_with_requests
from utils.http.selenium import fetch_with_selenium

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Replace hard-coded values
PROJECT_ID = Variable.get("GCP_PROJECT_ID", "news-api-421321")
ARTICLES_DATASET = Variable.get("ARTICLES_DATASET", "articles")
URL_LIMIT = Variable.get("URL_BATCH_SIZE", 2)
DOMAIN_TO_SCRAPE = Variable.get("DOMAIN_TO_SCRAPE", "newsbtc.com")
default_args = {
    # 'owner': 'airflow',
    # 'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

@dag(
        default_args=default_args,
        # schedule_interval='@daily',
        catchup=False
    )
def scrape_urls_and_upload_to_bigquery():
    @task
    def get_domain_strategies():
        """Fetch domain-specific scraping strategies from BigQuery."""
        client = bigquery.Client()
        query = f"""
        SELECT domain, strategies
        FROM `{PROJECT_ID}.{ARTICLES_DATASET}.domain_strategies`
        """
        df = client.query(query).to_dataframe()
        # Convert strategies string to list format {domain: [strategy]}
        return df.set_index('domain')['strategies'].apply(lambda x: [x]).to_dict()

    @task
    def update_domain_strategies(new_strategies: dict):
        """Upload new successful domain strategies to BigQuery."""
        if not new_strategies:
            return
        
        df = pd.DataFrame([
            {'domain': domain, 'strategies': strategies}
            for domain, strategies in new_strategies.items()
        ])
        
        client = bigquery.Client()
        table_id = f'{PROJECT_ID}.{ARTICLES_DATASET}.domain_strategies'
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("domain", "STRING"),
                bigquery.SchemaField("strategies", "STRING"),
            ],
            write_disposition="WRITE_APPEND",
        )
        
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

    @task
    def extract_urls(time_filter: bool = False):
        client = bigquery.Client()
        SOURCE = f'{PROJECT_ID}.{ARTICLES_DATASET}.raw'
        CONDITIONAL_LOCATION = f'{PROJECT_ID}.{ARTICLES_DATASET}.url_text'
        BLACKLIST = f'{PROJECT_ID}.{ARTICLES_DATASET}.blacklisted_urls'

        # Initialize time filter query
        time_filter_query = ""
        
        # Add time filter if flag is set
        if time_filter:
            time_filter_query = """
                AND PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', uploadedAt) >= 
                    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            """
        
        query = f"""
        WITH blacklisted AS (
            SELECT url FROM `{BLACKLIST}`
        ),
        processed AS (
            SELECT url FROM `{CONDITIONAL_LOCATION}`
        )
        SELECT DISTINCT url
        FROM `{SOURCE}`
        WHERE url NOT IN (SELECT url FROM blacklisted)
        AND url NOT IN (SELECT url FROM processed)
        AND url LIKE '%{DOMAIN_TO_SCRAPE}%'
        {time_filter_query}
        LIMIT {URL_LIMIT}
        """

        query_job = client.query(query)
        results = query_job.result()

        valid_urls = [row.url for row in results if is_valid_url(row.url)]
        return valid_urls

    @task
    def process_urls(urls: list[str], domain_strategies: dict) -> dict:
        """Process URLs using domain-specific strategies from BigQuery."""
        DEFAULT_STRATEGIES = ['requests', 'trafilatura', 'selenium']
        new_successful_strategies = {}
        domains_to_remove = set()  # Track failed domain strategies
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
                                    
                                    # Record successful strategy if it's a new domain
                                    if not using_domain_strategy:
                                        new_successful_strategies[domain] = scraping_method
                                    
                                    break  # Success - break out of strategy loop
                        
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
                    
                    # If we got a successful result, break out of retry loop
                    if success:
                        break
                    
                    # If using domain strategy and it failed, mark for removal
                    if not success and using_domain_strategy:
                        domains_to_remove.add(domain)
                        logging.warning(f"Domain strategy failed for {domain}, will remove from domain_strategies")
                    
                except Exception as e:
                    if attempt == MAX_RETRIES - 1:  # Last attempt
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
                        
                        # If using domain strategy and all retries failed, mark for removal
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

    @task
    def upload_to_bigquery(data: dict):
        if not data["processed_data"]:
            logging.warning("No data was found to upload to BigQuery")
            raise AirflowSkipException("No data was found to upload to BigQuery")
        
        df = pd.DataFrame(data["processed_data"])
        
        # Convert numeric columns to strings before upload
        df['status_code'] = df['status_code'].astype(str)
        df['retry_count'] = df['retry_count'].astype(str)
        df['success'] = df['success'].astype(str)
        df['scraping_method'] = df['scraping_method'].astype(str)
        
        schema = [
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("text", "STRING"), 
            bigquery.SchemaField("status_code", "STRING"),
            bigquery.SchemaField("success", "STRING"),
            bigquery.SchemaField("retry_count", "STRING"),
            bigquery.SchemaField("scraping_method", "STRING")
        ]
        client = bigquery.Client()
        DESTINATION = f'{PROJECT_ID}.{ARTICLES_DATASET}.url_text'

        try:
            job_config = bigquery.LoadJobConfig(schema=schema)
            job = client.load_table_from_dataframe(df, DESTINATION, job_config=job_config)
            job.result()  # Wait for the job to complete
            logging.info(f"Uploaded {len(df)} records to {DESTINATION}")
        except Exception as e:
            raise Exception(f"Failed to upload dataframe: {e}")

    @task
    def remove_failed_strategies(domains_to_remove: list):
        """Remove failed strategies from BigQuery domain_strategies table."""
        if not domains_to_remove:
            return

        client = bigquery.Client()
        table_id = f'{PROJECT_ID}.{ARTICLES_DATASET}.domain_strategies'
        
        # Delete rows for failed domains
        query = f"""
        DELETE FROM `{table_id}`
        WHERE domain IN UNNEST(@domains)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("domains", "STRING", domains_to_remove),
            ]
        )
        
        client.query(query, job_config=job_config).result()
        logging.info(f"Removed failed strategies for domains: {domains_to_remove}")

    def is_valid_url(url: str) -> bool:
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False

    # Update DAG workflow
    domain_strategies = get_domain_strategies()
    urls = extract_urls()
    results = process_urls(urls, domain_strategies)
    remove_failed = remove_failed_strategies(results["strategies_to_remove"])
    upload_task = upload_to_bigquery(results)
    remove_failed >> update_domain_strategies(results["new_strategies"]) >> upload_task

scrape_urls_and_upload_to_bigquery()