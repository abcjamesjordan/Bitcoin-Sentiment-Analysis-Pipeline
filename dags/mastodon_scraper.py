# Python standard library imports
from datetime import datetime, timedelta
import logging

# Third-party imports
from bs4 import BeautifulSoup
from mastodon import Mastodon

# Airflow imports
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Set logging level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = Variable.get("GCP_PROJECT_ID", "news-api-421321")
DESTINATION_TABLE = Variable.get("DESTINATION_TABLE", "mastodon.raw_toots")

default_args = {
    'start_date': datetime(2025, 1, 1),
}

@dag(
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    description="""
    Scrapes bitcoin-related Mastodon posts, extracts content and metadata to BigQuery, and triggers URL processing.
    """,
    tags=['bitcoin', 'mastodon', 'web-scraping', 'social-media', 'bigquery']
)
def mastodon_scraper():
    """
    Scrapes bitcoin-related posts from Mastodon.social and loads them into BigQuery.
    
    The DAG performs the following steps:
    1. Searches Mastodon for posts containing "bitcoin"
    2. Extracts post content, metadata, and embedded URLs
    3. Loads data into BigQuery table {PROJECT_ID}.{DESTINATION_TABLE}
    4. Triggers a child DAG to process extracted URLs
    
    Requirements:
        - Mastodon API credentials in Airflow connection 'mastodon_api'
        - GCP credentials configured for BigQuery access
        - Variables:
            - GCP_PROJECT_ID
            - DESTINATION_TABLE
    
    Schedule: Hourly
    """

    @task
    def search_mastodon(q: str):
        # Get Mastodon credentials from Airflow connection
        conn = BaseHook.get_connection("mastodon_api")
        
        mastodon = Mastodon(
            access_token=conn.get_password(),
            api_base_url='https://mastodon.social'
        )

        results = []
        try:
            search_results = mastodon.search(
                # Limit set to 20 by default
                q=q, 
                result_type='statuses'
            )
            
            for toot in search_results['statuses']:
                # Get account info safely
                account = toot.get('account') or {}
                card = toot.get('card') or {}
                
                # Parse HTML content and extract clean text
                content_html = toot.get('content') or ''
                soup = BeautifulSoup(content_html, 'html.parser')
                clean_content = soup.get_text(separator=' ', strip=True)

                logger.info(f"Content HTML: {content_html}")
                logger.info(f"Clean content: {clean_content}")
                
                results.append({
                    'id': str(toot.get('id') or ''),
                    'author': account.get('username') or '',
                    'author_display_name': account.get('display_name') or '',
                    'content': clean_content,  # Use cleaned content instead of raw HTML
                    'created_at': toot.get('created_at') or '',  # Keep as None if missing
                    'url': toot.get('url') or '',
                    'language': toot.get('language') or '',
                    'card_url': card.get('url') or '',
                    'card_title': card.get('title') or '',
                    'card_description': card.get('description') or '',
                    'card_provider': card.get('provider_name') or '',
                    'tags': [tag.get('name') for tag in toot.get('tags', [])] or []
                })
                
            logger.info(f"Successfully retrieved {len(results)} toots")
            logger.info(results)
            return results

        except Exception as e:
            logger.error(f"Error searching Mastodon: {str(e)}")
            raise

    @task
    def upload_to_bigquery(results):
        import json
        from tempfile import NamedTemporaryFile
        from google.cloud import bigquery
        from datetime import datetime
        
        if not results:
            logger.warning("No results to upload")
            return
            
        logger.info(f"Uploading {len(results)} toots to BigQuery")
        
        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{DESTINATION_TABLE}"
        
        # Write results to a temporary file
        with NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            for record in results:
                # Convert datetime to ISO format string if it's a datetime object
                if isinstance(record.get('created_at'), datetime):
                    record['created_at'] = record['created_at'].isoformat()
                json_str = json.dumps(record)
                f.write(json_str + '\n')
            temp_file = f.name

        # Define the schema explicitly to ensure correct types
        schema = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("author", "STRING"),
            bigquery.SchemaField("author_display_name", "STRING"),
            bigquery.SchemaField("content", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("language", "STRING"),
            bigquery.SchemaField("card_url", "STRING"),
            bigquery.SchemaField("card_title", "STRING"),
            bigquery.SchemaField("card_description", "STRING"),
            bigquery.SchemaField("card_provider", "STRING"),
            bigquery.SchemaField("tags", "STRING", mode="REPEATED")
        ]
        
        # Configure the load job with explicit schema
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=schema  # Use explicit schema instead of autodetect
        )

        try:
            with open(temp_file, "rb") as source_file:
                job = client.load_table_from_file(
                    source_file,
                    table_id,
                    job_config=job_config
                )
            # Wait for job to complete
            job.result()
            logger.info(f"Successfully uploaded {len(results)} toots to BigQuery")
        except Exception as e:
            logger.error(f"Error uploading to BigQuery: {str(e)}")
            raise

    trigger_child = TriggerDagRunOperator(
        task_id='trigger_child',
        trigger_dag_id='scrape_urls_and_upload_to_bigquery',
        wait_for_completion=False,
        conf={
            'source': 'mastodon',
        }
    )

    # Define task dependencies
    results = search_mastodon(q="bitcoin")
    upload_to_bigquery(results) >> trigger_child
# Instantiate DAG
dag = mastodon_scraper()
