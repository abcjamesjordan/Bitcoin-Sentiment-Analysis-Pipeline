# Standard library imports
from datetime import datetime, timedelta

# Airflow imports 
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Local imports
from utils.bigquery_ops import (
    get_domain_strategies,
    update_domain_strategies,
    extract_urls,
    upload_processed_data,
    remove_failed_strategies
)
from utils.url_processor import process_urls

# Replace hard-coded values
PROJECT_ID = Variable.get("GCP_PROJECT_ID", "news-api-421321")
ARTICLES_DATASET = Variable.get("ARTICLES_DATASET", "articles")
URL_LIMIT = Variable.get("URL_BATCH_SIZE", 101)
DOMAIN_TO_SCRAPE = Variable.get("DOMAIN_TO_SCRAPE", "newsbtc.com")
TIME_FILTER = Variable.get("TIME_FILTER", True)

default_args = {
    'start_date': datetime(2025, 1, 1),
}

@dag(
    default_args=default_args,
    catchup=False,
    schedule=None,
    description="""
    Scrapes and processes article URLs using dynamic strategies, uploads to BigQuery, and triggers sentiment analysis.
    """,
    tags=['bitcoin', 'bigquery', 'web-scraping']
)
def scrape_urls_and_upload_to_bigquery():
    """
    DAG for scraping and processing cryptocurrency news articles.

    Workflow:
    1. Fetches domain-specific scraping strategies from BigQuery
    2. Extracts URLs based on configured limits and time filters
    3. Processes URLs using dynamic scraping strategies
    4. Handles strategy updates and failures:
        - Removes failed scraping strategies
        - Updates with newly discovered strategies
        - Uploads processed article data
    5. Triggers downstream sentiment analysis DAG

    Configuration (via Airflow Variables):
    - GCP_PROJECT_ID: Google Cloud project ID
    - ARTICLES_DATASET: BigQuery dataset name
    - URL_BATCH_SIZE: Number of URLs to process per run
    - DOMAIN_TO_SCRAPE: Target domain for scraping
    - TIME_FILTER: Whether to filter by time

    Dependencies:
    - BigQuery for data storage
    - Custom utils for URL processing and BigQuery operations
    """
    @task
    def get_strategies():
        return get_domain_strategies(PROJECT_ID, ARTICLES_DATASET)

    @task
    def get_urls(**context):
        return extract_urls(PROJECT_ID, ARTICLES_DATASET, URL_LIMIT, TIME_FILTER, context)

    @task
    def process_url_batch(urls: list[str], domain_strategies: dict) -> dict:
        return process_urls(urls, domain_strategies)

    @task
    def handle_failed_strategies(results: dict):
        failed_strategies = results.get("strategies_to_remove", [])
        if failed_strategies:
            remove_failed_strategies(PROJECT_ID, ARTICLES_DATASET, failed_strategies)

    @task
    def handle_new_strategies(results: dict):
        new_strategies = results.get("new_strategies", {})
        if new_strategies:
            update_domain_strategies(PROJECT_ID, ARTICLES_DATASET, new_strategies)

    @task
    def upload_results(results: dict):
        upload_processed_data(PROJECT_ID, ARTICLES_DATASET, results)

    trigger_child = TriggerDagRunOperator(
        task_id='trigger_child',
        trigger_dag_id='sentiment_analysis',
        wait_for_completion=False
    )

    # DAG workflow
    domain_strategies = get_strategies()
    urls = get_urls()
    results = process_url_batch(urls, domain_strategies)
    
    # Process results in parallel where possible
    results >> [
        handle_failed_strategies(results),
        handle_new_strategies(results),
        upload_results(results),
    ] >> trigger_child

scrape_urls_and_upload_to_bigquery()