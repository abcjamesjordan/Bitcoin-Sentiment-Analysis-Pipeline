# Airflow imports
from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime

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
URL_LIMIT = Variable.get("URL_BATCH_SIZE", 2)
DOMAIN_TO_SCRAPE = Variable.get("DOMAIN_TO_SCRAPE", "newsbtc.com")
TIME_FILTER = Variable.get("TIME_FILTER", False)

default_args = {
    'start_date': datetime(2025, 1, 1),
}

@dag(
    default_args=default_args,
    catchup=False
)
def scrape_urls_and_upload_to_bigquery():
    @task
    def get_strategies():
        return get_domain_strategies(PROJECT_ID, ARTICLES_DATASET)

    @task
    def get_urls():
        return extract_urls(PROJECT_ID, ARTICLES_DATASET, URL_LIMIT, TIME_FILTER)

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

    # DAG workflow
    domain_strategies = get_strategies()
    urls = get_urls()
    results = process_url_batch(urls, domain_strategies)

    # Process results in parallel where possible
    results >> [
        handle_failed_strategies(results),
        handle_new_strategies(results),
        upload_results(results)
    ]

scrape_urls_and_upload_to_bigquery()