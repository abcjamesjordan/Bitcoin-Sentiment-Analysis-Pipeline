from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
import pandas as pd
import logging
from airflow.hooks.base import BaseHook

from utils.analyzers.gemini_sentiment import GeminiSentimentAnalyzer
from utils.sentiment_processing import process_article, get_failed_result

# Set logging level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = Variable.get("GCP_PROJECT_ID", "news-api-421321")
ARTICLES_DATASET = Variable.get("ARTICLES_DATASET", "articles")
BATCH_SIZE = 1

default_args = {
    'start_date': datetime(2025, 1, 1),
}

@dag(
    default_args=default_args,
    schedule="30 14 * * *",  # Run at 14:30 UTC (after scraping completes)
    catchup=False,
)
def sentiment_analysis():
    # Wait for scraping DAG to complete
    wait_for_scraping = ExternalTaskSensor(
        task_id='wait_for_scraping',
        external_dag_id='scrape_urls_and_upload_to_bigquery',
        external_task_id=None,
        timeout=1800,  # 30 minute timeout
        poke_interval=60,  # Check every minute
        mode='reschedule',
        allowed_states=['success'],
        failed_states=['failed']
    )

    @task
    def get_unanalyzed_articles() -> list:
        """Fetch unanalyzed articles from BigQuery."""
        client = bigquery.Client()
        query = f"""
        SELECT a.url as article_url, a.text as article_text
        FROM `{PROJECT_ID}.{ARTICLES_DATASET}.url_text` a
        WHERE NOT EXISTS (
            SELECT 1 
            FROM `{PROJECT_ID}.{ARTICLES_DATASET}.article_sentiments` s 
            WHERE s.article_url = a.url
        )
        AND a.success = 'True'
        AND a.text IS NOT NULL
        LIMIT {BATCH_SIZE}
        """
        df = client.query(query).to_dataframe()
        return df.to_dict('records')

    @task
    def analyze_batch(articles: list) -> list:
        """Process a batch of articles using GeminiSentimentAnalyzer."""
        logger.info(f"Starting batch analysis of {len(articles)} articles")
        analyzer = GeminiSentimentAnalyzer(
            api_key=BaseHook.get_connection("gemini_api").get_password()
        )
        
        results = []
        for article in articles:
            try:
                logger.info(f"Processing article: {article['article_url']}")
                analysis = analyzer.analyze_article(
                    article_text=article['article_text'],
                    article_id=article['article_url']
                )
                results.append(process_article(article, analysis))
                logger.info(f"Successfully processed article {article['article_url']}")
                
            except Exception as e:
                logger.exception(f"Full error details for {article['article_url']}:")
                results.append(get_failed_result(article['article_url']))
        
        return results

    @task
    def upload_sentiment_results(results: list):
        """Upload sentiment analysis results to BigQuery."""
        if not results:
            logging.info("No results to upload")
            return
            
        df = pd.DataFrame(results)
        client = bigquery.Client()
        table_id = f"{PROJECT_ID}.{ARTICLES_DATASET}.article_sentiments"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        
        logging.info(f"Successfully uploaded {len(results)} sentiment analysis results")

    # Define task dependencies
    articles = get_unanalyzed_articles()
    results = analyze_batch(articles)
    upload_sentiment_results(results)
    
    # Set task order
    wait_for_scraping >> articles

sentiment_analysis() 