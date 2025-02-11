# Airflow imports
from airflow import DAG
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

# Python imports
from datetime import datetime, timedelta, timezone
import logging
import time

# Third party imports
from google.cloud import bigquery
import google.generativeai as genai
import pandas as pd

# Local imports
from utils.analyzers.gemini_sentiment import GeminiSentimentAnalyzer
from utils.sentiment_processing import process_article, get_failed_result

# Set logging level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = Variable.get("GCP_PROJECT_ID", "news-api-421321")
ARTICLES_DATASET = Variable.get("ARTICLES_DATASET", "articles")
BATCH_SIZE = 100

default_args = {
    'start_date': datetime(2025, 1, 1),
}

@dag(
    default_args=default_args,
    schedule="15 14 * * *",  # Run at 14:15 UTC (after scraping completes)
    catchup=False,
)
def sentiment_analysis():
    # Wait for scraping DAG to complete
    wait_for_scraping = ExternalTaskSensor(
        task_id='wait_for_scraping',
        external_dag_id='scrape_urls_and_upload_to_bigquery',
        external_task_id=None,
        execution_delta=timedelta(hours=1), # Run 1 hour after scraping DAG completes
        timeout=1800,  # 30 minute timeout
        poke_interval=60,  # Check every minute
        # mode='reschedule',
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
        for i, article in enumerate(articles):
            # Add delay every 25 articles to stay under 30 RPM
            if i > 0 and i % 25 == 0:
                logger.info("Rate limit pause - waiting 60 seconds")
                time.sleep(60)
                
            retries = 0
            max_retries = 2
            while retries <= max_retries:
                try:
                    logger.info(f"Processing article: {article['article_url']}")
                    analysis = analyzer.analyze_article(
                        article_text=article['article_text'],
                        article_id=article['article_url']
                    )
                    results.append(process_article(article, analysis))
                    logger.info(f"Successfully processed article {article['article_url']}")
                    break
                    
                except genai.types.generation_types.BlockedPromptException as e:
                    logger.error(f"Content blocked: {str(e)}")
                    results.append(get_failed_result(article['article_url']))
                    break
                    
                except Exception as e:
                    if "RESOURCE_EXHAUSTED" in str(e) or "quota" in str(e).lower():
                        retries += 1
                        if retries <= max_retries:
                            wait_time = 60 * (2 ** (retries - 1))  # Exponential backoff
                            logger.warning(f"Rate limit hit, waiting {wait_time} seconds before retry {retries}")
                            time.sleep(wait_time)
                            continue
                    
                    logger.exception(f"Failed to process {article['article_url']} after {retries} retries:")
                    results.append(get_failed_result(article['article_url']))
                    break
        
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