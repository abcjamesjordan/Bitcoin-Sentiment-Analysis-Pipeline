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
from utils.sentiment_processing import process_article, get_failed_result, extract_source_from_url

# Set logging level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = Variable.get("GCP_PROJECT_ID", "news-api-421321")
ARTICLES_DATASET = Variable.get("ARTICLES_DATASET", "articles")
PRICING_DATASET = Variable.get("PRICING_DATASET", "pricing")
BATCH_SIZE = 40


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
    def analyze_batch(articles: list):
        """Process a batch of articles and collect metrics."""
        analyzer = GeminiSentimentAnalyzer(
            api_key=BaseHook.get_connection("gemini_api").get_password()
        )
        start_time = time.time()
        results = []
        error_counts = {}
        source_stats = {}
        
        for i, article in enumerate(articles):
            source = extract_source_from_url(article['article_url'])
            source_stats[source] = source_stats.get(source, {'success': 0, 'total': 0})
            source_stats[source]['total'] += 1
            
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
                    source_stats[source]['success'] += 1
                    logger.info(f"Successfully processed article {article['article_url']}")
                    break
                    
                except genai.types.generation_types.BlockedPromptException as e:
                    error_type = 'blocked_content'
                    error_counts[error_type] = error_counts.get(error_type, 0) + 1
                    logger.error(f"Content blocked: {str(e)}")
                    results.append(get_failed_result(article['article_url']))
                    break
                    
                except Exception as e:
                    if "RESOURCE_EXHAUSTED" in str(e) or "quota" in str(e).lower():
                        retries += 1
                        if retries <= max_retries:
                            wait_time = 60 * (2 ** (retries - 1))
                            logger.warning(f"Rate limit hit, waiting {wait_time} seconds before retry {retries}")
                            time.sleep(wait_time)
                            continue
                    
                    error_type = type(e).__name__
                    error_counts[error_type] = error_counts.get(error_type, 0) + 1
                    logger.exception(f"Failed to process {article['article_url']} after {retries} retries:")
                    results.append(get_failed_result(article['article_url']))
                    break
        
        metrics = {
            'date': datetime.now(timezone.utc).date(),
            'total_articles': len(articles),
            'successful_scrapes': len([r for r in results if r.get('model_version') != 'failed']),
            'failed_scrapes': len([r for r in results if r.get('model_version') == 'failed']),
            'avg_processing_time': (time.time() - start_time) / len(articles),
            'errors': [{'error_type': k, 'count': v} for k,v in error_counts.items()],
            'source_stats': [{
                'source': source,
                'success_rate': stats['success'] / stats['total']
            } for source, stats in source_stats.items()]
        }

        return_variable = {
            'results': results,
            'metrics': metrics
        }

        return return_variable

    @task
    def update_sentiment_results(results: list):
        """Update the sentiment results in the article_sentiments table."""
        results = results['results']
        logger.info(f"Uploading sentiment results: {results}")
        try:
            if results:
                df = pd.DataFrame(results)
                client = bigquery.Client()
            table_id = f"{PROJECT_ID}.{ARTICLES_DATASET}.article_sentiments"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND"
            )
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            
            logger.info(f"Successfully uploaded {len(results)} sentiment analysis results")
        except Exception as e:
            logger.exception(f"Failed to upload sentiment results: {e}")


    @task
    def update_sentiment_metrics(metrics: dict):
        """Update the sentiment metrics in the processing_metrics table."""
        # Upload metrics
        metrics = metrics['metrics']
        logger.info(f"Uploading metrics: {metrics}")
        try:
            client = bigquery.Client()
            table_id = f"{PROJECT_ID}.{ARTICLES_DATASET}.processing_metrics"
            
            df = pd.DataFrame([metrics])
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND"
            )
            
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result()
            logger.info("Successfully uploaded processing metrics")
        except Exception as e:
            logger.exception(f"Failed to upload metrics: {e}")
    

    @task
    def aggregate_hourly_metrics():
        """Aggregate hourly metrics."""
        client = bigquery.Client()
        query = """
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{ARTICLES_DATASET}.hourly_metrics` AS
        WITH hourly_sentiment AS (
            SELECT 
                DATETIME_TRUNC(timestamp, HOUR) as hour,
                AVG(overall_sentiment) as avg_sentiment,
                AVG(CASE WHEN sentiment_aspects.price.relevant THEN sentiment_aspects.price.sentiment END) as price_sentiment,
                COUNT(*) as article_count
            FROM `{PROJECT_ID}.{ARTICLES_DATASET}.article_sentiments`
            GROUP BY 1
        )
        SELECT 
            s.*,
            CAST(p.price AS FLOAT64) as btc_price
        FROM hourly_sentiment s
        LEFT JOIN `{PROJECT_ID}.{PRICING_DATASET}.raw` p
        ON s.hour = DATETIME_TRUNC(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', p.timestamp), hour)
        ORDER BY hour
        """
        client.query(query)

    # Define task dependencies
    articles = get_unanalyzed_articles()
    batch_results = analyze_batch(articles)
    sentiment_results = update_sentiment_results(batch_results)
    sentiment_metrics = update_sentiment_metrics(batch_results)
    aggregate_metrics = aggregate_hourly_metrics()
    
    # Set task order
    wait_for_scraping >> articles >> batch_results >> [sentiment_results, sentiment_metrics] >> aggregate_metrics

sentiment_analysis() 