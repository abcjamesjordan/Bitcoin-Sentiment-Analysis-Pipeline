# Standard library imports
import io
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# Third-party imports
import pytz
from newsapi import NewsApiClient
import pandas as pd
from google.cloud import bigquery
from newsapi.newsapi_exception import NewsAPIException
# Airflow imports
from airflow.decorators import dag, task
from airflow.models import Variable

# API Key retrieval
api_key = Variable.get('NEWS_API_KEY')
newsapi = NewsApiClient(api_key=api_key)

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Constants
class NewsAPIConfig:
    TOPIC = 'bitcoin'
    PROJECT = Variable.get('GCP_PROJECT_ID')
    DATASET = Variable.get('ARTICLES_DATASET')
    DESTINATION = f'{PROJECT}.{DATASET}.raw'
    LOOKBACK_DAYS = 1
    LANGUAGE = 'en'
    SORT_BY = 'publishedAt' # 'relevancy'
    PAGE_SIZE = 100
    PAGE = 1
    SCHEMA = [
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("author", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("publishedAt", "STRING"),
        bigquery.SchemaField("uploadedAt", "STRING")
    ]

@task()
def extract_articles(logical_date: datetime) -> Optional[Dict[str, Any]]:
    today = logical_date.replace(tzinfo=pytz.UTC)
    from_date = today - timedelta(days=NewsAPIConfig.LOOKBACK_DAYS)

    try:
        return newsapi.get_everything(
            q=NewsAPIConfig.TOPIC,
            from_param=from_date.strftime("%Y-%m-%d"),
            to=today.strftime("%Y-%m-%d"),
            language=NewsAPIConfig.LANGUAGE,
            sort_by=NewsAPIConfig.SORT_BY,
            page=NewsAPIConfig.PAGE,
            page_size=NewsAPIConfig.PAGE_SIZE
        )
    except NewsAPIException as e:
        if any(error in str(e) for error in ["rateLimited", "429", "maximumResultsReached"]):
            logging.warning("NewsAPI limit reached (100 articles/day for free tier). Stopping execution.")
            return None
        raise

@task
def prepare_dataframe(articles: Dict[str, Any], data_interval_end: datetime) -> pd.DataFrame:
    if not articles or 'articles' not in articles:
        return pd.DataFrame()  # Return empty DataFrame instead of potential error
        
    df = pd.DataFrame([{
        'source': article['source']['name'],
        'title': article['title'],
        'author': article.get('author'),  # Use get() to handle missing fields
        'description': article.get('description'),
        'url': article['url'],
        'publishedAt': article['publishedAt'],
        'uploadedAt': data_interval_end
    } for article in articles['articles']])

    # Format dates in one pass
    for col in ['publishedAt', 'uploadedAt']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return df

@task
def deduplicate_articles(df):
    return df.drop_duplicates(subset=['url'])

@task(trigger_rule="all_success")
def confirm_extract(articles):
    if articles is None:
        logging.info("No articles to process - likely due to rate limiting")
        return None
    logging.info(f"Successfully extracted articles")
    logging.info(f"Articles type: {type(articles)}")
    return articles

@task(trigger_rule="all_success")
def confirm_transform(df):
    if df is None:
        logging.error("Failed to transform dataframe")
        raise Exception("Failed to transform dataframe")
    else:
        logging.info(f"Successfully transformed df")
        logging.info(f"Dataframe df type: {type(df)}")
        logging.info(f"{df.head()}")
        return df

@task(trigger_rule="all_success")
def load_data(df):
    if df.empty:
        logging.info("No data to load")
        return
    else:
        logging.info(f"Dataframe: {df.head()}")
        logging.info(f"Dataframe columns: {df.columns}")
        logging.info(f"Dataframe shape: {df.shape}")

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=NewsAPIConfig.SCHEMA
    )

    with io.BytesIO() as stream:
        df.to_parquet(stream)
        stream.seek(0)
        job = client.load_table_from_file(
            stream,
            destination=NewsAPIConfig.DESTINATION,
            project=NewsAPIConfig.PROJECT,
            job_config=job_config
        )
        job.result()
    
    logging.info(f"Successfully loaded {len(df)} rows to {NewsAPIConfig.DESTINATION}")

@dag(schedule='1 13 * * *', start_date=datetime(2025, 1, 1), catchup=False)
def newsapi_get_data():
    articles = confirm_extract(extract_articles())
    
    df = prepare_dataframe(
        articles=articles,
        data_interval_end="{{ data_interval_end }}"  # Airflow automatically converts this to datetime
    )
    
    # Dedupe and load
    validated_df = confirm_transform(deduplicate_articles(df))
    load_data(validated_df)

newsapi_get_data()