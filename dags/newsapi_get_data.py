import logging, io
from datetime import datetime, timedelta

# Airflow imports
from airflow.decorators import dag, task
from airflow.models import Variable

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

@task()
def extract_articles(logical_date: datetime):
    from newsapi import NewsApiClient

    TODAY = logical_date
    YESTERDAY = TODAY + timedelta(days=-1)
    TOPIC = 'bitcoin'

    api_key = Variable.get('NEWS_API_KEY')
    newsapi = NewsApiClient(api_key=api_key)

    try:
        all_articles = newsapi.get_everything(
            q=TOPIC,
            from_param=YESTERDAY.strftime("%Y-%m-%d"),
            to=TODAY.strftime("%Y-%m-%d"),
            language='en',
            sort_by='relevancy',
            page=1
        )
        return all_articles
    except Exception as e:
        logging.error(f"Failed to fetch articles: {e}")
        raise e

@task()
def transform_articles(articles, logical_date:datetime):
    import pandas as pd

    TODAY = logical_date
    data = []

    for article in articles['articles']:
        data.append({
            'source': article['source']['name'],
            'title': article['title'],
            'author': article['author'],
            'description': article['description'],
            'url': article['url'],
            'publishedAt': article['publishedAt'],
            'uploadedAt': TODAY
        })

    df = pd.DataFrame(data)

    # Convert 'publishedAt' to datetime
    df['publishedAt'] = pd.to_datetime(df['publishedAt'], format='%Y-%m-%dT%H:%M:%SZ')

    # Format 'publishedAt' and 'uploadedAt' to BigQuery formats
    df['publishedAt'] = df['publishedAt'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['uploadedAt'] = df['uploadedAt'].dt.strftime('%Y/%m/%d %H:%M:%S')

    return df

@task(trigger_rule="all_done")
def confirm_extract(articles):
    if articles is None:
        logging.error("Failed to extract articles")
        raise Exception("Failed to extract articles")
    else:
        logging.info(f"Successfully extracted articles")
        logging.info(f"Articles type: {type(articles)}")

@task(trigger_rule="all_done")
def confirm_transform(df):
    if df is None:
        logging.error("Failed to transform dataframe")
        raise Exception("Failed to transform dataframe")
    else:
        logging.info(f"Successfully transformed df")
        logging.info(f"Dataframe df type: {type(df)}")
        logging.info(f"{df.head()}")

@task()
def load_data(df):
    from google.cloud import bigquery

    client = bigquery.Client()

    DESTINATION = 'news-api-421321.articles.raw'
    PROJECT = 'news-api-421321'

    try:
        # Write DataFrame to stream as parquet file; does not hit disk
        with io.BytesIO() as stream:
            # TODO: rewrite this section to use pandas instead of Polars...
            df.to_parquet(stream)
            stream.seek(0)
            job = client.load_table_from_file(
                stream,
                destination=DESTINATION,
                project=PROJECT,
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                ),
            )
        job.result()  # Waits for the job to complete
    except Exception as e:
        raise Exception(f"Failed to upload dataframe: {e}")
    return True

@dag(schedule='1 13 * * *', start_date=datetime(2021, 12, 1), catchup=False)
def newsapi_get_data():
    all_articles = extract_articles()
    confirm_extract(all_articles)
    df = transform_articles(all_articles)
    confirm_transform(df) >> load_data(df)

newsapi_get_data()