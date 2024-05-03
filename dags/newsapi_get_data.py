import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

TODAY = datetime.now()
YESTERDAY = TODAY + timedelta(days=-1)
TOPIC = 'bitcoin'
DESTINATION = 'news-api-421321.articles.raw'
PROJECT = 'news-api-421321'

@dag(start_date=datetime(2021, 12, 1), catchup=False)
def newsapi_get_data():
    @task(task_id="extract", retries=2)
    def extract_articles():
        from newsapi import NewsApiClient

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
            logger.error(f"Failed to fetch articles")
            return None
    
    @task(task_id="confirm")
    def confirm_extract(articles):
        if articles is None:
            logger.error("Failed to extract articles")
            logger.error(f"articles is None: {type(articles)}")
            raise Exception("Failed to extract articles")
        else:
            logger.info(f"Successfully extracted articles")
            logger.info(f"Articles type: {type(articles)}")
        logger.info("End of pipeline..")
    
    @task(task_id="transform")
    def transform_articles(articles):
        import pandas as pd

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

        # Format 'publishedAt' and 'uploadedAt' to desired formats
        df['publishedAt'] = df['publishedAt'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['uploadedAt'] = df['uploadedAt'].dt.strftime('%Y/%m/%d %H:%M:%S')

        return df
    
    @task(task_id="confirm_transform")
    def confirm_transform(df):
        if df is None:
            logger.error("Failed to transform dataframe")
            logger.error(f"Dataframe df is None: {type(df)}")
            raise Exception("Failed to transform dataframe")
        else:
            logger.info(f"Successfully transformed df")
            logger.info(f"Dataframe df type: {type(df)}")
            logger.info(f"{df.head()}")
        logger.info("End of pipeline..")


    all_articles = extract_articles()
    confirm_extract(all_articles)
    df = transform_articles(all_articles)
    confirm_transform(df)


newsapi_get_data()