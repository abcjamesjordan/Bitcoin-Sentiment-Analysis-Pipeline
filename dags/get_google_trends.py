# Standard library imports
from datetime import datetime, timedelta

# Third party imports
import pandas as pd
from pytrends.request import TrendReq

# Framework imports
from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin

# Google Cloud imports
from google.cloud import bigquery

logger = LoggingMixin().log

PROJECT_ID = "news-api-421321"
DATASET = "articles"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=25),
    'start_date': datetime(2025, 1, 1)
}

@dag(
    dag_id='get_google_trends',
    default_args=default_args,
    description='Collects Bitcoin search trends data and stores in BigQuery',
    schedule_interval='0 13 * * *',
    catchup=False,
    tags=['bitcoin', 'google_trends', 'data_ingestion']
)
def get_google_trends_dag():
    """
    DAG that collects daily Bitcoin search interest data from Google Trends.

    The DAG performs two main tasks:
    1. Fetches hourly Bitcoin search volume data for the last 7 days
    2. Uploads the data to BigQuery, replacing the existing table

    Schedule: Runs daily
    Destination: PROJECT_ID.DATASET.trends_data
    Failure Handling: Returns dummy data (value=50) if API fails
    """

    @task()
    def get_trends_data() -> pd.DataFrame:
        """
        Fetches Bitcoin search interest data from Google Trends for the last 7 days.
        
        Returns:
            pd.DataFrame: DataFrame with datetime index and 'bitcoin' column containing search interest values (0-100).
            Falls back to dummy data if the API call fails.
        """
        try:
            pytrends = TrendReq(hl='en-US', retries=2, backoff_factor=2)
            
            # Build payload for the last 7 days
            timeframe = 'now 7-d'
            pytrends.build_payload(['bitcoin'], timeframe=timeframe)
            
            # Get interest over time data
            df = pytrends.interest_over_time()
            
            # Clean up the data
            if 'isPartial' in df.columns:
                df = df.drop('isPartial', axis=1)
            
            # Ensure datetime index
            df = df.reset_index(names=['datetime'])
            
            return df
        except Exception as e:
            logger.warning(f"Couldn't fetch Google Trends data: {str(e)}")
            # Return dummy data with same structure
            return pd.DataFrame(
                index=pd.date_range(end=pd.Timestamp.now(), periods=168, freq='h'),
                data={'bitcoin': 50}  # Neutral value
            )


    @task()
    def upload_to_bigquery(df: pd.DataFrame) -> None:
        """
        Uploads trends data to BigQuery, recreating the table if it exists.
        
        Args:
            df (pd.DataFrame): DataFrame containing trends data with datetime index
            and 'bitcoin' column.
            
        Raises:
            AirflowSkipException: If input DataFrame is empty or None
        """
        if df.empty or df is None:
            logger.warning("No data to upload to BigQuery")
            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException("No data to upload to BigQuery")
        
        client = bigquery.Client()
        DESTINATION = f'{PROJECT_ID}.{DATASET}.trends_data'
        client.delete_table(DESTINATION, not_found_ok=True)
        logger.info(f"Deleted table {DESTINATION}")
        client.create_table(DESTINATION)
        logger.info(f"Created table {DESTINATION}")

        schema = [
            bigquery.SchemaField("datetime", "DATETIME"),
            bigquery.SchemaField("bitcoin", "FLOAT")
        ]
        client = bigquery.Client()
        DESTINATION = f'{PROJECT_ID}.{DATASET}.trends_data'
        try:
            job_config = bigquery.LoadJobConfig(schema=schema)
            job = client.load_table_from_dataframe(df, DESTINATION, job_config=job_config)
            job.result()
            logger.info(f"Uploaded {len(df)} records to {DESTINATION}")
        except Exception as e:
            logger.error(f"Failed to upload dataframe: {e}")


    # Define task dependencies
    trends_data = get_trends_data()
    upload_to_bigquery(trends_data)

# Instantiate the DAG
dag = get_google_trends_dag()
