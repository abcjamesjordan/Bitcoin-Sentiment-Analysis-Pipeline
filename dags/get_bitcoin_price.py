from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

@dag(
    dag_id='get_bitcoin_price',
    default_args={
        # 'owner': 'airflow',
        'start_date': days_ago(1),
        # 'retries': 1,
    },
    schedule_interval='@hourly',
    catchup=False,
)

def get_bitcoin_price():

    @task
    def get_BTC_price():
        import requests
        try:
            # URL for Coinbase API to get Bitcoin price in USD
            url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
            response = requests.get(url)
            response.raise_for_status()
            return response.json()['data']['amount']
        except requests.RequestException as e:
            print(f"An error occurred while fetching the Bitcoin price: {e}")
            return None

    @task()
    def upload_to_bigquery(price):
        import google.cloud.bigquery as bigquery
        from datetime import datetime

        DESTINATION = 'news-api-421321.pricing.raw'
        if price:
            with bigquery.Client() as client:
                table_id = DESTINATION
                rows_to_insert = [
                    {"price": str(price), "timestamp": datetime.now().isoformat()}
                ]
                errors = client.insert_rows_json(table_id, rows_to_insert)
                if errors:
                    print(f"Encountered errors while inserting rows: {errors}")
                else:
                    print("New row inserted successfully.")
        else:
            print("No price data to upload.")
        

    # This line defines the task sequence and dependency
    upload_to_bigquery(get_BTC_price())

# Instantiate the DAG
get_bitcoin_price_dag = get_bitcoin_price()