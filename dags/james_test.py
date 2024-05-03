from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2024, 5, 3),
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(default_args=default_args, schedule_interval='* * * * *')
def james_test():
    @task
    def my_test_function():
        print(datetime.today().strftime('%A'))

    run_this = my_test_function()

james_test = james_test()