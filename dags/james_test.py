from datetime import datetime, timedelta
from airflow.decorators import task, dag

default_args = {
    'start_date': datetime(2024, 5, 3),
    # 'email': ['your_email@example.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

@task
def my_test_function(execution_date: datetime, logical_date: datetime):
    TODAY = logical_date
    print(f"TODAY: {TODAY}")


@dag(
        default_args=default_args,
        catchup=False
    )
def james_test():
    run_this = my_test_function()

james_test()