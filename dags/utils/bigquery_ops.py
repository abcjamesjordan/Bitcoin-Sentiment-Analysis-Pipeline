from google.cloud import bigquery
import pandas as pd
import logging

def get_domain_strategies(project_id: str, dataset: str) -> dict:
    """Fetch domain-specific scraping strategies from BigQuery."""
    client = bigquery.Client()
    query = f"""
    SELECT domain, strategies
    FROM `{project_id}.{dataset}.domain_strategies`
    """
    df = client.query(query).to_dataframe()
    return df.set_index('domain')['strategies'].apply(lambda x: [x]).to_dict()

def update_domain_strategies(project_id: str, dataset: str, new_strategies: dict):
    """Upload new successful domain strategies to BigQuery."""
    if not new_strategies:
        return
    
    df = pd.DataFrame([
        {'domain': domain, 'strategies': strategies}
        for domain, strategies in new_strategies.items()
    ])
    
    client = bigquery.Client()
    table_id = f'{project_id}.{dataset}.domain_strategies'
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("domain", "STRING"),
            bigquery.SchemaField("strategies", "STRING"),
        ],
        write_disposition="WRITE_APPEND",
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

def extract_urls(project_id: str, dataset: str, url_limit: int, time_filter: bool = False, context = None) -> list:
    from .url_processor import is_valid_url

    if context:
        conf = context['dag_run'].conf
        source = conf.get('source', 'error')
        if source == 'error':
            raise ValueError("Invalid source: {source}")
    else:
        raise ValueError("Invalid context: {context}")

    client = bigquery.Client()

    if source == 'newsapi':
        SOURCE_LOCATION = f'{project_id}.{dataset}.raw'
        CONDITIONAL_LOCATION = f'{project_id}.{dataset}.url_text'
        BLACKLIST = f'{project_id}.{dataset}.blacklisted_urls'

        time_filter_query = ""
        if time_filter:
            time_filter_query = """
                AND PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', uploadedAt) >= 
                    TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
            """
        
        query = f"""
        WITH blacklisted AS (
            SELECT url FROM `{BLACKLIST}`
        ),
        processed AS (
            SELECT url FROM `{CONDITIONAL_LOCATION}`
        )
        SELECT DISTINCT url
        FROM `{SOURCE_LOCATION}`
        WHERE url NOT IN (SELECT url FROM blacklisted)
        AND url NOT IN (SELECT url FROM processed)
        {time_filter_query}
        LIMIT {url_limit}
        """
    elif source == 'mastodon':
        SOURCE_LOCATION = f'{project_id}.mastodon.raw_toots'
        CONDITIONAL_LOCATION = f'{project_id}.{dataset}.url_text'
        BLACKLIST = f'{project_id}.{dataset}.blacklisted_urls'

        query = f"""
        WITH blacklisted AS (
            SELECT url FROM `{BLACKLIST}`
        ),
        processed AS (
            SELECT url FROM `{CONDITIONAL_LOCATION}`
        ),
        toot_urls AS (
            SELECT DISTINCT
                case
                    when card_url <> '' then
                        card_url
                    else
                        url,
                end as url
            FROM `{SOURCE_LOCATION}`
            WHERE created_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
            AND language = 'en'
        )
        SELECT DISTINCT url
        FROM toot_urls
        WHERE url NOT IN (SELECT url FROM blacklisted)
        AND url NOT IN (SELECT url FROM processed)
        LIMIT {url_limit}
        """
    else:
        raise ValueError(f"Invalid source: {source}")
    
    query_job = client.query(query)
    results = query_job.result()

    return [row.url for row in results if is_valid_url(row.url)]

def upload_processed_data(project_id: str, dataset: str, data: dict):
    if not data["processed_data"]:
        logging.warning("No data was found to upload to BigQuery")
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException("No data was found to upload to BigQuery")
    
    df = pd.DataFrame(data["processed_data"])
    
    df['status_code'] = df['status_code'].astype(str)
    df['retry_count'] = df['retry_count'].astype(str)
    df['success'] = df['success'].astype(str)
    df['scraping_method'] = df['scraping_method'].astype(str)
    
    schema = [
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("text", "STRING"), 
        bigquery.SchemaField("status_code", "STRING"),
        bigquery.SchemaField("success", "STRING"),
        bigquery.SchemaField("retry_count", "STRING"),
        bigquery.SchemaField("scraping_method", "STRING")
    ]
    client = bigquery.Client()
    DESTINATION = f'{project_id}.{dataset}.url_text'

    try:
        job_config = bigquery.LoadJobConfig(schema=schema)
        job = client.load_table_from_dataframe(df, DESTINATION, job_config=job_config)
        job.result()
        logging.info(f"Uploaded {len(df)} records to {DESTINATION}")
    except Exception as e:
        raise Exception(f"Failed to upload dataframe: {e}")

def remove_failed_strategies(project_id: str, dataset: str, domains_to_remove: list):
    """Remove failed strategies from BigQuery domain_strategies table."""
    if not domains_to_remove:
        return

    client = bigquery.Client()
    table_id = f'{project_id}.{dataset}.domain_strategies'
    
    query = f"""
    DELETE FROM `{table_id}`
    WHERE domain IN UNNEST(@domains)
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("domains", "STRING", domains_to_remove),
        ]
    )
    
    client.query(query, job_config=job_config).result()
    logging.info(f"Removed failed strategies for domains: {domains_to_remove}") 