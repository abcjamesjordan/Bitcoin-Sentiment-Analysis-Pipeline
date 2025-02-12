# The streamlit app will generate a chart of the average sentiment vs the price of bitcoin over the last 7 days.

# To run this file, you need to set the environment variable GOOGLE_APPLICATION_CREDENTIALS to the path of the service account key file.
# To run this file run: streamlit run streamlit_idea.py

import os
import streamlit as st
import pandas as pd
from google.cloud import bigquery
import plotly.graph_objects as go
from plotly.subplots import make_subplots

PROJECT_ID = "news-api-421321"
ARTICLES_DATASET = "articles"

# Set environment variables
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/srv/github/airflow/config/news-api-421321-3a5c418f3870.json"


def load_data() -> pd.DataFrame:
    """Load hourly metrics data from BigQuery for the last 7 days.
    
    Returns:
        pd.DataFrame: DataFrame containing hourly metrics including price and sentiment data
    """
    query = f"""
    SELECT * FROM `{PROJECT_ID}.{ARTICLES_DATASET}.hourly_metrics`
    WHERE hour >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    ORDER BY hour DESC
    """
    
    try:
        df = pd.read_gbq(
            query,
            project_id=PROJECT_ID,
            progress_bar_type=None  # Cleaner output in Streamlit
        )
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error

def main():
    st.title("Bitcoin Sentiment vs Price Analysis")
    
    df = load_data()
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Price line
    fig.add_trace(go.Scatter(x=df['hour'], y=df['btc_price'], name="BTC Price"), secondary_y=True)
    
    # Sentiment bars
    fig.add_trace(go.Bar(x=df['hour'], y=df['avg_sentiment'], name="Overall Sentiment"), secondary_y=False)
    
    # Article count as bubble size
    fig.add_trace(go.Scatter(x=df['hour'], y=df['price_sentiment'], 
                           mode='markers',
                           marker=dict(size=df['article_count']*5),
                           name="Price Sentiment (bubble size = article count)"))

    st.plotly_chart(fig)

if __name__ == "__main__":
    main()