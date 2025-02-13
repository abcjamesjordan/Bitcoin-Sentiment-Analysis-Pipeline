# The streamlit app will generate a chart of the average sentiment vs the price of bitcoin over the last 7 days.

# To run this file, you need to set the environment variable GOOGLE_APPLICATION_CREDENTIALS to the path of the service account key file.
# To run this file run: streamlit run streamlit_idea.py

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.oauth2 import service_account

PROJECT_ID = "news-api-421321"
ARTICLES_DATASET = "articles"

# Remove the os.environ line and add credentials setup
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

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
            credentials=credentials,
            progress_bar_type=None  # Cleaner output in Streamlit
        )
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error

def main():
    st.title("Bitcoin Sentiment vs Price Analysis")
    
    df = load_data()
    
    # Create subplots with 2 rows
    fig = make_subplots(
        rows=2, 
        cols=1,
        row_heights=[0.7, 0.3],
        specs=[[{"secondary_y": True}],
               [{"secondary_y": False}]],
        vertical_spacing=0.1
    )
    
    # Price line on main chart - made more prominent
    fig.add_trace(
        go.Scatter(
            x=df['hour'], 
            y=df['btc_price'], 
            name="BTC Price",
            line=dict(width=3, color='#2962FF')  # Thicker, distinctive blue
        ),
        row=1, col=1,
        secondary_y=True
    )
    
    # Sentiment bars on main chart - made semi-transparent
    fig.add_trace(
        go.Bar(
            x=df['hour'], 
            y=df['avg_sentiment'], 
            name="Sentiment Score",
            marker_color='rgba(46, 204, 113, 0.3)',  # Semi-transparent green
            marker_line_color='rgba(46, 204, 113, 0.8)',  # Darker border
            marker_line_width=1
        ),
        row=1, col=1,
        secondary_y=False
    )
    
    # Enhanced heatmap in second row
    fig.add_trace(
        go.Heatmap(
            x=df['hour'],
            y=['Sentiment Volume'],  # Renamed for clarity
            z=[df['avg_sentiment'] * df['article_count']],
            colorscale=[
                [0, 'rgb(255,65,54)'],      # Red for negative
                [0.5, 'rgb(255,255,255)'],  # White for neutral
                [1, 'rgb(46,204,113)']      # Green for positive
            ],
            showscale=True,
            colorbar=dict(
                title=dict(
                    text="Sentiment × Volume",
                    side='right'
                ),
                thickness=15,
                len=0.7
            ),
            hoverongaps=False
        ),
        row=2, col=1
    )
    
    # Update layout with improved styling
    fig.update_layout(
        height=800,
        title_text="Bitcoin Price vs. News Sentiment Analysis",
        title_x=0.5,  # Center title
        showlegend=True,
        hovermode='x unified',
        plot_bgcolor='rgba(17,17,17,0.9)',  # Dark background
        paper_bgcolor='rgb(17,17,17)',      # Dark background
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            font=dict(color='white')
        ),
        font=dict(color='white')  # Make all text white
    )
    
    # Update axes styling with light grid lines
    fig.update_yaxes(
        title_text="Sentiment Score", 
        row=1, col=1, 
        secondary_y=False, 
        gridcolor='rgba(255,255,255,0.1)',
        color='white'
    )
    fig.update_yaxes(
        title_text="BTC Price ($)", 
        row=1, col=1, 
        secondary_y=True, 
        gridcolor='rgba(255,255,255,0.1)',
        color='white'
    )
    fig.update_xaxes(
        gridcolor='rgba(255,255,255,0.1)', 
        showgrid=True,
        color='white'
    )
    
    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()