# The streamlit app will generate a chart of the average sentiment vs the price of bitcoin over the last 7 days.

# To run this file, you need to set the environment variable GOOGLE_APPLICATION_CREDENTIALS to the path of the service account key file.
# To run this file run: streamlit run streamlit_app.py

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.oauth2 import service_account
import numpy as np
import pandas_gbq
from pytrends.request import TrendReq

PROJECT_ID = "news-api-421321"
ARTICLES_DATASET = "articles"

# Remove the os.environ line and add credentials setup
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)

@st.cache_data(ttl="3600")  # Cache for 1 hour
def load_metrics_data() -> pd.DataFrame:
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
        df = pandas_gbq.read_gbq(
            query,
            project_id=PROJECT_ID,
            credentials=credentials
        )
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error

@st.cache_data(ttl="1d")  # Cache for 1 day
def get_trends_data():
    """Load trends data from BigQuery for the last 7 days.
    
    Returns:
        pd.DataFrame: DataFrame containing trends data
    """
    query = f"""
    SELECT * FROM `{PROJECT_ID}.{ARTICLES_DATASET}.trends_data`
    ORDER BY datetime DESC
    """
    
    try:
        df = pandas_gbq.read_gbq(
            query,
            project_id=PROJECT_ID,
            credentials=credentials
        )
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error

@st.cache_data(ttl="3600")  # Cache for 1 hour
def calculate_fear_greed_index(df, trends_df):
    """Calculate the Fear & Greed Index based on multiple indicators."""
    
    # Ensure both DataFrames have timezone-naive datetime indices
    df_index = pd.to_datetime(df['hour']).dt.tz_localize(None)
    trends_df.index = pd.to_datetime(trends_df['datetime']).dt.tz_localize(None)
    
    # Merge trends data with main DataFrame
    merged_df = pd.DataFrame(index=df_index)
    merged_df['btc_price'] = df['btc_price'].values
    merged_df['avg_sentiment'] = df['avg_sentiment'].values
    merged_df['trends'] = trends_df['bitcoin'].reindex(df_index, method='ffill')
    
    # Parameters
    WINDOW = 6  # 6-hour rolling window
    
    # 1. Price Volatility (25%)
    volatility = merged_df['btc_price'].rolling(WINDOW, min_periods=1).std()
    volatility_norm = 100 - (volatility / volatility.max() * 100)
    
    # 2. Price Momentum (25%)
    momentum = merged_df['btc_price'].pct_change(WINDOW) * 100
    momentum_min, momentum_max = momentum.min(), momentum.max()
    momentum_norm = ((momentum - momentum_min) / (momentum_max - momentum_min)) * 100
    
    # 3. Sentiment (35%)
    sentiment_norm = (merged_df['avg_sentiment'] + 1) * 50  # Map -1 to 1 range to 0-100
    
    # 4. Trends (15%)
    # Trends data should already be in 0-100 range
    trends_norm = merged_df['trends']
    
    # Calculate weighted index
    fear_greed_index = (
        0.25 * volatility_norm.fillna(50) +  # Fill NaN with neutral value
        0.25 * momentum_norm.fillna(50) +    # Fill NaN with neutral value
        0.35 * sentiment_norm.fillna(50) +   # Fill NaN with neutral value
        0.15 * trends_norm.fillna(50)        # Fill NaN with neutral value
    )
    
    return fear_greed_index.clip(0, 100)  # Ensure output is between 0 and 100

def main():
    st.title("Bitcoin Sentiment vs Price Analysis")

    st.write("""
    This dashboard analyzes Bitcoin-related news sentiment across multiple sources including mainstream media and Mastodon social posts. 
    
    The chart shows Bitcoin's price (blue line) overlaid with average sentiment scores (green bars) over the past 7 days.

    Data is collected hourly and processed using Google's Gemini API for multi-aspect sentiment analysis covering price predictions, adoption trends, regulatory news, and technological developments.
    """)

    df = load_metrics_data()
    
    # Create single plot instead of subplots
    fig = make_subplots(
        specs=[[{"secondary_y": True}]],
        vertical_spacing=0.1
    )
    
    # Add price line
    fig.add_trace(
        go.Scatter(
            x=df['hour'], 
            y=df['btc_price'], 
            name="BTC Price",
            line=dict(width=3, color='#2962FF')
        ),
        secondary_y=True
    )
    
    # Sentiment bars
    fig.add_trace(
        go.Bar(
            x=df['hour'], 
            y=df['avg_sentiment'], 
            name="Sentiment Score",
            marker_color='rgba(46, 204, 113, 0.3)',
            marker_line_color='rgba(46, 204, 113, 0.8)',
            marker_line_width=1
        ),
        secondary_y=False
    )
    
    # Update layout
    fig.update_layout(
        xaxis_rangeslider_visible=False,
        height=600,  # Reduced height since we removed the heatmap
        title_text="Bitcoin Price vs. News Sentiment",
        title_x=0.5,
        showlegend=True,
        hovermode='x unified',
        plot_bgcolor='rgba(17,17,17,0.9)',
        paper_bgcolor='rgb(17,17,17)',
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            font=dict(color='white')
        ),
        font=dict(color='white')
    )
    
    st.plotly_chart(fig, use_container_width=True)

    st.write("""
    This scatter plot reveals the relationship between Bitcoin price and sentiment scores. Each point represents an hourly data point, 
    with color indicating the time (darker = more recent). The red dashed line shows the overall trend - a positive slope indicates 
    that higher sentiment generally corresponds with higher prices.

    This visualization is crucial for understanding market psychology and potential price movements:
    • Strong positive correlation suggests sentiment drives price action
    • Divergences (when sentiment and price move in opposite directions) often precede major trend reversals
    • Clusters show periods of market consensus while outliers highlight unusual events
    • The time-based coloring helps identify if these patterns are strengthening or weakening recently
    """)

    # Create scatter plot of Sentiment vs Price
    scatter_fig = go.Figure()
    
    # Add scatter plot with color gradient based on date
    scatter_fig.add_trace(
        go.Scatter(
            x=df['avg_sentiment'],
            y=df['btc_price'],
            mode='markers',
            marker=dict(
                size=8,
                color=df['hour'].astype(np.int64) // 10**9,  # Convert datetime to unix timestamp
                colorscale='Viridis',
                showscale=True,
                colorbar=dict(title='Date')
            ),
            text=df['hour'].dt.strftime('%Y-%m-%d %H:%M'),
            name='Price vs Sentiment'
        )
    )
    
    # Add trendline with error handling
    try:
        # Clean data first
        mask = ~(df['avg_sentiment'].isna() | df['btc_price'].isna())
        if mask.any():  # Only create trendline if we have valid data
            x = df.loc[mask, 'avg_sentiment']
            y = df.loc[mask, 'btc_price']
            z = np.polyfit(x, y, 1)
            p = np.poly1d(z)
            scatter_fig.add_trace(
                go.Scatter(
                    x=x,
                    y=p(x),
                    mode='lines',
                    name='Trend',
                    line=dict(color='red', dash='dash')
                )
            )
    except Exception as e:
        st.warning("Couldn't generate trend line due to insufficient data")
    
    # Update layout with dark theme
    scatter_fig.update_layout(
        title="Bitcoin Price vs Sentiment Correlation",
        title_x=0.5,
        xaxis_title="Sentiment Score",
        yaxis_title="BTC Price ($)",
        plot_bgcolor='rgba(17,17,17,0.9)',
        paper_bgcolor='rgb(17,17,17)',
        font=dict(color='white'),
        height=600,
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            font=dict(color='white')
        )
    )
    
    # Update axes styling
    scatter_fig.update_xaxes(gridcolor='rgba(255,255,255,0.1)', showgrid=True, color='white')
    scatter_fig.update_yaxes(gridcolor='rgba(255,255,255,0.1)', showgrid=True, color='white')
    
    st.plotly_chart(scatter_fig, use_container_width=True)

    # Add this after your existing charts
    trends_df = get_trends_data()

    trends_df.to_csv('trends_df.csv', index=False)
    
    st.write("""
    This chart compares Bitcoin's Google Search interest against its price. Search trends often act as a leading indicator of price movements:
    
    • Search spikes typically precede major price moves
    • Low search interest during price increases may signal limited retail participation
    • High search volume during price drops could indicate panic or capitulation
    • The relationship helps gauge market cycle phases - early bull markets often see rising prices before search interest catches up
    """)
    
    # Create Plotly figure for trends data with secondary y-axis
    trends_fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add Google Trends data
    trends_fig.add_trace(
        go.Scatter(
            x=trends_df['datetime'],
            y=trends_df['bitcoin'],
            mode='lines',
            name='Search Interest',
            line=dict(color='#1E88E5')  # Blue for trends
        ),
        secondary_y=False
    )
    
    # Add Bitcoin price data
    trends_fig.add_trace(
        go.Scatter(
            x=df['hour'],
            y=df['btc_price'],
            mode='lines',
            name='BTC Price',
            line=dict(color='#FFC107')  # Gold for bitcoin
        ),
        secondary_y=True
    )
    
    # Update layout with dual y-axes titles
    trends_fig.update_layout(
        title="Bitcoin Google Search Interest vs Price",
        title_x=0.5,
        plot_bgcolor='rgba(17,17,17,0.9)',
        paper_bgcolor='rgb(17,17,17)',
        font=dict(color='white'),
        height=600,
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            font=dict(color='white')
        )
    )
    
    # Update axes styling with separate labels
    trends_fig.update_xaxes(gridcolor='rgba(255,255,255,0.1)', showgrid=True, color='white')
    trends_fig.update_yaxes(
        title_text="Search Interest",
        gridcolor='rgba(255,255,255,0.1)',
        showgrid=True,
        color='white',
        secondary_y=False
    )
    trends_fig.update_yaxes(
        title_text="BTC Price ($)",
        gridcolor='rgba(255,255,255,0.1)',
        showgrid=True,
        color='white',
        secondary_y=True
    )
    
    st.plotly_chart(trends_fig, use_container_width=True)

    st.write("""
    The Fear & Greed Index combines multiple indicators to gauge market sentiment:
    
    • Price Volatility (25%): Higher volatility typically indicates fear
    • Price Momentum (25%): Strong upward momentum suggests greed
    • News Sentiment (35%): Overall sentiment from news and social media
    • Search Trends (15%): Google search volume as a proxy for retail interest
    
    Trading implications:
    • Extreme fear (0-25) often presents buying opportunities
    • Extreme greed (75-100) suggests potential market tops
    • Divergences between price and sentiment can signal trend reversals
    • The index works best as a contrarian indicator - be fearful when others are greedy
    """)

    # Calculate and display Fear & Greed Index
    fear_greed_index = calculate_fear_greed_index(df, trends_df)
    
    # Create Fear & Greed dual-axis visualization
    fg_fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add Bitcoin price line
    fg_fig.add_trace(
        go.Scatter(
            x=df['hour'],
            y=df['btc_price'],
            name="BTC Price",
            line=dict(color='#2962FF', width=2),
        ),
        secondary_y=True
    )
    
    # Add Fear & Greed Index line
    fg_fig.add_trace(
        go.Scatter(
            x=df['hour'],
            y=fear_greed_index,
            name="Fear & Greed Index",
            line=dict(color='#2ecc71', width=2),
        ),
        secondary_y=False
    )
    
    # Add shaded regions for extreme fear and greed
    fg_fig.add_traces([
        # Extreme Fear region (0-25)
        go.Scatter(
            x=df['hour'],
            y=[25] * len(df),
            fill=None,
            mode='lines',
            line=dict(color='rgba(255,0,0,0)'),
            showlegend=False
        ),
        go.Scatter(
            x=df['hour'],
            y=[0] * len(df),
            fill='tonexty',
            mode='lines',
            line=dict(color='rgba(255,0,0,0)'),
            fillcolor='rgba(255,0,0,0.1)',
            name='Extreme Fear'
        ),
        # Extreme Greed region (75-100)
        go.Scatter(
            x=df['hour'],
            y=[100] * len(df),
            fill=None,
            mode='lines',
            line=dict(color='rgba(0,255,0,0)'),
            showlegend=False
        ),
        go.Scatter(
            x=df['hour'],
            y=[75] * len(df),
            fill='tonexty',
            mode='lines',
            line=dict(color='rgba(0,255,0,0)'),
            fillcolor='rgba(0,255,0,0.1)',
            name='Extreme Greed'
        )
    ], secondary_ys=[False, False, False, False])
    
    # Update layout
    fg_fig.update_layout(
        title="Bitcoin Price vs Fear & Greed Index",
        title_x=0.5,
        plot_bgcolor='rgba(17,17,17,0.9)',
        paper_bgcolor='rgb(17,17,17)',
        font=dict(color='white'),
        height=600,
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            font=dict(color='white')
        )
    )
    
    # Update axes
    fg_fig.update_xaxes(
        gridcolor='rgba(255,255,255,0.1)',
        showgrid=True,
        color='white'
    )
    fg_fig.update_yaxes(
        title_text="Fear & Greed Index",
        range=[0, 100],
        gridcolor='rgba(255,255,255,0.1)',
        showgrid=True,
        color='white',
        secondary_y=False,
        ticktext=['Extreme Fear', 'Fear', 'Neutral', 'Greed', 'Extreme Greed'],
        tickvals=[10, 30, 50, 70, 90]
    )
    fg_fig.update_yaxes(
        title_text="BTC Price ($)",
        gridcolor='rgba(255,255,255,0.1)',
        showgrid=True,
        color='white',
        secondary_y=True
    )
    
    st.plotly_chart(fg_fig, use_container_width=True)

if __name__ == "__main__":
    main()