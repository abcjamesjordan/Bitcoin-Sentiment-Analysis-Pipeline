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
import json

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

@st.cache_data(ttl="3600")  # Cache for 1 hour
def load_processing_metrics() -> pd.DataFrame:
    """Load processing metrics data from BigQuery.
    
    Returns:
        pd.DataFrame: DataFrame containing processing metrics data
    """
    query = f"""
    WITH daily_metrics AS (
        SELECT 
            DATE(date) as date,
            SUM(total_articles) as total_articles,
            SUM(successful_scrapes) as successful_scrapes,
            SUM(failed_scrapes) as failed_scrapes,
            AVG(avg_processing_time) as avg_processing_time
        FROM `{PROJECT_ID}.{ARTICLES_DATASET}.processing_metrics`
        GROUP BY date
    ),
    error_metrics AS (
        SELECT 
            DATE(date) as date,
            error.error_type,
            SUM(error.count) as error_count
        FROM `{PROJECT_ID}.{ARTICLES_DATASET}.processing_metrics`
        LEFT JOIN UNNEST(errors) as error
        WHERE error IS NOT NULL
        GROUP BY date, error.error_type
    ),
    source_metrics AS (
        SELECT 
            DATE(date) as date,
            source_stat
        FROM `{PROJECT_ID}.{ARTICLES_DATASET}.processing_metrics`
        LEFT JOIN UNNEST(source_stats) as source_stat
        WHERE source_stat IS NOT NULL
    ),
    error_rollup AS (
        SELECT 
            date,
            TO_JSON_STRING(
                ARRAY_AGG(
                    STRUCT(error_type, error_count as count)
                    ORDER BY error_count DESC
                )
            ) as error_summary
        FROM error_metrics
        GROUP BY date
    ),
    source_rollup AS (
        SELECT
            date,
            TO_JSON_STRING(
                ARRAY_AGG(
                    source_stat
                )
            ) AS source_stats_summary
        FROM source_metrics
        GROUP BY date
    )
    SELECT 
        m.date,
        m.total_articles,
        m.successful_scrapes,
        m.failed_scrapes,
        m.avg_processing_time,
        e.error_summary,
        s.source_stats_summary
    FROM daily_metrics m
    LEFT JOIN error_rollup e ON m.date = e.date
    LEFT JOIN source_rollup s ON m.date = s.date
    GROUP BY 
        m.date,
        m.total_articles,
        m.successful_scrapes,
        m.failed_scrapes,
        m.avg_processing_time,
        e.error_summary,
        s.source_stats_summary
    ORDER BY m.date DESC
    LIMIT 30
    ;
    """
    
    try:
        df = pandas_gbq.read_gbq(
            query,
            project_id=PROJECT_ID,
            credentials=credentials
        )
        return df
    except Exception as e:
        st.error(f"Error loading processing metrics: {str(e)}")
        return pd.DataFrame()

def main():
    st.title("Bitcoin Sentiment vs Price Analysis")

    st.write(
        """
        Welcome to the project dashboard! You likely came here from my [Github repo](https://github.com/abcjamesjordan/Bitcoin-Sentiment-Analysis-Pipeline), but if not, welcome! 

        Here, we explore the relationship between market sentiment and Bitcoin's price movements. Dive in to discover how news sentiment, search trends, and fear & greed indicators can provide valuable insights into potential market trends. And keep an eye on the data pipeline to ensure the data is being processed correctly.

        All of the code for this project can be found [here](https://github.com/abcjamesjordan/Bitcoin-Sentiment-Analysis-Pipeline).

        If you have any questions, please reach out to me on [LinkedIn](https://www.linkedin.com/in/abcjamesjordan/).
        """
    )

    # Add a header for the first section
    st.subheader("Price vs Sentiment Overview")

    st.write("""
    Analysis of Bitcoin (BTC) price and news sentiment over the last week.
    
    BTC price (blue line) is overlaid with average news sentiment (green bars).

    Sentiment scores range from 0 (negative) to 1 (positive).
    Sentiment scores are derived from an automated AI powered analysis (Google Gemini AI), providing a performant and scalable solution.
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
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Sentiment Correlation Analysis")
    st.write("""
    Bitcoin price vs. sentiment correlation, with points colored by time (darker = recent). The red dashed line indicates the trend.

    This visualization is insightful for understanding market psychology and potential price movements:
    - Strong positive correlation suggests sentiment drives price action

    - Divergences (when sentiment and price move in opposite directions) often precede major trend reversals

    - Clusters show periods of market consensus while outliers highlight unusual events

    - The time-based coloring helps identify if these patterns are strengthening or weakening recently
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
        height=600,
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )
    
    # Update axes styling
    scatter_fig.update_xaxes(gridcolor='rgba(255,255,255,0.1)', showgrid=True, color='white')
    scatter_fig.update_yaxes(gridcolor='rgba(255,255,255,0.1)', showgrid=True, color='white')
    
    st.plotly_chart(scatter_fig, use_container_width=True)

    # Add this after your existing charts
    trends_df = get_trends_data()
    
    st.subheader("Search Interest Analysis")
    st.write("""
    Bitcoin's Google Search interest against its price. Search trends often act as a leading indicator of price movements:
    
    - Search spikes could precede major price moves

    - Low search interest during price increases may signal limited participation

    - High search volume during price drops could indicate panic or capitulation

    - The relationship helps gauge market cycle phases - early bull markets often see rising prices before search interest catches up
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
        height=600,
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )
    
    # Update axes styling with separate labels
    trends_fig.update_xaxes(
        showgrid=True
    )
    trends_fig.update_yaxes(
        title_text="Search Interest",
        showgrid=True,
        secondary_y=False
    )
    trends_fig.update_yaxes(
        title_text="BTC Price ($)",
        showgrid=True,
        secondary_y=True
    )
    
    st.plotly_chart(trends_fig, use_container_width=True)

    st.subheader("Fear & Greed Index Analysis")
    st.write("""
    The Fear & Greed Index combines multiple indicators to gauge market sentiment in a simple and quantifiable way:
    
    - Price Volatility (25%): Higher volatility typically indicates fear

    - Price Momentum (25%): Strong upward momentum suggests greed

    - News Sentiment (35%): Overall sentiment from news and social media

    - Search Trends (15%): Google search volume as a proxy for retail interest
    
    Trading implications:
    - Extreme fear (0-25) often presents buying opportunities

    - Extreme greed (75-100) suggests potential market tops

    - Divergences between price and sentiment can signal trend reversals

    - The index works best as a contrarian indicator - be fearful when others are greedy
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
        height=600,
        showlegend=True,
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )
    
    # Update axes
    fg_fig.update_xaxes(
        showgrid=True
    )
    fg_fig.update_yaxes(
        title_text="Fear & Greed Index",
        range=[0, 100],
        showgrid=True,
        secondary_y=False,
        ticktext=['Extreme Fear', 'Fear', 'Neutral', 'Greed', 'Extreme Greed'],
        tickvals=[10, 30, 50, 70, 90]
    )
    fg_fig.update_yaxes(
        title_text="BTC Price ($)",
        showgrid=True,
        secondary_y=True
    )
    
    st.plotly_chart(fg_fig, use_container_width=True)

    # Add new Processing Statistics section
    st.header("Data Processing Pipeline Analytics")
    st.write("""
    Welcome to the data pipeline section! Here's what you'll find here:
    
    - Collection Success Stories: See how well we're gathering articles each day and how the success rate is trending
    - Speed Check: Track how quickly we're processing articles and spot any performance gaps
    - Problem Spotting: Keep an eye on any hiccups in our system and where they're happening
    - News Source Report Card: Discover which news sources are our low performers
    - Article Count Tracker: Watch our growing collection of processed articles day by day
    
    These insights help keep the sentiment analysis pipeline running smoothly, ensuring the most accurate market insights possible!
    """)

    # Load processing metrics data
    metrics_df = load_processing_metrics()
    
    if not metrics_df.empty:
        # 1. Daily Success vs. Failure Rates
        success_fail_fig = go.Figure()
        success_fail_fig.add_trace(
            go.Bar(
                x=metrics_df['date'],
                y=metrics_df['successful_scrapes'],
                name='Successful Scrapes',
                marker_color='#2ecc71'
            )
        )
        success_fail_fig.add_trace(
            go.Bar(
                x=metrics_df['date'],
                y=metrics_df['failed_scrapes'],
                name='Failed Scrapes',
                marker_color='#e74c3c'
            )
        )
        success_fail_fig.update_layout(
            title="Daily Success vs. Failure Rates",
            xaxis_title="Date",
            yaxis_title="Number of Scrapes",
            height=400,
            barmode='stack'
        )
        st.plotly_chart(success_fail_fig, use_container_width=True)

        # 2. Average Processing Time Trend
        processing_time_fig = go.Figure()
        processing_time_fig.add_trace(
            go.Scatter(
                x=metrics_df['date'],
                y=metrics_df['avg_processing_time'],
                name='Avg Processing Time',
                line=dict(color='#3498db', width=2),
                fill='tozeroy'
            )
        )
        processing_time_fig.update_layout(
            title="Average Processing Time Trend",
            xaxis_title="Date",
            yaxis_title="Processing Time (seconds)",
            height=400
        )
        st.plotly_chart(processing_time_fig, use_container_width=True)

        # 3. Error Type Distribution
        # Parse error_summary JSON string into a list of dictionaries
        error_data = []
        for _, row in metrics_df.iterrows():
            if row['error_summary']:
                errors = json.loads(row['error_summary'])
                for error in errors:
                    error_data.append({
                        'date': row['date'],
                        'error_type': error['error_type'],
                        'count': error['count']
                    })
        
        if error_data:
            error_df = pd.DataFrame(error_data)
            error_summary = error_df.groupby('error_type')['count'].sum().reset_index()

            error_fig = go.Figure()
            error_fig.add_trace(
                go.Bar(
                    x=error_summary['error_type'],
                    y=error_summary['count'],
                    marker_color='#e74c3c'
                )
            )
            error_fig.update_layout(
                title="Error Type Distribution",
                xaxis_title="Error Type",
                yaxis_title="Number of Occurrences",
                height=400
            )
            st.plotly_chart(error_fig, use_container_width=True)

        # 4. Source Success Rate Analysis
        source_data = []
        for _, row in metrics_df.iterrows():
            if row['source_stats_summary']:
                try:
                    source_stats = json.loads(row['source_stats_summary'])
                    for stat in source_stats:
                        source_data.append({
                            'date': row['date'],
                            'source': stat['source'],
                            'success_rate': stat['success_rate']
                        })
                except json.JSONDecodeError as e:
                    st.error(f"JSON decoding error: {e}")
                    continue

        if source_data:
            source_df = pd.DataFrame(source_data)
            # Calculate average success rate per source
            source_summary = source_df.groupby('source')['success_rate'].mean().reset_index()
            # Filter to show only sources with non-zero success rates
            source_summary = source_summary[source_summary['success_rate'] > 0]
            # Sort by success rate descending
            source_summary = source_summary.sort_values('success_rate', ascending=True)
            # Take top 20 sources
            source_summary = source_summary.head(20)

            # Define color scale based on success rate
            colors = ['#e74c3c' if rate < 0.5 else '#f1c40f' if rate < 0.75 else '#2ecc71' for rate in source_summary['success_rate']]

            source_fig = go.Figure()
            source_fig.add_trace(
                go.Bar(
                    x=source_summary['source'],
                    y=source_summary['success_rate'] * 100,  # Convert to percentage
                    marker_color=colors
                )
            )
            source_fig.update_layout(
                title="Bottom 20 Sources by Average Success Rate",
                xaxis_title="Source",
                yaxis_title="Success Rate (%)",
                height=500,
                xaxis_tickangle=-45,
                yaxis=dict(range=[0, 100])
            )
            st.plotly_chart(source_fig, use_container_width=True)
        else:
            st.warning("No source data available to display.")

        # 5. Total Articles Processed Over Time
        total_articles_fig = go.Figure()
        total_articles_fig.add_trace(
            go.Scatter(
                x=metrics_df['date'],
                y=metrics_df['total_articles'],
                name='Total Articles',
                line=dict(color='#9b59b6', width=2),
                fill='tozeroy'
            )
        )
        total_articles_fig.update_layout(
            title="Total Articles Processed Over Time",
            xaxis_title="Date",
            yaxis_title="Number of Articles",
            height=400
        )
        st.plotly_chart(total_articles_fig, use_container_width=True)
    else:
        st.warning("No processing metrics data available.")

if __name__ == "__main__":
    main()