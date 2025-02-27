# Bitcoin News Sentiment Analysis Pipeline

![- visual selection(3)](https://github.com/user-attachments/assets/fd59f75b-dc7d-422f-add8-cde4e5fd81a1)

A production-ready data engineering project that demonstrates practical experience with ETL pipeline development, cloud services integration, and real-time data processing. Built using industry-standard tools like Apache Airflow, Google BigQuery, and modern APIs, this system processes Bitcoin-related news and price data to deliver actionable insights.

Data visualizations can be viewed via Steamlit.io: https://ai-sentiment-analysis-pipeline.streamlit.app/

## Technology Stack

- **Orchestration**: Apache Airflow
- **Storage**: Google BigQuery
- **Data Sources**: NewsAPI, Coinbase API, Mastodon API
- **Processing**: Python ecosystem (pandas, trafilatura, selenium)
- **AI/ML**: Google Gemini API for sentiment analysis
- **Visualization**: Streamlit with Plotly
- **Future**: dbt implementation for transformations

## Core Features

### Data Collection & Processing
![Airflow flow](https://github.com/user-attachments/assets/d458b33e-72ac-494d-90b0-dde5ad0c553f)

- **Multi-Source Data Pipeline**:
  - NewsAPI integration for mainstream media
  - Mastodon social scraping with keyword filtering
  - Coinbase API for real-time Bitcoin prices
  - Google Trends integration for search interest

- **Intelligent Web Scraping**:
  - Dynamic scraping strategy detection
  - Domain-specific optimizations
  - Automatic strategy updates
  - Fallback mechanisms:
    - Primary: Trafilatura for clean extraction
    - Secondary: Custom regex patterns
    - Fallback: Selenium for JavaScript content
  - Rate limiting and retry logic
  - Success rate tracking by domain

- **Sentiment Analysis Engine**:
  - Google Gemini 2.0 Flash integration
  - Multi-aspect analysis:
    - Price predictions
    - Adoption trends
    - Regulatory impact
    - Technical developments
  - Confidence scoring
  - Automated batch processing
  - Rate limit handling
  - Error recovery with exponential backoff

- **Data Quality & Processing**:
  - Automated validation pipelines
  - Duplicate detection
  - Content relevance scoring
  - Source reliability metrics
  - Real-time aggregation
  - Historical pattern analysis

### Infrastructure
- **Pipeline Architecture**: Production-grade implementation
  - Modular DAG structure with clear separation of concerns
  - Automated batch processing with configurable limits
  - Comprehensive error handling and recovery
  - Rate limiting and quota management for APIs
  - Source-level success rate tracking
  - Automated retries with exponential backoff

- **BigQuery Integration**: Enterprise-ready data storage
  - Optimized table schemas for analytics
  - Efficient batch processing and updates
  - Real-time aggregation for metrics
  - Tables:
    - `articles.url_text`: Source articles and metadata
    - `articles.article_sentiments`: Sentiment analysis results
    - `articles.processing_metrics`: Batch processing statistics
    - `articles.hourly_metrics`: Aggregated sentiment/price data
    - `pricing.raw`: Bitcoin price timeseries
    - `mastodon.raw`: Social media data

- **Monitoring & Reliability**:
  - Processing metrics and error tracking
  - Source-specific success rate monitoring
  - Automated failure handling and recovery
  - Configurable batch sizes and rate limits
  - Comprehensive logging and diagnostics

### Analytics & Visualization
Check it out on streamlit! 

[https://bitcoin-sentiment-analysis-pipeline-r8iayx28fi7zcg4tapbtqw.streamlit.app/](https://ai-sentiment-analysis-pipeline.streamlit.app/)

- **Interactive Dashboard**: Real-time data visualization
  - Price vs. Sentiment correlation analysis
  - Time-series sentiment tracking
  - Scatter plot with trend analysis
  - Google Trends integration
  - Custom Fear & Greed Index:
    - Price Volatility (25%)
    - Price Momentum (25%)
    - News Sentiment (35%)
    - Search Trends (15%)

- **Technical Analysis**:
  - Rolling sentiment aggregation
  - Price-sentiment divergence detection
  - Market psychology indicators
  - Trend strength visualization
  - Extreme sentiment alerts

- **Data Insights**:
  - Aspect-based sentiment breakdown
  - Source reliability metrics
  - Market consensus tracking
  - Pattern recognition
  - Trading signal generation

## Planned Enhancements

1. **Pipeline Optimization**
   - ✓ Streamlined article content extraction
   - ✓ Enhanced monitoring coverage
   - ✓ Performance optimization

2. **Operational Reliability**
   - ✓ Pipeline health metrics and alerts
   - Automated testing and deployment

3. **AI Integration**
   - ✓ Sentiment analysis using Google's Gemini API
   - ✓ Multi-aspect sentiment tracking
   - ✓ Confidence scoring and validation
   - Historical sentiment aggregation (planned)
   - Pattern recognition capabilities (planned)

4. **Analytics & Data Transformation**
   - dbt implementation for modular transformations
   - ✓ Sentiment vs. price correlation models
   - ✓ Trend analysis and visualization
   - Historical pattern analysis


Note: This project is actively maintained and worked on based on real-world requirements and feedback.
