# Bitcoin News Sentiment Analysis Pipeline
![- visual selection(3)](https://github.com/user-attachments/assets/fd59f75b-dc7d-422f-add8-cde4e5fd81a1)
A production-ready data engineering project that demonstrates practical experience with ETL pipeline development, cloud services integration, and real-time data processing. Built using industry-standard tools like Apache Airflow, Google BigQuery, and modern APIs, this system processes Bitcoin-related news and price data to deliver actionable insights.

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
- **Bitcoin Price Data**: Automated hourly price collection from Coinbase API
- **News Articles**: Multi-source collection strategy
  - NewsAPI integration for mainstream coverage
  - Mastodon social media scraping (hourly bitcoin keyword search)
  - Comprehensive article metadata and content extraction
  - Configurable multi-source collection
- **Robust Web Scraping**: Implemented with practical fallback strategies
  - Multi-approach content extraction using requests, trafilatura, and Selenium
  - Error handling and automatic retries
  - Source-specific optimizations
- **Sentiment Analysis**: Automated analysis using Google's Gemini API
  - Aspect-based sentiment analysis (price, adoption, regulation, technology)
  - Confidence scoring for analysis reliability
  - Automated processing pipeline with error handling
  - Configurable batch processing

### Infrastructure
- **Google BigQuery Integration**: Structured data storage solution
  - Organized schema for news articles, price data, and metadata
  - Optimized for analytical queries and cost effective-storage
- **Pipeline Architecture**: Built with real-world considerations
  - Error handling with recovery mechanisms
  - Cost-effective cloud resource utilization
  - Automated retries and failure handling

### Analytics & Visualization
- **Streamlit Dashboard**: Real-time sentiment analysis visualization
  - Interactive price vs. sentiment correlation charts
  - Sentiment volume heatmap visualization
  - 7-day rolling analysis window
  - Multi-aspect sentiment tracking

## Planned Enhancements

1. **Pipeline Optimization**
   - ✓ Streamlined article content extraction
   - Enhanced monitoring coverage
   - ✓ Performance optimization

2. **Operational Reliability**
   - Pipeline health metrics and alerts
   - Automated testing and deployment
   - Infrastructure as Code using Terraform

3. **AI Integration**
   - ✓ Sentiment analysis using Google's Gemini API
   - ✓ Multi-aspect sentiment tracking
   - ✓ Confidence scoring and validation
   - Historical sentiment aggregation (planned)
   - Pattern recognition capabilities (planned)

4. **Analytics & Data Transformation**
   - dbt implementation for modular transformations
   - Sentiment vs. price correlation models
   - Trend analysis and visualization
   - Historical pattern analysis

## Technical Implementation

- **Data Quality**: Systematic validation and monitoring
- **Code Structure**: Type-annotated, tested, and documented
- **Security**: Standard credential and API key management
- **Architecture**: Component-based design for maintainability
- **Performance**: Designed for efficient resource utilization

## Project Structure

Organized into logical components:
- Data collection DAGs
  - News API article collection
  - Mastodon social scraping
  - Bitcoin price tracking
- Content processing modules
  - Web scraping with fallback strategies
  - Sentiment analysis pipeline
- Storage operations
  - BigQuery integration
  - Schema management
- Analytics framework
  - Streamlit visualization
  - Sentiment analysis dashboard
  - Price correlation tracking

Note: This project is actively maintained and worked on based on real-world requirements and feedback.
