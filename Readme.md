# Bitcoin News Sentiment Analysis Pipeline

A production-ready data engineering project that demonstrates practical experience with ETL pipeline development, cloud services integration, and real-time data processing. Built using industry-standard tools like Apache Airflow, Google BigQuery, and modern APIs, this system processes Bitcoin-related news and price data to deliver actionable insights.

## Technology Stack

- **Orchestration**: Apache Airflow
- **Storage**: Google BigQuery
- **Data Sources**: NewsAPI, Coinbase API
- **Processing**: Python ecosystem (pandas, trafilatura, selenium)
- **Future**: Google Gemini API integration, dbt implementation for transformations

## Core Features

### Data Collection & Processing
- **Bitcoin Price Data**: Automated hourly price collection from Coinbase API
- **News Articles**: Systematic collection of Bitcoin-related news using NewsAPI
  - Comprehensive article metadata and content extraction
  - Configurable multi-source collection
- **Robust Web Scraping**: Implemented with practical fallback strategies
  - Multi-approach content extraction using requests, trafilatura, and Selenium
  - Error handling and automatic retries
  - Source-specific optimizations

### Infrastructure
- **Google BigQuery Integration**: Structured data storage solution
  - Organized schema for news articles, price data, and metadata
  - Optimized for analytical queries
- **Pipeline Architecture**: Built with real-world considerations
  - Configurable concurrency for resource management
  - Error handling with recovery mechanisms
  - Cost-effective cloud resource utilization

## Planned Enhancements

1. **Pipeline Optimization**
   - Streamlined article content extraction
   - Enhanced monitoring coverage
   - Performance optimization

2. **Operational Reliability**
   - Pipeline health metrics and alerts
   - Automated testing and deployment
   - Infrastructure as Code using Terraform

3. **AI Integration**
   - Sentiment analysis using Google's Gemini API
   - Historical sentiment tracking
   - Pattern recognition capabilities

4. **Analytics & Data Transformation**
   - dbt implementation for modular transformations
   - Sentiment vs. price correlation models
   - Trend analysis and visualization
   - Historical pattern analysis
   - Reusable analytics building blocks

## Technical Implementation

- **Data Quality**: Systematic validation and monitoring
- **Code Structure**: Type-annotated, tested, and documented
- **Security**: Standard credential and API key management
- **Architecture**: Component-based design for maintainability
- **Performance**: Designed for efficient resource utilization

## Project Structure

Organized into logical components:
- Data collection DAGs
- Content processing modules
- Storage operations
- Analytics framework (in development)

Note: This project is actively maintained and worked on based on real-world requirements and feedback.