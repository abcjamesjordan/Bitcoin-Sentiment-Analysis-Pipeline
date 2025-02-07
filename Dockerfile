FROM apache/airflow:latest

USER root

# Install Chrome and dependencies for Ubuntu Server
RUN apt-get update && apt-get install -y \
    wget \
    gnupg2 \
    apt-transport-https \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    libxshmfence1

# Add Chrome repository and install
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create directory for ChromeDriver
RUN mkdir -p /opt/chromedriver && \
    chown -R airflow:root /opt/chromedriver && \
    chmod -R 777 /opt/chromedriver

USER airflow

COPY requirements.txt /
COPY --chown=airflow:root dags/*.py /opt/airflow/dags
COPY config/news-api-421321-3a5c418f3870.json config/news-api-421321-3a5c418f3870.json

ENV GOOGLE_APPLICATION_CREDENTIALS="/opt/airflow/config/news-api-421321-3a5c418f3870.json"
ENV CHROMEDRIVER_PATH="/opt/chromedriver/chromedriver"

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

USER airflow
