# Dockerfile
FROM python:3.9-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    AIRFLOW_HOME=/app/airflow

# Use a reliable mirror
RUN sed -i 's|http://deb.debian.org|http://deb.debian.org|' /etc/apt/sources.list && \
    apt-get update --fix-missing && \
    apt-get install -y \
    openjdk-11-jdk \
    curl \
    make && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Spark
ENV SPARK_VERSION=3.4.0
RUN curl -O https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz
ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop3
ENV PATH=$SPARK_HOME/bin:$PATH

# Install Python dependencies
COPY pyproject.toml poetry.lock ./
RUN pip install poetry && poetry install --no-dev

# Copy project files
WORKDIR /app
COPY . .

# Expose ports
EXPOSE 8080 8081

# Default command
CMD ["make", "start-airflow"]