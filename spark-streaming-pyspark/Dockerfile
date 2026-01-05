FROM python:3.11-slim-bookworm

WORKDIR /app

# Installer Java 17 et procps
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*

# Configurer JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Installer PySpark et PyMongo
RUN pip install --no-cache-dir pyspark==3.5.0 pymongo==4.6.1

# Copier le script
COPY spark_streaming.py .

# Lancer Spark avec SEULEMENT le package Kafka
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--conf", "spark.sql.streaming.checkpointLocation=/tmp/checkpoint", \
     "/app/spark_streaming.py"]