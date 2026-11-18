# Imagem do Airflow com Java 17, PySpark e as bibliotecas do pipeline do Pé na Trilha.
FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && ln -s "/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" /usr/lib/jvm/java-17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17

RUN mkdir -p /opt/ivy /opt/airflow/dados /opt/airflow/relatorios \
    && chown -R airflow:0 /opt/ivy /opt/airflow/dados /opt/airflow/relatorios \
    && chmod -R g+rwX /opt/ivy /opt/airflow/dados /opt/airflow/relatorios

USER airflow
ARG AIRFLOW_VERSION=2.10.5
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

# O MongoDB Spark Connector é baixado na construção da imagem,
# assim a carga não depende de acesso ao Maven Central durante a execução.
ARG MONGO_SPARK_CONNECTOR=org.mongodb.spark:mongo-spark-connector_2.12:10.4.1
ENV MONGO_SPARK_CONNECTOR=${MONGO_SPARK_CONNECTOR} \
    SPARK_JARS_IVY=/opt/ivy \
    PYTHONPATH=/opt/airflow
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[1]').config('spark.jars.packages', '${MONGO_SPARK_CONNECTOR}').config('spark.jars.ivy', '/opt/ivy').getOrCreate().stop()" \
    && chmod -R g+rwX /opt/ivy \
    && ls /opt/ivy/jars
