FROM apache/airflow:2.8.1

USER root

# Instalar Java para el motor Apache Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Descargar driver JDBC de PostgreSQL para Spark
RUN mkdir -p /opt/spark-jars && \
    curl -fSL -o /opt/spark-jars/postgresql-42.5.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.5.0.jar && \
    chmod 644 /opt/spark-jars/postgresql-42.5.0.jar

ENV SPARK_CLASSPATH=/opt/spark-jars/postgresql-42.5.0.jar

# Cambiar a usuario airflow para instalar dependencias de Python
USER airflow

COPY requirements.txt /tmp/requirements.txt

# Actualizar pip e instalar dependencias declaradas
# Se fija una versión estable de Great Expectations para no romper la instalación existente.
RUN python -m pip install --upgrade pip setuptools wheel && \
    python -m pip install --no-cache-dir -r /tmp/requirements.txt && \
    rm -rf ~/.cache/pip

# Variables de entorno para Spark y Great Expectations
ENV PYSPARK_SUBMIT_ARGS='--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 pyspark-shell'
ENV GX_NO_USAGE_STATS=true
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/dags:/opt/airflow/scripts"

# Healthcheck para verificar que Airflow está listo
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl --fail "http://localhost:8080/health" || exit 1