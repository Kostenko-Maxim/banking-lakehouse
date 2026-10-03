FROM banking-spark:3.5.3 AS spark
FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
COPY --from=spark --chown=airflow:root /usr/local/lib/python3.11/site-packages/pyspark /opt/spark
COPY infra/hadoop/conf/ /opt/hadoop-conf/
COPY infra/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
RUN mkdir -p /data /opt/spark-events && chown -R airflow:root /data /opt/spark-events
USER airflow
COPY requirements.txt /tmp/requirements.txt
COPY requirements-runtime.lock /tmp/requirements-runtime.lock
RUN pip install --no-cache-dir 'apache-airflow==2.10.5' -r /tmp/requirements.txt
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 SPARK_HOME=/opt/spark PATH=/opt/spark/bin:$PATH PYSPARK_PYTHON=python3 HADOOP_USER_NAME=bank HADOOP_CONF_DIR=/opt/hadoop-conf PYTHONPATH=/opt/project/src:/opt/project
