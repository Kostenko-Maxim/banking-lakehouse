FROM python:3.11.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl procps && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyspark==3.5.3 && ln -s /usr/local/lib/python3.11/site-packages/pyspark /opt/spark
RUN curl -fSL --retry 5 https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar -o /opt/spark/jars/iceberg-spark-runtime.jar
COPY requirements-runtime.lock /tmp/requirements-runtime.lock
RUN pip install --no-cache-dir -c /tmp/requirements-runtime.lock requests==2.32.3
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 SPARK_HOME=/opt/spark PATH=/opt/spark/bin:$PATH PYSPARK_PYTHON=python3 HADOOP_USER_NAME=bank PYTHONPATH=/opt/project/src:/opt/project
COPY infra/hadoop/conf/ /opt/hadoop-conf/
COPY infra/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV HADOOP_CONF_DIR=/opt/hadoop-conf
RUN mkdir -p /opt/spark-events && chmod 777 /opt/spark-events
WORKDIR /opt/project
