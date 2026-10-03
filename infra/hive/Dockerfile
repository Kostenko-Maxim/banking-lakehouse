FROM banking-hadoop:3.3.6
RUN curl -fSL --retry 5 https://archive.apache.org/dist/hive/hive-3.1.3/apache-hive-3.1.3-bin.tar.gz | tar -xz -C /opt && mv /opt/apache-hive-3.1.3-bin /opt/hive
RUN rm /opt/hive/lib/guava-*.jar && cp /opt/hadoop/share/hadoop/common/lib/guava-27.0-jre.jar /opt/hive/lib/
RUN curl -fSL --retry 5 https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar -o /opt/hive/lib/postgresql.jar
ENV HIVE_HOME=/opt/hive PATH=/opt/hive/bin:$PATH
COPY infra/hive/entrypoint.py /hive-entrypoint.py
ENTRYPOINT ["python3", "/hive-entrypoint.py"]
