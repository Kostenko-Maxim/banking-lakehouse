FROM eclipse-temurin:8u432-b06-jre-jammy AS distribution
RUN apt-get update && apt-get install -y --no-install-recommends curl python3 procps && rm -rf /var/lib/apt/lists/*
RUN curl -fSL --retry 5 https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz | tar -xz -C /opt && mv /opt/hadoop-3.3.6 /opt/hadoop
RUN rm -rf /opt/hadoop/share/doc /opt/hadoop/share/hadoop/tools
FROM eclipse-temurin:8u432-b06-jre-jammy
RUN apt-get update && apt-get install -y --no-install-recommends curl python3 procps && rm -rf /var/lib/apt/lists/*
COPY --from=distribution /opt/hadoop /opt/hadoop
ENV HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop PATH=/opt/hadoop/bin:$PATH HADOOP_HEAPSIZE=384
COPY infra/hadoop/conf/ /opt/hadoop/etc/hadoop/
COPY infra/hadoop/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
