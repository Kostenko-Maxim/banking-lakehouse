#!/bin/bash
set -euo pipefail
docker compose up -d --no-build --wait --wait-timeout 600 namenode datanode hdfs-init spark-master spark-worker
docker compose run --rm --no-deps spark-worker python /opt/project/scripts/hdfs-check.py
docker compose run --rm --no-deps --name banking-cluster-check spark-worker bash -ec '
  mkdir -p /opt/spark-events
  spark-submit --master spark://spark-master:7077 --driver-memory 1g \
    --conf spark.driver.host=banking-cluster-check \
    --conf spark.sql.catalog.lake.type=hadoop \
    --conf spark.sql.catalog.lake.warehouse=hdfs://namenode:8020/warehouse/smoke \
    --conf spark.sql.shuffle.partitions=2 /opt/project/jobs/smoke_hdfs.py
'
