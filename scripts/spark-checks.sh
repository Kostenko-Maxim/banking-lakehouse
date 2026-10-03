#!/bin/bash
set -euo pipefail
docker run --rm --memory 2g --cpus 2 \
  --mount "type=bind,source=$PWD,target=/opt/project,readonly" \
  banking-spark:3.5.3 bash -ec '
    mkdir -p /opt/spark-events
    spark-submit --master local[2] --driver-memory 1g \
      --conf spark.driver.host=localhost \
      --conf spark.sql.catalog.lake.type=hadoop \
      --conf spark.sql.catalog.lake.warehouse=file:///tmp/check-warehouse \
      --conf spark.sql.shuffle.partitions=2 /opt/project/jobs/checks.py
  '
