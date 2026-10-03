#!/bin/bash
set -euo pipefail
if [ "$1" = namenode ]; then
  if [ ! -f /hadoop/name/current/VERSION ]; then
    hdfs namenode -format -nonInteractive -clusterid banking-local
  fi
  exec hdfs namenode
fi
exec hdfs datanode
