#!/bin/bash
set -euo pipefail
# Preserve the image's JDK compatibility flags (including Hadoop's security
# manager setting), replacing percentage-based heap sizing with our limit.
{
  while IFS= read -r option || [[ -n "$option" ]]; do
    case "$option" in
      -XX:InitialRAMPercentage=*|-XX:MaxRAMPercentage=*|-Xmx*|-Xms*) ;;
      *) printf '%s\n' "$option" ;;
    esac
  done < /etc/trino/jvm.config
  printf '%s\n' "-Xmx${TRINO_HEAP:-2G}"
} > /tmp/jvm.config
exec /usr/lib/trino/bin/launcher run --etc-dir=/etc/trino --jvm-config=/tmp/jvm.config
