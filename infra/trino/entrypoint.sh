#!/bin/bash
set -euo pipefail
printf '%s\n' '-server' "-Xmx${TRINO_HEAP:-2G}" '-XX:+UseG1GC' '-XX:G1HeapRegionSize=32M' '-XX:+ExitOnOutOfMemoryError' '-Djdk.attach.allowAttachSelf=true' > /tmp/jvm.config
exec /usr/lib/trino/bin/launcher run --jvm-config=/tmp/jvm.config
