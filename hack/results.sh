#!/usr/bin/env bash
# Prints the retained records of a channel: results.sh <workload> <channel>.
# Reads through the API server proxy, so it works with any CNI.
set -euo pipefail
WL=$1; CH=$2; NS=${NS:-default}
kubectl get --raw "/api/v1/namespaces/$NS/services/$WL-coordinator:8080/proxy/channels/$CH/records" \
  | python3 -c 'import json,sys; [print(r["key"], r["value"]) for r in json.load(sys.stdin)]'
