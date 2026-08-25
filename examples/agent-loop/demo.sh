#!/usr/bin/env bash
# Runs the agent-loop example end to end with kubectl: apply the workload,
# post prompts, resume the interrupted thread, and print the answers and the
# record diverted by the recursion limit. Requires kubectl and jq.
set -euo pipefail

NS=${NS:-default}
WORKLOAD=agent-loop
API="/api/v1/namespaces/$NS/services/$WORKLOAD-coordinator:8080/proxy"
HERE=$(cd "$(dirname "$0")" && pwd)
TIMEOUT=${TIMEOUT:-180}

post() { # post <channel> <json-array-of-records>
  echo "$2" | kubectl create --raw "$API/channels/$1/records" -f - >/dev/null
}

get() { # get <channel> <key>
  kubectl get --raw "$API/channels/$1/records?key=$2"
}

# wait_for <channel> <key>: poll until the channel holds a record for the key.
wait_for() {
  local deadline=$((SECONDS + TIMEOUT)) out
  while :; do
    out=$(get "$1" "$2" 2>/dev/null || true)
    if [ -n "$out" ] && [ "$(echo "$out" | jq -r --arg k "$2" '[.[]? | select(.key == $k)] | length')" != "0" ]; then
      echo "$out" | jq -c --arg k "$2" '[.[] | select(.key == $k)]'
      return 0
    fi
    if [ $SECONDS -ge $deadline ]; then
      echo "timed out waiting for a record with key $2 on $1" >&2
      return 1
    fi
    sleep 2
  done
}

echo "== applying workload"
kubectl -n "$NS" apply -f "$HERE/workload.yaml"

echo "== waiting for the coordinator and the three operations"
deadline=$((SECONDS + TIMEOUT))
until kubectl get --raw "$API/healthz" >/dev/null 2>&1; do
  [ $SECONDS -ge $deadline ] && { echo "coordinator not reachable" >&2; exit 1; }
  sleep 2
done
for op in agent search calc; do
  until [ "$(kubectl -n "$NS" get pods -l stark8s.io/operation=$op \
      -o jsonpath='{.items[*].status.containerStatuses[*].ready}' 2>/dev/null | grep -c true)" -ge 1 ]; do
    [ $SECONDS -ge $deadline ] && { echo "operation $op has no ready pod" >&2; exit 1; }
    sleep 2
  done
done

echo "== thread t1: two tool calls then an answer"
post prompts '[{"key":"t1","value":"search: stark8s; calc: 2+2; done","epoch":0}]'

echo "== thread t2: a tool call, then an interrupt for a human"
post prompts '[{"key":"t2","value":"search: pagerank; confirm publish; done","epoch":0}]'

echo "== thread loop: a question that never finishes"
post prompts '[{"key":"loop","value":"search: again","epoch":0}]'

echo "== t1 answer"
wait_for answers t1 | jq -r '.[0].value.scratch'

echo "== t2 is parked on ask-human"
parked=$(wait_for ask-human t2)
echo "$parked" | jq -r '.[0].value.steps | join(" -> ")'

echo "== resuming t2 with an approval on human"
resume=$(echo "$parked" | jq -c '[{key: "t2", value: (.[0].value | .scratch = "approved by operator"), epoch: 0}]')
post human "$resume"

echo "== t2 answer"
wait_for answers t2 | jq -r '.[0].value.scratch'

echo "== loop was diverted to stuck-search at the loop bound"
wait_for stuck-search loop | jq -r '.[0] | "epoch \(.epoch): \(.value.steps | length) steps recorded"'
kubectl -n "$NS" get workload "$WORKLOAD" \
  -o jsonpath='search-results overflowed: {.status.channels[?(@.name=="search-results")].overflowed}{"\n"}'
