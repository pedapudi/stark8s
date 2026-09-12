#!/usr/bin/env bash
# Qualifies the example ETL and iterative workloads on an existing kind cluster.
# The caller supplies an isolated kubeconfig and cluster name. The script leaves
# the cluster running so logs and objects remain available for inspection.
set -euo pipefail

cd "$(dirname "$0")/.."
: "${STARK8S_QUALIFICATION_KUBECONFIG:?set STARK8S_QUALIFICATION_KUBECONFIG to an isolated kubeconfig path}"
: "${STARK8S_QUALIFICATION_CLUSTER:?set STARK8S_QUALIFICATION_CLUSTER to an existing kind cluster name}"

export KUBECONFIG="$STARK8S_QUALIFICATION_KUBECONFIG"
run_id="${STARK8S_QUALIFICATION_RUN_ID:-$(date -u +%Y%m%d%H%M%S)-$$}"
namespace="stark8s-qualification-$run_id"
image="stark8s:qualification-$run_id"
artifacts="${STARK8S_QUALIFICATION_ARTIFACTS:-/tmp/$namespace}"
context="kind-$STARK8S_QUALIFICATION_CLUSTER"
mkdir -p "$artifacts"

kind get clusters | grep -Fxq "$STARK8S_QUALIFICATION_CLUSTER" || {
	echo "kind cluster $STARK8S_QUALIFICATION_CLUSTER does not exist" >&2
	exit 1
}
kubectl config use-context "$context" >/dev/null

docker build -t "$image" .
kind load docker-image --name "$STARK8S_QUALIFICATION_CLUSTER" "$image"
kubectl apply --server-side -f config/crd
manager_manifest="$artifacts/manager.yaml"
sed "s|stark8s:dev|$image|g" config/manager/manager.yaml >"$manager_manifest"
grep -Fq -- "--coordinator-image=$image" "$manager_manifest"
kubectl apply -f "$manager_manifest"
kubectl -n stark8s-system rollout status deployment/stark8s-controller --timeout=120s

kubectl create namespace "$namespace"
wordcount_manifest="$artifacts/wordcount.yaml"
sed -e "s/image: stark8s:dev/image: $image/" \
	-e 's/value: "200"/value: "20000"/' examples/wordcount/workload.yaml >"$wordcount_manifest"
grep -Fq "image: $image" "$wordcount_manifest"
grep -Fq 'value: "20000"' "$wordcount_manifest"
kubectl -n "$namespace" apply -f "$wordcount_manifest"

# Pod-local Ephemeral output cannot survive producer deletion. Enable the fault
# case to verify that the workload fails explicitly and reports lost records.
if [ "${STARK8S_QUALIFICATION_DELETE_MAP:-false}" = true ]; then
	map_pod=""
	for _ in $(seq 1 100); do
		map_pod=$(kubectl -n "$namespace" get pods -l stark8s.io/workload=wordcount,stark8s.io/operation=map -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
		if [ -n "$map_pod" ]; then
			metrics_candidate="$artifacts/wordcount-metrics-candidate.json"
			kubectl -n "$namespace" get --raw "/api/v1/namespaces/$namespace/services/wordcount-coordinator:8080/proxy/metrics" >"$metrics_candidate"
			if MAP_METRICS="$metrics_candidate" python3 - <<'PYMETRICS' 2>/dev/null
import json
import os
metrics = json.load(open(os.environ["MAP_METRICS"], encoding="utf-8"))
operation = next(item for item in metrics["operations"] if item["name"] == "map")
lines = next(item for item in metrics["channels"] if item["name"] == "lines")
assert operation["livePods"] >= 1
assert lines["pending"] + lines["inFlight"] > 0
PYMETRICS
			then
				mv "$metrics_candidate" "$artifacts/wordcount-metrics-before-delete.json"
				kubectl -n "$namespace" get pod "$map_pod" -o yaml >"$artifacts/map-pod-before-delete.yaml"
				kubectl -n "$namespace" delete pod "$map_pod" --wait=false
				break
			fi
		fi
		sleep 0.1
	done
	if [ -z "$map_pod" ]; then
		echo "wordcount map worker completed before fault injection" >&2
		exit 1
	fi
	for _ in $(seq 1 300); do
		phase=$(kubectl -n "$namespace" get workload wordcount -o jsonpath='{.status.phase}')
		[ "$phase" = Failed ] && break
		[ "$phase" = Succeeded ] && { echo "pod-local worker loss unexpectedly succeeded" >&2; exit 1; }
		sleep 1
	done
	[ "$phase" = Failed ] || { echo "wordcount did not report worker-output loss" >&2; exit 1; }
	metrics=$(kubectl get --raw "/api/v1/namespaces/$namespace/services/wordcount-coordinator:8080/proxy/metrics")
	WORDCOUNT_METRICS="$metrics" python3 - <<'PYFAULT'
import json
import os
metrics = json.loads(os.environ["WORDCOUNT_METRICS"])
shuffle = next(item for item in metrics["channels"] if item["name"] == "shuffle")
assert shuffle["lost"] > 0, f"failed workload reported no lost shuffle records: {shuffle}"
PYFAULT
else
	kubectl -n "$namespace" wait workload/wordcount --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
	wordcount_json=$(kubectl get --raw "/api/v1/namespaces/$namespace/services/wordcount-coordinator:8080/proxy/channels/totals/records")
	WORDCOUNT_JSON="$wordcount_json" python3 - <<'PYWORDCOUNT'
import json
import os
records = json.loads(os.environ["WORDCOUNT_JSON"])
values = {record["key"]: int(record["value"]) for record in records}
assert len(values) == 43, f"wordcount returned {len(values)} unique words, want 43"
assert sum(values.values()) == 1260000, f"wordcount total is {sum(values.values())}, want 1260000"
assert values.get("the") == 120000, f"count for 'the' is {values.get('the')}, want 120000"
PYWORDCOUNT
fi

sed "s/image: stark8s:dev/image: $image/" examples/pagerank/workload.yaml | kubectl -n "$namespace" apply -f -
kubectl -n "$namespace" wait workload/pagerank --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
pagerank_json=$(kubectl get --raw "/api/v1/namespaces/$namespace/services/pagerank-coordinator:8080/proxy/channels/ranks/records")
PAGERANK_JSON="$pagerank_json" python3 - <<'PY'
import json
import math
import os

records = json.loads(os.environ["PAGERANK_JSON"])
values = {record["key"]: float(record["value"]) for record in records}
assert set(values) == {"a", "b", "c", "d", "e"}, f"PageRank vertices are {sorted(values)}"
assert all(math.isfinite(value) and value >= 0 for value in values.values()), f"invalid ranks: {values}"
assert abs(sum(values.values()) - 1.0) < 0.001, f"rank sum is {sum(values.values())}, want 1"
PY

kubectl -n stark8s-system rollout restart deployment/stark8s-controller
kubectl -n stark8s-system rollout status deployment/stark8s-controller --timeout=120s
kubectl -n "$namespace" get workload wordcount pagerank
echo "qualification namespace: $namespace"
echo "qualification artifacts: $artifacts"

sed "s/image: stark8s:dev/image: $image/" examples/grpo/workload.yaml | kubectl -n "$namespace" apply -f -
kubectl -n "$namespace" wait workload/grpo --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
grpo_json=$(kubectl get --raw "/api/v1/namespaces/$namespace/services/grpo-coordinator:8080/proxy/channels/metrics/records")
GRPO_JSON="$grpo_json" python3 - <<'PY'
import json
import math
import os

records = json.loads(os.environ["GRPO_JSON"])
metrics = [record["value"] for record in records]
assert len(metrics) == 24, f"GRPO returned {len(metrics)} updates, want 24"
metrics.sort(key=lambda metric: metric["step"])
assert [metric["step"] for metric in metrics] == list(range(24)), "GRPO steps are incomplete or duplicated"
assert all(all(math.isfinite(metric[field]) for field in ("rewardMean", "objective", "kl")) for metric in metrics), f"GRPO returned non-finite metrics: {metrics}"
assert metrics[-1]["rewardMean"] > metrics[0]["rewardMean"], f"reward did not improve: {metrics[0]} -> {metrics[-1]}"
assert metrics[-1]["rewardMean"] >= 0.85, f"final reward is {metrics[-1]['rewardMean']}, want at least 0.85"
PY
kubectl -n "$namespace" get pods -l stark8s.io/workload=grpo -o yaml >"$artifacts/grpo-pods.yaml"
echo "qualification checks passed; artifacts: $artifacts"
