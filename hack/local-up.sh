#!/usr/bin/env bash
# Creates a kind cluster, builds the image, installs the controller, and
# runs the two example workloads. Requires docker, kind, kubectl, go.
#
# NetworkPolicy is enforced only if the cluster CNI supports it. Set
# STARK8S_CNI=calico to install Calico instead of kind's default CNI.
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER=${CLUSTER:-stark8s}
IMAGE=${IMAGE:-stark8s:dev}

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  if [ "${STARK8S_CNI:-}" = calico ]; then
    kind create cluster --name "$CLUSTER" --config hack/kind-calico.yaml
    kubectl apply -f https://raw.githubusercontent.com/projectcalico/calico/v3.29.1/manifests/calico.yaml
    kubectl -n kube-system rollout status ds/calico-node --timeout=300s
  else
    kind create cluster --name "$CLUSTER"
  fi
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

docker build -t "$IMAGE" .
kind load docker-image --name "$CLUSTER" "$IMAGE"

kubectl apply --server-side -f config/crd
kubectl apply -f config/manager
kubectl -n stark8s-system rollout restart deploy/stark8s-controller
kubectl -n stark8s-system rollout status deploy/stark8s-controller --timeout=120s

if [ "${1:-}" != "--no-examples" ]; then
  kubectl delete workload --all --ignore-not-found >/dev/null
  kubectl apply -f examples/wordcount/workload.yaml -f examples/pagerank/workload.yaml
  echo "Waiting for workloads to succeed..."
  for wl in wordcount pagerank; do
    kubectl wait workload/$wl --for=jsonpath='{.status.phase}'=Succeeded --timeout=300s
  done
  kubectl get workloads
  hack/results.sh wordcount totals | sort -k2 -n -r | head -5
  hack/results.sh pagerank ranks
fi
