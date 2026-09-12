#!/usr/bin/env bash
# Exercises ordinary collective placement against an existing kind cluster.
set -euo pipefail

: "${KUBECONFIG:?set KUBECONFIG to the preserved kind kubeconfig}"
CONTEXT=${CONTEXT:?set CONTEXT to the kind context}
NAMESPACE=${NAMESPACE:-stark8s-collective-acceptance}
WORKLOAD=${WORKLOAD:-collective-acceptance}
PROBE_IMAGE=${PROBE_IMAGE:-busybox:1.37.0}
TIMEOUT=${TIMEOUT:-180s}

kube=(kubectl --kubeconfig "$KUBECONFIG" --context "$CONTEXT")
kns=("${kube[@]}" --namespace "$NAMESPACE")

cleanup() {
  "${kube[@]}" delete namespace "$NAMESPACE" --ignore-not-found --wait=true >/dev/null
}
trap cleanup EXIT

cleanup
"${kube[@]}" create namespace "$NAMESPACE" >/dev/null
cat <<EOF | "${kns[@]}" apply -f - >/dev/null
apiVersion: stark8s.io/v1alpha1
kind: Workload
metadata:
  name: ${WORKLOAD}
spec:
  operations:
    - name: workers
      scaling:
        horizontal: {min: 1, max: 7}
      collective:
        size: 2
        maxAttempts: 2
        placement: Ordinary
        checkpoint: framework://checkpoints/acceptance
      template:
        spec:
          containers:
            - name: main
              image: ${PROBE_IMAGE}
              imagePullPolicy: Never
              resources:
                requests: {cpu: 10m, memory: 8Mi}
              command: ["/bin/sh", "-c"]
              args:
                - |
                  set -eu
                  test "\$STARK8S_COLLECTIVE_SIZE" = "2"
                  test "\$STARK8S_COLLECTIVE_CHECKPOINT" = "framework://checkpoints/acceptance"
                  expected="\$STARK8S_COLLECTIVE_ATTEMPT-0.\$STARK8S_COLLECTIVE_ATTEMPT.${NAMESPACE}.svc"
                  test "\$STARK8S_COLLECTIVE_RENDEZVOUS" = "\$expected"
                  tries=0
                  until ping -c 1 -W 1 "\$STARK8S_COLLECTIVE_RENDEZVOUS" >/dev/null 2>&1; do
                    tries=\$((tries + 1))
                    test "\$tries" -lt 60
                    sleep 1
                  done
                  echo "rank=\$STARK8S_COLLECTIVE_RANK size=\$STARK8S_COLLECTIVE_SIZE attempt=\$STARK8S_COLLECTIVE_ATTEMPT checkpoint=\$STARK8S_COLLECTIVE_CHECKPOINT rendezvous=\$STARK8S_COLLECTIVE_RENDEZVOUS"
                  if [ "\$STARK8S_COLLECTIVE_RANK" = "1" ] && [ "\$STARK8S_COLLECTIVE_ATTEMPT" = "${WORKLOAD}-workers-attempt-1" ]; then
                    exit 17
                  fi
EOF

first=${WORKLOAD}-workers-attempt-1
second=${WORKLOAD}-workers-attempt-2
for job in "$first" "$second"; do
  for _ in $(seq 1 60); do
    if "${kns[@]}" get "job/$job" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
  "${kns[@]}" get "job/$job" >/dev/null
done
"${kns[@]}" wait --for=condition=failed "job/$first" --timeout="$TIMEOUT" >/dev/null
"${kns[@]}" wait --for=condition=complete "job/$second" --timeout="$TIMEOUT" >/dev/null
"${kns[@]}" wait "workload/$WORKLOAD" --for=jsonpath='{.status.phase}'=Succeeded --timeout="$TIMEOUT" >/dev/null

first_exit_codes=$("${kns[@]}" get pods -l "job-name=$first" -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.state.terminated.exitCode}{"\n"}{end}{end}')
grep -qx '17' <<<"$first_exit_codes"

read -r completion_mode parallelism completions succeeded <<<"$("${kns[@]}" get "job/$second" -o jsonpath='{.spec.completionMode} {.spec.parallelism} {.spec.completions} {.status.succeeded}')"
test "$completion_mode" = Indexed
test "$parallelism" = 2
test "$completions" = 2
test "$succeeded" = 2

pods=( $("${kns[@]}" get pods -l "job-name=$second" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}') )
test "${#pods[@]}" -eq 2
for pod in "${pods[@]}"; do
  log=$("${kns[@]}" logs "$pod")
  grep -q 'size=2' <<<"$log"
  grep -q 'checkpoint=framework://checkpoints/acceptance' <<<"$log"
  grep -q "attempt=$second" <<<"$log"
  grep -q "rendezvous=$second-0.$second.$NAMESPACE.svc" <<<"$log"
  printf '%s %s\n' "$pod" "$log"
done

printf 'first_attempt=%s failed_exit_codes=%s\n' "$first" "$(tr '\n' ',' <<<"$first_exit_codes" | sed 's/,$//')"
printf 'replacement_attempt=%s indexed_ranks=%s succeeded=%s workload=Succeeded\n' "$second" "$completions" "$succeeded"
