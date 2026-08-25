#!/usr/bin/env bash
set -euo pipefail
kind delete cluster --name "${CLUSTER:-stark8s}"
