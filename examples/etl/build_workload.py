"""Build the ETL Workload API object with per-operation resources."""

import json
import sys


def operation(name, cpu, memory, replicas=1, checkpoint=False):
    result = {
        "name": name,
        "completion": "Drain",
        "slots": 1,
        "scaling": {"horizontal": {"min": replicas, "max": replicas}},
        "template": {
            "spec": {
                "containers": [{
                    "name": "main",
                    "image": "stark8s:dev",
                    "command": ["/etl", name],
                    "env": [
                        {"name": "ETL_STORE_DIR", "value": "/data"},
                        {"name": "ETL_ATTEMPT", "value": "replace-with-run-id"},
                    ],
                    "resources": {"requests": {"cpu": cpu, "memory": memory}},
                    "volumeMounts": [{"name": "data", "mountPath": "/data"}],
                }],
                "volumes": [{
                    "name": "data",
                    "persistentVolumeClaim": {"claimName": "replace-with-shared-files"},
                }],
            }
        },
    }
    if checkpoint:
        result["checkpoint"] = True
    return result


def workload():
    return {
        "apiVersion": "stark8s.io/v1alpha1",
        "kind": "Workload",
        "metadata": {"name": "file-etl"},
        "spec": {
            "coordinator": {"objectStore": {
                "endpoint": "http://replace-with-object-store/bucket",
                "region": "local",
                "credentialsSecret": "replace-with-object-store-credentials",
            }},
            "operations": [
                operation("read", "100m", "128Mi"),
                operation("transform", "500m", "256Mi", checkpoint=True),
                operation("aggregate", "500m", "256Mi", checkpoint=True),
                operation("publish", "100m", "128Mi"),
            ],
            "channels": [
                {"name": "splits", "from": "read", "to": "transform",
                 "delivery": "Materialized", "partitioning": {"mode": "Hash", "partitions": 4}},
                {"name": "values", "from": "transform", "to": "aggregate",
                 "delivery": "Materialized", "partitioning": {"mode": "Hash", "partitions": 2},
                 "combine": "Sum"},
                {"name": "output-files", "from": "aggregate", "to": "publish",
                 "delivery": "Materialized", "partitioning": {"mode": "Hash", "partitions": 1}},
                {"name": "completed", "from": "publish", "durability": "Retained"},
            ],
        },
    }


if __name__ == "__main__":
    json.dump(workload(), sys.stdout, indent=2)
    sys.stdout.write("\n")
