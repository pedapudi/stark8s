package controller

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/util/yaml"
)

func TestManagerRoleCanReconcileJobs(t *testing.T) {
	f, err := os.Open(filepath.Join("..", "..", "config", "manager", "manager.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	type rule struct {
		APIGroups []string `json:"apiGroups"`
		Resources []string `json:"resources"`
		Verbs     []string `json:"verbs"`
	}
	type manifest struct {
		Kind  string `json:"kind"`
		Rules []rule `json:"rules"`
	}
	decoder := yaml.NewYAMLOrJSONDecoder(f, 4096)
	for {
		var doc manifest
		if err := decoder.Decode(&doc); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if doc.Kind != "ClusterRole" {
			continue
		}
		for _, r := range doc.Rules {
			if contains(r.APIGroups, "batch") && contains(r.Resources, "jobs") {
				for _, verb := range []string{"get", "list", "watch", "create", "update"} {
					if !contains(r.Verbs, verb) && !contains(r.Verbs, "*") {
						t.Fatalf("Job rule lacks %q: %+v", verb, r)
					}
				}
				return
			}
		}
	}
	t.Fatal("manager ClusterRole has no batch/jobs rule")
}
