package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/coordinator"
)

// fakeCoordinator serves canned metrics and records seal requests.
type fakeCoordinator struct {
	*httptest.Server
	mu      sync.Mutex
	metrics coordinator.Metrics
	sealed  []string
}

func newFakeCoordinator(t *testing.T) *fakeCoordinator {
	f := &fakeCoordinator{}
	mux := http.NewServeMux()
	mux.HandleFunc(coordinator.PathTopology, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc(coordinator.PathMetrics, func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(f.metrics)
	})
	mux.HandleFunc(coordinator.PathChannels+"/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, coordinator.PathChannels+"/")
		if r.Method == "POST" && strings.HasSuffix(rest, coordinator.SuffixSeal) {
			f.mu.Lock()
			f.sealed = append(f.sealed, strings.TrimSuffix(rest, coordinator.SuffixSeal))
			f.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.NotFound(w, r)
	})
	f.Server = httptest.NewServer(mux)
	t.Cleanup(f.Close)
	return f
}

func (f *fakeCoordinator) set(m coordinator.Metrics) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metrics = m
}

func (f *fakeCoordinator) sealedChannels() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sealed...)
}

type harness struct {
	t   *testing.T
	c   client.Client
	r   *Reconciler
	co  *fakeCoordinator
	key types.NamespacedName
}

func container() corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "img"}}}}
}

func newHarness(t *testing.T, wl *v1alpha1.Workload) *harness {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if wl.Namespace == "" {
		wl.Namespace = "default"
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.Workload{}).WithObjects(wl).Build()
	co := newFakeCoordinator(t)
	r := &Reconciler{
		Client:              c,
		CoordinatorImage:    "coord:test",
		ControllerNamespace: "stark8s-system",
		CoordinatorURL:      func(*v1alpha1.Workload) string { return co.URL },
		HTTP:                co.Client(),
	}
	return &harness{t: t, c: c, r: r, co: co, key: client.ObjectKeyFromObject(wl)}
}

func (h *harness) reconcile() *v1alpha1.Workload {
	h.t.Helper()
	if _, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key}); err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
	wl := &v1alpha1.Workload{}
	if err := h.c.Get(context.Background(), h.key, wl); err != nil {
		h.t.Fatal(err)
	}
	return wl
}

func (h *harness) deployment(name string) (*appsv1.Deployment, bool) {
	h.t.Helper()
	d := &appsv1.Deployment{}
	err := h.c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, d)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		h.t.Fatal(err)
	}
	return d, true
}

func (h *harness) opStatus(wl *v1alpha1.Workload, name string) v1alpha1.OperationStatus {
	h.t.Helper()
	for _, s := range wl.Status.Operations {
		if s.Name == name {
			return s
		}
	}
	h.t.Fatalf("no status for operation %q", name)
	return v1alpha1.OperationStatus{}
}

func replicas(d *appsv1.Deployment) int32 {
	if d.Spec.Replicas == nil {
		return -1
	}
	return *d.Spec.Replicas
}

// mapReduce is a source, a pipelined consumer, and a materialized consumer.
func mapReduce() *v1alpha1.Workload {
	return &v1alpha1.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: "wc", Namespace: "default"},
		Spec: v1alpha1.WorkloadSpec{
			Operations: []v1alpha1.Operation{
				{Name: "read", Template: container()},
				{Name: "map", Slots: 2, Scaling: v1alpha1.Scaling{Horizontal: v1alpha1.HorizontalScaling{Min: 1, Max: 4}}, Template: container()},
				{Name: "reduce", Slots: 1, Scaling: v1alpha1.Scaling{Horizontal: v1alpha1.HorizontalScaling{Min: 1, Max: 3}}, Template: container()},
			},
			Channels: []graph.Channel{
				{Name: "lines", From: "read", To: "map", Delivery: graph.DeliveryPipelined},
				{Name: "shuffle", From: "map", To: "reduce", Delivery: graph.DeliveryMaterialized},
				{Name: "totals", From: "reduce"},
			},
		},
	}
}

func TestWaitingUntilInboundMaterializedSealed(t *testing.T) {
	h := newHarness(t, mapReduce())
	h.co.set(coordinator.Metrics{
		Channels:   []coordinator.ChannelMetrics{{Name: "lines"}, {Name: "shuffle", Sealed: false}, {Name: "totals"}},
		Operations: []coordinator.OperationMetrics{{Name: "read"}, {Name: "map"}, {Name: "reduce", RunnableTasks: 3}},
	})
	wl := h.reconcile()
	if _, ok := h.deployment("wc-reduce"); ok {
		t.Fatal("reduce Deployment created while shuffle is unsealed")
	}
	if got := h.opStatus(wl, "reduce").Phase; got != v1alpha1.OperationWaiting {
		t.Fatalf("reduce phase %q, want Waiting", got)
	}
	if _, ok := h.deployment("wc-read"); !ok {
		t.Fatal("source Deployment missing")
	}
	if _, ok := h.deployment("wc-map"); !ok {
		t.Fatal("pipelined consumer Deployment missing")
	}
	if wl.Status.Phase != v1alpha1.WorkloadRunning {
		t.Fatalf("workload phase %q", wl.Status.Phase)
	}

	h.co.set(coordinator.Metrics{
		Channels:   []coordinator.ChannelMetrics{{Name: "lines", Sealed: true}, {Name: "shuffle", Sealed: true}, {Name: "totals"}},
		Operations: []coordinator.OperationMetrics{{Name: "read"}, {Name: "map"}, {Name: "reduce", RunnableTasks: 3}},
	})
	wl = h.reconcile()
	d, ok := h.deployment("wc-reduce")
	if !ok {
		t.Fatal("reduce Deployment not created after shuffle sealed")
	}
	if replicas(d) != 3 {
		t.Fatalf("reduce replicas %d, want 3", replicas(d))
	}
	if got := h.opStatus(wl, "reduce"); got.Phase != v1alpha1.OperationRunning || got.RunnableTasks != 3 {
		t.Fatalf("reduce status %+v", got)
	}
}

func TestReplicasFromRunnableTasksAndSlots(t *testing.T) {
	h := newHarness(t, mapReduce())
	// No metrics yet: source and pipelined consumer at min (one).
	h.reconcile()
	for _, name := range []string{"wc-read", "wc-map"} {
		d, _ := h.deployment(name)
		if replicas(d) != 1 {
			t.Fatalf("%s replicas %d before metrics, want 1", name, replicas(d))
		}
	}
	// Five runnable partitions over two slots: three replicas.
	h.co.set(coordinator.Metrics{Operations: []coordinator.OperationMetrics{{Name: "map", RunnableTasks: 5}}})
	h.reconcile()
	d, _ := h.deployment("wc-map")
	if replicas(d) != 3 {
		t.Fatalf("map replicas %d, want ceil(5/2)=3", replicas(d))
	}
	// Clamped to max.
	h.co.set(coordinator.Metrics{Operations: []coordinator.OperationMetrics{{Name: "map", RunnableTasks: 50}}})
	h.reconcile()
	d, _ = h.deployment("wc-map")
	if replicas(d) != 4 {
		t.Fatalf("map replicas %d, want max 4", replicas(d))
	}
	// Back to idle: min, which is one.
	h.co.set(coordinator.Metrics{Operations: []coordinator.OperationMetrics{{Name: "map", RunnableTasks: 0}}})
	h.reconcile()
	d, _ = h.deployment("wc-map")
	if replicas(d) != 1 {
		t.Fatalf("map replicas %d when idle, want 1", replicas(d))
	}
}

func TestCompleteScalesToZeroUnlessHoldingSegments(t *testing.T) {
	h := newHarness(t, mapReduce())
	h.co.set(coordinator.Metrics{Operations: []coordinator.OperationMetrics{{Name: "map", RunnableTasks: 5}}})
	h.reconcile()

	// map is complete but its shuffle segments are unconsumed: keep pods,
	// seal outbound channels.
	h.co.set(coordinator.Metrics{
		Channels:   []coordinator.ChannelMetrics{{Name: "lines", Sealed: true}, {Name: "shuffle"}, {Name: "totals"}},
		Operations: []coordinator.OperationMetrics{{Name: "read", Complete: true}, {Name: "map", Complete: true, HoldsUnconsumed: true}},
	})
	wl := h.reconcile()
	d, _ := h.deployment("wc-map")
	if replicas(d) != 3 {
		t.Fatalf("map replicas %d while holding segments, want 3", replicas(d))
	}
	st := h.opStatus(wl, "map")
	if st.Phase != v1alpha1.OperationRunning || !st.HoldsUnconsumed {
		t.Fatalf("map status %+v, want Running and holding", st)
	}
	sealed := h.co.sealedChannels()
	if !contains(sealed, "shuffle") {
		t.Fatalf("shuffle not sealed on completion: %v", sealed)
	}
	if contains(sealed, "lines") {
		t.Fatalf("already sealed channel sealed again: %v", sealed)
	}
	if d, _ := h.deployment("wc-read"); replicas(d) != 0 {
		t.Fatalf("read replicas %d after completion, want 0", replicas(d))
	}
	if got := h.opStatus(wl, "read").Phase; got != v1alpha1.OperationSucceeded {
		t.Fatalf("read phase %q, want Succeeded", got)
	}

	// Segments consumed: scale to zero.
	h.co.set(coordinator.Metrics{
		Channels:   []coordinator.ChannelMetrics{{Name: "lines", Sealed: true}, {Name: "shuffle", Sealed: true}, {Name: "totals", Sealed: true}},
		Operations: []coordinator.OperationMetrics{{Name: "read", Complete: true}, {Name: "map", Complete: true}, {Name: "reduce", Complete: true}},
	})
	wl = h.reconcile()
	d, _ = h.deployment("wc-map")
	if replicas(d) != 0 {
		t.Fatalf("map replicas %d after release, want 0", replicas(d))
	}
	if got := h.opStatus(wl, "map").Phase; got != v1alpha1.OperationSucceeded {
		t.Fatalf("map phase %q, want Succeeded", got)
	}
	if wl.Status.Phase != v1alpha1.WorkloadSucceeded {
		t.Fatalf("workload phase %q, want Succeeded", wl.Status.Phase)
	}
}

func TestPerEdgeNetworkPolicies(t *testing.T) {
	h := newHarness(t, mapReduce())
	h.reconcile()
	list := func() map[string]networkingv1.NetworkPolicy {
		var l networkingv1.NetworkPolicyList
		if err := h.c.List(context.Background(), &l, client.InNamespace("default")); err != nil {
			t.Fatal(err)
		}
		out := map[string]networkingv1.NetworkPolicy{}
		for _, p := range l.Items {
			out[p.Name] = p
		}
		return out
	}
	pols := list()
	for _, name := range []string{"wc-operations", "wc-coordinator", "wc-edge-lines", "wc-edge-shuffle"} {
		if _, ok := pols[name]; !ok {
			t.Fatalf("policy %s missing; have %v", name, keys(pols))
		}
	}
	if _, ok := pols["wc-edge-totals"]; ok {
		t.Fatal("edge policy created for a channel with no consumer")
	}
	edge := pols["wc-edge-shuffle"]
	if edge.Spec.PodSelector.MatchLabels[LabelOperation] != "map" {
		t.Fatalf("edge selects %v, want producer map", edge.Spec.PodSelector.MatchLabels)
	}
	if len(edge.Spec.Ingress) != 1 || len(edge.Spec.Ingress[0].From) != 1 ||
		edge.Spec.Ingress[0].From[0].PodSelector.MatchLabels[LabelOperation] != "reduce" {
		t.Fatalf("edge ingress %+v, want from consumer reduce", edge.Spec.Ingress)
	}
	if p := edge.Spec.Ingress[0].Ports; len(p) != 1 || p[0].Port.IntValue() != coordinator.SegmentPort {
		t.Fatalf("edge ports %+v, want segment port", p)
	}
	if len(edge.Spec.PolicyTypes) != 1 || edge.Spec.PolicyTypes[0] != networkingv1.PolicyTypeIngress {
		t.Fatalf("edge policy types %v", edge.Spec.PolicyTypes)
	}
	if edge.Labels[LabelChannel] != "shuffle" {
		t.Fatalf("edge labels %v", edge.Labels)
	}
	ops := pols["wc-operations"]
	if len(ops.Spec.Ingress) != 0 || len(ops.Spec.PolicyTypes) != 2 {
		t.Fatalf("operations policy should deny ingress: %+v", ops.Spec)
	}

	// Drop the shuffle channel: its edge policy is deleted.
	wl := &v1alpha1.Workload{}
	if err := h.c.Get(context.Background(), h.key, wl); err != nil {
		t.Fatal(err)
	}
	wl.Spec.Channels = []graph.Channel{wl.Spec.Channels[0], wl.Spec.Channels[2]}
	if err := h.c.Update(context.Background(), wl); err != nil {
		t.Fatal(err)
	}
	h.reconcile()
	pols = list()
	if _, ok := pols["wc-edge-shuffle"]; ok {
		t.Fatal("stale edge policy not deleted")
	}
	if _, ok := pols["wc-edge-lines"]; !ok {
		t.Fatal("live edge policy deleted")
	}
}

func TestPodTemplateInjection(t *testing.T) {
	h := newHarness(t, mapReduce())
	h.reconcile()
	d, _ := h.deployment("wc-map")
	pod := d.Spec.Template
	if pod.Spec.ServiceAccountName != "wc-map" {
		t.Fatalf("serviceAccountName %q", pod.Spec.ServiceAccountName)
	}
	sa := &corev1.ServiceAccount{}
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "wc-map"}, sa); err != nil {
		t.Fatalf("service account: %v", err)
	}
	if len(sa.OwnerReferences) != 1 || sa.OwnerReferences[0].Kind != "Workload" {
		t.Fatalf("service account owner %v", sa.OwnerReferences)
	}
	if pod.Labels[LabelRole] != RoleOperation || pod.Labels[LabelOperation] != "map" || pod.Labels[LabelWorkload] != "wc" {
		t.Fatalf("labels %v", pod.Labels)
	}
	env := map[string]corev1.EnvVar{}
	for _, e := range pod.Spec.Containers[0].Env {
		env[e.Name] = e
	}
	want := map[string]string{
		coordinator.EnvCoordinator: "http://wc-coordinator:8080",
		coordinator.EnvWorkload:    "wc",
		coordinator.EnvOperation:   "map",
		coordinator.EnvSlots:       "2",
		coordinator.EnvInbound:     "lines",
		coordinator.EnvOutbound:    "shuffle",
		coordinator.EnvSegmentDir:  SegmentDir,
	}
	for k, v := range want {
		if env[k].Value != v {
			t.Errorf("%s = %q, want %q", k, env[k].Value, v)
		}
	}
	if f := env[coordinator.EnvPodIP].ValueFrom; f == nil || f.FieldRef.FieldPath != "status.podIP" {
		t.Errorf("%s not from status.podIP", coordinator.EnvPodIP)
	}
	if f := env[coordinator.EnvInstance].ValueFrom; f == nil || f.FieldRef.FieldPath != "metadata.name" {
		t.Errorf("%s not from metadata.name", coordinator.EnvInstance)
	}
	if m := pod.Spec.Containers[0].VolumeMounts; len(m) != 1 || m[0].MountPath != SegmentDir {
		t.Errorf("volume mounts %+v", m)
	}
	if v := pod.Spec.Volumes; len(v) != 1 || v[0].EmptyDir == nil {
		t.Errorf("volumes %+v", v)
	}
	if p := pod.Spec.Containers[0].Ports; len(p) != 1 || p[0].ContainerPort != coordinator.SegmentPort {
		t.Errorf("ports %+v", p)
	}
	co, ok := h.deployment("wc-coordinator")
	if !ok {
		t.Fatal("coordinator Deployment missing")
	}
	if c := co.Spec.Template.Spec.Containers[0]; c.Image != "coord:test" || c.Command[0] != "/coordinator" {
		t.Errorf("coordinator container %+v", c)
	}
	if co.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType {
		t.Errorf("coordinator strategy %v", co.Spec.Strategy.Type)
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func keys(m map[string]networkingv1.NetworkPolicy) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
