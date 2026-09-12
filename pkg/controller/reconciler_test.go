package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
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
	mux.HandleFunc(coordinator.PathOperations, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
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
	t      *testing.T
	c      client.Client
	r      *Reconciler
	co     *fakeCoordinator
	key    types.NamespacedName
	events *record.FakeRecorder
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
	events := record.NewFakeRecorder(100)
	r := &Reconciler{
		Client:              c,
		CoordinatorImage:    "coord:test",
		ControllerNamespace: "stark8s-system",
		CoordinatorURL:      func(*v1alpha1.Workload) string { return co.URL },
		HTTP:                co.Client(),
		Recorder:            events,
	}
	return &harness{t: t, c: c, r: r, co: co, key: client.ObjectKeyFromObject(wl), events: events}
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
	// Membership is established at the configured maximum before work starts.
	h.reconcile()
	d, _ := h.deployment("wc-map")
	if replicas(d) != 4 {
		t.Fatalf("map replicas %d before metrics, want max 4", replicas(d))
	}
	// Runnable work does not alter membership during the attempt.
	h.co.set(coordinator.Metrics{Operations: []coordinator.OperationMetrics{{Name: "map", RunnableTasks: 5}}})
	h.reconcile()
	d, _ = h.deployment("wc-map")
	if replicas(d) != 4 {
		t.Fatalf("map replicas %d with work, want 4", replicas(d))
	}
	// Clamped to max.
	h.co.set(coordinator.Metrics{Operations: []coordinator.OperationMetrics{{Name: "map", RunnableTasks: 50}}})
	h.reconcile()
	d, _ = h.deployment("wc-map")
	if replicas(d) != 4 {
		t.Fatalf("map replicas %d, want max 4", replicas(d))
	}
	// Empty input does not prove that pods hold no application state or output.
	h.co.set(coordinator.Metrics{Operations: []coordinator.OperationMetrics{{Name: "map", RunnableTasks: 0}}})
	h.reconcile()
	d, _ = h.deployment("wc-map")
	if replicas(d) != 4 {
		t.Fatalf("map replicas %d when idle, want 4", replicas(d))
	}
}

func TestAutomaticScaleDownIsDisabled(t *testing.T) {
	wl := mapReduce()
	wl.Spec.Operations[1].Completion = v1alpha1.CompletionNever
	wl.Spec.Operations[1].Scaling.Horizontal.CPUUtilizationPercent = 70
	wl.Spec.Operations[1].Scaling.Vertical = &v1alpha1.VerticalScaling{Mode: v1alpha1.VerticalAuto}
	h := newHarness(t, wl)
	got := h.reconcile()
	d, _ := h.deployment("wc-map")
	if replicas(d) != 4 {
		t.Fatalf("map replicas %d, want fixed membership of 4", replicas(d))
	}
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	err := h.c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "wc-map"}, hpa)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("unsafe horizontal autoscaler exists or lookup failed: %v", err)
	}
	if !strings.Contains(got.Status.Message, "automatic scaling disabled") {
		t.Fatalf("workload message %q does not explain fixed scaling", got.Status.Message)
	}
}

func TestDeliveryFailureRemainsInStatusAfterQueueDrains(t *testing.T) {
	h := newHarness(t, mapReduce())
	h.co.set(coordinator.Metrics{
		Channels: []coordinator.ChannelMetrics{{
			Name: "lines", Sealed: true, Acknowledged: 4,
			LatestDeliveryFailure: "fetch from holder timed out",
		}},
		Operations: []coordinator.OperationMetrics{{Name: "map"}},
	})
	wl := h.reconcile()
	status := h.opStatus(wl, "map")
	if status.Reason != "DeliveryFailureRecorded" || !strings.Contains(status.Message, "holder timed out") {
		t.Fatalf("map status %+v", status)
	}
	if len(wl.Status.Channels) == 0 ||
		wl.Status.Channels[0].LatestDeliveryFailure != "fetch from holder timed out" ||
		wl.Status.Channels[0].Acknowledged != 4 {
		t.Fatalf("channel status %+v", wl.Status.Channels)
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
	if replicas(d) != 4 {
		t.Fatalf("map replicas %d while holding segments, want 4", replicas(d))
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

func TestLostRecordsFailWorkload(t *testing.T) {
	h := newHarness(t, mapReduce())
	h.co.set(coordinator.Metrics{Operations: []coordinator.OperationMetrics{{Name: "map"}}})
	h.reconcile()
	h.co.set(coordinator.Metrics{
		Channels: []coordinator.ChannelMetrics{
			{Name: "lines", Sealed: true, Lost: 7},
			{Name: "shuffle", Sealed: true},
			{Name: "totals", Sealed: true},
		},
		Operations: []coordinator.OperationMetrics{
			{Name: "read", Complete: true},
			{Name: "map", Complete: true},
			{Name: "reduce", Complete: true},
		},
	})
	wl := h.reconcile()
	if wl.Status.Phase != v1alpha1.WorkloadFailed {
		t.Fatalf("workload phase %q, want Failed", wl.Status.Phase)
	}
	for _, want := range []string{"lines", "7 records"} {
		if !strings.Contains(wl.Status.Message, want) {
			t.Errorf("failure message %q does not contain %q", wl.Status.Message, want)
		}
	}
	d, _ := h.deployment("wc-map")
	if replicas(d) != 4 {
		t.Fatalf("loss reconciliation changed map replicas to %d", replicas(d))
	}
	if got := h.co.sealedChannels(); len(got) != 0 {
		t.Fatalf("loss reconciliation sealed channels: %v", got)
	}
}

func TestFeedbackOverflowDoesNotFailWorkload(t *testing.T) {
	wl := mapReduce()
	wl.Spec.Channels[0].Feedback = &graph.Feedback{
		Mode: graph.FeedbackAsynchronous, MaxEpochs: 2, Overflow: "totals",
	}
	h := newHarness(t, wl)
	h.co.set(coordinator.Metrics{
		Channels: []coordinator.ChannelMetrics{
			{Name: "lines", Sealed: true, Overflowed: 7},
			{Name: "shuffle", Sealed: true},
			{Name: "totals", Sealed: true},
		},
		Operations: []coordinator.OperationMetrics{
			{Name: "read", Complete: true},
			{Name: "map", Complete: true},
			{Name: "reduce", Complete: true},
		},
	})
	got := h.reconcile()
	if got.Status.Phase != v1alpha1.WorkloadSucceeded {
		t.Fatalf("workload phase %q after declared overflow, want Succeeded", got.Status.Phase)
	}
}

func TestCompletedProducerKeepsRetainedInternalSegments(t *testing.T) {
	wl := mapReduce()
	wl.Spec.Channels[1].Durability = graph.DurabilityRetained
	h := newHarness(t, wl)
	h.co.set(coordinator.Metrics{Operations: []coordinator.OperationMetrics{{Name: "map"}}})
	h.reconcile()
	h.co.set(coordinator.Metrics{
		Channels:   []coordinator.ChannelMetrics{{Name: "lines", Sealed: true}, {Name: "shuffle"}},
		Operations: []coordinator.OperationMetrics{{Name: "map", Complete: true}},
	})
	got := h.reconcile()
	d, _ := h.deployment("wc-map")
	if replicas(d) != 4 {
		t.Fatalf("retained producer replicas %d after completion, want 4", replicas(d))
	}
	if h.opStatus(got, "map").Phase != v1alpha1.OperationRunning {
		t.Fatalf("retained producer reported complete while its segments remain local")
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

func TestNamedRuntimeSidecarReceivesWorkerSettings(t *testing.T) {
	wl := mapReduce()
	always := corev1.ContainerRestartPolicyAlways
	wl.Spec.Operations[1].Template.Spec.InitContainers = []corev1.Container{{
		Name: runtimeContainer, Image: "runtime:test", RestartPolicy: &always,
	}}
	h := newHarness(t, wl)
	h.reconcile()
	d, _ := h.deployment("wc-map")
	runtime := d.Spec.Template.Spec.InitContainers[0]
	if runtime.StartupProbe == nil || runtime.StartupProbe.HTTPGet == nil ||
		runtime.StartupProbe.HTTPGet.Path != "/healthz" ||
		runtime.StartupProbe.HTTPGet.Port.IntValue() != runtimePort {
		t.Fatalf("runtime startup probe %+v", runtime.StartupProbe)
	}
	if len(runtime.Env) == 0 || len(runtime.VolumeMounts) == 0 ||
		len(runtime.Ports) != 1 || runtime.Ports[0].ContainerPort != coordinator.SegmentPort {
		t.Fatalf("runtime settings incomplete: %+v", runtime)
	}
	application := d.Spec.Template.Spec.Containers[0]
	if len(application.Env) != 0 || len(application.VolumeMounts) != 0 || len(application.Ports) != 0 {
		t.Fatalf("runtime settings injected into application: %+v", application)
	}
}

func TestNativeSidecarStartupProbeGatesApplication(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	wl := mapReduce()
	wl.Spec.Operations[1].Template.Spec.InitContainers = []corev1.Container{{
		Name:          "dependency",
		Image:         "dependency:test",
		RestartPolicy: &always,
		StartupProbe: &corev1.Probe{
			ProbeHandler:     corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(9000)}},
			FailureThreshold: 30,
			PeriodSeconds:    2,
		},
	}}
	wl.Spec.Operations[1].Template.Spec.Containers = append(
		wl.Spec.Operations[1].Template.Spec.Containers,
		corev1.Container{Name: "metrics", Image: "metrics:test"},
	)
	h := newHarness(t, wl)
	h.reconcile()
	d, _ := h.deployment("wc-map")
	sidecar := d.Spec.Template.Spec.InitContainers[0]
	if sidecar.RestartPolicy == nil || *sidecar.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Fatalf("dependency restart policy is %v, want Always", sidecar.RestartPolicy)
	}
	if sidecar.StartupProbe == nil || sidecar.StartupProbe.TCPSocket == nil ||
		sidecar.StartupProbe.TCPSocket.Port.IntValue() != 9000 {
		t.Fatalf("dependency startup probe changed: %+v", sidecar.StartupProbe)
	}
	if len(sidecar.Env) != 0 || len(sidecar.VolumeMounts) != 0 {
		t.Fatalf("runtime settings injected into dependency: env=%v mounts=%v", sidecar.Env, sidecar.VolumeMounts)
	}
	app := d.Spec.Template.Spec.Containers[0]
	if len(app.Env) == 0 || len(app.VolumeMounts) == 0 {
		t.Fatalf("application lacks runtime settings: env=%v mounts=%v", app.Env, app.VolumeMounts)
	}
	helper := d.Spec.Template.Spec.Containers[1]
	if len(helper.Env) != 0 || len(helper.VolumeMounts) != 0 {
		t.Fatalf("runtime settings injected into helper: env=%v mounts=%v", helper.Env, helper.VolumeMounts)
	}
}

func TestDependencyWaitingStatusAndEventTransition(t *testing.T) {
	wl := mapReduce()
	always := corev1.ContainerRestartPolicyAlways
	wl.Spec.Operations[1].Template.Spec.InitContainers = []corev1.Container{{
		Name: "dependency", Image: "dependency:test", RestartPolicy: &always,
		StartupProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(9000)},
		}},
	}}
	h := newHarness(t, wl)
	started := false
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "wc-map-0", Namespace: "default", Labels: opLabels(wl, &wl.Spec.Operations[1])},
		Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{
			Name: "dependency", Started: &started,
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}},
		}}},
	}
	if err := h.c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	got := h.reconcile()
	status := h.opStatus(got, "map")
	if status.Reason != "DependencyWaiting" || !strings.Contains(status.Message, "dependency") {
		t.Fatalf("map status %+v", status)
	}
	found := false
	for len(h.events.Events) > 0 {
		if event := <-h.events.Events; strings.Contains(event, "DependencyWaiting") {
			found = true
		}
	}
	if !found {
		t.Fatal("no dependency wait event emitted")
	}
	h.reconcile()
	select {
	case event := <-h.events.Events:
		t.Fatalf("unchanged dependency emitted another event: %q", event)
	default:
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

// --- tick interval and externally fed operations -----------------------------

func opNamed(name string) v1alpha1.Operation {
	return v1alpha1.Operation{
		Name: name,
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "img"}}},
		},
	}
}

// TestValidateRejectsNegativeTickInterval keeps a typo from becoming a worker
// that never ticks.
func TestValidateRejectsNegativeTickInterval(t *testing.T) {
	op := opNamed("poll")
	op.TickInterval = &metav1.Duration{Duration: -time.Second}
	spec := &v1alpha1.WorkloadSpec{
		Operations: []v1alpha1.Operation{op},
		Channels:   []graph.Channel{{Name: "config", To: "poll"}},
	}
	if err := Validate(spec); err == nil {
		t.Fatal("a negative tickInterval was accepted")
	}
	op.TickInterval = &metav1.Duration{Duration: 30 * time.Second}
	spec.Operations = []v1alpha1.Operation{op}
	if err := Validate(spec); err != nil {
		t.Errorf("a positive tickInterval was rejected: %v", err)
	}
}

// TestTickIntervalReachesThePod: the interval is declared on the operation and
// has to arrive as the environment variable the SDK reads, or the handler
// never fires in a real cluster.
func TestTickIntervalReachesThePod(t *testing.T) {
	wl := mapReduce()
	for i := range wl.Spec.Operations {
		if wl.Spec.Operations[i].Name == "map" {
			wl.Spec.Operations[i].TickInterval = &metav1.Duration{Duration: 90 * time.Second}
		}
	}
	h := newHarness(t, wl)
	h.reconcile()

	d, _ := h.deployment("wc-map")
	var got string
	for _, e := range d.Spec.Template.Spec.Containers[0].Env {
		if e.Name == coordinator.EnvTickInterval {
			got = e.Value
		}
	}
	if got != "1m30s" {
		t.Errorf("%s = %q, want %q", coordinator.EnvTickInterval, got, "1m30s")
	}

	// An operation without an interval must not carry the variable at all, so
	// that FromEnv leaves ticking off rather than parsing an empty string.
	// wc-read is used here because wc-reduce sits behind a Materialized edge
	// and has no deployment until that edge is sealed.
	r, ok := h.deployment("wc-read")
	if !ok {
		t.Fatal("wc-read has no deployment")
	}
	for _, e := range r.Spec.Template.Spec.Containers[0].Env {
		if e.Name == coordinator.EnvTickInterval {
			t.Errorf("an operation with no tickInterval carries %s=%q", e.Name, e.Value)
		}
	}
}

// --- per-operation egress -----------------------------------------------------

// policies lists every network policy in the test namespace by name.
func (h *harness) policies() map[string]networkingv1.NetworkPolicy {
	h.t.Helper()
	var l networkingv1.NetworkPolicyList
	if err := h.c.List(context.Background(), &l, client.InNamespace("default")); err != nil {
		h.t.Fatal(err)
	}
	out := map[string]networkingv1.NetworkPolicy{}
	for _, p := range l.Items {
		out[p.Name] = p
	}
	return out
}

// setEgress replaces one operation's egress declaration in the stored spec.
func (h *harness) setEgress(op string, to ...v1alpha1.EgressDestination) {
	h.t.Helper()
	wl := &v1alpha1.Workload{}
	if err := h.c.Get(context.Background(), h.key, wl); err != nil {
		h.t.Fatal(err)
	}
	for i := range wl.Spec.Operations {
		if wl.Spec.Operations[i].Name != op {
			continue
		}
		wl.Spec.Operations[i].Egress = nil
		for _, d := range to {
			wl.Spec.Operations[i].Egress = append(wl.Spec.Operations[i].Egress, v1alpha1.EgressRule{To: d})
		}
	}
	if err := h.c.Update(context.Background(), wl); err != nil {
		h.t.Fatal(err)
	}
}

// TestNoEgressLeavesThePoliciesUnchanged is the one that matters most. Adding
// this field must not widen anything for a workload that does not use it, so
// the whole set of policies produced without egress is compared against the
// set produced by the same workload before any operation declares any.
func TestNoEgressLeavesThePoliciesUnchanged(t *testing.T) {
	h := newHarness(t, mapReduce())
	h.reconcile()
	before := h.policies()

	// Nothing declared anywhere: no egress policy exists at all.
	for name := range before {
		if strings.Contains(name, "-egress-") {
			t.Errorf("a workload that declares no egress produced policy %s", name)
		}
	}

	// Reconciling again is a no-op, and the shared operations policy still
	// grants exactly the three rules it always did.
	h.reconcile()
	after := h.policies()
	if len(before) != len(after) {
		t.Fatalf("policy count changed from %d to %d across reconciles", len(before), len(after))
	}
	ops := after["wc-operations"]
	if len(ops.Spec.Egress) != 3 {
		t.Fatalf("the operations policy has %d egress rules, want the original 3: %+v", len(ops.Spec.Egress), ops.Spec.Egress)
	}
	for _, rule := range ops.Spec.Egress {
		for _, peer := range rule.To {
			if peer.IPBlock != nil {
				t.Errorf("the shared operations policy gained an ipBlock rule: %+v", peer.IPBlock)
			}
		}
	}
	if !reflect.DeepEqual(before["wc-operations"].Spec, after["wc-operations"].Spec) {
		t.Error("the shared operations policy is not stable across reconciles")
	}
}

// TestEgressMetadataRule pins the metadata grant: the link-local address on
// plain HTTP, and nothing else.
func TestEgressMetadataRule(t *testing.T) {
	h := newHarness(t, mapReduce())
	h.setEgress("map", v1alpha1.EgressMetadata)
	h.reconcile()

	np, ok := h.policies()["wc-egress-map"]
	if !ok {
		t.Fatalf("no egress policy for map; have %v", keys(h.policies()))
	}
	if np.Spec.PodSelector.MatchLabels[LabelOperation] != "map" {
		t.Errorf("the policy selects %v, want only map's pods", np.Spec.PodSelector.MatchLabels)
	}
	if len(np.Spec.PolicyTypes) != 1 || np.Spec.PolicyTypes[0] != networkingv1.PolicyTypeEgress {
		t.Errorf("policy types %v, want Egress alone so ingress stays denied", np.Spec.PolicyTypes)
	}
	if len(np.Spec.Egress) != 1 {
		t.Fatalf("%d egress rules, want 1: %+v", len(np.Spec.Egress), np.Spec.Egress)
	}
	rule := np.Spec.Egress[0]
	if len(rule.To) != 1 || rule.To[0].IPBlock == nil || rule.To[0].IPBlock.CIDR != metadataCIDR {
		t.Errorf("metadata rule targets %+v, want %s", rule.To, metadataCIDR)
	}
	if len(rule.Ports) != 1 || rule.Ports[0].Port.IntValue() != 80 {
		t.Errorf("metadata ports %+v, want TCP 80", rule.Ports)
	}
}

// TestEgressInternetExcludesPrivateRanges pins the exclusion. Without it,
// "reach the internet" would also mean "reach every pod and service in the
// cluster", which would undo the per-edge isolation the policies exist for.
// The failure would widen access rather than deny it, so nothing about a
// running workload would look wrong.
func TestEgressInternetExcludesPrivateRanges(t *testing.T) {
	h := newHarness(t, mapReduce())
	h.setEgress("map", v1alpha1.EgressInternet)
	h.reconcile()

	np := h.policies()["wc-egress-map"]
	if len(np.Spec.Egress) != 1 {
		t.Fatalf("%d egress rules, want 1: %+v", len(np.Spec.Egress), np.Spec.Egress)
	}
	rule := np.Spec.Egress[0]
	if len(rule.To) != 1 || rule.To[0].IPBlock == nil {
		t.Fatalf("internet rule targets %+v, want one ipBlock", rule.To)
	}
	block := rule.To[0].IPBlock
	if block.CIDR != "0.0.0.0/0" {
		t.Errorf("internet CIDR %q, want 0.0.0.0/0", block.CIDR)
	}
	if len(rule.Ports) != 1 || rule.Ports[0].Port.IntValue() != 443 {
		t.Errorf("internet ports %+v, want TCP 443", rule.Ports)
	}
	for _, want := range []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"} {
		found := false
		for _, got := range block.Except {
			if got == want {
				found = true
			}
		}
		if !found {
			t.Errorf("the internet rule does not exclude %s, so it reaches inside the cluster: except=%v", want, block.Except)
		}
	}
	// Link-local is excluded too, so reaching the internet does not quietly
	// include the metadata server.
	for _, got := range block.Except {
		if got == "169.254.0.0/16" {
			return
		}
	}
	t.Errorf("the internet rule does not exclude link-local, so it also grants the metadata server: except=%v", block.Except)
}

// TestEgressBothDestinations: the two grants are independent and compose.
func TestEgressBothDestinations(t *testing.T) {
	h := newHarness(t, mapReduce())
	h.setEgress("map", v1alpha1.EgressMetadata, v1alpha1.EgressInternet)
	h.reconcile()

	np := h.policies()["wc-egress-map"]
	if len(np.Spec.Egress) != 2 {
		t.Fatalf("%d egress rules, want 2: %+v", len(np.Spec.Egress), np.Spec.Egress)
	}
	ports := map[int]string{}
	for _, rule := range np.Spec.Egress {
		if len(rule.Ports) != 1 || len(rule.To) != 1 || rule.To[0].IPBlock == nil {
			t.Fatalf("unexpected rule shape %+v", rule)
		}
		ports[rule.Ports[0].Port.IntValue()] = rule.To[0].IPBlock.CIDR
	}
	if ports[80] != metadataCIDR {
		t.Errorf("port 80 targets %q, want %s", ports[80], metadataCIDR)
	}
	if ports[443] != "0.0.0.0/0" {
		t.Errorf("port 443 targets %q, want 0.0.0.0/0", ports[443])
	}
}

// TestEgressIsPerOperation is the whole reason the field sits on the
// operation. One operation reaching outside must not carry its neighbours
// with it.
func TestEgressIsPerOperation(t *testing.T) {
	h := newHarness(t, mapReduce())
	h.setEgress("map", v1alpha1.EgressInternet)
	h.reconcile()

	pols := h.policies()
	if _, ok := pols["wc-egress-map"]; !ok {
		t.Fatalf("no egress policy for the operation that asked; have %v", keys(pols))
	}
	for _, other := range []string{"read", "reduce"} {
		if _, ok := pols["wc-egress-"+other]; ok {
			t.Errorf("operation %s got an egress policy it never asked for", other)
		}
	}
	// The shared policy still selects every operation and still grants only
	// the original three rules, so the neighbours are where they were.
	ops := pols["wc-operations"]
	if ops.Spec.PodSelector.MatchLabels[LabelOperation] != "" {
		t.Errorf("the shared policy narrowed to %v", ops.Spec.PodSelector.MatchLabels)
	}
	if len(ops.Spec.Egress) != 3 {
		t.Errorf("the shared policy has %d rules, want the original 3", len(ops.Spec.Egress))
	}

	// Withdrawing the declaration withdraws the grant.
	h.setEgress("map")
	h.reconcile()
	if _, ok := h.policies()["wc-egress-map"]; ok {
		t.Error("the egress policy outlived the declaration that asked for it")
	}
}

// TestValidateRejectsUnknownEgressDestination: a typo has to fail admission
// rather than be ignored, or it surfaces as a pod that cannot connect with
// nothing to explain why.
func TestValidateRejectsUnknownEgressDestination(t *testing.T) {
	spec := &v1alpha1.WorkloadSpec{
		Operations: []v1alpha1.Operation{{
			Name:     "feed",
			Template: container(),
			Egress:   []v1alpha1.EgressRule{{To: v1alpha1.EgressDestination("internet")}},
		}},
	}
	err := Validate(spec)
	if err == nil {
		t.Fatal("an unknown egress destination was accepted")
	}
	for _, want := range []string{"feed", "internet"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}

	// The two known destinations pass, including both together.
	spec.Operations[0].Egress = []v1alpha1.EgressRule{{To: v1alpha1.EgressMetadata}, {To: v1alpha1.EgressInternet}}
	if err := Validate(spec); err != nil {
		t.Errorf("the known destinations were rejected: %v", err)
	}
	// So does an operation that declares none.
	spec.Operations[0].Egress = nil
	if err := Validate(spec); err != nil {
		t.Errorf("an operation with no egress was rejected: %v", err)
	}
}

func TestSegmentVolumeSizedFromSpec(t *testing.T) {
	wl := mapReduce()
	size := resource.MustParse("50Gi")
	wl.Spec.Operations[1].Segments = &v1alpha1.SegmentStorage{Size: size}
	h := newHarness(t, wl)
	h.reconcile()

	d, _ := h.deployment("wc-map")
	pod := d.Spec.Template
	v := pod.Spec.Volumes[0]
	if v.Name != segmentVolumeName || v.EmptyDir == nil || v.EmptyDir.SizeLimit == nil {
		t.Fatalf("segment volume %+v", v)
	}
	if v.EmptyDir.SizeLimit.Cmp(size) != 0 {
		t.Errorf("sizeLimit %s, want %s", v.EmptyDir.SizeLimit, &size)
	}
	// The scheduler places the pod by the request. No limit is set: the pod's
	// ephemeral-storage budget is charged the volume together with the
	// writable layers and logs, so a limit equal to size would evict the pod
	// before the volume filled.
	c := pod.Spec.Containers[0]
	got, ok := c.Resources.Requests[corev1.ResourceEphemeralStorage]
	if !ok {
		t.Fatalf("requests has no ephemeral-storage: %v", c.Resources.Requests)
	}
	if got.Cmp(size) != 0 {
		t.Errorf("request ephemeral-storage %s, want %s", &got, &size)
	}
	if lim, ok := c.Resources.Limits[corev1.ResourceEphemeralStorage]; ok {
		t.Errorf("ephemeral-storage limit %s set, want none", &lim)
	}

	// An operation that declares nothing is left as it was.
	read, _ := h.deployment("wc-read")
	rc := read.Spec.Template.Spec.Containers[0]
	if v := read.Spec.Template.Spec.Volumes[0]; v.EmptyDir == nil || v.EmptyDir.SizeLimit != nil {
		t.Errorf("undeclared operation got a sizeLimit: %+v", v)
	}
	if _, ok := rc.Resources.Requests[corev1.ResourceEphemeralStorage]; ok {
		t.Errorf("undeclared operation got an ephemeral-storage request: %v", rc.Resources.Requests)
	}
}

func TestSegmentSizeIsAFloorNotAnOverride(t *testing.T) {
	wl := mapReduce()
	tpl := container()
	tpl.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse("250m"),
			corev1.ResourceEphemeralStorage: resource.MustParse("10Gi"),
		},
		Limits: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("100Gi")},
	}
	wl.Spec.Operations[1].Template = tpl
	wl.Spec.Operations[1].Segments = &v1alpha1.SegmentStorage{Size: resource.MustParse("50Gi")}
	h := newHarness(t, wl)
	h.reconcile()

	d, _ := h.deployment("wc-map")
	c := d.Spec.Template.Spec.Containers[0]
	// A request too small to hold the segments is raised to size; the limit
	// the template named is its own business and is passed through.
	if got, want := c.Resources.Requests[corev1.ResourceEphemeralStorage], resource.MustParse("50Gi"); got.Cmp(want) != 0 {
		t.Errorf("request %s, want %s", &got, &want)
	}
	if got, want := c.Resources.Limits[corev1.ResourceEphemeralStorage], resource.MustParse("100Gi"); got.Cmp(want) != 0 {
		t.Errorf("limit %s, want %s", &got, &want)
	}
	if got, want := c.Resources.Requests[corev1.ResourceCPU], resource.MustParse("250m"); got.Cmp(want) != 0 {
		t.Errorf("cpu request %s clobbered, want %s", &got, &want)
	}
}

// A template that already asks for more than segments.size keeps its own
// request, and still gets no limit invented for it.
func TestSegmentSizeLeavesALargerRequestAlone(t *testing.T) {
	wl := mapReduce()
	tpl := container()
	tpl.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("100Gi")},
	}
	wl.Spec.Operations[1].Template = tpl
	wl.Spec.Operations[1].Segments = &v1alpha1.SegmentStorage{Size: resource.MustParse("50Gi")}
	h := newHarness(t, wl)
	h.reconcile()

	d, _ := h.deployment("wc-map")
	c := d.Spec.Template.Spec.Containers[0]
	req := c.Resources.Requests[corev1.ResourceEphemeralStorage]
	if want := resource.MustParse("100Gi"); req.Cmp(want) != 0 {
		t.Errorf("request %s, want %s", &req, &want)
	}
	if lim, ok := c.Resources.Limits[corev1.ResourceEphemeralStorage]; ok {
		t.Errorf("ephemeral-storage limit %s set, want none", &lim)
	}
	// The volume is still sized, so the operation is not silently opted out.
	if v := d.Spec.Template.Spec.Volumes[0]; v.EmptyDir == nil || v.EmptyDir.SizeLimit == nil {
		t.Fatalf("segment volume lost its sizeLimit: %+v", v)
	}
}

// Kubernetes defaults an absent request to the container's limit, so a
// template naming only a limit is already asking for that much. Writing a
// smaller request would stop the defaulting and shrink the reservation.
func TestSegmentSizeDoesNotLowerARequestDefaultedFromALimit(t *testing.T) {
	wl := mapReduce()
	tpl := container()
	tpl.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("100Gi")},
	}
	wl.Spec.Operations[1].Template = tpl
	wl.Spec.Operations[1].Segments = &v1alpha1.SegmentStorage{Size: resource.MustParse("50Gi")}
	h := newHarness(t, wl)
	h.reconcile()

	d, _ := h.deployment("wc-map")
	c := d.Spec.Template.Spec.Containers[0]
	// Either the request is left absent, so Kubernetes defaults it to the
	// 100Gi limit, or it is written at no less than that. Writing 50Gi would
	// suppress the defaulting and shrink the reservation.
	if req, ok := c.Resources.Requests[corev1.ResourceEphemeralStorage]; ok && req.Cmp(resource.MustParse("100Gi")) < 0 {
		t.Errorf("request %s undercuts the limit it would have defaulted from", &req)
	}
	if v := d.Spec.Template.Spec.Volumes[0]; v.EmptyDir == nil || v.EmptyDir.SizeLimit == nil {
		t.Fatalf("segment volume lost its sizeLimit: %+v", v)
	}
}

// The request goes on the first container, so that container's own limit has
// to be able to carry it even when the pod's total budget clears the size.
func TestSegmentSizeRejectedAboveTheRequestCarrierLimit(t *testing.T) {
	wl := mapReduce()
	tpl := container()
	tpl.Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("30Gi")}
	tpl.Spec.Containers = append(tpl.Spec.Containers, corev1.Container{
		Name:      "sidecar",
		Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("30Gi")}},
	})
	wl.Spec.Operations[1].Template = tpl
	wl.Spec.Operations[1].Segments = &v1alpha1.SegmentStorage{Size: resource.MustParse("50Gi")}
	// The pod budget is 60Gi, so the pod-level rule alone would admit this and
	// the emitted container would carry request 50Gi against its own 30Gi
	// limit, which the API server refuses.
	if err := Validate(&wl.Spec); err == nil {
		t.Fatal("segments.size above the request carrier's own limit accepted")
	}
}

func TestPodEphemeralStorageLimit(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	lim := func(q string) corev1.ResourceRequirements {
		return corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse(q)}}
	}
	cases := []struct {
		name string
		spec corev1.PodSpec
		want string
		set  bool
	}{
		{name: "none", spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "a"}}}},
		{
			name: "containers add up",
			spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "a", Resources: lim("40Gi")}, {Name: "b", Resources: lim("20Gi")}}},
			want: "60Gi", set: true,
		},
		{
			// A container naming no limit contributes nothing, so the sidecar
			// alone caps the pod.
			name: "one container unlimited",
			spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "a"}, {Name: "b", Resources: lim("1Gi")}}},
			want: "1Gi", set: true,
		},
		{
			// Restartable init containers are sidecars: they run alongside the
			// regular containers, so the kubelet adds them.
			name: "restartable init adds",
			spec: corev1.PodSpec{
				Containers:     []corev1.Container{{Name: "a", Resources: lim("40Gi")}},
				InitContainers: []corev1.Container{{Name: "s", RestartPolicy: &always, Resources: lim("20Gi")}},
			},
			want: "60Gi", set: true,
		},
		{
			// A plain init container has finished by then, so it is a floor.
			name: "plain init is a floor",
			spec: corev1.PodSpec{
				Containers:     []corev1.Container{{Name: "a", Resources: lim("40Gi")}},
				InitContainers: []corev1.Container{{Name: "i", Resources: lim("80Gi")}},
			},
			want: "80Gi", set: true,
		},
		{
			// Presence arms the kubelet's check, so zero is set, not unset.
			name: "zero init limit is still set",
			spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: "i", Resources: lim("0")}}},
			want: "0", set: true,
		},
	}
	for _, c := range cases {
		got, set := podEphemeralStorageLimit(&c.spec)
		if set != c.set {
			t.Errorf("%s: set %v, want %v", c.name, set, c.set)
			continue
		}
		if !set {
			continue
		}
		if want := resource.MustParse(c.want); got.Cmp(want) != 0 {
			t.Errorf("%s: limit %s, want %s", c.name, &got, &want)
		}
	}
}
