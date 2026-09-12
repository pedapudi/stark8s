package controller

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/coordinator"
)

func collectiveWorkload() *v1alpha1.Workload {
	return &v1alpha1.Workload{ObjectMeta: metav1.ObjectMeta{Name: "train", Namespace: "default"}, Spec: v1alpha1.WorkloadSpec{Operations: []v1alpha1.Operation{{Name: "workers", Template: container(), Collective: &v1alpha1.CollectiveSpec{Size: 3, MaxAttempts: 2, Checkpoint: "checkpoints/train"}}}}}
}

func TestCollectiveCreatesIndexedJobAndHeadlessService(t *testing.T) {
	wl := collectiveWorkload()
	always := corev1.ContainerRestartPolicyAlways
	wl.Spec.Operations[0].Template.Spec.InitContainers = []corev1.Container{{Name: "stark8s-runtime", RestartPolicy: &always}}
	h := newHarness(t, wl)
	defer h.co.Close()
	h.reconcile()
	job := &batchv1.Job{}
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "train-workers-attempt-1"}, job); err != nil {
		t.Fatal(err)
	}
	if job.Spec.CompletionMode == nil || *job.Spec.CompletionMode != batchv1.IndexedCompletion || *job.Spec.Parallelism != 3 || *job.Spec.BackoffLimit != 0 {
		t.Fatalf("job spec: %+v", job.Spec)
	}
	if job.Spec.Template.Spec.Subdomain != "train-workers-attempt-1" {
		t.Fatalf("subdomain %q", job.Spec.Template.Spec.Subdomain)
	}
	env := map[string]corev1.EnvVar{}
	for _, v := range job.Spec.Template.Spec.Containers[0].Env {
		env[v.Name] = v
	}
	if env[coordinator.EnvCollectiveRendezvous].Value != "train-workers-attempt-1-0.train-workers-attempt-1.default.svc" {
		t.Fatalf("rendezvous %+v", env[coordinator.EnvCollectiveRendezvous])
	}
	if env[coordinator.EnvCollectiveRank].ValueFrom == nil {
		t.Fatal("rank does not use completion index")
	}
	runtimeEnv := map[string]corev1.EnvVar{}
	for _, v := range job.Spec.Template.Spec.InitContainers[0].Env {
		runtimeEnv[v.Name] = v
	}
	if runtimeEnv[coordinator.EnvCollectiveRank].ValueFrom == nil {
		t.Fatal("runtime sidecar rank does not use completion index")
	}
	service := &corev1.Service{}
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "train-workers-attempt-1"}, service); err != nil {
		t.Fatal(err)
	}
	if service.Spec.ClusterIP != corev1.ClusterIPNone || !service.Spec.PublishNotReadyAddresses ||
		len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Name != "rank-discovery" {
		t.Fatalf("service: %+v", service.Spec)
	}
	shared := &networkingv1.NetworkPolicy{}
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "train-operations"}, shared); err != nil {
		t.Fatal(err)
	}
	if len(shared.Spec.Ingress) != 0 {
		t.Fatalf("collective widened ingress to every operation: %+v", shared.Spec.Ingress)
	}
	peers := &networkingv1.NetworkPolicy{}
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "train-collective-workers"}, peers); err != nil {
		t.Fatal(err)
	}
	if peers.Spec.PodSelector.MatchLabels[LabelOperation] != "workers" ||
		len(peers.Spec.Ingress) != 1 || len(peers.Spec.Egress) != 1 {
		t.Fatalf("collective peer policy: %+v", peers.Spec)
	}
}

func TestCollectivePublishesOneGraphParticipant(t *testing.T) {
	wl := collectiveWorkload()
	wl.Spec.Operations[0].Scaling.Horizontal.Max = 9
	h := newHarness(t, wl)
	defer h.co.Close()
	h.reconcile()
	specs := h.co.operationSpecs()
	if len(specs) != 1 || specs[0].Name != "workers" || specs[0].Replicas != 1 {
		t.Fatalf("collective operation specs: %+v", specs)
	}
}

func TestFailedCollectiveCreatesBoundedReplacementAttempt(t *testing.T) {
	h := newHarness(t, collectiveWorkload())
	defer h.co.Close()
	h.reconcile()
	job := &batchv1.Job{}
	key := types.NamespacedName{Namespace: "default", Name: "train-workers-attempt-1"}
	if err := h.c.Get(context.Background(), key, job); err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	if err := h.c.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	h.reconcile()
	replacement := &batchv1.Job{}
	if err := h.c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "train-workers-attempt-2"}, replacement); err != nil {
		t.Fatal(err)
	}
	if got := replacement.Spec.Template.Spec.Containers[0].Env[0].Name; got != "STARK8S_COLLECTIVE_RANK" {
		t.Fatalf("replacement env starts with %q", got)
	}
	replacement.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	if err := h.c.Status().Update(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	want := h.reconcile()
	if want.Status.Phase != v1alpha1.WorkloadFailed {
		t.Fatalf("phase %s, want Failed", want.Status.Phase)
	}
	third := &batchv1.Job{}
	err := h.c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "train-workers-attempt-3"}, third)
	if err == nil {
		t.Fatal("created attempt beyond maxAttempts")
	}
}

func TestCollectiveIgnoresMatchingJobItDoesNotOwn(t *testing.T) {
	wl := collectiveWorkload()
	h := newHarness(t, wl)
	defer h.co.Close()
	h.reconcile()
	foreign := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      "foreign-attempt",
		Namespace: wl.Namespace,
		Labels: map[string]string{
			LabelWorkload:          wl.Name,
			LabelOperation:         "workers",
			LabelRole:              RoleOperation,
			labelCollectiveAttempt: "99",
		},
	}, Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
	if err := h.c.Create(context.Background(), foreign); err != nil {
		t.Fatal(err)
	}
	got := h.reconcile()
	if got.Status.Phase != v1alpha1.WorkloadRunning {
		t.Fatalf("foreign Job changed workload phase to %s", got.Status.Phase)
	}
}

func TestCompletedCollectiveSealsOutboundChannels(t *testing.T) {
	wl := collectiveWorkload()
	wl.Spec.Operations[0].Collective.MaxAttempts = 1
	wl.Spec.Channels = []graph.Channel{{Name: "result", From: "workers"}}
	h := newHarness(t, wl)
	defer h.co.Close()
	h.reconcile()
	job := &batchv1.Job{}
	key := types.NamespacedName{Namespace: "default", Name: "train-workers-attempt-1"}
	if err := h.c.Get(context.Background(), key, job); err != nil {
		t.Fatal(err)
	}
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := h.c.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	got := h.reconcile()
	if got.Status.Phase != v1alpha1.WorkloadSucceeded {
		t.Fatalf("workload phase %s, want Succeeded", got.Status.Phase)
	}
	if !contains(h.co.sealedChannels(), "result") {
		t.Fatal("completed collective did not seal its output")
	}
}

func TestGangPlacementFailsValidationWithoutNativeCapability(t *testing.T) {
	wl := collectiveWorkload()
	wl.Spec.Operations[0].Collective.Placement = v1alpha1.CollectivePlacementGang
	if err := Validate(&wl.Spec); err == nil {
		t.Fatal("gang placement accepted without native capability")
	}
}

func TestCollectiveInternalOutputRequiresDurableStorage(t *testing.T) {
	wl := collectiveWorkload()
	wl.Spec.Operations[0].Checkpoint = true
	wl.Spec.Operations = append(wl.Spec.Operations, v1alpha1.Operation{Name: "consumer", Template: container()})
	wl.Spec.Channels = []graph.Channel{{Name: "result", From: "workers", To: "consumer"}}
	if err := Validate(&wl.Spec); err == nil {
		t.Fatal("accepted collective internal output without durable storage")
	}
	wl.Spec.Coordinator.ObjectStore = &v1alpha1.ObjectStoreSpec{
		Endpoint: "http://objects.example/bucket", CredentialsSecret: "object-store-credentials",
	}
	if err := Validate(&wl.Spec); err != nil {
		t.Fatalf("rejected collective internal output with durable storage: %v", err)
	}
}

func TestCollectiveGraphRetryRequiresOperationCheckpoint(t *testing.T) {
	wl := collectiveWorkload()
	wl.Spec.Channels = []graph.Channel{{Name: "result", From: "workers"}}
	if err := Validate(&wl.Spec); err == nil {
		t.Fatal("accepted collective graph retry without an operation checkpoint")
	}
	wl.Spec.Operations[0].Checkpoint = true
	if err := Validate(&wl.Spec); err == nil {
		t.Fatal("accepted collective graph retry without checkpoint storage")
	}
	wl.Spec.Coordinator.ObjectStore = &v1alpha1.ObjectStoreSpec{
		Endpoint: "http://objects.example/bucket", CredentialsSecret: "object-store-credentials",
	}
	if err := Validate(&wl.Spec); err != nil {
		t.Fatalf("rejected fenced collective graph retry: %v", err)
	}
}
