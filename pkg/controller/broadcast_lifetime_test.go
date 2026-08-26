package controller

import (
	"context"
	"net/http/httptest"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/coordinator"
)

// liveHarness drives the reconciler against a real coordinator, so the
// metrics under test are the ones the engine computes and the replica counts
// the reconciler publishes reach something that reads them.
type liveHarness struct {
	*harness
	co *coordinator.Coordinator
}

func newLiveHarness(t *testing.T, wl *v1alpha1.Workload) *liveHarness {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(&v1alpha1.Workload{}).WithObjects(wl).Build()
	co := coordinator.New("self:8090")
	co.Configure(wl.Spec.Channels)
	srv := httptest.NewServer(coordinator.Handler(co))
	t.Cleanup(srv.Close)
	r := &Reconciler{
		Client:              c,
		CoordinatorImage:    "coord:test",
		ControllerNamespace: "stark8s-system",
		CoordinatorURL:      func(*v1alpha1.Workload) string { return srv.URL },
		HTTP:                srv.Client(),
	}
	return &liveHarness{harness: &harness{t: t, c: c, r: r, key: client.ObjectKeyFromObject(wl)}, co: co}
}

func (h *liveHarness) pass() {
	h.t.Helper()
	if _, err := h.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: h.key}); err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
}

// A source feeding a Broadcast Materialized channel into a three-replica
// consumer: the shape that lost a segment on a cluster.
func broadcastStage() *v1alpha1.Workload {
	return &v1alpha1.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: "bcast", Namespace: "default"},
		Spec: v1alpha1.WorkloadSpec{
			Operations: []v1alpha1.Operation{
				{Name: "vocab", Scaling: v1alpha1.Scaling{Horizontal: v1alpha1.HorizontalScaling{Min: 1, Max: 1}}, Template: container()},
				{Name: "extract", Scaling: v1alpha1.Scaling{Horizontal: v1alpha1.HorizontalScaling{Min: 3, Max: 3}}, Template: container()},
			},
			Channels: []v1alpha1.Channel{
				{Name: "dict", From: "vocab", To: "extract",
					Partitioning: v1alpha1.Partitioning{Mode: v1alpha1.PartitionBroadcast},
					Delivery:     v1alpha1.DeliveryMaterialized},
			},
		},
	}
}

func TestProducerKeptWhileBroadcastReplicasAreStillStarting(t *testing.T) {
	h := newLiveHarness(t, broadcastStage())
	h.pass()
	if d, ok := h.deployment("bcast-vocab"); !ok || replicas(d) != 1 {
		t.Fatalf("vocab not started: ok=%v", ok)
	}

	// The source runs: one segment written, then done. The controller seals
	// "dict" on the next pass, which starts the consumer.
	if err := h.co.Register(coordinator.PodRegistration{Operation: "vocab", Pod: "vocab-0", Addr: "10.0.0.1:8090", Slots: 1}); err != nil {
		t.Fatal(err)
	}
	if err := h.co.Announce("dict", "vocab", []coordinator.SegmentAnnouncement{{
		ID: "seg-1", Channel: "dict", Records: 100, Bytes: 4096,
		Holder: "10.0.0.1:8090", Producer: "vocab-0"}}); err != nil {
		t.Fatal(err)
	}
	if err := h.co.SourceDone(coordinator.PodRegistration{Operation: "vocab", Pod: "vocab-0"}); err != nil {
		t.Fatal(err)
	}
	h.pass()
	h.pass()
	d, ok := h.deployment("bcast-extract")
	if !ok || replicas(d) != 3 {
		t.Fatalf("extract not started with three replicas: ok=%v", ok)
	}

	// One of the three replicas comes up first and reads the whole channel.
	if _, err := h.co.Consume("dict", "extract", "extract-0", 10); err != nil {
		t.Fatal(err)
	}
	if err := h.co.Ack("dict", []coordinator.SegmentAck{{ID: "seg-1", Holder: "10.0.0.1:8090", Pod: "extract-0"}}); err != nil {
		t.Fatal(err)
	}
	if err := h.co.SourceDone(coordinator.PodRegistration{Operation: "extract", Pod: "extract-0"}); err != nil {
		t.Fatal(err)
	}

	h.pass()
	d, _ = h.deployment("bcast-vocab")
	if replicas(d) == 0 {
		t.Errorf("vocab scaled to zero while two of three replicas have not read seg-1")
	}
	if rel := h.co.Released("vocab-0"); len(rel) != 0 {
		t.Errorf("vocab told to delete %v while two of three replicas have not read it", rel)
	}
	d, _ = h.deployment("bcast-extract")
	if replicas(d) != 3 {
		t.Errorf("extract scaled to %d before its other replicas read the channel", replicas(d))
	}

	// The other two arrive and read it.
	for _, pod := range []string{"extract-1", "extract-2"} {
		if _, err := h.co.Consume("dict", "extract", pod, 10); err != nil {
			t.Fatal(err)
		}
		if err := h.co.Ack("dict", []coordinator.SegmentAck{{ID: "seg-1", Holder: "10.0.0.1:8090", Pod: pod}}); err != nil {
			t.Fatal(err)
		}
		if err := h.co.SourceDone(coordinator.PodRegistration{Operation: "extract", Pod: pod}); err != nil {
			t.Fatal(err)
		}
	}
	h.pass()
	d, _ = h.deployment("bcast-vocab")
	if replicas(d) != 0 {
		t.Errorf("vocab still at %d replicas after every consumer replica read the channel", replicas(d))
	}
}
