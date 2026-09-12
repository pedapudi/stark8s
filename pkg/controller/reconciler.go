// Package controller reconciles Workload objects into Kubernetes resources.
//
// Mapping, per workload:
//
//	coordinator     -> one Deployment + Service (<workload>-coordinator) holding
//	                   topology, pod registry, partition ownership, segment
//	                   index, seals, and loop epochs
//	operation       -> Deployment (<workload>-<operation>) whatever its
//	                   completion rule, plus a ServiceAccount of the same name,
//	                   labelled stark8s.io/workload and stark8s.io/operation,
//	                   with STARK8S_* environment, a segment volume sized by
//	                   segments.size, and the segment port injected into
//	                   every pod
//	channels        -> pushed to the coordinator as topology; sealed when the
//	                   coordinator reports their producing operation complete
//	scaling         -> fixed maximum replica membership during an attempt;
//	                   initial vertical resource sizing when requested
//	network         -> one NetworkPolicy per channel edge (consumer pods may
//	                   open the producer's segment port), one for all
//	                   operation pods, one for the coordinator
//
// Completion of a batch operation is a coordinator decision, expressed by
// the controller as scaling the Deployment to zero. Pods that still hold
// Local segments a consumer has not fetched are kept until the coordinator
// reports ephemeral segments released. Retained segments keep their producer
// pods because they have no durable backing store.
//
// An operation whose inbound Materialized channels are not all sealed is not
// started: its Deployment is created only when its inputs are complete. This
// is the stage barrier of a shuffle-based engine expressed as scheduling.
package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/pedapudi/stark8s/api/graph"
	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/coordinator"
)

const (
	LabelWorkload  = "stark8s.io/workload"
	LabelOperation = "stark8s.io/operation"
	LabelRole      = "stark8s.io/role"
	// LabelChannel is set on per-edge NetworkPolicies so that policies for
	// channels removed from the spec can be found and deleted.
	LabelChannel = "stark8s.io/channel"

	RoleOperation   = "operation"
	RoleCoordinator = "coordinator"

	// SegmentDir is where operation pods keep local segments; an emptyDir
	// volume is mounted there in every container.
	SegmentDir        = "/var/lib/stark8s/segments"
	segmentVolumeName = "stark8s-segments"
	runtimeContainer  = "stark8s-runtime"
	runtimePort       = 8081

	pollInterval = 3 * time.Second
)

// minSegmentSize is the smallest segments.size Validate accepts. Quantities
// without a unit suffix are bytes, so `size: 50` asks for fifty bytes; the
// floor is there to catch that slip rather than to say anything about how much
// a real workload needs.
var minSegmentSize = resource.MustParse("1Mi")

// Reconciler reconciles Workloads.
type Reconciler struct {
	client.Client
	// CoordinatorImage is used when the workload does not name one.
	CoordinatorImage string
	// ControllerNamespace is allowed through the coordinator's ingress policy.
	ControllerNamespace string
	// CoordinatorURL overrides the in-cluster coordinator address (tests).
	CoordinatorURL func(wl *v1alpha1.Workload) string
	HTTP           *http.Client
	Recorder       record.EventRecorder
}

// SetupWithManager registers the reconciler.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.HTTP == nil {
		r.HTTP = &http.Client{Timeout: 10 * time.Second}
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("stark8s-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Workload{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Complete(r)
}

// Reconcile drives one workload toward its spec.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if r.HTTP == nil {
		r.HTTP = &http.Client{Timeout: 10 * time.Second}
	}
	wl := &v1alpha1.Workload{}
	if err := r.Get(ctx, req.NamespacedName, wl); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if wl.Status.Phase == v1alpha1.WorkloadSucceeded || wl.Status.Phase == v1alpha1.WorkloadFailed {
		return ctrl.Result{}, nil
	}
	if err := Validate(&wl.Spec); err != nil {
		return r.setPhase(ctx, wl, v1alpha1.WorkloadFailed, "InvalidWorkload", "invalid workload: "+err.Error())
	}
	if err := r.ensureCoordinator(ctx, wl); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureServiceAccounts(ctx, wl); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureNetworkPolicies(ctx, wl); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.pushTopology(ctx, wl); err != nil {
		logger.Info("coordinator not ready", "err", err.Error())
		if _, err := r.setPhase(ctx, wl, v1alpha1.WorkloadPending, "CoordinatorUnavailable", "waiting for coordinator"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	metrics, err := r.metrics(ctx, wl)
	if err != nil {
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	for _, m := range metrics.Channels {
		if m.Lost == 0 {
			continue
		}
		wl.Status.Channels = nil
		var lost []string
		for _, channel := range metrics.Channels {
			wl.Status.Channels = append(wl.Status.Channels, channelStatus(channel))
			if channel.Lost > 0 {
				lost = append(lost, fmt.Sprintf("%s (%d records)", channel.Name, channel.Lost))
			}
		}
		oldOperations := wl.Status.Operations
		previous := map[string]v1alpha1.OperationStatus{}
		for _, status := range oldOperations {
			previous[status.Name] = status
		}
		wl.Status.Operations = nil
		for i := range wl.Spec.Operations {
			op := &wl.Spec.Operations[i]
			status := previous[op.Name]
			status.Name = op.Name
			for _, channel := range metrics.Channels {
				if channel.To == op.Name && channel.Lost > 0 {
					status.Phase, status.Reason = v1alpha1.OperationFailed, "UnavailableData"
					status.Message = fmt.Sprintf("channel %s lost %d required records", channel.Name, channel.Lost)
				}
			}
			wl.Status.Operations = append(wl.Status.Operations, status)
		}
		if r.Recorder != nil {
			for _, status := range wl.Status.Operations {
				if status.Reason == "UnavailableData" && previous[status.Name].Reason != status.Reason {
					r.Recorder.Eventf(wl, corev1.EventTypeWarning, status.Reason, "operation %s: %s", status.Name, status.Message)
				}
			}
		}
		return r.setPhase(ctx, wl, v1alpha1.WorkloadFailed, "RequiredRecordsLost",
			"required records lost on channel "+strings.Join(lost, ", "))
	}
	for i := range wl.Spec.Operations {
		op := &wl.Spec.Operations[i]
		if err := r.ensureHPA(ctx, wl, op); err != nil {
			return ctrl.Result{}, err
		}
		if op.Scaling.Vertical != nil && op.Scaling.Vertical.Mode == v1alpha1.VerticalAuto {
			if err := r.ensureVPA(ctx, wl, op); err != nil {
				return ctrl.Result{}, err
			}
		}
	}
	// Publish fixed membership before creating operation Deployments. A worker
	// may finish before a later reconcile observes every intended replica.
	if err := r.pushOperations(ctx, wl); err != nil {
		return ctrl.Result{}, err
	}

	var opStatus []v1alpha1.OperationStatus
	allDrained, anyStreaming, anyFailed := true, false, false
	for i := range wl.Spec.Operations {
		op := &wl.Spec.Operations[i]
		st, err := r.reconcileOperation(ctx, wl, op, metrics)
		if err != nil {
			return ctrl.Result{}, err
		}
		opStatus = append(opStatus, st)
		if st.Phase == v1alpha1.OperationFailed {
			anyFailed = true
		}
		if op.Completion == v1alpha1.CompletionNever {
			anyStreaming = true
		} else if st.Phase != v1alpha1.OperationSucceeded {
			allDrained = false
		}
	}

	// Refresh metrics after sealing so status reflects this pass.
	if m, err := r.metrics(ctx, wl); err == nil {
		metrics = m
	}
	oldOperations := wl.Status.Operations
	wl.Status.Operations = opStatus
	wl.Status.Channels = nil
	for _, m := range metrics.Channels {
		wl.Status.Channels = append(wl.Status.Channels, channelStatus(m))
	}
	phase, msg := v1alpha1.WorkloadRunning, ""
	for _, op := range wl.Spec.Operations {
		if op.Scaling.Horizontal.CPUUtilizationPercent > 0 ||
			(op.Scaling.Vertical != nil && op.Scaling.Vertical.Mode == v1alpha1.VerticalAuto) {
			msg = "automatic scaling disabled because operation pods may hold local state or output"
			break
		}
	}
	if anyFailed {
		phase, msg = v1alpha1.WorkloadFailed, "one or more operations failed"
	} else if allDrained && !anyStreaming {
		phase, msg = v1alpha1.WorkloadSucceeded, "all operations drained"
	}
	oldReason := wl.Status.Reason
	wl.Status.Phase, wl.Status.Reason, wl.Status.Message = phase, reasonForPhase(phase, msg), msg
	if err := r.Status().Update(ctx, wl); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	r.emitStatusTransitions(wl, oldReason, oldOperations)
	if phase == v1alpha1.WorkloadRunning {
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	return ctrl.Result{}, nil
}

func channelStatus(m coordinator.ChannelMetrics) v1alpha1.ChannelStatus {
	return v1alpha1.ChannelStatus{
		Name: m.Name, Sealed: m.Sealed, Pending: m.Pending, InFlight: m.InFlight,
		Produced: m.Produced, Acknowledged: m.Acknowledged, Epoch: m.Epoch,
		Overflowed: m.Overflowed, Lost: m.Lost, LatestDeliveryFailure: m.LatestDeliveryFailure,
	}
}

func (r *Reconciler) setPhase(ctx context.Context, wl *v1alpha1.Workload, phase v1alpha1.WorkloadPhase, reason, msg string) (ctrl.Result, error) {
	oldReason := wl.Status.Reason
	wl.Status.Phase, wl.Status.Reason, wl.Status.Message = phase, reason, msg
	err := client.IgnoreNotFound(r.Status().Update(ctx, wl))
	if err == nil && r.Recorder != nil && oldReason != reason {
		eventType := corev1.EventTypeNormal
		if phase == v1alpha1.WorkloadFailed {
			eventType = corev1.EventTypeWarning
		}
		r.Recorder.Event(wl, eventType, reason, msg)
	}
	return ctrl.Result{}, err
}

func reasonForPhase(phase v1alpha1.WorkloadPhase, message string) string {
	switch {
	case phase == v1alpha1.WorkloadSucceeded:
		return "OperationsDrained"
	case phase == v1alpha1.WorkloadFailed:
		return "OperationFailed"
	case strings.Contains(message, "automatic scaling disabled"):
		return "AutomaticScalingDisabled"
	default:
		return "OperationsRunning"
	}
}

func (r *Reconciler) emitStatusTransitions(wl *v1alpha1.Workload, oldReason string, old []v1alpha1.OperationStatus) {
	if r.Recorder == nil {
		return
	}
	if wl.Status.Reason != oldReason {
		r.Recorder.Event(wl, corev1.EventTypeNormal, wl.Status.Reason, wl.Status.Message)
	}
	previous := map[string]string{}
	for _, status := range old {
		previous[status.Name] = status.Reason
	}
	for _, status := range wl.Status.Operations {
		if status.Reason != "" && previous[status.Name] != status.Reason {
			eventType := corev1.EventTypeNormal
			if status.Phase == v1alpha1.OperationFailed {
				eventType = corev1.EventTypeWarning
			}
			r.Recorder.Eventf(wl, eventType, status.Reason, "operation %s: %s", status.Name, status.Message)
		}
	}
}

// Validate checks graph integrity: channels reference declared operations,
// names are unique, feedback overflow targets are declared channels, slot
// counts are positive, and every cycle passes through a feedback channel.
func Validate(s *v1alpha1.WorkloadSpec) error {
	ops := map[string]bool{}
	for _, o := range s.Operations {
		if ops[o.Name] {
			return fmt.Errorf("duplicate operation %q", o.Name)
		}
		ops[o.Name] = true
		// Zero is the unset value and means one slot.
		if o.Slots < 0 {
			return fmt.Errorf("operation %q: slots must be at least 1", o.Name)
		}
		if o.TickInterval != nil && o.TickInterval.Duration < 0 {
			return fmt.Errorf("operation %q: tickInterval must not be negative", o.Name)
		}
		for _, e := range o.Egress {
			switch e.To {
			case v1alpha1.EgressMetadata, v1alpha1.EgressInternet:
			default:
				// Rejected here rather than ignored, so that a typo is a
				// failure to admit the workload instead of a pod that cannot
				// open a connection for reasons nothing explains.
				return fmt.Errorf("operation %q: unknown egress destination %q, want one of %q or %q",
					o.Name, e.To, v1alpha1.EgressMetadata, v1alpha1.EgressInternet)
			}
		}
		if o.Segments != nil {
			if o.Segments.Size.Sign() <= 0 {
				return fmt.Errorf("operation %q: segments.size must be greater than zero", o.Name)
			}
			// A bare number is bytes, so a missing unit suffix asks for a
			// volume no segment can fit in, and the kubelet evicts the pod as
			// soon as the directory exists. Refuse it rather than emit a pod
			// that crash-loops on eviction.
			if o.Segments.Size.Cmp(minSegmentSize) < 0 {
				return fmt.Errorf("operation %q: segments.size %s is below %s; a size without a unit suffix is bytes", o.Name, &o.Segments.Size, &minSegmentSize)
			}
			// The controller leaves a pre-declared segment volume alone, so
			// segments.size would silently not apply to it.
			for _, v := range o.Template.Spec.Volumes {
				if v.Name == segmentVolumeName {
					return fmt.Errorf("operation %q: segments.size and a pod template volume named %q both size the segment volume; declare one or the other", o.Name, segmentVolumeName)
				}
			}
			// The pod's ephemeral-storage limit is charged the segment volume
			// together with every container's writable layer and logs, so a
			// budget that only just covers the volume evicts the pod before
			// the volume can fill. Raising the limit to fit would produce a
			// pod the cluster may refuse; the contradiction is the user's to
			// resolve.
			if lim, ok := podEphemeralStorageLimit(&o.Template.Spec); ok && lim.Cmp(o.Segments.Size) <= 0 {
				return fmt.Errorf("operation %q: segments.size %s leaves nothing under the pod's ephemeral-storage limit of %s, which is charged the segment volume together with every container's writable layer and logs", o.Name, &o.Segments.Size, &lim)
			}
			// The request goes on the first container, and Kubernetes refuses a
			// container whose request exceeds its own limit. The pod's budget
			// can clear segments.size on the strength of its other containers
			// while this one cannot carry the request.
			if len(o.Template.Spec.Containers) > 0 {
				if lim, ok := o.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceEphemeralStorage]; ok && lim.Cmp(o.Segments.Size) < 0 {
					return fmt.Errorf("operation %q: segments.size %s exceeds the %s ephemeral-storage limit on container %q, which carries the request", o.Name, &o.Segments.Size, &lim, o.Template.Spec.Containers[0].Name)
				}
			}
		}
	}
	chans := map[string]bool{}
	for _, c := range s.Channels {
		if chans[c.Name] {
			return fmt.Errorf("duplicate channel %q", c.Name)
		}
		chans[c.Name] = true
	}
	adj := map[string][]string{}
	for _, c := range s.Channels {
		if c.From != "" && !ops[c.From] {
			return fmt.Errorf("channel %q: unknown producer %q", c.Name, c.From)
		}
		if c.To != "" && !ops[c.To] {
			return fmt.Errorf("channel %q: unknown consumer %q", c.Name, c.To)
		}
		if c.Feedback != nil {
			if c.To == "" {
				return fmt.Errorf("channel %q: feedback channels need a consuming operation", c.Name)
			}
			if o := c.Feedback.Overflow; o != "" {
				if !chans[o] {
					return fmt.Errorf("channel %q: overflow channel %q is not declared", c.Name, o)
				}
				if o == c.Name {
					return fmt.Errorf("channel %q: overflow channel must be a different channel", c.Name)
				}
			}
		}
		// Feedback channels of either mode close cycles; with them removed
		// the graph must be acyclic.
		if c.From != "" && c.To != "" && c.Feedback == nil {
			adj[c.From] = append(adj[c.From], c.To)
		}
	}
	const white, grey, black = 0, 1, 2
	color := map[string]int{}
	var visit func(string) error
	visit = func(n string) error {
		color[n] = grey
		for _, m := range adj[n] {
			switch color[m] {
			case grey:
				return fmt.Errorf("cycle through %q and %q has no feedback channel", n, m)
			case white:
				if err := visit(m); err != nil {
					return err
				}
			}
		}
		color[n] = black
		return nil
	}
	for name := range ops {
		if color[name] == white {
			if err := visit(name); err != nil {
				return err
			}
		}
	}
	return nil
}

// --- coordinator ----------------------------------------------------------

func coordinatorName(wl *v1alpha1.Workload) string { return wl.Name + "-coordinator" }

func coordinatorLabels(wl *v1alpha1.Workload) map[string]string {
	return map[string]string{LabelWorkload: wl.Name, LabelRole: RoleCoordinator}
}

func (r *Reconciler) coordinatorURL(wl *v1alpha1.Workload) string {
	if r.CoordinatorURL != nil {
		return r.CoordinatorURL(wl)
	}
	return fmt.Sprintf("http://%s.%s.svc:%d", coordinatorName(wl), wl.Namespace, coordinator.ControlPort)
}

func (r *Reconciler) ensureCoordinator(ctx context.Context, wl *v1alpha1.Workload) error {
	labels := coordinatorLabels(wl)
	image := wl.Spec.Coordinator.Image
	if image == "" {
		image = r.CoordinatorImage
	}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(wl), Namespace: wl.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Labels = labels
		one := int32(1)
		dep.Spec.Replicas = &one
		dep.Spec.Strategy = appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		dep.Spec.Template.Labels = labels
		dep.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:    "coordinator",
			Image:   image,
			Command: []string{"/coordinator"},
			// The coordinator serves the segments of external channels. It
			// announces the coordinator Service's DNS name so worker pods
			// reach it the same way they reach the control API, rather than
			// its own pod hostname, which cluster DNS does not resolve.
			Env: []corev1.EnvVar{{
				Name:  "STARK8S_SEGMENT_ADDR",
				Value: fmt.Sprintf("%s.%s.svc:%d", coordinatorName(wl), wl.Namespace, coordinator.SegmentPort),
			}, {Name: coordinator.EnvWorkload, Value: wl.Name}},
			Ports: []corev1.ContainerPort{
				{ContainerPort: coordinator.ControlPort, Name: "control"},
				{ContainerPort: coordinator.SegmentPort, Name: "segments"},
			},
			ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: coordinator.PathHealth, Port: intstr.FromInt(coordinator.ControlPort)}}},
		}}
		return controllerutil.SetControllerReference(wl, dep, r.Scheme())
	})
	if err != nil {
		return err
	}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(wl), Namespace: wl.Namespace}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = labels
		svc.Spec.Selector = labels
		svc.Spec.Ports = []corev1.ServicePort{
			{Port: coordinator.ControlPort, TargetPort: intstr.FromInt(coordinator.ControlPort), Name: "control"},
			{Port: coordinator.SegmentPort, TargetPort: intstr.FromInt(coordinator.SegmentPort), Name: "segments"},
		}
		return controllerutil.SetControllerReference(wl, svc, r.Scheme())
	})
	return err
}

func (r *Reconciler) pushTopology(ctx context.Context, wl *v1alpha1.Workload) error {
	body, _ := json.Marshal(wl.Spec.Channels)
	req, _ := http.NewRequestWithContext(ctx, "PUT", r.coordinatorURL(wl)+coordinator.PathTopology, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("topology push: %s", resp.Status)
	}
	return nil
}

// metricsView indexes a coordinator report by channel and operation name.
type metricsView struct {
	coordinator.Metrics
	channels   map[string]coordinator.ChannelMetrics
	operations map[string]coordinator.OperationMetrics
}

func (r *Reconciler) metrics(ctx context.Context, wl *v1alpha1.Workload) (metricsView, error) {
	v := metricsView{channels: map[string]coordinator.ChannelMetrics{}, operations: map[string]coordinator.OperationMetrics{}}
	req, _ := http.NewRequestWithContext(ctx, "GET", r.coordinatorURL(wl)+coordinator.PathMetrics, nil)
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return v, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return v, fmt.Errorf("metrics: %s", resp.Status)
	}
	if err := json.NewDecoder(resp.Body).Decode(&v.Metrics); err != nil {
		return v, err
	}
	for _, c := range v.Channels {
		v.channels[c.Name] = c
	}
	for _, o := range v.Operations {
		v.operations[o.Name] = o
	}
	return v, nil
}

// pushOperations publishes fixed membership before worker registration.
func (r *Reconciler) pushOperations(ctx context.Context, wl *v1alpha1.Workload) error {
	specs := make([]coordinator.OperationSpec, 0, len(wl.Spec.Operations))
	for i := range wl.Spec.Operations {
		o := &wl.Spec.Operations[i]
		specs = append(specs, coordinator.OperationSpec{Name: o.Name, Replicas: desiredReplicas(o)})
	}
	body, _ := json.Marshal(specs)
	req, _ := http.NewRequestWithContext(ctx, "PUT", r.coordinatorURL(wl)+coordinator.PathOperations, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("operation push: %s", resp.Status)
	}
	return nil
}

func (r *Reconciler) seal(ctx context.Context, wl *v1alpha1.Workload, channel string) error {
	url := r.coordinatorURL(wl) + coordinator.PathChannels + "/" + channel + coordinator.SuffixSeal
	req, _ := http.NewRequestWithContext(ctx, "POST", url, nil)
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("seal %s: %s", channel, resp.Status)
	}
	return nil
}

// --- operations -----------------------------------------------------------

func opName(wl *v1alpha1.Workload, op *v1alpha1.Operation) string { return wl.Name + "-" + op.Name }

func opLabels(wl *v1alpha1.Workload, op *v1alpha1.Operation) map[string]string {
	return map[string]string{LabelWorkload: wl.Name, LabelOperation: op.Name, LabelRole: RoleOperation}
}

func slots(op *v1alpha1.Operation) int32 {
	if op.Slots < 1 {
		return 1
	}
	return op.Slots
}

// desiredReplicas establishes fixed membership before an operation receives
// work. Pods may hold acknowledged application state or local output even
// when no input is pending, so the controller cannot safely reduce membership
// during an execution attempt.
func desiredReplicas(op *v1alpha1.Operation) int32 {
	max := op.Scaling.Horizontal.Max
	if max < 1 {
		max = 1
	}
	return max
}

func (r *Reconciler) reconcileOperation(ctx context.Context, wl *v1alpha1.Workload, op *v1alpha1.Operation, metrics metricsView) (v1alpha1.OperationStatus, error) {
	st := v1alpha1.OperationStatus{Name: op.Name, Phase: v1alpha1.OperationWaiting, Reason: "InputNotReady", Message: "waiting for materialized input"}
	om, hasMetrics := metrics.operations[op.Name]
	st.RunnableTasks, st.HoldsUnconsumed = om.RunnableTasks, om.HoldsUnconsumed

	// Stage barrier: a consumer of a Materialized channel is not started
	// until that channel is sealed. Feedback channels are excluded because
	// they seal only when the loop terminates.
	for _, c := range wl.Spec.Inbound(op.Name) {
		cm := metrics.channels[c.Name]
		if c.Delivery == graph.DeliveryMaterialized && c.Feedback == nil && !cm.Sealed && !cm.ProductionClosed {
			return st, nil
		}
	}

	drainComplete := hasMetrics && om.Complete && op.Completion != v1alpha1.CompletionNever
	if drainComplete {
		for _, ch := range wl.Spec.Outbound(op.Name) {
			if ch.Feedback == nil && !metrics.channels[ch.Name].Sealed {
				if err := r.seal(ctx, wl, ch.Name); err != nil {
					return st, err
				}
			}
		}
	}

	streaming := op.Completion == v1alpha1.CompletionNever
	labels := opLabels(wl, op)
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: opName(wl, op), Namespace: wl.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Labels = labels
		current := int32(0)
		if dep.Spec.Replicas != nil {
			current = *dep.Spec.Replicas
		}
		var want int32
		switch {
		case drainComplete && (om.HoldsUnconsumed || holdsRetainedSegments(&wl.Spec, op)):
			// Pods hold Ephemeral segments a consumer has not fetched.
			want = current
			if want < 1 {
				want = 1
			}
		case drainComplete:
			want = 0
		default:
			want = desiredReplicas(op)
		}
		dep.Spec.Replicas = &want
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		dep.Spec.Template = r.podTemplate(wl, op)
		return controllerutil.SetControllerReference(wl, dep, r.Scheme())
	})
	if err != nil {
		return st, err
	}
	st.Replicas = *dep.Spec.Replicas
	st.Ready = dep.Status.ReadyReplicas
	st.Phase = v1alpha1.OperationRunning
	st.Reason, st.Message = r.operationReason(ctx, wl, op, dep, metrics)
	if st.Reason == "OperationFailed" {
		st.Phase = v1alpha1.OperationFailed
	}
	if drainComplete && st.Replicas == 0 && dep.Status.Replicas == 0 {
		st.Phase = v1alpha1.OperationSucceeded
		st.Reason, st.Message = "OperationDrained", "operation completed and released its pods"
	}
	if !streaming {
		return st, nil
	}
	return st, r.ensureVPA(ctx, wl, op)
}

func (r *Reconciler) operationReason(ctx context.Context, wl *v1alpha1.Workload, op *v1alpha1.Operation, dep *appsv1.Deployment, metrics metricsView) (string, string) {
	for _, channel := range wl.Spec.Inbound(op.Name) {
		if failure := metrics.channels[channel.Name].LatestDeliveryFailure; failure != "" {
			return "DeliveryFailureRecorded", fmt.Sprintf("channel %s: %s", channel.Name, failure)
		}
	}
	for _, condition := range dep.Status.Conditions {
		if condition.Type == appsv1.DeploymentProgressing && condition.Status == corev1.ConditionFalse {
			return "OperationFailed", condition.Reason + ": " + condition.Message
		}
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(wl.Namespace), client.MatchingLabels(opLabels(wl, op))); err == nil {
		dependencies := map[string]bool{}
		for _, container := range dep.Spec.Template.Spec.InitContainers {
			if container.RestartPolicy != nil && *container.RestartPolicy == corev1.ContainerRestartPolicyAlways && container.StartupProbe != nil {
				dependencies[container.Name] = true
			}
		}
		for _, pod := range pods.Items {
			for _, status := range pod.Status.InitContainerStatuses {
				if !dependencies[status.Name] {
					continue
				}
				if status.Started != nil && *status.Started {
					continue
				}
				if status.State.Terminated != nil && status.State.Terminated.ExitCode != 0 {
					return "DependencyFailed", fmt.Sprintf("pod %s dependency %s exited: %s", pod.Name, status.Name, status.State.Terminated.Reason)
				}
				reason := "startup probe has not succeeded"
				if status.State.Waiting != nil && status.State.Waiting.Reason != "" {
					reason = status.State.Waiting.Reason
				}
				return "DependencyWaiting", fmt.Sprintf("pod %s dependency %s: %s", pod.Name, status.Name, reason)
			}
		}
	}
	if dep.Status.ReadyReplicas < *dep.Spec.Replicas {
		return "PodsNotReady", fmt.Sprintf("%d of %d replicas ready", dep.Status.ReadyReplicas, *dep.Spec.Replicas)
	}
	return "Processing", "operation replicas are ready"
}

func holdsRetainedSegments(spec *v1alpha1.WorkloadSpec, op *v1alpha1.Operation) bool {
	for _, channel := range spec.Outbound(op.Name) {
		if channel.To != "" && channel.Durability == graph.DurabilityRetained {
			return true
		}
	}
	return false
}

func finiteEpochOperation(spec *v1alpha1.WorkloadSpec, name string) bool {
	seen := map[string]bool{name: true}
	for changed := true; changed; {
		changed = false
		for _, c := range spec.Channels {
			if c.Feedback != nil && c.Feedback.Mode != graph.FeedbackAsynchronous {
				if !seen[c.From] || !seen[c.To] {
					seen[c.From], seen[c.To], changed = true, true, true
				}
			}
			if seen[c.From] && c.To != "" && !seen[c.To] {
				seen[c.To], changed = true, true
			}
			if seen[c.To] && c.From != "" && !seen[c.From] {
				seen[c.From], changed = true, true
			}
		}
	}
	for _, c := range spec.Channels {
		if c.Feedback != nil && c.Feedback.Mode != graph.FeedbackAsynchronous && (seen[name] && (seen[c.From] || seen[c.To])) {
			return true
		}
	}
	return false
}

// podTemplate returns the operation's template with graph discovery, the
// segment volume, the segment port, and the service account injected.
func (r *Reconciler) podTemplate(wl *v1alpha1.Workload, op *v1alpha1.Operation) corev1.PodTemplateSpec {
	tpl := *op.Template.DeepCopy()
	if tpl.Labels == nil {
		tpl.Labels = map[string]string{}
	}
	for k, v := range opLabels(wl, op) {
		tpl.Labels[k] = v
	}
	tpl.Spec.ServiceAccountName = opName(wl, op)
	var in, out, fb, fbOut []string
	for _, c := range wl.Spec.Inbound(op.Name) {
		in = append(in, c.Name)
		if c.Feedback != nil {
			fb = append(fb, c.Name)
		}
	}
	for _, c := range wl.Spec.Outbound(op.Name) {
		out = append(out, c.Name)
		if c.Feedback != nil {
			fbOut = append(fbOut, c.Name)
		}
	}
	env := []corev1.EnvVar{
		{Name: coordinator.EnvCoordinator, Value: fmt.Sprintf("http://%s:%d", coordinatorName(wl), coordinator.ControlPort)},
		{Name: coordinator.EnvWorkload, Value: wl.Name},
		{Name: coordinator.EnvOperation, Value: op.Name},
		{Name: coordinator.EnvInstance, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: coordinator.EnvPodIP, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
		{Name: coordinator.EnvSlots, Value: strconv.Itoa(int(slots(op)))},
		{Name: coordinator.EnvInbound, Value: strings.Join(in, ",")},
		{Name: coordinator.EnvOutbound, Value: strings.Join(out, ",")},
		{Name: coordinator.EnvFeedback, Value: strings.Join(fb, ",")},
		{Name: coordinator.EnvFeedbackOut, Value: strings.Join(fbOut, ",")},
		{Name: coordinator.EnvSegmentDir, Value: SegmentDir},
	}
	if op.TickInterval != nil && op.TickInterval.Duration > 0 {
		env = append(env, corev1.EnvVar{Name: coordinator.EnvTickInterval, Value: op.TickInterval.Duration.String()})
	}
	hasVolume := false
	for _, v := range tpl.Spec.Volumes {
		if v.Name == segmentVolumeName {
			hasVolume = true
		}
	}
	if !hasVolume {
		dir := &corev1.EmptyDirVolumeSource{}
		if op.Segments != nil {
			size := op.Segments.Size.DeepCopy()
			dir.SizeLimit = &size
		}
		tpl.Spec.Volumes = append(tpl.Spec.Volumes, corev1.Volume{
			Name:         segmentVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: dir},
		})
	}
	var worker *corev1.Container
	if len(tpl.Spec.Containers) > 0 {
		worker = &tpl.Spec.Containers[0]
	}
	for i := range tpl.Spec.InitContainers {
		container := &tpl.Spec.InitContainers[i]
		if container.Name != runtimeContainer || container.RestartPolicy == nil ||
			*container.RestartPolicy != corev1.ContainerRestartPolicyAlways {
			continue
		}
		worker = container
		if container.StartupProbe == nil {
			container.StartupProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt(runtimePort)},
			}}
		}
		break
	}
	if worker != nil {
		c := worker
		c.Env = append(env, c.Env...)
		mounted := false
		for _, m := range c.VolumeMounts {
			if m.Name == segmentVolumeName {
				mounted = true
			}
		}
		if !mounted {
			c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{Name: segmentVolumeName, MountPath: SegmentDir})
		}
		hasPort := false
		for _, p := range c.Ports {
			if p.ContainerPort == coordinator.SegmentPort {
				hasPort = true
			}
		}
		if !hasPort {
			c.Ports = append(c.Ports, corev1.ContainerPort{ContainerPort: coordinator.SegmentPort, Name: "segments"})
		}
		if op.Segments != nil {
			requestEphemeralStorage(c, op.Segments.Size)
		}
	}
	return tpl
}

// podEphemeralStorageLimit reports the ephemeral-storage limit the kubelet
// would enforce over the whole pod, and whether one is set at all. A container
// naming no limit contributes nothing rather than leaving the budget
// unbounded, so a lone sidecar with a small limit caps the pod.
//
// It follows the kubelet's arithmetic. Containers that run at the same time
// add up: the regular containers, and the init containers marked restartable,
// which are sidecars and stay up alongside them. A plain init container has
// finished before any of those start, so it only has to fit on its own and
// contributes as a floor rather than a summand.
func podEphemeralStorageLimit(spec *corev1.PodSpec) (resource.Quantity, bool) {
	total, set := resource.Quantity{}, false
	add := func(c corev1.Container) {
		if lim, ok := c.Resources.Limits[corev1.ResourceEphemeralStorage]; ok {
			total.Add(lim)
			set = true
		}
	}
	sidecar := func(c corev1.Container) bool {
		return c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways
	}
	for _, c := range spec.InitContainers {
		if sidecar(c) {
			add(c)
		}
	}
	for _, c := range spec.Containers {
		add(c)
	}
	for _, c := range spec.InitContainers {
		if sidecar(c) {
			continue
		}
		// Presence is what arms the kubelet's check, so a limit of zero
		// counts as set even though it raises nothing.
		if lim, ok := c.Resources.Limits[corev1.ResourceEphemeralStorage]; ok {
			set = true
			if lim.Cmp(total) > 0 {
				total = lim.DeepCopy()
			}
		}
	}
	return total, set
}

// requestEphemeralStorage raises the container's ephemeral-storage request to
// at least size, so that the scheduler places the pod where that much disk
// exists and the eviction ranking, which sorts by usage over request, does not
// treat the pod as though it were using disk it never asked for.
//
// A template already asking for at least size keeps what it has, so the field
// is a floor rather than an override. That includes a template that names only
// a limit: Kubernetes defaults an absent request to the container's limit, so
// writing a smaller request here would lower the reservation the pod would
// otherwise have been scheduled against.
//
// It deliberately sets no limit. The volume's own sizeLimit already caps the
// segments, and the kubelet charges a pod's ephemeral-storage limit the volume
// together with every container's writable layer and logs, so a limit equal to
// size would evict the pod before the volume could ever reach it. A template
// that names its own limit keeps it, and Validate rejects a pod budget that
// does not exceed size.
func requestEphemeralStorage(c *corev1.Container, size resource.Quantity) {
	have, ok := c.Resources.Requests[corev1.ResourceEphemeralStorage]
	if !ok {
		have, ok = c.Resources.Limits[corev1.ResourceEphemeralStorage]
	}
	if ok && have.Cmp(size) >= 0 {
		return
	}
	if c.Resources.Requests == nil {
		c.Resources.Requests = corev1.ResourceList{}
	}
	c.Resources.Requests[corev1.ResourceEphemeralStorage] = size.DeepCopy()
}

// ensureServiceAccounts creates one ServiceAccount per operation. The
// coordinator can bind a pod's token to its operation through it.
func (r *Reconciler) ensureServiceAccounts(ctx context.Context, wl *v1alpha1.Workload) error {
	for i := range wl.Spec.Operations {
		op := &wl.Spec.Operations[i]
		sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: opName(wl, op), Namespace: wl.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
			sa.Labels = opLabels(wl, op)
			return controllerutil.SetControllerReference(wl, sa, r.Scheme())
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) ensureHPA(ctx context.Context, wl *v1alpha1.Workload, op *v1alpha1.Operation) error {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: opName(wl, op), Namespace: wl.Namespace}}
	err := r.Delete(ctx, hpa)
	return client.IgnoreNotFound(err)
}

var vpaGVK = schema.GroupVersionKind{Group: "autoscaling.k8s.io", Version: "v1", Kind: "VerticalPodAutoscaler"}

// ensureVPA creates a VerticalPodAutoscaler when requested and the API
// exists. Only streaming operations receive one: the VPA updater evicts
// pods, and a batch pod's local segments would be lost with it.
func (r *Reconciler) ensureVPA(ctx context.Context, wl *v1alpha1.Workload, op *v1alpha1.Operation) error {
	if op.Scaling.Vertical == nil || op.Scaling.Vertical.Mode == "" || op.Scaling.Vertical.Mode == v1alpha1.VerticalOff {
		return nil
	}
	if _, err := r.RESTMapper().RESTMapping(vpaGVK.GroupKind(), vpaGVK.Version); err != nil {
		if meta.IsNoMatchError(err) {
			log.FromContext(ctx).Info("VerticalPodAutoscaler API not installed; skipping", "operation", op.Name)
			return nil
		}
		return err
	}
	vpa := &unstructured.Unstructured{}
	vpa.SetGroupVersionKind(vpaGVK)
	vpa.SetName(opName(wl, op))
	vpa.SetNamespace(wl.Namespace)
	if op.Scaling.Vertical.Mode == v1alpha1.VerticalAuto {
		return client.IgnoreNotFound(r.Delete(ctx, vpa))
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, vpa, func() error {
		vpa.SetLabels(opLabels(wl, op))
		_ = unstructured.SetNestedMap(vpa.Object, map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment", "name": opName(wl, op),
		}, "spec", "targetRef")
		_ = unstructured.SetNestedField(vpa.Object, string(op.Scaling.Vertical.Mode), "spec", "updatePolicy", "updateMode")
		return controllerutil.SetControllerReference(wl, vpa, r.Scheme())
	})
	return err
}

// --- network --------------------------------------------------------------

func edgePolicyName(wl *v1alpha1.Workload, channel string) string {
	return wl.Name + "-edge-" + channel
}

// egressPolicyName is the policy that carries one operation's declared reach
// outside the workload.
func egressPolicyName(wl *v1alpha1.Workload, op string) string {
	return wl.Name + "-egress-" + op
}

// metadataCIDR is the link-local address the instance metadata server answers
// on. It is the same address across the cloud providers that offer one.
const metadataCIDR = "169.254.169.254/32"

// privateRanges are the addresses an Internet grant must not reach. The three
// RFC 1918 blocks cover cluster pod and service networks on every deployment
// this has been used on. Link-local is excluded as well, so that asking for
// the internet does not also hand over the metadata server: that is a
// separate destination and has to be asked for by name.
//
// A cluster whose pod or service network sits outside these ranges would need
// its own block listed here. Nothing in the API can discover that, so it is
// stated rather than derived.
var privateRanges = []string{
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
	"169.254.0.0/16",
}

// egressRulesFor turns an operation's declared destinations into policy
// rules. An operation that declares none gets none, which leaves it with
// exactly the rules the shared operations policy already grants.
func egressRulesFor(op v1alpha1.Operation) []networkingv1.NetworkPolicyEgressRule {
	tcp := corev1.ProtocolTCP
	http := intstr.FromInt(80)
	https := intstr.FromInt(443)
	var out []networkingv1.NetworkPolicyEgressRule
	for _, e := range op.Egress {
		switch e.To {
		case v1alpha1.EgressMetadata:
			out = append(out, networkingv1.NetworkPolicyEgressRule{
				To:    []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: metadataCIDR}}},
				Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &http}},
			})
		case v1alpha1.EgressInternet:
			out = append(out, networkingv1.NetworkPolicyEgressRule{
				To: []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{
					CIDR:   "0.0.0.0/0",
					Except: append([]string(nil), privateRanges...),
				}}},
				Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &https}},
			})
		}
	}
	return out
}

func (r *Reconciler) ensureNetworkPolicies(ctx context.Context, wl *v1alpha1.Workload) error {
	workloadPods := metav1.LabelSelector{MatchLabels: map[string]string{LabelWorkload: wl.Name, LabelRole: RoleOperation}}
	coordinatorPods := metav1.LabelSelector{MatchLabels: coordinatorLabels(wl)}
	tcp := corev1.ProtocolTCP
	udp := corev1.ProtocolUDP
	dns := intstr.FromInt(53)
	control := intstr.FromInt(coordinator.ControlPort)
	segment := intstr.FromInt(coordinator.SegmentPort)
	dnsRule := networkingv1.NetworkPolicyEgressRule{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &udp, Port: &dns}, {Protocol: &tcp, Port: &dns}}}
	bothPorts := []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &control}, {Protocol: &tcp, Port: &segment}}

	// Operation pods: egress to the coordinator, to DNS, and to the segment
	// port of any pod of the same workload. Ingress is granted only by the
	// per-edge policies below.
	ops := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: wl.Name + "-operations", Namespace: wl.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, ops, func() error {
		ops.Labels = map[string]string{LabelWorkload: wl.Name}
		ops.Spec = networkingv1.NetworkPolicySpec{
			PodSelector: workloadPods,
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{To: []networkingv1.NetworkPolicyPeer{{PodSelector: &coordinatorPods}}, Ports: bothPorts},
				{To: []networkingv1.NetworkPolicyPeer{{PodSelector: &workloadPods}}, Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &segment}}},
				dnsRule,
			},
		}
		return controllerutil.SetControllerReference(wl, ops, r.Scheme())
	}); err != nil {
		return err
	}

	// Coordinator: ingress from this workload's operation pods and the
	// controller's namespace on both ports; egress only DNS.
	co := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: coordinatorName(wl), Namespace: wl.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, co, func() error {
		co.Labels = map[string]string{LabelWorkload: wl.Name}
		from := []networkingv1.NetworkPolicyPeer{{PodSelector: &workloadPods}}
		if r.ControllerNamespace != "" {
			from = append(from, networkingv1.NetworkPolicyPeer{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": r.ControllerNamespace}},
			})
		}
		co.Spec = networkingv1.NetworkPolicySpec{
			PodSelector: coordinatorPods,
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			Ingress:     []networkingv1.NetworkPolicyIngressRule{{From: from, Ports: bothPorts}},
			Egress:      []networkingv1.NetworkPolicyEgressRule{dnsRule},
		}
		return controllerutil.SetControllerReference(wl, co, r.Scheme())
	}); err != nil {
		return err
	}

	// One policy per operation that declared egress. Network policies are
	// additive, so this grants the extra destinations to that operation's
	// pods alone and leaves the shared operations policy above untouched. An
	// operation that declared nothing gets no policy here and so keeps
	// exactly the reach it had before this field existed.
	wantEgress := map[string]bool{}
	for i := range wl.Spec.Operations {
		op := wl.Spec.Operations[i]
		rules := egressRulesFor(op)
		if len(rules) == 0 {
			continue
		}
		wantEgress[op.Name] = true
		selector := metav1.LabelSelector{MatchLabels: map[string]string{
			LabelWorkload: wl.Name, LabelRole: RoleOperation, LabelOperation: op.Name,
		}}
		np := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: egressPolicyName(wl, op.Name), Namespace: wl.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, np, func() error {
			np.Labels = map[string]string{LabelWorkload: wl.Name, LabelOperation: op.Name}
			np.Spec = networkingv1.NetworkPolicySpec{
				PodSelector: selector,
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
				Egress:      rules,
			}
			return controllerutil.SetControllerReference(wl, np, r.Scheme())
		}); err != nil {
			return err
		}
	}

	// One policy per edge: the consumer's pods may open the producer's
	// segment port.
	wanted := map[string]bool{}
	for _, c := range wl.Spec.Channels {
		if c.From == "" || c.To == "" {
			continue
		}
		wanted[c.Name] = true
		producer := metav1.LabelSelector{MatchLabels: map[string]string{LabelWorkload: wl.Name, LabelRole: RoleOperation, LabelOperation: c.From}}
		consumer := metav1.LabelSelector{MatchLabels: map[string]string{LabelWorkload: wl.Name, LabelRole: RoleOperation, LabelOperation: c.To}}
		np := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: edgePolicyName(wl, c.Name), Namespace: wl.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, np, func() error {
			np.Labels = map[string]string{LabelWorkload: wl.Name, LabelChannel: c.Name}
			np.Spec = networkingv1.NetworkPolicySpec{
				PodSelector: producer,
				PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
				Ingress: []networkingv1.NetworkPolicyIngressRule{{
					From:  []networkingv1.NetworkPolicyPeer{{PodSelector: &consumer}},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &segment}},
				}},
			}
			return controllerutil.SetControllerReference(wl, np, r.Scheme())
		}); err != nil {
			return err
		}
	}

	// Delete edge policies for channels no longer in the spec.
	var list networkingv1.NetworkPolicyList
	if err := r.List(ctx, &list, client.InNamespace(wl.Namespace), client.MatchingLabels{LabelWorkload: wl.Name}); err != nil {
		return err
	}
	for i := range list.Items {
		np := &list.Items[i]
		if ch, isEdge := np.Labels[LabelChannel]; isEdge {
			if wanted[ch] {
				continue
			}
		} else if op, isEgress := np.Labels[LabelOperation]; isEgress {
			// An operation that gave up its egress declaration loses the
			// policy, so the grant does not outlive the spec that asked for it.
			if wantEgress[op] {
				continue
			}
		} else {
			continue
		}
		if err := r.Delete(ctx, np); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

// Namespace returns the namespace the controller runs in, from the
// downward API or the service account mount.
func Namespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	if b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		return strings.TrimSpace(string(b))
	}
	return ""
}
