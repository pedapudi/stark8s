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
//	                   with STARK8S_* environment, a segment volume, and the
//	                   segment port injected into every pod
//	channels        -> pushed to the coordinator as topology; sealed when the
//	                   coordinator reports their producing operation complete
//	scaling         -> replicas = clamp(ceil(runnableTasks / slots), min, max)
//	                   from coordinator metrics; HorizontalPodAutoscaler for
//	                   CPU on streaming operations; VerticalPodAutoscaler for
//	                   streaming operations when requested and the API exists
//	network         -> one NetworkPolicy per channel edge (consumer pods may
//	                   open the producer's segment port), one for all
//	                   operation pods, one for the coordinator
//
// Completion of a batch operation is a coordinator decision, expressed by
// the controller as scaling the Deployment to zero. Pods that still hold
// Ephemeral segments a consumer has not fetched are kept until the
// coordinator reports them released.
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
	"math"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

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

	pollInterval = 3 * time.Second
)

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
}

// SetupWithManager registers the reconciler.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.HTTP == nil {
		r.HTTP = &http.Client{Timeout: 10 * time.Second}
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
		return r.setPhase(ctx, wl, v1alpha1.WorkloadFailed, "invalid workload: "+err.Error())
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
		if _, err := r.setPhase(ctx, wl, v1alpha1.WorkloadPending, "waiting for coordinator"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	metrics, err := r.metrics(ctx, wl)
	if err != nil {
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}

	var opStatus []v1alpha1.OperationStatus
	allDrained, anyStreaming := true, false
	for i := range wl.Spec.Operations {
		op := &wl.Spec.Operations[i]
		st, err := r.reconcileOperation(ctx, wl, op, metrics)
		if err != nil {
			return ctrl.Result{}, err
		}
		opStatus = append(opStatus, st)
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
	wl.Status.Operations = opStatus
	wl.Status.Channels = nil
	for _, m := range metrics.Channels {
		wl.Status.Channels = append(wl.Status.Channels, v1alpha1.ChannelStatus{
			Name: m.Name, Sealed: m.Sealed, Pending: m.Pending, InFlight: m.InFlight,
			Produced: m.Produced, Epoch: m.Epoch, Overflowed: m.Overflowed, Lost: m.Lost,
		})
	}
	phase, msg := v1alpha1.WorkloadRunning, ""
	if allDrained && !anyStreaming {
		phase, msg = v1alpha1.WorkloadSucceeded, "all operations drained"
	}
	wl.Status.Phase, wl.Status.Message = phase, msg
	if err := r.Status().Update(ctx, wl); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if phase == v1alpha1.WorkloadRunning {
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	return ctrl.Result{}, nil
}

func (r *Reconciler) setPhase(ctx context.Context, wl *v1alpha1.Workload, phase v1alpha1.WorkloadPhase, msg string) (ctrl.Result, error) {
	wl.Status.Phase, wl.Status.Message = phase, msg
	return ctrl.Result{}, client.IgnoreNotFound(r.Status().Update(ctx, wl))
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

// desiredReplicas sizes an operation from the number of runnable tasks:
// clamp(ceil(runnable / slots), min, max). With no runnable tasks the count
// is min, raised to one when the operation must run to make progress on its
// own: a source, or a consumer of a Pipelined channel that must be present
// while its producer runs.
func desiredReplicas(spec *v1alpha1.WorkloadSpec, op *v1alpha1.Operation, runnable int32) int32 {
	h := op.Scaling.Horizontal
	min, max := h.Min, h.Max
	if max < 1 {
		max = 1
	}
	if min > max {
		min = max
	}
	if runnable <= 0 {
		want := min
		if want < 1 && mustRunIdle(spec, op) {
			want = 1
		}
		return want
	}
	want := int32(math.Ceil(float64(runnable) / float64(slots(op))))
	if want < min {
		want = min
	}
	if want > max {
		want = max
	}
	return want
}

// mustRunIdle reports whether an operation needs a pod even when the
// coordinator reports nothing runnable: sources produce without input, and
// consumers of Pipelined channels receive records as they are produced.
func mustRunIdle(spec *v1alpha1.WorkloadSpec, op *v1alpha1.Operation) bool {
	inbound := spec.Inbound(op.Name)
	if len(inbound) == 0 {
		return true
	}
	for _, c := range inbound {
		if c.Delivery != v1alpha1.DeliveryMaterialized {
			return true
		}
	}
	return false
}

func (r *Reconciler) reconcileOperation(ctx context.Context, wl *v1alpha1.Workload, op *v1alpha1.Operation, metrics metricsView) (v1alpha1.OperationStatus, error) {
	st := v1alpha1.OperationStatus{Name: op.Name, Phase: v1alpha1.OperationWaiting}
	om, hasMetrics := metrics.operations[op.Name]
	st.RunnableTasks, st.HoldsUnconsumed = om.RunnableTasks, om.HoldsUnconsumed

	// Stage barrier: a consumer of a Materialized channel is not started
	// until that channel is sealed. Feedback channels are excluded because
	// they seal only when the loop terminates.
	for _, c := range wl.Spec.Inbound(op.Name) {
		if c.Delivery == v1alpha1.DeliveryMaterialized && c.Feedback == nil && !metrics.channels[c.Name].Sealed {
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
	useHPA := streaming && op.Scaling.Horizontal.CPUUtilizationPercent > 0
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
		case drainComplete && om.HoldsUnconsumed:
			// Pods hold Ephemeral segments a consumer has not fetched.
			want = current
			if want < 1 {
				want = 1
			}
		case drainComplete:
			want = 0
		case useHPA && dep.Spec.Replicas != nil:
			want = current
		default:
			want = desiredReplicas(&wl.Spec, op, om.RunnableTasks)
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
	if drainComplete && st.Replicas == 0 && dep.Status.Replicas == 0 {
		st.Phase = v1alpha1.OperationSucceeded
	}
	if !streaming {
		return st, nil
	}
	if err := r.ensureHPA(ctx, wl, op); err != nil {
		return st, err
	}
	return st, r.ensureVPA(ctx, wl, op)
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
	hasVolume := false
	for _, v := range tpl.Spec.Volumes {
		if v.Name == segmentVolumeName {
			hasVolume = true
		}
	}
	if !hasVolume {
		tpl.Spec.Volumes = append(tpl.Spec.Volumes, corev1.Volume{
			Name:         segmentVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		})
	}
	for i := range tpl.Spec.Containers {
		c := &tpl.Spec.Containers[i]
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
		if i == 0 {
			hasPort := false
			for _, p := range c.Ports {
				if p.ContainerPort == coordinator.SegmentPort {
					hasPort = true
				}
			}
			if !hasPort {
				c.Ports = append(c.Ports, corev1.ContainerPort{ContainerPort: coordinator.SegmentPort, Name: "segments"})
			}
		}
	}
	return tpl
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
	pct := op.Scaling.Horizontal.CPUUtilizationPercent
	if pct == 0 {
		err := r.Delete(ctx, hpa)
		return client.IgnoreNotFound(err)
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, hpa, func() error {
		hpa.Labels = opLabels(wl, op)
		min := op.Scaling.Horizontal.Min
		if min < 1 {
			min = 1
		}
		max := op.Scaling.Horizontal.Max
		if max < min {
			max = min
		}
		hpa.Spec.MinReplicas = &min
		hpa.Spec.MaxReplicas = max
		hpa.Spec.ScaleTargetRef = autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "Deployment", Name: opName(wl, op)}
		hpa.Spec.Metrics = []autoscalingv2.MetricSpec{{
			Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{Name: corev1.ResourceCPU,
				Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: &pct}},
		}}
		return controllerutil.SetControllerReference(wl, hpa, r.Scheme())
	})
	return err
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
		ch, isEdge := np.Labels[LabelChannel]
		if !isEdge || wanted[ch] {
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
