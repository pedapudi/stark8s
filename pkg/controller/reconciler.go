// Package controller reconciles Workload objects into Kubernetes resources.
//
// Mapping, per workload:
//
//	exchange        -> one Deployment + Service (<workload>-exchange)
//	operation       -> Job (completion Drain) or Deployment (completion Never),
//	                   labelled stark8s.io/workload and stark8s.io/operation,
//	                   with STARK8S_* environment injected into every container
//	channels        -> pushed to the exchange as topology; sealed when their
//	                   producing operation's Job completes
//	scaling         -> Job parallelism / Deployment replicas computed from
//	                   exchange backlog; HorizontalPodAutoscaler for CPU on
//	                   streaming operations; VerticalPodAutoscaler when requested
//	                   and the VPA API is installed
//	network         -> two NetworkPolicies: operation pods may only talk to the
//	                   exchange (and DNS); the exchange accepts only workload
//	                   pods and the controller namespace
//
// An operation whose inbound Materialized channels are not all sealed is not
// started: its Job is created only when its inputs are complete. This is the
// stage barrier of a shuffle-based engine expressed as scheduling.
package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	"github.com/pedapudi/stark8s/pkg/exchange"
)

const (
	LabelWorkload  = "stark8s.io/workload"
	LabelOperation = "stark8s.io/operation"
	LabelRole      = "stark8s.io/role"
	exchangePort   = 8080
	pollInterval   = 3 * time.Second
)

// Reconciler reconciles Workloads.
type Reconciler struct {
	client.Client
	// ExchangeImage is used when the workload does not name one.
	ExchangeImage string
	// ControllerNamespace is allowed through the exchange's ingress policy.
	ControllerNamespace string
	// ExchangeURL overrides the in-cluster exchange address (tests).
	ExchangeURL func(wl *v1alpha1.Workload) string
	HTTP        *http.Client
}

// SetupWithManager registers the reconciler.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.HTTP == nil {
		r.HTTP = &http.Client{Timeout: 10 * time.Second}
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.Workload{}).
		Owns(&batchv1.Job{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Complete(r)
}

// Reconcile drives one workload toward its spec.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
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
	if err := r.ensureExchange(ctx, wl); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.ensureNetworkPolicies(ctx, wl); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.pushTopology(ctx, wl); err != nil {
		logger.Info("exchange not ready", "err", err.Error())
		if _, err := r.setPhase(ctx, wl, v1alpha1.WorkloadPending, "waiting for exchange"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	metrics, err := r.metrics(ctx, wl)
	if err != nil {
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	byName := map[string]exchange.Metrics{}
	for _, m := range metrics {
		byName[m.Name] = m
	}

	var opStatus []v1alpha1.OperationStatus
	allDone, anyFailed, anyStreaming := true, false, false
	for i := range wl.Spec.Operations {
		op := &wl.Spec.Operations[i]
		st, err := r.reconcileOperation(ctx, wl, op, byName)
		if err != nil {
			return ctrl.Result{}, err
		}
		opStatus = append(opStatus, st)
		switch st.Phase {
		case v1alpha1.OperationFailed:
			anyFailed = true
		case v1alpha1.OperationSucceeded:
		default:
			allDone = false
		}
		if op.Completion == v1alpha1.CompletionNever {
			anyStreaming = true
		}
	}

	// Refresh metrics after sealing so status reflects this pass.
	if m, err := r.metrics(ctx, wl); err == nil {
		metrics = m
	}
	wl.Status.Operations = opStatus
	wl.Status.Channels = nil
	for _, m := range metrics {
		wl.Status.Channels = append(wl.Status.Channels, v1alpha1.ChannelStatus{
			Name: m.Name, Sealed: m.Sealed, Pending: m.Pending, InFlight: m.InFlight,
			Produced: m.Produced, Epoch: m.Epoch,
		})
	}
	phase, msg := v1alpha1.WorkloadRunning, ""
	switch {
	case anyFailed:
		phase, msg = v1alpha1.WorkloadFailed, "an operation failed"
	case allDone && !anyStreaming:
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
// names are unique, and every cycle passes through a feedback channel.
func Validate(s *v1alpha1.WorkloadSpec) error {
	ops := map[string]bool{}
	for _, o := range s.Operations {
		if ops[o.Name] {
			return fmt.Errorf("duplicate operation %q", o.Name)
		}
		ops[o.Name] = true
	}
	chans := map[string]bool{}
	adj := map[string][]string{}
	for _, c := range s.Channels {
		if chans[c.Name] {
			return fmt.Errorf("duplicate channel %q", c.Name)
		}
		chans[c.Name] = true
		if c.From != "" && !ops[c.From] {
			return fmt.Errorf("channel %q: unknown producer %q", c.Name, c.From)
		}
		if c.To != "" && !ops[c.To] {
			return fmt.Errorf("channel %q: unknown consumer %q", c.Name, c.To)
		}
		if c.Feedback != nil && c.To == "" {
			return fmt.Errorf("channel %q: feedback channels need a consuming operation", c.Name)
		}
		if c.From != "" && c.To != "" && c.Feedback == nil {
			adj[c.From] = append(adj[c.From], c.To)
		}
	}
	// With feedback edges removed the graph must be acyclic.
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

// --- exchange -------------------------------------------------------------

func exchangeName(wl *v1alpha1.Workload) string { return wl.Name + "-exchange" }

func (r *Reconciler) exchangeURL(wl *v1alpha1.Workload) string {
	if r.ExchangeURL != nil {
		return r.ExchangeURL(wl)
	}
	return fmt.Sprintf("http://%s.%s.svc:%d", exchangeName(wl), wl.Namespace, exchangePort)
}

func (r *Reconciler) ensureExchange(ctx context.Context, wl *v1alpha1.Workload) error {
	labels := map[string]string{LabelWorkload: wl.Name, LabelRole: "exchange"}
	image := wl.Spec.Coordinator.Image
	if image == "" {
		image = r.ExchangeImage
	}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: exchangeName(wl), Namespace: wl.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Labels = labels
		one := int32(1)
		dep.Spec.Replicas = &one
		dep.Spec.Strategy = appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		dep.Spec.Template.Labels = labels
		dep.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:    "exchange",
			Image:   image,
			Command: []string{"/exchange"},
			Ports:   []corev1.ContainerPort{{ContainerPort: exchangePort, Name: "http"}},
			ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt(exchangePort)}}},
		}}
		return controllerutil.SetControllerReference(wl, dep, r.Scheme())
	})
	if err != nil {
		return err
	}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: exchangeName(wl), Namespace: wl.Namespace}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = labels
		svc.Spec.Selector = labels
		svc.Spec.Ports = []corev1.ServicePort{{Port: exchangePort, TargetPort: intstr.FromInt(exchangePort), Name: "http"}}
		return controllerutil.SetControllerReference(wl, svc, r.Scheme())
	})
	return err
}

func (r *Reconciler) pushTopology(ctx context.Context, wl *v1alpha1.Workload) error {
	body, _ := json.Marshal(wl.Spec.Channels)
	req, _ := http.NewRequestWithContext(ctx, "PUT", r.exchangeURL(wl)+"/topology", bytes.NewReader(body))
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

func (r *Reconciler) metrics(ctx context.Context, wl *v1alpha1.Workload) ([]exchange.Metrics, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", r.exchangeURL(wl)+"/metrics", nil)
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out []exchange.Metrics
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

func (r *Reconciler) seal(ctx context.Context, wl *v1alpha1.Workload, channel string) error {
	req, _ := http.NewRequestWithContext(ctx, "POST", r.exchangeURL(wl)+"/channels/"+channel+"/seal", nil)
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

// desiredReplicas computes the replica count from backlog and bounds.
func desiredReplicas(op *v1alpha1.Operation, current int32, backlog int64) int32 {
	h := op.Scaling.Horizontal
	min, max := h.Min, h.Max
	if max < 1 {
		max = 1
	}
	if min > max {
		min = max
	}
	want := current
	if want < min {
		want = min
	}
	if h.TargetBacklogPerReplica > 0 {
		need := int32(math.Ceil(float64(backlog) / float64(h.TargetBacklogPerReplica)))
		if need > want {
			want = need
		}
	}
	if want > max {
		want = max
	}
	if want < min {
		want = min
	}
	return want
}

func (r *Reconciler) reconcileOperation(ctx context.Context, wl *v1alpha1.Workload, op *v1alpha1.Operation, metrics map[string]exchange.Metrics) (v1alpha1.OperationStatus, error) {
	st := v1alpha1.OperationStatus{Name: op.Name, Phase: v1alpha1.OperationWaiting}
	inbound := wl.Spec.Inbound(op.Name)

	var backlog int64
	ready := true
	for _, c := range inbound {
		m := metrics[c.Name]
		backlog += m.Pending
		if c.Delivery == v1alpha1.DeliveryMaterialized && c.Feedback == nil && !m.Sealed {
			ready = false
		}
	}
	if !ready {
		return st, nil
	}

	if op.Completion == v1alpha1.CompletionNever {
		return r.reconcileStreaming(ctx, wl, op, backlog)
	}
	return r.reconcileBatch(ctx, wl, op, backlog)
}

func (r *Reconciler) reconcileBatch(ctx context.Context, wl *v1alpha1.Workload, op *v1alpha1.Operation, backlog int64) (v1alpha1.OperationStatus, error) {
	st := v1alpha1.OperationStatus{Name: op.Name, Phase: v1alpha1.OperationRunning}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: opName(wl, op), Namespace: wl.Namespace}}
	err := r.Get(ctx, client.ObjectKeyFromObject(job), job)
	if apierrors.IsNotFound(err) {
		parallelism := desiredReplicas(op, 0, backlog)
		if parallelism < 1 {
			parallelism = 1
		}
		job.Labels = opLabels(wl, op)
		job.Spec.Parallelism = &parallelism
		backoff := int32(3)
		job.Spec.BackoffLimit = &backoff
		job.Spec.Template = r.podTemplate(wl, op)
		if job.Spec.Template.Spec.RestartPolicy == "" || job.Spec.Template.Spec.RestartPolicy == corev1.RestartPolicyAlways {
			job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
		}
		if err := controllerutil.SetControllerReference(wl, job, r.Scheme()); err != nil {
			return st, err
		}
		if err := r.Create(ctx, job); err != nil {
			return st, err
		}
		st.Replicas = parallelism
		return st, nil
	}
	if err != nil {
		return st, err
	}
	st.Replicas = *job.Spec.Parallelism
	st.Ready = job.Status.Active
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete:
			st.Phase = v1alpha1.OperationSucceeded
			for _, ch := range wl.Spec.Outbound(op.Name) {
				if ch.Feedback == nil {
					if err := r.seal(ctx, wl, ch.Name); err != nil {
						return st, err
					}
				}
			}
			return st, nil
		case batchv1.JobFailed:
			st.Phase = v1alpha1.OperationFailed
			return st, nil
		}
	}
	// Scale up on backlog while no pod has succeeded yet (a Job stops
	// creating pods once one succeeds).
	if job.Status.Succeeded == 0 {
		want := desiredReplicas(op, *job.Spec.Parallelism, backlog)
		if want > *job.Spec.Parallelism {
			job.Spec.Parallelism = &want
			if err := r.Update(ctx, job); err != nil {
				return st, client.IgnoreNotFound(err)
			}
			st.Replicas = want
		}
	}
	return st, nil
}

func (r *Reconciler) reconcileStreaming(ctx context.Context, wl *v1alpha1.Workload, op *v1alpha1.Operation, backlog int64) (v1alpha1.OperationStatus, error) {
	st := v1alpha1.OperationStatus{Name: op.Name, Phase: v1alpha1.OperationRunning}
	labels := opLabels(wl, op)
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: opName(wl, op), Namespace: wl.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Labels = labels
		current := int32(0)
		if dep.Spec.Replicas != nil {
			current = *dep.Spec.Replicas
		}
		if op.Scaling.Horizontal.CPUUtilizationPercent == 0 || dep.Spec.Replicas == nil {
			want := desiredReplicas(op, current, backlog)
			// Backlog scaling for streaming operations scales both ways.
			if op.Scaling.Horizontal.TargetBacklogPerReplica > 0 {
				want = desiredReplicas(op, 0, backlog)
			}
			dep.Spec.Replicas = &want
		}
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		dep.Spec.Template = r.podTemplate(wl, op)
		return controllerutil.SetControllerReference(wl, dep, r.Scheme())
	})
	if err != nil {
		return st, err
	}
	st.Replicas = *dep.Spec.Replicas
	st.Ready = dep.Status.ReadyReplicas
	if err := r.ensureHPA(ctx, wl, op); err != nil {
		return st, err
	}
	return st, r.ensureVPA(ctx, wl, op, "Deployment")
}

func opLabels(wl *v1alpha1.Workload, op *v1alpha1.Operation) map[string]string {
	return map[string]string{LabelWorkload: wl.Name, LabelOperation: op.Name, LabelRole: "operation"}
}

// podTemplate returns the operation's template with graph discovery injected.
func (r *Reconciler) podTemplate(wl *v1alpha1.Workload, op *v1alpha1.Operation) corev1.PodTemplateSpec {
	tpl := *op.Template.DeepCopy()
	if tpl.Labels == nil {
		tpl.Labels = map[string]string{}
	}
	for k, v := range opLabels(wl, op) {
		tpl.Labels[k] = v
	}
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
		{Name: "STARK8S_EXCHANGE", Value: fmt.Sprintf("http://%s:%d", exchangeName(wl), exchangePort)},
		{Name: "STARK8S_WORKLOAD", Value: wl.Name},
		{Name: "STARK8S_OPERATION", Value: op.Name},
		{Name: "STARK8S_INSTANCE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "STARK8S_INBOUND", Value: strings.Join(in, ",")},
		{Name: "STARK8S_OUTBOUND", Value: strings.Join(out, ",")},
		{Name: "STARK8S_FEEDBACK", Value: strings.Join(fb, ",")},
		{Name: "STARK8S_FEEDBACK_OUT", Value: strings.Join(fbOut, ",")},
	}
	for i := range tpl.Spec.Containers {
		tpl.Spec.Containers[i].Env = append(env, tpl.Spec.Containers[i].Env...)
	}
	return tpl
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
		hpa.Spec.MinReplicas = &min
		hpa.Spec.MaxReplicas = op.Scaling.Horizontal.Max
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

// ensureVPA creates a VerticalPodAutoscaler when requested and the API exists.
func (r *Reconciler) ensureVPA(ctx context.Context, wl *v1alpha1.Workload, op *v1alpha1.Operation, kind string) error {
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
			"apiVersion": "apps/v1", "kind": kind, "name": opName(wl, op),
		}, "spec", "targetRef")
		_ = unstructured.SetNestedField(vpa.Object, string(op.Scaling.Vertical.Mode), "spec", "updatePolicy", "updateMode")
		return controllerutil.SetControllerReference(wl, vpa, r.Scheme())
	})
	return err
}

// --- network --------------------------------------------------------------

func (r *Reconciler) ensureNetworkPolicies(ctx context.Context, wl *v1alpha1.Workload) error {
	workloadPods := metav1.LabelSelector{MatchLabels: map[string]string{LabelWorkload: wl.Name, LabelRole: "operation"}}
	exchangePods := metav1.LabelSelector{MatchLabels: map[string]string{LabelWorkload: wl.Name, LabelRole: "exchange"}}
	tcp := corev1.ProtocolTCP
	udp := corev1.ProtocolUDP
	dns := intstr.FromInt(53)
	xport := intstr.FromInt(exchangePort)

	// Operation pods: no ingress; egress only to the exchange and to DNS.
	ops := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: wl.Name + "-operations", Namespace: wl.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, ops, func() error {
		ops.Labels = map[string]string{LabelWorkload: wl.Name}
		ops.Spec = networkingv1.NetworkPolicySpec{
			PodSelector: workloadPods,
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{To: []networkingv1.NetworkPolicyPeer{{PodSelector: &exchangePods}}, Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &xport}}},
				{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &udp, Port: &dns}, {Protocol: &tcp, Port: &dns}}},
			},
		}
		return controllerutil.SetControllerReference(wl, ops, r.Scheme())
	}); err != nil {
		return err
	}

	// Exchange: ingress only from this workload's operation pods and the
	// controller's namespace; egress only DNS.
	ex := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: wl.Name + "-exchange", Namespace: wl.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ex, func() error {
		ex.Labels = map[string]string{LabelWorkload: wl.Name}
		from := []networkingv1.NetworkPolicyPeer{{PodSelector: &workloadPods}}
		if r.ControllerNamespace != "" {
			from = append(from, networkingv1.NetworkPolicyPeer{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": r.ControllerNamespace}},
			})
		}
		ex.Spec = networkingv1.NetworkPolicySpec{
			PodSelector: exchangePods,
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			Ingress:     []networkingv1.NetworkPolicyIngressRule{{From: from, Ports: []networkingv1.NetworkPolicyPort{{Protocol: &tcp, Port: &xport}}}},
			Egress:      []networkingv1.NetworkPolicyEgressRule{{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &udp, Port: &dns}, {Protocol: &tcp, Port: &dns}}}},
		}
		return controllerutil.SetControllerReference(wl, ex, r.Scheme())
	})
	return err
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
