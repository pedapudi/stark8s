package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/pedapudi/stark8s/api/v1alpha1"
	"github.com/pedapudi/stark8s/pkg/coordinator"
)

const labelCollectiveAttempt = "stark8s.io/collective-attempt"

func (r *Reconciler) reconcileCollective(ctx context.Context, wl *v1alpha1.Workload, op *v1alpha1.Operation) (v1alpha1.OperationStatus, error) {
	status := v1alpha1.OperationStatus{Name: op.Name, Phase: v1alpha1.OperationWaiting}
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(wl.Namespace), client.MatchingLabels(opLabels(wl, op))); err != nil {
		return status, err
	}
	owned := jobs.Items[:0]
	for i := range jobs.Items {
		if metav1.IsControlledBy(&jobs.Items[i], wl) {
			owned = append(owned, jobs.Items[i])
		}
	}
	jobs.Items = owned
	sort.Slice(jobs.Items, func(i, j int) bool { return collectiveAttempt(&jobs.Items[i]) < collectiveAttempt(&jobs.Items[j]) })
	attempt := 1
	if len(jobs.Items) > 0 {
		current := &jobs.Items[len(jobs.Items)-1]
		attempt = collectiveAttempt(current)
		if jobComplete(current) {
			for _, channel := range wl.Spec.Outbound(op.Name) {
				if channel.Feedback == nil {
					if err := r.seal(ctx, wl, channel.Name); err != nil {
						return status, err
					}
				}
			}
			status.Phase = v1alpha1.OperationSucceeded
			status.Ready = current.Status.Succeeded
			status.Replicas = op.Collective.Size
			return status, nil
		}
		if !jobFailed(current) {
			status.Phase = v1alpha1.OperationRunning
			if current.Status.Ready != nil {
				status.Ready = *current.Status.Ready
			}
			status.Replicas = op.Collective.Size
			return status, nil
		}
		max := int(op.Collective.MaxAttempts)
		if max < 1 {
			max = 1
		}
		if attempt >= max {
			status.Phase = v1alpha1.OperationFailed
			return status, nil
		}
		if op.Collective.Checkpoint == "" {
			return status, fmt.Errorf("collective operation %q failed and cannot replace the group without a committed checkpoint", op.Name)
		}
		attempt++
	}
	jobName := fmt.Sprintf("%s-attempt-%d", opName(wl, op), attempt)
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: wl.Namespace}}
	labels := opLabels(wl, op)
	labels[labelCollectiveAttempt] = strconv.Itoa(attempt)
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		service.Labels = labels
		service.Spec.ClusterIP = corev1.ClusterIPNone
		service.Spec.PublishNotReadyAddresses = true
		service.Spec.Selector = labels
		service.Spec.Ports = []corev1.ServicePort{{Name: "rank-discovery", Port: coordinator.SegmentPort}}
		return controllerutil.SetControllerReference(wl, service, r.Scheme())
	}); err != nil {
		return status, err
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: wl.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, job, func() error {
		job.Labels = labels
		zero := int32(0)
		size := op.Collective.Size
		indexed := batchv1.IndexedCompletion
		job.Spec.BackoffLimit = &zero
		job.Spec.Parallelism = &size
		job.Spec.Completions = &size
		job.Spec.CompletionMode = &indexed
		job.Spec.Template = r.podTemplate(wl, op)
		job.Spec.Template.Labels = copyLabels(labels)
		job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
		job.Spec.Template.Spec.Subdomain = jobName
		job.Spec.Template.Spec.SetHostnameAsFQDN = boolPtr(false)
		collectiveEnv := []corev1.EnvVar{
			{Name: coordinator.EnvCollectiveRank, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations['batch.kubernetes.io/job-completion-index']"}}},
			{Name: coordinator.EnvCollectiveSize, Value: strconv.Itoa(int(size))}, {Name: coordinator.EnvCollectiveAttempt, Value: jobName},
			{Name: coordinator.EnvCollectiveRendezvous, Value: fmt.Sprintf("%s-0.%s.%s.svc", jobName, jobName, wl.Namespace)}, {Name: coordinator.EnvCollectiveCheckpoint, Value: op.Collective.Checkpoint},
		}
		for i := range job.Spec.Template.Spec.Containers {
			c := &job.Spec.Template.Spec.Containers[i]
			c.Env = append(collectiveEnv, c.Env...)
		}
		for i := range job.Spec.Template.Spec.InitContainers {
			c := &job.Spec.Template.Spec.InitContainers[i]
			if c.Name == "stark8s-runtime" && c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
				c.Env = append(collectiveEnv, c.Env...)
			}
		}
		return controllerutil.SetControllerReference(wl, job, r.Scheme())
	})
	if err != nil {
		return status, err
	}
	status.Phase = v1alpha1.OperationRunning
	status.Replicas = op.Collective.Size
	return status, nil
}

func collectiveAttempt(job *batchv1.Job) int {
	n, _ := strconv.Atoi(job.Labels[labelCollectiveAttempt])
	return n
}
func jobComplete(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
func jobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
func copyLabels(in map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		out[k] = v
	}
	return out
}
func boolPtr(v bool) *bool { return &v }
