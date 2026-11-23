/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workloadmixing

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/controller/slurmjob"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/controller/workloadmixing/rules"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
)

// WorkloadMixingReconciler reconciles a WorkloadMixing object
type WorkloadMixingReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Rules    []rules.ValidationRule
}

// +kubebuilder:rbac:groups=k8s-bridge.x-k8s.io,resources=workloadmixings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=k8s-bridge.x-k8s.io,resources=workloadmixings/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=k8s-bridge.x-k8s.io,resources=workloadmixings/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=slinky.slurm.net,resources=controllers,verbs=get;list;watch
// +kubebuilder:rbac:groups=slinky.slurm.net,resources=restapis,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *WorkloadMixingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	workloadMixing := &k8sbridgev1alpha1.WorkloadMixing{}
	if err := r.Get(ctx, req.NamespacedName, workloadMixing); err != nil {
		if client.IgnoreNotFound(err) == nil {
			logger.V(1).Info("WorkloadMixing resource not found, cleaning up metrics")
			r.cleanupWorkloadMixingMetrics(ctx, req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !workloadMixing.DeletionTimestamp.IsZero() {
		logger.Info("WorkloadMixing is being deleted, cleaning up metrics")
		r.cleanupWorkloadMixingMetrics(ctx, workloadMixing.Namespace, workloadMixing.Name)
		return ctrl.Result{}, nil
	}

	logger.V(1).Info("Reconciling WorkloadMixing", "controllerRef", workloadMixing.Spec.ControllerRef.Name)

	// Read the condition before updateAvailabilityStatus, which records the current generation.
	newGeneration := !isGenerationObserved(workloadMixing)

	err := r.updateAvailabilityStatus(ctx, workloadMixing)
	if err == nil && newGeneration {
		r.warnAboutPodTemplate(workloadMixing)
	}
	r.updateWorkloadMixingMetrics(ctx, workloadMixing)

	return ctrl.Result{}, err
}

// isGenerationObserved reports whether the Available condition shows the current spec generation.
func isGenerationObserved(wm *k8sbridgev1alpha1.WorkloadMixing) bool {
	available := meta.FindStatusCondition(wm.Status.Conditions, k8sbridgev1alpha1.ConditionTypeAvailable)
	return available != nil && available.ObservedGeneration == wm.Generation
}

// warnAboutPodTemplate sends a Warning Event for each problem in the podTemplate.
// Reconcile calls it only after updateAvailabilityStatus records a new spec generation.
func (r *WorkloadMixingReconciler) warnAboutPodTemplate(wm *k8sbridgev1alpha1.WorkloadMixing) {
	if r.Recorder == nil {
		return
	}
	var slurmd corev1.ResourceRequirements
	if wm.Spec.PodTemplate != nil {
		for _, c := range wm.Spec.PodTemplate.Spec.Containers {
			if c.Name == slurmjob.ContainerSlurmd {
				slurmd = c.Resources
			}
		}
	}
	var missingLimits []string
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		if _, ok := slurmd.Limits[name]; !ok {
			missingLimits = append(missingLimits, string(name))
		}
	}
	if len(missingLimits) > 0 {
		r.Recorder.Event(wm, corev1.EventTypeWarning, "MaxNodeSizeNotConfigured",
			"Maximum node size not configured: the slurmd container in spec.podTemplate has no limit for "+strings.Join(missingLimits, " and "))
	}
	_, hasCPURequest := slurmd.Requests[corev1.ResourceCPU]
	_, hasMemRequest := slurmd.Requests[corev1.ResourceMemory]
	if hasCPURequest || hasMemRequest {
		r.Recorder.Event(wm, corev1.EventTypeWarning, "SlurmdRequestsIgnored",
			"Ignored requests in spec.podTemplate: k8s-bridge sets the cpu and memory requests of the slurmd container for each Slurm job")
	}
}

func (r *WorkloadMixingReconciler) getRules() []rules.ValidationRule {
	if len(r.Rules) > 0 {
		return r.Rules
	}
	return rules.GetRules()
}

func (r *WorkloadMixingReconciler) cleanupWorkloadMixingMetrics(ctx context.Context, namespace, name string) {
	metrics.DeleteWorkloadMixingMetrics(namespace, name)
	r.updateActiveWorkloadMixingsMetrics(ctx)
}

func (r *WorkloadMixingReconciler) updateWorkloadMixingMetrics(ctx context.Context, wm *k8sbridgev1alpha1.WorkloadMixing) {
	metrics.SetWorkloadMixingAvailable(wm.Namespace, wm.Name, wm.IsAvailable())
	metrics.SetTrackedPartitions(wm.Name, len(wm.Spec.PartitionsMapping))
	r.updateActiveWorkloadMixingsMetrics(ctx)
	r.updateActiveJobsMetrics(ctx, wm)
}

func (r *WorkloadMixingReconciler) updateActiveJobsMetrics(ctx context.Context, wm *k8sbridgev1alpha1.WorkloadMixing) {
	for _, pm := range wm.Spec.PartitionsMapping {
		count, err := slurmjob.CountActiveJobSets(ctx, r.Client, wm, pm.Partition)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to count active JobSets for metric", "partition", pm.Partition)
			continue
		}
		metrics.SetActiveJobs(wm.Name, pm.Partition, count)
	}
}

func (r *WorkloadMixingReconciler) updateActiveWorkloadMixingsMetrics(ctx context.Context) {
	logger := log.FromContext(ctx)
	wmList := &k8sbridgev1alpha1.WorkloadMixingList{}
	if err := r.List(ctx, wmList); err != nil {
		logger.V(1).Info("Failed to list WorkloadMixing resources for active count metric", "error", err)
		return
	}
	count := 0
	for _, item := range wmList.Items {
		if item.DeletionTimestamp.IsZero() {
			count++
		}
	}
	metrics.SetActiveWorkloadMixings(count)
}

// ReasonReconciled indicates all WorkloadMixing validation rules succeeded.
const ReasonReconciled = "Reconciled"

func (r *WorkloadMixingReconciler) evaluateAvailability(ctx context.Context, wm *k8sbridgev1alpha1.WorkloadMixing) (metav1.Condition, error) {
	var firstReason string
	var messages []string

	for _, rule := range r.getRules() {
		failure, err := rule.Validate(ctx, r.Client, wm)
		if err != nil {
			return metav1.Condition{}, err
		}
		if failure != nil {
			if firstReason == "" {
				firstReason = failure.Reason
			}
			messages = append(messages, failure.Message)
		}
	}

	if len(messages) > 0 {
		return metav1.Condition{
			Type:               k8sbridgev1alpha1.ConditionTypeAvailable,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: wm.Generation,
			Reason:             firstReason,
			Message:            strings.Join(messages, "; "),
		}, nil
	}

	return metav1.Condition{
		Type:               k8sbridgev1alpha1.ConditionTypeAvailable,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: wm.Generation,
		Reason:             ReasonReconciled,
		Message:            "WorkloadMixing successfully reconciled",
	}, nil
}

func (r *WorkloadMixingReconciler) updateAvailabilityStatus(ctx context.Context, workloadMixing *k8sbridgev1alpha1.WorkloadMixing) error {
	logger := log.FromContext(ctx)

	condition, err := r.evaluateAvailability(ctx, workloadMixing)
	if err != nil {
		return err
	}

	updated := workloadMixing.DeepCopy()
	changed := meta.SetStatusCondition(&updated.Status.Conditions, condition)
	if changed {
		if err := r.Status().Patch(ctx, updated, client.MergeFrom(workloadMixing)); err != nil {
			if r.Recorder != nil {
				r.Recorder.Event(workloadMixing, corev1.EventTypeWarning, "StatusUpdateFailed", "Failed to update WorkloadMixing status")
			}
			return err
		}
		*workloadMixing = *updated
		logger.Info("Updated WorkloadMixing status condition",
			"status", condition.Status,
			"reason", condition.Reason,
			"message", condition.Message,
			"partitionsMapping", workloadMixing.Spec.PartitionsMapping)
		if r.Recorder != nil {
			eventType := corev1.EventTypeNormal
			if condition.Status == metav1.ConditionFalse {
				eventType = corev1.EventTypeWarning
			}
			r.Recorder.Event(workloadMixing, eventType, condition.Reason, condition.Message)
		}
	} else {
		logger.V(1).Info("WorkloadMixing status condition unchanged",
			"status", condition.Status,
			"reason", condition.Reason)
	}

	return nil
}

func (r *WorkloadMixingReconciler) enqueueMatchingWMs(
	matches func(eventObj client.Object, wm *k8sbridgev1alpha1.WorkloadMixing) bool,
) handler.MapFunc {
	return func(ctx context.Context, eventObj client.Object) []reconcile.Request {
		if eventObj == nil || matches == nil {
			return nil
		}

		wmList := &k8sbridgev1alpha1.WorkloadMixingList{}
		var listOpts []client.ListOption
		if ns := eventObj.GetNamespace(); ns != "" {
			listOpts = append(listOpts, client.InNamespace(ns))
		}
		if err := r.List(ctx, wmList, listOpts...); err != nil {
			log.FromContext(ctx).Error(err, "Failed to list WorkloadMixings", "namespace", eventObj.GetNamespace())
			return nil
		}

		var requests []reconcile.Request
		for i := range wmList.Items {
			wm := &wmList.Items[i]
			if matches(eventObj, wm) {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Name:      wm.Name,
						Namespace: wm.Namespace,
					},
				})
			}
		}
		return requests
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadMixingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	b := ctrl.NewControllerManagedBy(mgr).For(&k8sbridgev1alpha1.WorkloadMixing{})
	for _, rule := range r.getRules() {
		if watchable, ok := rule.(rules.WatchableRule); ok {
			if w := watchable.Watch(); w != nil {
				b = b.Watches(w.Object, handler.EnqueueRequestsFromMapFunc(r.enqueueMatchingWMs(w.Matches)), w.Opts...)
			}
		}
	}
	return b.Named("workloadmixing").Complete(r)
}
