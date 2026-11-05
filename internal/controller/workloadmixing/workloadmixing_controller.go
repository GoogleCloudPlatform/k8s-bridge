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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/controller/slurmjob"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/utils/refresolver"
)

const conditionTypeAvailable = "Available"

// WorkloadMixingReconciler reconciles a WorkloadMixing object
type WorkloadMixingReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
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
			r.cleanupWorkloadMixingMetrics(ctx, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !workloadMixing.DeletionTimestamp.IsZero() {
		logger.Info("WorkloadMixing is being deleted, cleaning up metrics")
		r.cleanupWorkloadMixingMetrics(ctx, workloadMixing.Name)
		return ctrl.Result{}, nil
	}

	r.updateWorkloadMixingMetrics(ctx, workloadMixing)

	original := workloadMixing.DeepCopy()

	logger.V(1).Info("Reconciling WorkloadMixing", "controllerRef", workloadMixing.Spec.ControllerRef.Name)

	r.warnAboutPodTemplate(workloadMixing)

	if err := r.updateAvailabilityStatus(ctx, workloadMixing, original); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// warnAboutPodTemplate sends a Warning Event for each problem in the podTemplate. It runs
// only for a spec generation that the Available condition does not show yet.
func (r *WorkloadMixingReconciler) warnAboutPodTemplate(wm *k8sbridgev1alpha1.WorkloadMixing) {
	available := meta.FindStatusCondition(wm.Status.Conditions, conditionTypeAvailable)
	if r.Recorder == nil || (available != nil && available.ObservedGeneration == wm.Generation) {
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

func (r *WorkloadMixingReconciler) cleanupWorkloadMixingMetrics(ctx context.Context, name string) {
	metrics.DeleteWorkloadMixingMetrics(name)
	r.updateActiveWorkloadMixingsMetrics(ctx)
}

func (r *WorkloadMixingReconciler) updateWorkloadMixingMetrics(ctx context.Context, wm *k8sbridgev1alpha1.WorkloadMixing) {
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

func (r *WorkloadMixingReconciler) updateAvailabilityStatus(ctx context.Context, workloadMixing *k8sbridgev1alpha1.WorkloadMixing, original *k8sbridgev1alpha1.WorkloadMixing) error {
	logger := log.FromContext(ctx)

	var condition metav1.Condition

	controllerCR, err := r.getController(ctx, workloadMixing)
	if err != nil {
		return err
	}
	if controllerCR == nil {
		condition = metav1.Condition{
			Type:               conditionTypeAvailable,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: workloadMixing.Generation,
			Reason:             "ControllerNotFound",
			Message:            fmt.Sprintf("Referenced Controller %q not found", workloadMixing.Spec.ControllerRef.Name),
		}
	} else {
		restAPI, err := r.getRestAPI(ctx, controllerCR)
		if err != nil {
			return err
		}
		if restAPI == nil {
			condition = metav1.Condition{
				Type:               conditionTypeAvailable,
				Status:             metav1.ConditionFalse,
				ObservedGeneration: workloadMixing.Generation,
				Reason:             "RestApiNotFound",
				Message:            fmt.Sprintf("No RestApi found for Controller %q", controllerCR.Name),
			}
		} else {
			condition = metav1.Condition{
				Type:               conditionTypeAvailable,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: workloadMixing.Generation,
				Reason:             "Reconciled",
				Message:            "WorkloadMixing successfully reconciled",
			}
		}
	}

	changed := meta.SetStatusCondition(&workloadMixing.Status.Conditions, condition)
	if changed {
		if err := r.Status().Patch(ctx, workloadMixing, client.MergeFrom(original)); err != nil {
			if r.Recorder != nil {
				r.Recorder.Event(workloadMixing, corev1.EventTypeWarning, "StatusUpdateFailed", "Failed to update WorkloadMixing status")
			}
			return err
		}
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

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadMixingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&k8sbridgev1alpha1.WorkloadMixing{}).
		Watches(
			&slinkyv1beta1.Controller{},
			handler.EnqueueRequestsFromMapFunc(r.mapControllerToWorkloadMixing),
		).
		Watches(
			&slinkyv1beta1.RestApi{},
			handler.EnqueueRequestsFromMapFunc(r.mapRestAPIToWorkloadMixing),
		).
		Named("workloadmixing").
		Complete(r)
}

func (r *WorkloadMixingReconciler) getController(ctx context.Context, workloadMixing *k8sbridgev1alpha1.WorkloadMixing) (*slinkyv1beta1.Controller, error) {
	logger := log.FromContext(ctx)
	controllerCR := &slinkyv1beta1.Controller{}
	controllerKey := types.NamespacedName{
		Namespace: workloadMixing.Namespace,
		Name:      workloadMixing.Spec.ControllerRef.Name,
	}

	if err := r.Get(ctx, controllerKey, controllerCR); err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(1).Info("Referenced Controller not found", "slurmController", controllerKey)
			return nil, nil
		}
		return nil, err
	}
	return controllerCR, nil
}

func (r *WorkloadMixingReconciler) getRestAPI(ctx context.Context, controllerCR *slinkyv1beta1.Controller) (*slinkyv1beta1.RestApi, error) {
	logger := log.FromContext(ctx)
	resolver := refresolver.New(r.Client)
	controllerKey := client.ObjectKeyFromObject(controllerCR)

	restAPIList, err := resolver.GetRestapisForController(ctx, controllerCR)
	if err != nil {
		return nil, err
	}

	if len(restAPIList.Items) == 0 {
		logger.V(1).Info("No RestApi found for Controller", "slurmController", controllerKey)
		return nil, nil
	}

	return &restAPIList.Items[0], nil
}

func (r *WorkloadMixingReconciler) mapControllerToWorkloadMixing(ctx context.Context, obj client.Object) []reconcile.Request {
	controller, ok := obj.(*slinkyv1beta1.Controller)
	if !ok {
		return nil
	}

	wmList := &k8sbridgev1alpha1.WorkloadMixingList{}
	if err := r.List(ctx, wmList, client.InNamespace(controller.Namespace)); err != nil {
		logger := log.FromContext(ctx)
		logger.Error(err, "Failed to list WorkloadMixings", "namespace", controller.Namespace)
		return nil
	}

	var requests []reconcile.Request
	for _, wm := range wmList.Items {
		if wm.Spec.ControllerRef.Name == controller.Name {
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

func (r *WorkloadMixingReconciler) mapRestAPIToWorkloadMixing(ctx context.Context, obj client.Object) []reconcile.Request {
	restAPI, ok := obj.(*slinkyv1beta1.RestApi)
	if !ok {
		return nil
	}
	if restAPI.Spec.ControllerRef.Name == "" {
		return nil
	}

	wmList := &k8sbridgev1alpha1.WorkloadMixingList{}
	if err := r.List(ctx, wmList, client.InNamespace(restAPI.Namespace)); err != nil {
		logger := log.FromContext(ctx)
		logger.Error(err, "Failed to list WorkloadMixings", "namespace", restAPI.Namespace)
		return nil
	}

	var requests []reconcile.Request
	for _, wm := range wmList.Items {
		if wm.Spec.ControllerRef.Name == restAPI.Spec.ControllerRef.Name {
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
