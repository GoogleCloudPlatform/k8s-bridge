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

package slurmjob

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/clientmap"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/utils/slurmtypes"
	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	"github.com/SlinkyProject/slurm-client/pkg/object"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
)

const (
	KindWorkloadMixing = "WorkloadMixing"

	ReasonJobSetCompleted = "JobSet completed"
	ReasonJobSetFailed    = "JobSet failed"
	ReasonJobSetMissing   = "JobSet missing"

	UnheldCommentPrefix = "Unheld by k8s-bridge:"
)

// SlurmJobReconciler reconciles Slurm jobs received from the poller.
type SlurmJobReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	Recorder            record.EventRecorder
	AdmissionEventsChan <-chan slurmtypes.SlurmJobAdmissionEvent
	genericEvents       chan event.GenericEvent
	ClientMap           *clientmap.ClientMap
	Translator          JobTranslator
}

// NeedLeaderElection implements the manager.LeaderElectionRunnable interface.
func (r *SlurmJobReconciler) NeedLeaderElection() bool {
	return true
}

// Start runs the background event forwarding loop. It implements manager.Runnable.
func (r *SlurmJobReconciler) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("slurmjob-forwarder")
	logger.Info("Starting SlurmJobAdmissionEvent forwarding loop")

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping SlurmJobAdmissionEvent forwarding loop")
			return nil
		case ev, ok := <-r.AdmissionEventsChan:
			if !ok {
				logger.Info("AdmissionEventsChan closed, stopping forwarding loop")
				return nil
			}

			// Format object name to include workloadMixing metadata if available
			var objName string
			if ev.WorkloadMixing.Name != "" {
				objName = fmt.Sprintf("slurm-job-%s-%s", ev.WorkloadMixing.Name, ev.JobID)
			} else {
				objName = fmt.Sprintf("slurm-job-%s", ev.JobID)
			}

			select {
			case r.genericEvents <- event.GenericEvent{
				Object: &metav1.PartialObjectMetadata{
					ObjectMeta: metav1.ObjectMeta{
						Name:      objName,
						Namespace: ev.WorkloadMixing.Namespace,
					},
				},
			}:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *SlurmJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.genericEvents = make(chan event.GenericEvent, 1024)
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("slurmjob-controller") //nolint:staticcheck
	}

	if err := mgr.Add(r); err != nil {
		return fmt.Errorf("failed to add SlurmJobReconciler as Runnable: %w", err)
	}

	c, err := controller.New("slurmjob-controller", mgr, controller.Options{
		Reconciler: r,
	})
	if err != nil {
		return fmt.Errorf("failed to create controller: %w", err)
	}

	err = c.Watch(
		source.Channel(r.genericEvents, &handler.EnqueueRequestForObject{}),
	)
	if err != nil {
		return err
	}

	return c.Watch(
		source.Kind(
			mgr.GetCache(),
			&jobsetv1alpha2.JobSet{},
			handler.TypedEnqueueRequestsFromMapFunc(r.mapJobSetToRequest),
		),
	)
}

func (r *SlurmJobReconciler) mapJobSetToRequest(ctx context.Context, js *jobsetv1alpha2.JobSet) []reconcile.Request {
	logger := log.FromContext(ctx)

	// Extract jobID from label
	jobID := js.Labels[LabelSlurmJobID]
	if jobID == "" {
		logger.Info("JobSet missing SlurmJobID label, ignoring", "jobSetName", js.Name, "namespace", js.Namespace)
		return nil
	}

	// Extract WorkloadMixing name from OwnerReferences
	var wmName string
	for _, owner := range js.OwnerReferences {
		if owner.Kind == KindWorkloadMixing {
			wmName = owner.Name
			break
		}
	}
	if wmName == "" {
		logger.Info("JobSet missing WorkloadMixing OwnerReference, ignoring", "jobSetName", js.Name, "namespace", js.Namespace)
		return nil
	}

	return []reconcile.Request{
		{NamespacedName: types.NamespacedName{
			Name:      fmt.Sprintf("slurm-job-%s-%s", wmName, jobID),
			Namespace: js.Namespace,
		}},
	}
}

type slurmJobState struct {
	WM          *k8sbridgev1alpha1.WorkloadMixing
	SlurmClient slurmclient.Client
	JobInfo     *slurmclienttypes.V0044JobInfo
	Result      ctrl.Result
}

// +kubebuilder:rbac:groups=k8s-bridge.x-k8s.io,resources=workloadmixings,verbs=get;list;watch
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets,verbs=get;list;watch;create;update;patch;delete

// Reconcile reconciles a Slurm job.
func (r *SlurmJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	wmName, jobID, err := parseRequestName(req.Name)
	if err != nil {
		logger.Error(err, "Failed to parse request name", "name", req.Name)
		return ctrl.Result{}, nil
	}

	if r.verifyWMName(ctx, req.Namespace, wmName, jobID) {
		return ctrl.Result{}, nil
	}

	state, done, err := r.fetchState(ctx, req.Namespace, wmName, jobID)
	if done || err != nil {
		return state.Result, err
	}

	partition := ptr.Deref(state.JobInfo.Partition, "")

	done, result, err := r.reconcileActiveJob(ctx, state.WM, state.SlurmClient, state.JobInfo, jobID, partition)
	if done || err != nil {
		return result, err
	}

	return r.reconcileNewJob(ctx, state.WM, state.SlurmClient, state.JobInfo, jobID, partition)
}

func parseRequestName(name string) (string, string, error) {
	if !strings.HasPrefix(name, "slurm-job-") {
		return "", "", fmt.Errorf("invalid request name format: %s", name)
	}
	trimmed := strings.TrimPrefix(name, "slurm-job-")
	idx := strings.LastIndex(trimmed, "-")
	if idx == -1 {
		return "", trimmed, nil
	}
	return trimmed[:idx], trimmed[idx+1:], nil
}

func (r *SlurmJobReconciler) verifyWMName(ctx context.Context, namespace, wmName, jobID string) ReconciliationStatus {
	logger := log.FromContext(ctx)
	if wmName == "" {
		logger.Info("New Slurm job appeared", "jobID", jobID, "reason", "WorkloadMixing name is empty, skipping reconciliation")
		return ReconciliationDone
	}
	logger.Info("New Slurm job appeared", "jobID", jobID, "workloadMixing", fmt.Sprintf("%s/%s", namespace, wmName))
	return ReconciliationContinue
}

func (r *SlurmJobReconciler) fetchState(ctx context.Context, namespace, wmName, jobID string) (slurmJobState, ReconciliationStatus, error) {
	logger := log.FromContext(ctx)

	wm := &k8sbridgev1alpha1.WorkloadMixing{}
	wmKey := client.ObjectKey{Namespace: namespace, Name: wmName}
	if err := r.Get(ctx, wmKey, wm); err != nil {
		logger.Error(err, "Failed to fetch WorkloadMixing", "workloadMixing", wmKey)
		return slurmJobState{Result: ctrl.Result{}}, ReconciliationDone, client.IgnoreNotFound(err)
	}

	if r.ClientMap == nil {
		logger.Error(nil, "ClientMap is not initialized")
		return slurmJobState{Result: ctrl.Result{}}, ReconciliationDone, fmt.Errorf("ClientMap is not initialized")
	}
	controllerKey := types.NamespacedName{
		Namespace: wm.Namespace,
		Name:      wm.Spec.ControllerRef.Name,
	}
	slurmClient := r.ClientMap.Get(controllerKey)
	if slurmClient == nil {
		logger.Error(nil, "Slurm client not found in ClientMap", "controller", controllerKey)
		return slurmJobState{Result: ctrl.Result{RequeueAfter: 5 * time.Second}}, ReconciliationDone, nil
	}

	var jobInfo slurmclienttypes.V0044JobInfo
	if err := slurmClient.Get(ctx, object.ObjectKey(jobID), &jobInfo); err != nil {
		if isSlurmNotFound(err) {
			// A job may no longer exist in Slurmctld if it reached a terminal state and was
			// purged after MinJobAge, while watch events on the associated JobSet still arrive.
			// Return nil to avoid entering an infinite exponential backoff retry loop;
			// JobTeardownReconciler handles cleaning up any orphaned JobSet resources.
			logger.Info("Slurm job not found in Slurm, skipping reconciliation", "jobID", jobID)
			return slurmJobState{Result: ctrl.Result{}}, ReconciliationDone, nil
		}
		logger.Error(err, "Failed to fetch job info from Slurm", "jobID", jobID)
		return slurmJobState{Result: ctrl.Result{}}, ReconciliationDone, err
	}

	return slurmJobState{WM: wm, SlurmClient: slurmClient, JobInfo: &jobInfo, Result: ctrl.Result{}}, ReconciliationContinue, nil
}

type ReconciliationStatus bool

const (
	ReconciliationDone     ReconciliationStatus = true
	ReconciliationContinue ReconciliationStatus = false
)

type K8sbridgeJobStatus string

const (
	StatusNewJob              K8sbridgeJobStatus = "NewJob"
	StatusExplicitlyHeld      K8sbridgeJobStatus = "ExplicitlyHeld"
	StatusExplicitHoldRemoved K8sbridgeJobStatus = "ExplicitHoldRemoved"
	StatusShouldUnhold        K8sbridgeJobStatus = "ShouldUnhold"
	StatusNoAction            K8sbridgeJobStatus = "NoAction"
	StatusJobSetCompleted     K8sbridgeJobStatus = "JobSetCompleted"
	StatusJobSetFailed        K8sbridgeJobStatus = "JobSetFailed"
	StatusJobSetMissing       K8sbridgeJobStatus = "JobSetMissing"
)

func activeJobStatus(jobInfo *slurmclienttypes.V0044JobInfo, existingJS *jobsetv1alpha2.JobSet) K8sbridgeJobStatus {
	if status, isTerminal := terminalJobSetStatus(existingJS); isTerminal {
		return status
	}

	if jobInfo == nil || hasTerminalSlurmState(jobInfo) {
		return StatusNoAction
	}

	if existingJS == nil {
		if isUntranslatedNewJob(jobInfo) {
			return StatusNewJob
		}
		return StatusJobSetMissing
	}

	// Treat a deleting JobSet as missing so its Slurm job is cancelled.
	if !existingJS.DeletionTimestamp.IsZero() {
		return StatusJobSetMissing
	}

	if isJobExplicitlyHeld(jobInfo) {
		if !ptr.Deref(existingJS.Spec.Suspend, false) {
			return StatusExplicitlyHeld
		}
		return StatusNoAction
	}

	if isJobSetSuspendedBySlurmHold(existingJS) {
		return StatusExplicitHoldRemoved
	}

	if ptr.Deref(jobInfo.Hold, false) && !ptr.Deref(existingJS.Spec.Suspend, false) {
		return StatusShouldUnhold
	}

	return StatusNoAction
}

// isUntranslatedNewJob reports whether a Slurm job is PENDING and held,
// and has not been translated yet (see isJobTranslated). A translated job
// may also be PENDING and held if Slurm or a user re-holds it.
func isUntranslatedNewJob(jobInfo *slurmclienttypes.V0044JobInfo) bool {
	if jobInfo == nil {
		return false
	}
	isPending := jobInfo.GetStateAsSet().Has(slurmapi.V0044JobInfoJobStatePENDING)
	isHeld := ptr.Deref(jobInfo.Hold, false)
	return isPending && isHeld && !isJobTranslated(jobInfo)
}

// isJobTranslated reports whether k8s-bridge has translated and unheld the job.
// It checks for the job feature constraint or the unheld comment set by releaseHold.
func isJobTranslated(jobInfo *slurmclienttypes.V0044JobInfo) bool {
	if jobInfo == nil {
		return false
	}
	if jobID := ptr.Deref(jobInfo.JobId, 0); jobID > 0 {
		uniqueFeature := fmt.Sprintf("%s%d", UniqueFeaturePrefix, jobID)
		if hasFeatureConstraint(ptr.Deref(jobInfo.Features, ""), uniqueFeature) {
			return true
		}
	}
	comment := ptr.Deref(jobInfo.Comment, "")
	return strings.Contains(comment, UnheldCommentPrefix) && !strings.Contains(comment, AutoHoldMarker)
}

func terminalJobSetStatus(js *jobsetv1alpha2.JobSet) (K8sbridgeJobStatus, bool) {
	if js == nil {
		return "", false
	}
	for _, cond := range js.Status.Conditions {
		if cond.Status == metav1.ConditionTrue {
			switch jobsetv1alpha2.JobSetConditionType(cond.Type) {
			case jobsetv1alpha2.JobSetCompleted:
				return StatusJobSetCompleted, true
			case jobsetv1alpha2.JobSetFailed:
				return StatusJobSetFailed, true
			}
		}
	}
	return "", false
}

func isJobSetSuspendedBySlurmHold(js *jobsetv1alpha2.JobSet) bool {
	return js != nil && ptr.Deref(js.Spec.Suspend, false) && js.Annotations[AnnotationSuspendedBySlurmHold] == ValueTrue
}

func (r *SlurmJobReconciler) reconcileActiveJob(
	ctx context.Context,
	wm *k8sbridgev1alpha1.WorkloadMixing,
	slurmClient slurmclient.Client,
	jobInfo *slurmclienttypes.V0044JobInfo,
	jobID string,
	partition string,
) (ReconciliationStatus, ctrl.Result, error) {
	logger := log.FromContext(ctx)
	jobSetName := fmt.Sprintf("slurm-job-%s", jobID)
	existingJS := &jobsetv1alpha2.JobSet{}

	err := r.Get(ctx, types.NamespacedName{Name: jobSetName, Namespace: wm.Namespace}, existingJS)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return ReconciliationDone, ctrl.Result{}, fmt.Errorf("failed to get JobSet: %w", err)
		}
		existingJS = nil
	}

	status := activeJobStatus(jobInfo, existingJS)

	switch status {
	case StatusJobSetCompleted:
		return r.handleTerminalJobSet(ctx, wm, slurmClient, jobInfo, jobID, wm.Namespace, partition, ReasonJobSetCompleted)
	case StatusJobSetFailed:
		return r.handleTerminalJobSet(ctx, wm, slurmClient, jobInfo, jobID, wm.Namespace, partition, ReasonJobSetFailed)

	case StatusJobSetMissing:
		logger.Info("JobSet missing for active Slurm job, cancelling job", "jobID", jobID)
		if cancelErr := r.cancelJobWithComment(ctx, slurmClient, jobInfo, ReasonJobSetMissing); cancelErr != nil {
			return ReconciliationDone, ctrl.Result{}, cancelErr
		}
		if partition != "" {
			metrics.RecordJobSync(metrics.StatusError, metrics.ReasonJobSetFailed, wm.Name, partition)
			r.updateActiveJobsMetric(ctx, wm, partition)
		}
		return ReconciliationDone, ctrl.Result{}, nil

	case StatusExplicitlyHeld:
		logger.Info("Slurm job explicitly held by user/admin, suspending existing JobSet", "jobSet", jobSetName, "jobID", jobID)
		existingJS.Spec.Suspend = ptr.To(true)
		if existingJS.Annotations == nil {
			existingJS.Annotations = make(map[string]string)
		}
		existingJS.Annotations[AnnotationSuspendedBySlurmHold] = ValueTrue
		if updateErr := r.Update(ctx, existingJS); updateErr != nil {
			logger.Error(updateErr, "Failed to suspend existing JobSet for explicitly held Slurm job", "jobID", jobID)
			return ReconciliationDone, ctrl.Result{}, updateErr
		}
		return ReconciliationDone, ctrl.Result{}, nil

	case StatusExplicitHoldRemoved:
		logger.Info("Explicit hold removed from Slurm job, resuming existing JobSet", "jobSet", jobSetName, "jobID", jobID)
		existingJS.Spec.Suspend = ptr.To(false)
		delete(existingJS.Annotations, AnnotationSuspendedBySlurmHold)
		if len(existingJS.Annotations) == 0 {
			existingJS.Annotations = nil
		}
		if updateErr := r.Update(ctx, existingJS); updateErr != nil {
			logger.Error(updateErr, "Failed to resume existing JobSet after explicit hold removed", "jobID", jobID)
			return ReconciliationDone, ctrl.Result{}, updateErr
		}
		return ReconciliationDone, ctrl.Result{}, nil

	case StatusShouldUnhold:
		if unholdErr := r.releaseHold(ctx, slurmClient, jobInfo, existingJS); unholdErr != nil {
			logger.Error(unholdErr, "Failed to release hold on Slurm job for existing JobSet", "jobID", jobID)
			return ReconciliationDone, ctrl.Result{}, unholdErr
		}
		return ReconciliationDone, ctrl.Result{}, nil

	case StatusNewJob:
		return ReconciliationContinue, ctrl.Result{}, nil

	default:
		return ReconciliationDone, ctrl.Result{}, nil
	}
}

func (r *SlurmJobReconciler) handleTerminalJobSet(ctx context.Context, wm *k8sbridgev1alpha1.WorkloadMixing, slurmClient slurmclient.Client, jobInfo *slurmclienttypes.V0044JobInfo, jobID, namespace, partition, reason string) (ReconciliationStatus, ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info(reason, "jobID", jobID)
	if cancelErr := r.cancelJobWithComment(ctx, slurmClient, jobInfo, reason); cancelErr != nil {
		return ReconciliationDone, ctrl.Result{}, cancelErr
	}
	if errDelete := r.deleteJobSet(ctx, namespace, jobID, reason); errDelete != nil {
		return ReconciliationDone, ctrl.Result{}, errDelete
	}

	if partition != "" {
		switch reason {
		case ReasonJobSetCompleted:
			metrics.RecordJobSync(metrics.StatusSuccess, metrics.ReasonJobSetCompleted, wm.Name, partition)
		case ReasonJobSetFailed:
			metrics.RecordJobSync(metrics.StatusError, metrics.ReasonJobSetFailed, wm.Name, partition)
		}
		r.updateActiveJobsMetric(ctx, wm, partition)
	}

	return ReconciliationDone, ctrl.Result{}, nil
}

func (r *SlurmJobReconciler) deleteJobSet(ctx context.Context, namespace, jobID, reason string) error {
	logger := log.FromContext(ctx)
	jobSetName := fmt.Sprintf("slurm-job-%s", jobID)

	jobSet := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobSetName,
			Namespace: namespace,
		},
	}

	logger.Info("Cleaning up terminal JobSet", "jobSetName", jobSetName, "reason", reason)
	if err := r.Delete(ctx, jobSet); err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "Failed to clean up JobSet", "jobSetName", jobSetName)
		return err
	}

	return nil
}

func (r *SlurmJobReconciler) reconcileNewJob(
	ctx context.Context,
	wm *k8sbridgev1alpha1.WorkloadMixing,
	slurmClient slurmclient.Client,
	jobInfo *slurmclienttypes.V0044JobInfo,
	jobID string,
	partition string,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !isUntranslatedNewJob(jobInfo) {
		logger.Info("Job is not an untranslated held pending job, skipping reconciliation", "jobID", jobID)
		return ctrl.Result{}, nil
	}

	metrics.RecordJobDiscovered(wm.Name, partition)

	done, err := r.newJobCheckMultiPartition(ctx, wm, slurmClient, jobInfo, jobID, partition)
	if done || err != nil {
		return ctrl.Result{}, err
	}

	obj, done, err := r.translateSlurmJobToJobset(ctx, wm, slurmClient, jobInfo, jobID, partition)
	if done || err != nil {
		return ctrl.Result{}, err
	}

	created, err := r.createK8sResource(ctx, obj)
	if err != nil {
		logger.Error(err, "Failed to ensure Kubernetes resource exists", "name", obj.GetName())
		return ctrl.Result{}, err
	}

	if created {
		metrics.RecordJobSync(metrics.StatusSuccess, metrics.ReasonJobSetCreated, wm.Name, partition)
		r.updateActiveJobsMetric(ctx, wm, partition)
	}

	if err := r.releaseHold(ctx, slurmClient, jobInfo, obj); err != nil {
		metrics.RecordJobSync(metrics.StatusError, metrics.ReasonSlurmUpdateError, wm.Name, partition)
		logger.Error(err, "Failed to release hold on Slurm job", "jobID", jobID)
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *SlurmJobReconciler) newJobCheckMultiPartition(ctx context.Context, wm *k8sbridgev1alpha1.WorkloadMixing, slurmClient slurmclient.Client, jobInfo *slurmclienttypes.V0044JobInfo, jobID string, partition string) (ReconciliationStatus, error) {
	logger := log.FromContext(ctx)

	partitions := strings.Split(partition, ",")
	if len(partitions) > 1 {
		observedPartitions := make(map[string]bool)
		for _, pm := range wm.Spec.PartitionsMapping {
			observedPartitions[pm.Partition] = true
		}

		anyMapped := false
		for _, part := range partitions {
			if observedPartitions[strings.TrimSpace(part)] {
				anyMapped = true
				break
			}
		}

		if anyMapped {
			metrics.RecordJobSync(metrics.StatusError, metrics.ReasonTranslationError, wm.Name, partition)
			logger.Info("Job requests multiple partitions and at least one is mapped, cancelling job", "jobID", jobID, "partitions", partition)
			if r.Recorder != nil {
				r.Recorder.Eventf(wm, corev1.EventTypeWarning, "JobTranslationFailed", "Multi-partition jobs with multiple mapped partitions are not supported: %s", partition)
			}

			if err := r.cancelJobWithComment(ctx, slurmClient, jobInfo, "Multi-partition jobs with multiple mapped partitions are not supported"); err != nil {
				return ReconciliationDone, err
			}
			return ReconciliationDone, nil
		}
	}

	return ReconciliationContinue, nil
}

func (r *SlurmJobReconciler) translateSlurmJobToJobset(ctx context.Context, wm *k8sbridgev1alpha1.WorkloadMixing, slurmClient slurmclient.Client, jobInfo *slurmclienttypes.V0044JobInfo, jobID string, partition string) (client.Object, ReconciliationStatus, error) {
	logger := log.FromContext(ctx)

	translator := r.Translator
	if translator == nil {
		translator = NewJobSetTranslator()
	}
	if wm.Spec.ControllerRef.Name == "" {
		err := errors.New("WorkloadMixing Spec.ControllerRef.Name is empty")
		logger.Error(err, "Controller reference is missing", "workloadMixing", wm.Name)
		return nil, ReconciliationDone, err
	}
	controllerKey := types.NamespacedName{
		Namespace: wm.Namespace,
		Name:      wm.Spec.ControllerRef.Name,
	}
	controllerCR := &slinkyv1beta1.Controller{}
	if err := r.Get(ctx, controllerKey, controllerCR); err != nil {
		logger.Error(err, "Failed to fetch Controller", "controller", controllerKey)
		return nil, ReconciliationDone, err
	}

	start := time.Now()
	obj, err := translator.Translate(jobInfo, &wm.Spec, controllerCR)
	duration := time.Since(start)
	metrics.RecordJobTranslationDuration(wm.Name, partition, duration)

	if err != nil {
		metrics.RecordJobSync(metrics.StatusError, metrics.ReasonTranslationError, wm.Name, partition)
		logger.Error(err, "Failed to translate Slurm job, cancelling job", "jobID", jobID)
		if r.Recorder != nil {
			r.Recorder.Eventf(wm, corev1.EventTypeWarning, "JobTranslationFailed", "Failed to translate Slurm job %s: %v", jobID, err)
		}

		cancelErr := r.cancelJobWithComment(ctx, slurmClient, jobInfo, fmt.Sprintf("Translation failed: %v", err))
		if cancelErr != nil {
			return nil, ReconciliationDone, fmt.Errorf("failed to cancel job after translation failure: %w (translation error: %v)", cancelErr, err)
		}
		return nil, ReconciliationDone, nil
	}

	if obj == nil {
		logger.V(1).Info("Job translation skipped (unmapped partition or empty job)", "jobID", jobID)
		return nil, ReconciliationDone, nil
	}

	obj.SetNamespace(wm.Namespace)

	if err := controllerutil.SetControllerReference(wm, obj, r.Scheme); err != nil {
		logger.Error(err, "Failed to set controller reference on translated object", "name", obj.GetName())
		return nil, ReconciliationDone, err
	}

	return obj, ReconciliationContinue, nil
}

func (r *SlurmJobReconciler) createK8sResource(ctx context.Context, obj client.Object) (bool, error) {
	logger := log.FromContext(ctx)

	logger.Info("Creating Kubernetes resource", "name", obj.GetName(), "type", fmt.Sprintf("%T", obj))
	if err := r.Create(ctx, obj); err != nil {
		if apierrors.IsAlreadyExists(err) {
			logger.Info("Kubernetes resource already exists, skipping creation", "name", obj.GetName())
			return false, nil
		}
		return false, fmt.Errorf("failed to create Kubernetes resource %s: %w", obj.GetName(), err)
	}
	return true, nil
}

func (r *SlurmJobReconciler) cancelJobWithComment(ctx context.Context, slurmClient slurmclient.Client, jobInfo *slurmclienttypes.V0044JobInfo, comment string) error {
	if jobInfo == nil {
		return nil
	}
	logger := log.FromContext(ctx)
	jobID := ptr.Deref(jobInfo.JobId, 0)

	if hasTerminalSlurmState(jobInfo) {
		logger.Info("Slurm job is already in a terminal state, assuming cancellation is successful", "jobID", jobID)
		return nil
	}

	// 1. Set failure comment in Slurm
	commentReq := slurmapi.V0044JobDescMsg{
		Comment: ptr.To(comment),
	}
	if err := slurmClient.Update(ctx, jobInfo, commentReq); err != nil {
		logger.Error(err, "Failed to set failure comment on Slurm job, proceeding with job cancellation", "jobID", jobID)
	}

	// 2. Cancel the job in Slurm
	if err := slurmClient.Delete(ctx, jobInfo); err != nil {
		logger.Error(err, "Failed to cancel Slurm job", "jobID", jobID)
		return fmt.Errorf("failed to cancel Slurm job: %w", err)
	}

	return nil
}

func (r *SlurmJobReconciler) releaseHold(ctx context.Context, slurmClient slurmclient.Client, jobInfo *slurmclienttypes.V0044JobInfo, obj client.Object) error {
	logger := log.FromContext(ctx)
	jobID := ptr.Deref(jobInfo.JobId, 0)

	logger.Info("Assigning extra job-specific constraint and unholding Slurm job", "jobID", jobID)
	msg := fmt.Sprintf("%s Kubernetes resource %s created successfully", UnheldCommentPrefix, obj.GetName())
	existingComment := stripAutoHoldMarker(strings.TrimSpace(ptr.Deref(jobInfo.Comment, "")))
	newComment := msg
	if strings.Contains(existingComment, msg) {
		newComment = existingComment
	} else if existingComment != "" {
		newComment = fmt.Sprintf("%s, %s", existingComment, msg)
	}

	uniqueFeature := fmt.Sprintf("%s%d", UniqueFeaturePrefix, jobID)
	existingFeatures := strings.TrimSpace(ptr.Deref(jobInfo.Features, ""))
	newFeatures := uniqueFeature
	if hasFeatureConstraint(existingFeatures, uniqueFeature) {
		newFeatures = existingFeatures
	} else if existingFeatures != "" {
		newFeatures = fmt.Sprintf("(%s)&%s", existingFeatures, uniqueFeature)
	}

	updateReq := slurmapi.V0044JobDescMsg{
		Hold:        ptr.To(false),
		Comment:     ptr.To(newComment),
		Constraints: ptr.To(newFeatures),
	}
	if err := slurmClient.Update(ctx, jobInfo, updateReq); err != nil {
		return fmt.Errorf("failed to release hold on Slurm job %d: %w", jobID, err)
	}
	return nil
}

func hasFeatureConstraint(features, targetFeature string) bool {
	if features == "" || targetFeature == "" {
		return false
	}
	pattern := fmt.Sprintf(`(^|[^a-zA-Z0-9_-])%s($|[^a-zA-Z0-9_-])`, regexp.QuoteMeta(targetFeature))
	matched, err := regexp.MatchString(pattern, features)
	return err == nil && matched
}

func stripAutoHoldMarker(comment string) string {
	if comment == "" {
		return ""
	}
	marker := AutoHoldMarker
	for _, pattern := range []string{
		", " + marker,
		"," + marker,
		marker + ", ",
		marker + ",",
		" " + marker,
		marker + " ",
		marker,
	} {
		comment = strings.ReplaceAll(comment, pattern, "")
	}
	return strings.TrimSpace(strings.Trim(comment, ","))
}

// CountActiveJobSets counts active (non-terminal, non-deleted) JobSets for a WorkloadMixing and partition,
// optionally excluding specified JobSet names to prevent race conditions with the informer cache during deletion.
func CountActiveJobSets(ctx context.Context, c client.Client, wm *k8sbridgev1alpha1.WorkloadMixing, partition string, excludedNames ...string) (int, error) {
	jsList := &jobsetv1alpha2.JobSetList{}
	if err := c.List(ctx, jsList,
		client.InNamespace(wm.Namespace),
		client.MatchingLabels{LabelSlurmPartition: partition},
	); err != nil {
		return 0, fmt.Errorf("failed to list JobSets for partition %s: %w", partition, err)
	}
	count := 0
	for i := range jsList.Items {
		js := &jsList.Items[i]
		if slices.Contains(excludedNames, js.Name) {
			continue
		}
		if !js.DeletionTimestamp.IsZero() {
			continue
		}
		if !isJobSetOwnedByWM(js, wm) {
			continue
		}

		if _, isTerminal := terminalJobSetStatus(js); !isTerminal {
			count++
		}
	}
	return count, nil
}

func (r *SlurmJobReconciler) updateActiveJobsMetric(ctx context.Context, wm *k8sbridgev1alpha1.WorkloadMixing, partition string) {
	if wm == nil || partition == "" {
		return
	}
	count, err := CountActiveJobSets(ctx, r.Client, wm, partition)
	if err != nil {
		log.FromContext(ctx).V(1).Info("Failed to count active JobSets for metric", "workloadMixing", wm.Name, "partition", partition, "error", err)
		return
	}
	metrics.SetActiveJobs(wm.Name, partition, count)
}
