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
	"unicode/utf8"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
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

	admissionRejectedCommentPrefix = "JobSet admission rejected: "
	admissionWebhookMarker         = "admission webhook"
	admissionDeniedMarker          = "denied the request"
	// maxSlurmCommentLength is the longest job comment, in bytes, that slurmctld accepts on job
	// submission and update.
	maxSlurmCommentLength  = 1024
	truncatedCommentSuffix = "..."
)

// SlurmJobReconciler reconciles Slurm jobs received from the poller.
type SlurmJobReconciler struct {
	client.Client
	APIReader             client.Reader
	Scheme                *runtime.Scheme
	Recorder              record.EventRecorder
	AdmissionEventsChan   <-chan slurmtypes.SlurmJobAdmissionEvent
	genericEvents         chan event.GenericEvent
	ClientMap             *clientmap.ClientMap
	Translator            JobTranslator
	slurmCallTimeout      time.Duration
	reconciliationTimeout time.Duration
}

func (r *SlurmJobReconciler) getSlurmCallTimeout() time.Duration {
	if r.slurmCallTimeout > 0 {
		return r.slurmCallTimeout
	}
	return defaultSlurmCallTimeout
}

func (r *SlurmJobReconciler) getReconciliationTimeout() time.Duration {
	if r.reconciliationTimeout > 0 {
		return r.reconciliationTimeout
	}
	return defaultReconciliationTimeout
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
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorderFor("slurmjob-controller") //nolint:staticcheck
	}

	if err := mgr.Add(r); err != nil {
		return fmt.Errorf("failed to add SlurmJobReconciler as Runnable: %w", err)
	}

	baseLogger := mgr.GetLogger().WithValues("controller", "slurmjob-controller")
	c, err := controller.New("slurmjob-controller", mgr, controller.Options{
		Reconciler:            r,
		ReconciliationTimeout: r.getReconciliationTimeout(),
		LogConstructor: func(req *reconcile.Request) logr.Logger {
			return requestLogger(baseLogger, req)
		},
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
			jobSetWatchPredicate(),
		),
	)
}

func jobSetWatchPredicate() predicate.TypedPredicate[*jobsetv1alpha2.JobSet] {
	return predicate.TypedFuncs[*jobsetv1alpha2.JobSet]{
		CreateFunc:  shouldReconcileJobSetCreate,
		UpdateFunc:  shouldReconcileJobSetUpdate,
		DeleteFunc:  shouldReconcileJobSetDelete,
		GenericFunc: shouldReconcileJobSetGeneric,
	}
}

func shouldReconcileJobSetCreate(e event.TypedCreateEvent[*jobsetv1alpha2.JobSet]) bool {
	js := e.Object
	if js == nil {
		return false
	}
	return isJobSetTerminal(js) ||
		isJobSetDeleting(js) ||
		!isJobSetSuspended(js)
}

func shouldReconcileJobSetUpdate(e event.TypedUpdateEvent[*jobsetv1alpha2.JobSet]) bool {
	oldJS, newJS := e.ObjectOld, e.ObjectNew
	if oldJS == nil || newJS == nil {
		return false
	}
	if !isJobSetDeleting(oldJS) && isJobSetDeleting(newJS) {
		// A deleting JobSet still carrying FinalizerJobSet needs a reconcile even if it is already terminal,
		// otherwise its deletion stays blocked.
		return !isJobSetTerminal(oldJS) || controllerutil.ContainsFinalizer(newJS, FinalizerJobSet)
	}
	if isJobSetTerminal(oldJS) {
		return false
	}
	if isJobSetTerminal(newJS) {
		return true
	}
	return isJobSetSuspended(oldJS) != isJobSetSuspended(newJS)
}

func shouldReconcileJobSetDelete(e event.TypedDeleteEvent[*jobsetv1alpha2.JobSet]) bool {
	return e.Object != nil && !isJobSetTerminal(e.Object)
}

func shouldReconcileJobSetGeneric(event.TypedGenericEvent[*jobsetv1alpha2.JobSet]) bool {
	return false
}

func isJobSetTerminal(js *jobsetv1alpha2.JobSet) bool {
	_, isTerminal := terminalJobSetStatus(js)
	return isTerminal
}

func isJobSetDeleting(js *jobsetv1alpha2.JobSet) bool {
	return js != nil && !js.DeletionTimestamp.IsZero()
}

func isJobSetSuspended(js *jobsetv1alpha2.JobSet) bool {
	return js != nil && ptr.Deref(js.Spec.Suspend, false)
}

func (r *SlurmJobReconciler) mapJobSetToRequest(ctx context.Context, js *jobsetv1alpha2.JobSet) []reconcile.Request {
	logger := log.FromContext(ctx)

	// Extract jobID from label
	jobID := js.Labels[LabelSlurmJobID]
	if jobID == "" {
		logger.V(1).Info("JobSet missing SlurmJobID label, ignoring", "jobSet", client.ObjectKeyFromObject(js))
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
		logger.V(1).Info("JobSet missing WorkloadMixing OwnerReference, ignoring",
			"jobSet", client.ObjectKeyFromObject(js), "jobID", jobID)
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
	// FinalizeDeletingJobSet is set when there is nothing left to reconcile against (the Slurm job,
	// WorkloadMixing or Slurm Controller is gone), so a deleting JobSet must be finalized now.
	// SlurmClient is nil if the dynamic nodes can no longer be cleaned up.
	FinalizeDeletingJobSet bool
}

// +kubebuilder:rbac:groups=k8s-bridge.x-k8s.io,resources=workloadmixings,verbs=get;list;watch
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets/finalizers,verbs=update

// requestLogger builds the per-request logger used by controller-runtime for
// both the Reconcile context and its "Reconciler error" log lines. It keeps the
// default controller-runtime fields and adds the Slurm jobID and WorkloadMixing
// parsed from the request name.
func requestLogger(base logr.Logger, req *reconcile.Request) logr.Logger {
	if req == nil {
		return base
	}
	logger := base.WithValues(
		"object", klog.KRef(req.Namespace, req.Name),
		"namespace", req.Namespace,
		"name", req.Name,
	)
	wmName, jobID, err := parseRequestName(req.Name)
	if err != nil {
		return logger
	}
	logger = logger.WithValues("jobID", jobID)
	if wmName != "" {
		logger = logger.WithValues("workloadMixing", types.NamespacedName{Namespace: req.Namespace, Name: wmName})
	}
	return logger
}

// Reconcile reconciles a Slurm job.
func (r *SlurmJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	wmName, jobID, err := parseRequestName(req.Name)
	if err != nil {
		logger.Error(err, "Failed to parse request name")
		return ctrl.Result{}, nil
	}

	if r.verifyWMName(ctx, wmName) {
		return ctrl.Result{}, nil
	}

	state, done, err := r.fetchState(ctx, req.Namespace, wmName, jobID)
	if err != nil {
		return state.Result, err
	}
	if done {
		if state.FinalizeDeletingJobSet {
			if err := r.releaseFinalizerForDeletingJobSet(ctx, req.Namespace, jobID, state.SlurmClient); err != nil {
				return ctrl.Result{}, err
			}
		}
		return state.Result, nil
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

func (r *SlurmJobReconciler) verifyWMName(ctx context.Context, wmName string) ReconciliationStatus {
	logger := log.FromContext(ctx)
	if wmName == "" {
		logger.Info("Skipping Slurm job reconciliation", "reason", "WorkloadMixing name is empty")
		return ReconciliationDone
	}
	logger.V(1).Info("Reconciling Slurm job")
	return ReconciliationContinue
}

func (r *SlurmJobReconciler) fetchState(ctx context.Context, namespace, wmName, jobID string) (slurmJobState, ReconciliationStatus, error) {
	logger := log.FromContext(ctx)

	wm := &k8sbridgev1alpha1.WorkloadMixing{}
	wmKey := client.ObjectKey{Namespace: namespace, Name: wmName}
	if err := r.Get(ctx, wmKey, wm); err != nil {
		if apierrors.IsNotFound(err) {
			// TODO: Without the WorkloadMixing the Slurm client cannot be resolved, so the finalizer is released
			// without dynamic node cleanup (only the slurmd PreStop hook deregisters nodes). Record the Slurm
			// Controller reference on the JobSet so cleanup does not depend on the WorkloadMixing.
			logger.V(1).Info("WorkloadMixing not found, skipping Slurm job reconciliation")
			return slurmJobState{FinalizeDeletingJobSet: true}, ReconciliationDone, nil
		}
		return slurmJobState{Result: ctrl.Result{}}, ReconciliationDone, err
	}

	if r.ClientMap == nil {
		return slurmJobState{Result: ctrl.Result{}}, ReconciliationDone, fmt.Errorf("ClientMap is not initialized")
	}
	controllerKey := types.NamespacedName{
		Namespace: wm.Namespace,
		Name:      wm.Spec.ControllerRef.Name,
	}
	slurmClient := r.ClientMap.Get(controllerKey)
	if slurmClient == nil {
		gone, err := isSlurmControllerGone(ctx, r.Client, controllerKey)
		if err != nil {
			return slurmJobState{Result: ctrl.Result{}}, ReconciliationDone, err
		}
		if gone {
			// The Slurm cluster is gone together with its dynamic nodes, so a deleting JobSet is finalized
			// without node cleanup instead of blocking its deletion forever.
			logger.Info("Slurm Controller not found, skipping reconciliation", "slurmController", controllerKey)
			return slurmJobState{FinalizeDeletingJobSet: true}, ReconciliationDone, nil
		}
		logger.Info("Slurm client not found in ClientMap, requeuing Slurm job", "slurmController", controllerKey, "requeueAfter", 5*time.Second)
		return slurmJobState{Result: ctrl.Result{RequeueAfter: 5 * time.Second}}, ReconciliationDone, nil
	}

	var jobInfo slurmclienttypes.V0044JobInfo
	callCtx, cancel := context.WithTimeout(ctx, r.getSlurmCallTimeout())
	defer cancel()
	if err := slurmClient.Get(callCtx, object.ObjectKey(jobID), &jobInfo); err != nil {
		if isSlurmNotFound(err) {
			// A job may no longer exist in Slurmctld if it reached a terminal state and was
			// purged after MinJobAge, while watch events on the associated JobSet still arrive.
			// Return nil to avoid entering an infinite exponential backoff retry loop. A deleting
			// JobSet is finalized by the caller; JobTeardownReconciler handles other orphaned JobSets.
			logger.Info("Slurm job not found in Slurm, skipping reconciliation")
			return slurmJobState{SlurmClient: slurmClient, FinalizeDeletingJobSet: true}, ReconciliationDone, nil
		}
		return slurmJobState{Result: ctrl.Result{}}, ReconciliationDone, fmt.Errorf("failed to fetch job info from Slurm: %w", err)
	}

	return slurmJobState{WM: wm, SlurmClient: slurmClient, JobInfo: &jobInfo, Result: ctrl.Result{}}, ReconciliationContinue, nil
}

// releaseFinalizerForDeletingJobSet finalizes the JobSet of the given Slurm job if it is being deleted.
// A nil slurmClient skips dynamic node cleanup.
func (r *SlurmJobReconciler) releaseFinalizerForDeletingJobSet(
	ctx context.Context,
	namespace string,
	jobID string,
	slurmClient slurmclient.Client,
) error {
	js := &jobsetv1alpha2.JobSet{}
	key := types.NamespacedName{Name: jobSetNameForJobID(jobID), Namespace: namespace}
	if err := r.Get(ctx, key, js); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !isJobSetDeleting(js) {
		return nil
	}
	return finalizeJobSet(ctx, r.Client, slurmClient, js, r.getSlurmCallTimeout())
}

type ReconciliationStatus bool

const (
	ReconciliationDone     ReconciliationStatus = true
	ReconciliationContinue ReconciliationStatus = false
)

type K8sbridgeJobStatus string

const (
	StatusNewJob          K8sbridgeJobStatus = "NewJob"
	StatusShouldUnhold    K8sbridgeJobStatus = "ShouldUnhold"
	StatusNoAction        K8sbridgeJobStatus = "NoAction"
	StatusJobSetCompleted K8sbridgeJobStatus = "JobSetCompleted"
	StatusJobSetFailed    K8sbridgeJobStatus = "JobSetFailed"
	StatusJobSetMissing   K8sbridgeJobStatus = "JobSetMissing"
	StatusJobSetDeleting  K8sbridgeJobStatus = "JobSetDeleting"
)

func activeJobStatus(jobInfo *slurmclienttypes.V0044JobInfo, existingJS *jobsetv1alpha2.JobSet) K8sbridgeJobStatus {
	if status, isTerminal := terminalJobSetStatus(existingJS); isTerminal {
		return status
	}

	if isJobSetDeleting(existingJS) {
		return StatusJobSetDeleting
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

	if isJobExplicitlyHeld(jobInfo) {
		return StatusNoAction
	}

	if ptr.Deref(jobInfo.Hold, false) && !ptr.Deref(existingJS.Spec.Suspend, false) {
		return StatusShouldUnhold
	}

	return StatusNoAction
}

// isUntranslatedNewJob check if a Slurm job seems active (PENDING + Held) but also that it is recent (untranslated)
func isUntranslatedNewJob(jobInfo *slurmclienttypes.V0044JobInfo) bool {
	if jobInfo == nil {
		return false
	}
	isPending := jobInfo.GetStateAsSet().Has(slurmapi.V0044JobInfoJobStatePENDING)
	isHeld := ptr.Deref(jobInfo.Hold, false)
	return isPending && isHeld && !isJobTranslated(jobInfo)
}

// isJobTranslated checks if a Slurm job was translated and unheld by k8s-bridge
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

// getExistingJobSet gets the JobSet from the cache, falling back to the API server
// for active translated jobs so cache lag does not look like a missing JobSet.
func (r *SlurmJobReconciler) getExistingJobSet(
	ctx context.Context,
	namespace string,
	jobSetName string,
	jobInfo *slurmclienttypes.V0044JobInfo,
) (*jobsetv1alpha2.JobSet, error) {
	key := types.NamespacedName{Name: jobSetName, Namespace: namespace}
	existingJS := &jobsetv1alpha2.JobSet{}
	err := r.Get(ctx, key, existingJS)
	if err == nil {
		return existingJS, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get JobSet: %w", err)
	}
	// Only active translated jobs expect a JobSet to exist.
	if r.APIReader == nil || jobInfo == nil || hasTerminalSlurmState(jobInfo) || isUntranslatedNewJob(jobInfo) {
		return nil, nil
	}
	if err := r.APIReader.Get(ctx, key, existingJS); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("failed to get JobSet from API server: %w", err)
		}
		return nil, nil
	}
	log.FromContext(ctx).V(1).Info("Found existing JobSet via direct API reader after cache miss", "jobSet", jobSetName)
	return existingJS, nil
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
	jobSetName := jobSetNameForJobID(jobID)

	existingJS, err := r.getExistingJobSet(ctx, wm.Namespace, jobSetName, jobInfo)
	if err != nil {
		return ReconciliationDone, ctrl.Result{}, err
	}

	status := activeJobStatus(jobInfo, existingJS)
	logger.V(1).Info("Evaluated active Slurm job status",
		"status", status,
		"partition", partition,
		"slurmState", jobInfo.GetStateAsSet().UnsortedList(),
		"hold", ptr.Deref(jobInfo.Hold, false),
		"stateReason", ptr.Deref(jobInfo.StateReason, ""),
		"jobSetExists", existingJS != nil,
		"jobSetSuspended", isJobSetSuspended(existingJS))

	switch status {
	case StatusJobSetCompleted:
		return r.handleTerminalJobSet(ctx, wm, slurmClient, jobInfo, existingJS, partition, ReasonJobSetCompleted)
	case StatusJobSetFailed:
		return r.handleTerminalJobSet(ctx, wm, slurmClient, jobInfo, existingJS, partition, ReasonJobSetFailed)

	case StatusJobSetMissing:
		return r.handleMissingJobSet(ctx, wm, slurmClient, jobInfo, jobID, partition)
	case StatusJobSetDeleting:
		return r.handleDeletingJobSet(ctx, wm, slurmClient, jobInfo, existingJS, jobID, partition)

	case StatusShouldUnhold:
		if unholdErr := r.releaseHold(ctx, slurmClient, jobInfo, existingJS); unholdErr != nil {
			return ReconciliationDone, ctrl.Result{}, unholdErr
		}
		return ReconciliationDone, ctrl.Result{}, nil

	case StatusNewJob:
		return ReconciliationContinue, ctrl.Result{}, nil

	default:
		return ReconciliationDone, ctrl.Result{}, nil
	}
}

// handleMissingJobSet cancels an active translated Slurm job whose JobSet no longer exists.
func (r *SlurmJobReconciler) handleMissingJobSet(
	ctx context.Context,
	wm *k8sbridgev1alpha1.WorkloadMixing,
	slurmClient slurmclient.Client,
	jobInfo *slurmclienttypes.V0044JobInfo,
	jobID string,
	partition string,
) (ReconciliationStatus, ctrl.Result, error) {
	log.FromContext(ctx).Info("JobSet missing for active Slurm job, cancelling job", "jobSet", jobSetNameForJobID(jobID), "partition", partition)
	if err := r.cancelJobWithComment(ctx, slurmClient, jobInfo, ReasonJobSetMissing); err != nil {
		return ReconciliationDone, ctrl.Result{}, err
	}
	if partition != "" {
		metrics.RecordJobSync(metrics.StatusError, metrics.ReasonJobSetFailed, wm.Name, partition)
		r.updateActiveJobsMetric(ctx, wm, partition)
	}
	return ReconciliationDone, ctrl.Result{}, nil
}

// handleDeletingJobSet cancels the Slurm job of a deleting JobSet if it is still active, then finalizes the JobSet.
func (r *SlurmJobReconciler) handleDeletingJobSet(
	ctx context.Context,
	wm *k8sbridgev1alpha1.WorkloadMixing,
	slurmClient slurmclient.Client,
	jobInfo *slurmclienttypes.V0044JobInfo,
	existingJS *jobsetv1alpha2.JobSet,
	jobID string,
	partition string,
) (ReconciliationStatus, ctrl.Result, error) {
	if jobInfo != nil && !hasTerminalSlurmState(jobInfo) {
		if done, res, err := r.handleMissingJobSet(ctx, wm, slurmClient, jobInfo, jobID, partition); err != nil {
			return done, res, err
		}
	}
	if err := finalizeJobSet(ctx, r.Client, slurmClient, existingJS, r.getSlurmCallTimeout()); err != nil {
		return ReconciliationDone, ctrl.Result{}, err
	}
	return ReconciliationDone, ctrl.Result{}, nil
}

func (r *SlurmJobReconciler) handleTerminalJobSet(
	ctx context.Context,
	wm *k8sbridgev1alpha1.WorkloadMixing,
	slurmClient slurmclient.Client,
	jobInfo *slurmclienttypes.V0044JobInfo,
	existingJS *jobsetv1alpha2.JobSet,
	partition string,
	reason string,
) (ReconciliationStatus, ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Handling terminal JobSet for Slurm job", "jobSet", existingJS.Name, "partition", partition, "reason", reason)
	if cancelErr := r.cancelJobWithComment(ctx, slurmClient, jobInfo, reason); cancelErr != nil {
		return ReconciliationDone, ctrl.Result{}, cancelErr
	}
	if err := finalizeJobSet(ctx, r.Client, slurmClient, existingJS, r.getSlurmCallTimeout()); err != nil {
		return ReconciliationDone, ctrl.Result{}, err
	}
	if errDelete := r.deleteJobSet(ctx, existingJS, reason); errDelete != nil {
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

// deleteJobSet deletes the JobSet unless it is already being deleted. Callers must call finalizeJobSet first:
// FinalizerJobSet must be removed before Delete, because Delete bumps the resourceVersion and the
// optimistic-lock finalizer patch would then always conflict.
func (r *SlurmJobReconciler) deleteJobSet(ctx context.Context, js *jobsetv1alpha2.JobSet, reason string) error {
	if js == nil || isJobSetDeleting(js) {
		return nil
	}
	logger := log.FromContext(ctx)
	logger.Info("Cleaning up terminal JobSet", "jobSet", js.Name, "reason", reason)
	if err := r.Delete(ctx, js); err != nil && !apierrors.IsNotFound(err) {
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
		logger.V(1).Info("Job is not an untranslated held pending job, skipping reconciliation",
			"slurmState", jobInfo.GetStateAsSet().UnsortedList(),
			"hold", ptr.Deref(jobInfo.Hold, false))
		return ctrl.Result{}, nil
	}

	done, err := r.newJobCheckMultiPartition(ctx, wm, slurmClient, jobInfo, partition)
	if done || err != nil {
		return ctrl.Result{}, err
	}

	if isJobExplicitlyHeld(jobInfo) {
		logger.V(1).Info("Job is explicitly held by user or admin, deferring reconciliation",
			"stateReason", ptr.Deref(jobInfo.StateReason, ""))
		return ctrl.Result{}, nil
	}

	logger.Info("New Slurm job appeared", "partition", partition, "jobName", ptr.Deref(jobInfo.Name, ""))

	metrics.RecordJobDiscovered(wm.Name, partition)

	obj, done, err := r.translateSlurmJobToJobset(ctx, wm, slurmClient, jobInfo, jobID, partition)
	if done || err != nil {
		return ctrl.Result{}, err
	}

	created, err := r.createK8sResource(ctx, obj)
	if err != nil {
		if isPermanentAdmissionRejection(err) {
			return ctrl.Result{}, r.handleJobSetAdmissionRejection(ctx, wm, slurmClient, jobInfo, jobID, partition, err)
		}
		return ctrl.Result{}, err
	}

	if created {
		metrics.RecordJobSync(metrics.StatusSuccess, metrics.ReasonJobSetCreated, wm.Name, partition)
		r.updateActiveJobsMetric(ctx, wm, partition)
	}

	if err := r.releaseHold(ctx, slurmClient, jobInfo, obj); err != nil {
		metrics.RecordJobSync(metrics.StatusError, metrics.ReasonSlurmUpdateError, wm.Name, partition)
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *SlurmJobReconciler) newJobCheckMultiPartition(ctx context.Context, wm *k8sbridgev1alpha1.WorkloadMixing, slurmClient slurmclient.Client, jobInfo *slurmclienttypes.V0044JobInfo, partition string) (ReconciliationStatus, error) {
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
			logger.Info("Job requests multiple partitions and at least one is mapped, cancelling job", "partitions", partition)
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
		return nil, ReconciliationDone, err
	}
	controllerKey := types.NamespacedName{
		Namespace: wm.Namespace,
		Name:      wm.Spec.ControllerRef.Name,
	}
	controllerCR := &slinkyv1beta1.Controller{}
	if err := r.Get(ctx, controllerKey, controllerCR); err != nil {
		return nil, ReconciliationDone, fmt.Errorf("failed to fetch Controller %s: %w", controllerKey, err)
	}

	start := time.Now()
	obj, err := translator.Translate(jobInfo, &wm.Spec, controllerCR)
	duration := time.Since(start)
	metrics.RecordJobTranslationDuration(wm.Name, partition, duration)

	if err != nil {
		metrics.RecordJobSync(metrics.StatusError, metrics.ReasonTranslationError, wm.Name, partition)
		logger.Error(err, "Failed to translate Slurm job, cancelling job", "partition", partition)
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
		logger.V(1).Info("Job translation skipped (unmapped partition or empty job)", "partition", partition)
		return nil, ReconciliationDone, nil
	}

	obj.SetNamespace(wm.Namespace)

	if err := controllerutil.SetControllerReference(wm, obj, r.Scheme); err != nil {
		return nil, ReconciliationDone, fmt.Errorf("failed to set controller reference on translated object %s: %w", obj.GetName(), err)
	}

	return obj, ReconciliationContinue, nil
}

func (r *SlurmJobReconciler) createK8sResource(ctx context.Context, obj client.Object) (bool, error) {
	logger := log.FromContext(ctx)

	logger.V(1).Info("Creating Kubernetes resource", "jobSet", obj.GetName(), "type", fmt.Sprintf("%T", obj))
	if err := r.Create(ctx, obj); err != nil {
		if apierrors.IsAlreadyExists(err) {
			logger.V(1).Info("Kubernetes resource already exists, skipping creation", "jobSet", obj.GetName())
			return false, nil
		}
		return false, fmt.Errorf("failed to create Kubernetes resource %s: %w", obj.GetName(), err)
	}
	labels := obj.GetLabels()
	logger.Info("Created Kubernetes JobSet for Slurm job",
		"jobSet", obj.GetName(),
		"partition", labels[LabelSlurmPartition],
		"localQueue", labels[LabelKueueQueue],
		"priorityClass", labels[LabelKueuePriorityClass])
	return true, nil
}

// isPermanentAdmissionRejection reports whether a JobSet Create error is a permanent rejection
// that retrying cannot fix: a 422 Invalid error, or a 403 returned by a validating admission
// webhook (for example Kueue's JobSet webhook rejecting invalid TAS annotations). Other 403
// errors, such as RBAC denials or terminating namespaces, and server errors stay retriable.
func isPermanentAdmissionRejection(err error) bool {
	if apierrors.IsInvalid(err) {
		return true
	}
	if apierrors.IsForbidden(err) {
		msg := admissionRejectionMessage(err)
		return strings.Contains(msg, admissionWebhookMarker) && strings.Contains(msg, admissionDeniedMarker)
	}
	return false
}

// admissionRejectionMessage returns the API status message of err, falling back to err.Error().
func admissionRejectionMessage(err error) string {
	var status apierrors.APIStatus
	if errors.As(err, &status) {
		if msg := status.Status().Message; msg != "" {
			return msg
		}
	}
	return err.Error()
}

// handleJobSetAdmissionRejection cancels a held Slurm job whose JobSet was permanently rejected
// at creation, recording the rejection reason in the Slurm job comment.
func (r *SlurmJobReconciler) handleJobSetAdmissionRejection(
	ctx context.Context,
	wm *k8sbridgev1alpha1.WorkloadMixing,
	slurmClient slurmclient.Client,
	jobInfo *slurmclienttypes.V0044JobInfo,
	jobID string,
	partition string,
	createErr error,
) error {
	logger := log.FromContext(ctx)
	msg := admissionRejectionMessage(createErr)

	metrics.RecordJobSync(metrics.StatusError, metrics.ReasonJobSetAdmissionRejected, wm.Name, partition)
	logger.Error(createErr, "JobSet creation was rejected, cancelling Slurm job", "partition", partition)
	if r.Recorder != nil {
		r.Recorder.Eventf(wm, corev1.EventTypeWarning, "JobSetAdmissionRejected", "JobSet for Slurm job %s was rejected: %s", jobID, msg)
	}

	comment := admissionRejectedCommentPrefix + msg
	if err := r.cancelJobWithComment(ctx, slurmClient, jobInfo, comment); err != nil {
		return fmt.Errorf("failed to cancel job after JobSet admission rejection: %w (rejection: %v)", err, createErr)
	}
	return nil
}

// truncateComment shortens a Slurm job comment to maxSlurmCommentLength bytes without splitting
// a UTF-8 character.
func truncateComment(comment string) string {
	if len(comment) <= maxSlurmCommentLength {
		return comment
	}
	cut := maxSlurmCommentLength - len(truncatedCommentSuffix)
	for cut > 0 && !utf8.RuneStart(comment[cut]) {
		cut--
	}
	return comment[:cut] + truncatedCommentSuffix
}

func (r *SlurmJobReconciler) cancelJobWithComment(ctx context.Context, slurmClient slurmclient.Client, jobInfo *slurmclienttypes.V0044JobInfo, comment string) error {
	if jobInfo == nil {
		return nil
	}
	logger := log.FromContext(ctx)

	if hasTerminalSlurmState(jobInfo) {
		logger.V(1).Info("Slurm job is already in a terminal state, skipping cancellation")
		return nil
	}

	// 1. Set failure comment in Slurm
	if truncated := truncateComment(comment); len(truncated) != len(comment) {
		logger.Info("Truncated Slurm job comment to fit Slurm's limit",
			"originalLength", len(comment), "maxLength", maxSlurmCommentLength)
		comment = truncated
	}
	commentReq := slurmapi.V0044JobDescMsg{
		Comment: ptr.To(comment),
	}
	updateCtx, updateCancel := context.WithTimeout(ctx, r.getSlurmCallTimeout())
	err := slurmClient.Update(updateCtx, jobInfo, commentReq)
	updateCancel()
	if err != nil {
		if isSlurmPartConfigUnavailable(err) {
			logger.V(1).Info("Slurm returned partition configuration unavailable when setting failure comment, proceeding with job cancellation", "error", err)
		} else {
			logger.Error(err, "Failed to set failure comment on Slurm job, proceeding with job cancellation")
		}
	}

	// 2. Cancel the job in Slurm
	deleteCtx, deleteCancel := context.WithTimeout(ctx, r.getSlurmCallTimeout())
	defer deleteCancel()
	if err := slurmClient.Delete(deleteCtx, jobInfo); err != nil {
		return fmt.Errorf("failed to cancel Slurm job: %w", err)
	}
	logger.Info("Cancelled Slurm job", "reason", comment)

	return nil
}

func (r *SlurmJobReconciler) releaseHold(ctx context.Context, slurmClient slurmclient.Client, jobInfo *slurmclienttypes.V0044JobInfo, obj client.Object) error {
	logger := log.FromContext(ctx)
	jobID := ptr.Deref(jobInfo.JobId, 0)

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

	logger.V(1).Info("Assigning extra job-specific constraint and unholding Slurm job",
		"jobSet", obj.GetName(), "constraint", uniqueFeature)

	updateReq := slurmapi.V0044JobDescMsg{
		Hold:        ptr.To(false),
		Comment:     ptr.To(newComment),
		Constraints: ptr.To(newFeatures),
	}
	callCtx, cancel := context.WithTimeout(ctx, r.getSlurmCallTimeout())
	defer cancel()
	if err := slurmClient.Update(callCtx, jobInfo, updateReq); err != nil {
		if isSlurmPartConfigUnavailable(err) {
			logger.V(1).Info("Slurm returned partition configuration unavailable after unholding job", "error", err)
			logger.Info("Released hold on Slurm job", "jobSet", obj.GetName(), "constraint", uniqueFeature)
			return nil
		}
		return fmt.Errorf("failed to release hold on Slurm job %d: %w", jobID, err)
	}
	logger.Info("Released hold on Slurm job", "jobSet", obj.GetName(), "constraint", uniqueFeature)
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

		if !isJobSetTerminal(js) {
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
		log.FromContext(ctx).Error(err, "Failed to count active JobSets for metric", "partition", partition)
		return
	}
	metrics.SetActiveJobs(wm.Name, partition, count)
}
