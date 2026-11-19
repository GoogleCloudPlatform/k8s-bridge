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
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
)

// Kueue Workloads are read as unstructured objects, so that k8s-bridge does not depend on the Kueue
// Go module. v1beta2 is the Workload storage version; v1beta1 is deprecated.
const (
	// kueueJobUIDLabel is set by Kueue on a Workload to the UID of the job (here the JobSet) that owns it.
	kueueJobUIDLabel = "kueue.x-k8s.io/job-uid"

	workloadSpecActive             = "active"
	workloadConditionAdmitted      = "Admitted"
	workloadConditionEvicted       = "Evicted"
	workloadConditionQuotaReserved = "QuotaReserved"
	admissionCheckStatePending     = "Pending"

	kueueReasonPrefix            = "Kueue: "
	admissionChecksSeparator     = "; "
	admissionCheckNamesSeparator = ", "
)

var workloadListGVK = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "WorkloadList"}

// syncPendingStatus keeps the Pending in Kubernetes status of a translated job up to date. It reads
// the JobSet's Workload, if the JobSet is suspended, and writes desiredPendingStatus if the comment
// changes.
func (r *SlurmJobReconciler) syncPendingStatus(
	ctx context.Context,
	slurmClient slurmclient.Client,
	jobInfo *slurmclienttypes.V0044JobInfo,
	js *jobsetv1alpha2.JobSet,
) error {
	var wl *unstructured.Unstructured
	if isJobSetSuspended(js) {
		var err error
		wl, err = r.workloadForJobSet(ctx, js)
		if err != nil {
			// The reason is informational only and is read again on the next poll.
			logWorkloadReadError(ctx, &r.workloadAPIUnavailableLogged, err)
			return nil
		}
		r.workloadAPIUnavailableLogged.Store(false)
	}
	status, ok := desiredPendingStatus(jobInfo, js, wl)
	if !ok {
		return nil
	}
	return r.setJobStatus(ctx, slurmClient, jobInfo, status, isJobSetSuspended(js))
}

// workloadForJobSet returns the Kueue Workload of js, or nil if there is none.
func (r *SlurmJobReconciler) workloadForJobSet(ctx context.Context, js *jobsetv1alpha2.JobSet) (*unstructured.Unstructured, error) {
	if r.APIReader == nil || js.UID == "" {
		return nil, nil
	}
	workloads, err := listWorkloads(ctx, r.APIReader, js.Namespace, client.MatchingLabels{kueueJobUIDLabel: string(js.UID)})
	if err != nil {
		return nil, err
	}
	return workloads[js.UID], nil
}

// workloadsByJobUID maps the UID of a job (here a JobSet) to its Kueue Workload.
type workloadsByJobUID map[types.UID]*unstructured.Unstructured

// listWorkloads lists the Kueue Workloads in namespace with an uncached reader, indexed by the
// kueue.x-k8s.io/job-uid label. Workloads being deleted are skipped.
func listWorkloads(ctx context.Context, reader client.Reader, namespace string, opts ...client.ListOption) (workloadsByJobUID, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(workloadListGVK)
	if err := reader.List(ctx, list, append([]client.ListOption{client.InNamespace(namespace)}, opts...)...); err != nil {
		return nil, fmt.Errorf("failed to list Kueue Workloads in namespace %s: %w", namespace, err)
	}
	workloads := make(workloadsByJobUID, len(list.Items))
	for i := range list.Items {
		wl := &list.Items[i]
		uid := types.UID(wl.GetLabels()[kueueJobUIDLabel])
		if uid == "" || wl.GetDeletionTimestamp() != nil {
			continue
		}
		// A job normally has one Workload; if it briefly has more, the newest one is current.
		if prev, seen := workloads[uid]; !seen || wl.GetCreationTimestamp().After(prev.GetCreationTimestamp().Time) {
			workloads[uid] = wl
		}
	}
	return workloads, nil
}

// logWorkloadReadError logs a failure to read Workloads. An unavailable Workload API is logged at
// Info once, until a read succeeds and resets latch; other errors usually affect all waiting jobs
// at once and are only logged at V(1).
func logWorkloadReadError(ctx context.Context, latch *atomic.Bool, err error) {
	logger := log.FromContext(ctx)
	switch {
	case !isWorkloadAPIUnavailable(err):
		logger.V(1).Info("Failed to read Kueue Workloads, Kueue pending reasons are not updated", "error", err.Error())
	case latch.CompareAndSwap(false, true):
		logger.Info("Kueue Workload API is not available, Kueue pending reasons are not reported", "error", err.Error())
	default:
		logger.V(2).Info("Kueue Workload API is not available", "error", err.Error())
	}
}

// desiredPendingStatus returns the k8s-bridge status that a translated job with JobSet js should
// show, given the JobSet's Workload wl (nil if there is none or it could not be read). ok is false
// if the status must not change:
//   - a JobSet that is no longer suspended restores the Unheld status, if the comment still has the
//     Pending in Kubernetes status;
//   - a suspended JobSet shows the reason Kueue does not admit wl, see kueuePendingReason. If there
//     is no reason (for example the Workload is admitted, or has QuotaReserved=True without a
//     message), the previous status is kept.
func desiredPendingStatus(job *slurmclienttypes.V0044JobInfo, js *jobsetv1alpha2.JobSet, wl *unstructured.Unstructured) (status string, ok bool) {
	if js == nil || job == nil {
		return "", false
	}
	if !isJobSetSuspended(js) {
		if !hasPendingStatus(ptr.Deref(job.Comment, "")) {
			return "", false
		}
		return unheldStatus(js.Name), true
	}
	if wl == nil {
		return "", false
	}
	reason := kueuePendingReason(wl, js.Labels[LabelKueueQueue])
	if reason == "" {
		return "", false
	}
	return pendingStatus(kueueReasonPrefix + reason), true
}

// desiredPendingComment returns the comment that job should have according to desiredPendingStatus.
// The AutoHoldMarker is kept while the JobSet waits and dropped with the Unheld status, as in
// releaseHold.
func desiredPendingComment(job *slurmclienttypes.V0044JobInfo, js *jobsetv1alpha2.JobSet, wl *unstructured.Unstructured) (string, bool) {
	status, ok := desiredPendingStatus(job, js, wl)
	if !ok {
		return "", false
	}
	comment, _ := buildJobComment(ptr.Deref(job.Comment, ""), status, isJobSetSuspended(js))
	return comment, true
}

// kueuePendingReason returns why Kueue does not admit wl, or "" if it cannot tell:
//  1. a deactivated Workload (spec.active=false, for example after a rejected admission check)
//     shows its Evicted=True message, else its QuotaReserved=False message. Its admission checks
//     are ignored, since Kueue resets them to Pending with a message that hides the reason;
//  2. the messages of the pending admission checks (for example a provisioning request);
//  3. the message of the most recent of the Evicted=True (for example a preemption) and
//     QuotaReserved=False (for example missing quota or topology) conditions. On a tie
//     QuotaReserved is used, since it describes what currently blocks the admission;
//  4. the names of the pending admission checks without a message;
//  5. a Workload that waits for quota without a message (Kueue only sets QuotaReserved=False on
//     the Workloads at the head of the queue) is reported as queued in its LocalQueue, which is
//     queueName or else spec.queueName.
//
// An admitted Workload, whose JobSet is about to be resumed, and a Workload with QuotaReserved=True
// and no message have no reason ("" is returned), so the job keeps its previous status.
func kueuePendingReason(wl *unstructured.Unstructured, queueName string) string {
	if hasWorkloadConditionStatus(wl, workloadConditionAdmitted, "True") {
		return ""
	}
	evicted, evictedFound := findWorkloadCondition(wl, workloadConditionEvicted, "True")
	quota, quotaFound := findWorkloadCondition(wl, workloadConditionQuotaReserved, "False")
	if active, found, _ := unstructured.NestedBool(wl.Object, "spec", workloadSpecActive); found && !active {
		switch {
		case evictedFound:
			return evicted.message
		case quotaFound:
			return quota.message
		}
		return ""
	}
	if msg := pendingAdmissionChecksMessage(wl); msg != "" {
		return msg
	}
	switch {
	case evictedFound && quotaFound:
		if evicted.lastTransition.After(quota.lastTransition) {
			return evicted.message
		}
		return quota.message
	case evictedFound:
		return evicted.message
	case quotaFound:
		return quota.message
	}
	if names := pendingAdmissionCheckNames(wl); len(names) > 0 {
		return "waiting for " + strings.Join(names, admissionCheckNamesSeparator)
	}
	if hasWorkloadConditionStatus(wl, workloadConditionQuotaReserved, "True") {
		return ""
	}
	if queueName == "" {
		queueName, _, _ = unstructured.NestedString(wl.Object, "spec", "queueName")
	}
	if queueName == "" {
		return ""
	}
	return "queued in " + queueName
}

// isWorkloadAPIUnavailable reports whether listing Workloads failed because k8s-bridge cannot use the
// Workload API at all, as opposed to a transient failure.
func isWorkloadAPIUnavailable(err error) bool {
	// NoMatch: the Kueue CRDs are not installed. NotFound: a LIST never returns NotFound for an
	// existing resource (an empty result is a success), so it means the API server does not serve
	// the resource, for example when the CRD was removed after discovery. Forbidden: the
	// ClusterRole does not grant access to Workloads, for example with an older chart.
	return meta.IsNoMatchError(err) || apierrors.IsNotFound(err) || apierrors.IsForbidden(err)
}

func pendingAdmissionChecksMessage(wl *unstructured.Unstructured) string {
	checks, _, _ := unstructured.NestedSlice(wl.Object, "status", "admissionChecks")
	var msgs []string
	for _, item := range checks {
		check, ok := item.(map[string]any)
		if !ok {
			continue
		}
		state, _, _ := unstructured.NestedString(check, "state")
		msg, _, _ := unstructured.NestedString(check, "message")
		msg = normalizeKueueMessage(msg)
		if state != admissionCheckStatePending || msg == "" {
			continue
		}
		name, _, _ := unstructured.NestedString(check, "name")
		msgs = append(msgs, name+": "+msg)
	}
	return strings.Join(msgs, admissionChecksSeparator)
}

// pendingAdmissionCheckNames returns the names of the pending admission checks without a message.
func pendingAdmissionCheckNames(wl *unstructured.Unstructured) []string {
	checks, _, _ := unstructured.NestedSlice(wl.Object, "status", "admissionChecks")
	var names []string
	for _, item := range checks {
		check, ok := item.(map[string]any)
		if !ok {
			continue
		}
		state, _, _ := unstructured.NestedString(check, "state")
		msg, _, _ := unstructured.NestedString(check, "message")
		name, _, _ := unstructured.NestedString(check, "name")
		if state == admissionCheckStatePending && normalizeKueueMessage(msg) == "" && name != "" {
			names = append(names, name)
		}
	}
	return names
}

// hasWorkloadConditionStatus reports whether wl has a condition of condType with status.
func hasWorkloadConditionStatus(wl *unstructured.Unstructured, condType, status string) bool {
	conditions, _, _ := unstructured.NestedSlice(wl.Object, "status", "conditions")
	for _, item := range conditions {
		cond, ok := item.(map[string]any)
		if !ok {
			continue
		}
		gotType, _, _ := unstructured.NestedString(cond, "type")
		gotStatus, _, _ := unstructured.NestedString(cond, "status")
		if gotType == condType && gotStatus == status {
			return true
		}
	}
	return false
}

// pendingCondition is the part of a Workload condition used to pick the pending reason.
type pendingCondition struct {
	message        string
	lastTransition time.Time
}

// findWorkloadCondition returns the condition of condType with status, if it has a message. A
// missing or invalid lastTransitionTime is read as the zero time.
func findWorkloadCondition(wl *unstructured.Unstructured, condType, status string) (pendingCondition, bool) {
	conditions, _, _ := unstructured.NestedSlice(wl.Object, "status", "conditions")
	for _, item := range conditions {
		cond, ok := item.(map[string]any)
		if !ok {
			continue
		}
		gotType, _, _ := unstructured.NestedString(cond, "type")
		gotStatus, _, _ := unstructured.NestedString(cond, "status")
		if gotType != condType || gotStatus != status {
			continue
		}
		msg, _, _ := unstructured.NestedString(cond, "message")
		msg = normalizeKueueMessage(msg)
		if msg == "" {
			return pendingCondition{}, false
		}
		transition, _, _ := unstructured.NestedString(cond, "lastTransitionTime")
		ts, _ := time.Parse(time.RFC3339, transition)
		return pendingCondition{message: msg, lastTransition: ts}, true
	}
	return pendingCondition{}, false
}

// normalizeKueueMessage puts a Kueue message on a single line, so it reads well in scontrol output.
func normalizeKueueMessage(msg string) string {
	return strings.Join(strings.Fields(msg), " ")
}
