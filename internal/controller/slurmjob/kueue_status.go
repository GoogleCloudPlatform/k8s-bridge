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

const (
	workloadSpecActive             = "active"
	workloadConditionAdmitted      = "Admitted"
	workloadConditionEvicted       = "Evicted"
	workloadConditionQuotaReserved = "QuotaReserved"
	admissionCheckStatePending     = "Pending"

	kueueReasonPrefix            = "Kueue: "
	admissionChecksSeparator     = "; "
	admissionCheckNamesSeparator = ", "
)

// workloadListGVK lists Workloads as unstructured objects, to avoid a Kueue Go dependency.
// v1beta2 is the storage version.
var workloadListGVK = schema.GroupVersionKind{Group: kueueGroup, Version: "v1beta2", Kind: "WorkloadList"}

// syncPendingStatus writes desiredPendingComment if it differs from the job's comment.
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
			// Informational only; retried on the next poll.
			logWorkloadReadError(ctx, &r.workloadAPIUnavailableLogged, err)
			return nil
		}
		r.workloadAPIUnavailableLogged.Store(false)
	}
	comment, ok := desiredPendingComment(jobInfo, js, wl)
	if !ok {
		return nil
	}
	return r.writeJobComment(ctx, slurmClient, jobInfo, comment)
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

// workloadsByJobUID maps a JobSet UID to its Kueue Workload.
type workloadsByJobUID map[types.UID]*unstructured.Unstructured

// listWorkloads lists Workloads in namespace, indexed by job UID, skipping deleting ones.
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
		// Keep the newest if a job briefly has more than one Workload.
		if prev, seen := workloads[uid]; !seen || wl.GetCreationTimestamp().After(prev.GetCreationTimestamp().Time) {
			workloads[uid] = wl
		}
	}
	return workloads, nil
}

// logWorkloadReadError logs an unavailable Workload API at Info once (until a read succeeds and
// resets latch), and other errors at V(1).
func logWorkloadReadError(ctx context.Context, latch *atomic.Bool, err error) {
	logger := log.FromContext(ctx)
	switch {
	case !isWorkloadAPIUnavailable(err):
		logger.V(1).Info("Failed to read Kueue Workloads, Kueue pending reasons are not updated", "error", err)
	case latch.CompareAndSwap(false, true):
		logger.Info("Kueue Workload API is not available, Kueue pending reasons are not reported", "error", err)
	default:
		logger.V(2).Info("Kueue Workload API is not available", "error", err)
	}
}

// desiredPendingStatus returns the status a translated job should show given its JobSet and
// Workload (wl may be nil). ok is false if the status must not change.
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

// desiredPendingComment returns the job comment with desiredPendingStatus, shared by the poller
// and the reconciler.
func desiredPendingComment(job *slurmclienttypes.V0044JobInfo, js *jobsetv1alpha2.JobSet, wl *unstructured.Unstructured) (string, bool) {
	status, ok := desiredPendingStatus(job, js, wl)
	if !ok {
		return "", false
	}
	comment, _ := buildJobComment(ptr.Deref(job.Comment, ""), status, false)
	return comment, true
}

// kueuePendingReason returns why Kueue does not admit wl, or "" if there is no reason to show
// (for example the Workload is admitted). queueName is the JobSet's LocalQueue, if known.
func kueuePendingReason(wl *unstructured.Unstructured, queueName string) string {
	if hasWorkloadConditionStatus(wl, workloadConditionAdmitted, "True") {
		return ""
	}
	evicted, evictedFound := findWorkloadCondition(wl, workloadConditionEvicted, "True")
	quota, quotaFound := findWorkloadCondition(wl, workloadConditionQuotaReserved, "False")
	// Kueue resets admission checks to Pending on deactivation, hiding the reason.
	if active, found, _ := unstructured.NestedBool(wl.Object, "spec", workloadSpecActive); found && !active {
		switch {
		case evictedFound:
			return evicted.message
		case quotaFound:
			return quota.message
		}
		return ""
	}
	// Admission checks only matter once quota is reserved.
	reserved := hasWorkloadConditionStatus(wl, workloadConditionQuotaReserved, "True")
	if reserved {
		if msg := pendingAdmissionChecksMessage(wl); msg != "" {
			return msg
		}
		if names := pendingAdmissionCheckNames(wl); len(names) > 0 {
			return "waiting for " + strings.Join(names, admissionCheckNamesSeparator)
		}
	}
	switch {
	case evictedFound && quotaFound:
		// On a tie, QuotaReserved describes what blocks admission now.
		if evicted.lastTransition.After(quota.lastTransition) {
			return evicted.message
		}
		return quota.message
	case evictedFound:
		return evicted.message
	case quotaFound:
		return quota.message
	}
	if reserved {
		return ""
	}
	// Kueue sets QuotaReserved=False only on Workloads at the head of the queue.
	if queueName == "" {
		queueName, _, _ = unstructured.NestedString(wl.Object, "spec", "queueName")
	}
	if queueName == "" {
		return ""
	}
	return "queued in " + queueName
}

// isWorkloadAPIUnavailable reports whether the Workload API cannot be used at all: CRD not
// installed, resource not served (a LIST returns NotFound only then), or no RBAC access.
func isWorkloadAPIUnavailable(err error) bool {
	return meta.IsNoMatchError(err) || apierrors.IsNotFound(err) || apierrors.IsForbidden(err)
}

// pendingAdmissionChecksMessage returns the pending admission checks as "<check>: <message>".
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
		msg = singleLine(msg)
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
		if state == admissionCheckStatePending && singleLine(msg) == "" && name != "" {
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

// findWorkloadCondition returns the condition of condType with status, if it has a message.
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
		msg = singleLine(msg)
		if msg == "" {
			return pendingCondition{}, false
		}
		transition, _, _ := unstructured.NestedString(cond, "lastTransitionTime")
		ts, _ := time.Parse(time.RFC3339, transition)
		return pendingCondition{message: msg, lastTransition: ts}, true
	}
	return pendingCondition{}, false
}

// singleLine puts a message on one line for scontrol output.
func singleLine(msg string) string {
	return strings.Join(strings.Fields(msg), " ")
}
