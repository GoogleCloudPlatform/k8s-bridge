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
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"go.uber.org/zap/zapcore"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
)

const testJobSetUID types.UID = "jobset-uid-1"

const (
	testQuotaMessage          = "couldn't assign flavors to pod set slurmd: insufficient quota for cpu in flavor default-flavor, request > maximum capacity (4 > 0)"
	testEvictedMessage        = "Preempted to accommodate a workload (UID: 1234) due to prioritization in the ClusterQueue"
	testProvisioningMessage   = "Waiting for the provisioning request to be provisioned"
	testProvisioningCheckName = "dws-prov"
	testDeactivatedMessage    = "The workload is deactivated due to the rejected admission check dws-prov: stockout"
	testResetCheckMessage     = "Reset to Pending after eviction. Previously: Rejected"
	testLocalQueue            = "some-queue"
	testQueuedReason          = "queued in " + testLocalQueue
	testEarlier               = "2026-11-18T10:00:00Z"
	testLater                 = "2026-11-18T10:05:00Z"
)

// addTestWorkloadTypes registers the Kueue Workload kinds as unstructured types, as if the Kueue
// CRDs were installed.
func addTestWorkloadTypes(scheme *runtime.Scheme) {
	scheme.AddKnownTypeWithName(workloadListGVK.GroupVersion().WithKind("Workload"), &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(workloadListGVK, &unstructured.UnstructuredList{})
}

func newTestWorkload(name string, jobSetUID types.UID, status map[string]any) *unstructured.Unstructured {
	wl := &unstructured.Unstructured{Object: map[string]any{}}
	if status != nil {
		wl.Object["status"] = status
	}
	wl.SetGroupVersionKind(workloadListGVK.GroupVersion().WithKind("Workload"))
	wl.SetName(name)
	wl.SetNamespace(reconNamespace)
	wl.SetLabels(map[string]string{kueueJobUIDLabel: string(jobSetUID)})
	return wl
}

func workloadConditions(conds ...map[string]any) map[string]any {
	items := make([]any, 0, len(conds))
	for _, c := range conds {
		items = append(items, c)
	}
	return map[string]any{"conditions": items}
}

func workloadCondition(condType, status, message string) map[string]any {
	return map[string]any{"type": condType, "status": status, "message": message}
}

func deactivatedSpec() map[string]any {
	return map[string]any{workloadSpecActive: false}
}

func workloadConditionAt(condType, status, message, lastTransitionTime string) map[string]any {
	cond := workloadCondition(condType, status, message)
	cond["lastTransitionTime"] = lastTransitionTime
	return cond
}

func admissionCheck(name, state, message string) map[string]any {
	return map[string]any{"name": name, "state": state, "message": message}
}

func withAdmissionChecks(status map[string]any, checks ...map[string]any) map[string]any {
	items := make([]any, 0, len(checks))
	for _, c := range checks {
		items = append(items, c)
	}
	status["admissionChecks"] = items
	return status
}

func TestKueuePendingReason(t *testing.T) {
	quotaFalse := workloadCondition(workloadConditionQuotaReserved, "False", testQuotaMessage)
	quotaTrue := workloadCondition(workloadConditionQuotaReserved, "True", "Quota reserved in ClusterQueue cq")
	evicted := workloadCondition(workloadConditionEvicted, "True", testEvictedMessage)
	provisioningCheck := admissionCheck(testProvisioningCheckName, admissionCheckStatePending, testProvisioningMessage)
	emptyCheck := admissionCheck(testProvisioningCheckName, admissionCheckStatePending, "")
	resetCheck := admissionCheck(testProvisioningCheckName, admissionCheckStatePending, testResetCheckMessage)

	tests := []struct {
		name      string
		status    map[string]any
		spec      map[string]any
		queueName string
		want      string
	}{
		{name: "no status is queued in the JobSet LocalQueue", queueName: testLocalQueue, want: testQueuedReason},
		{name: "QuotaReserved=False", status: workloadConditions(quotaFalse), queueName: testLocalQueue, want: testQuotaMessage},
		{name: "QuotaReserved=True without a check has no reason", status: workloadConditions(quotaTrue), queueName: testLocalQueue, want: ""},
		{name: "Evicted=True", status: workloadConditions(evicted), queueName: testLocalQueue, want: testEvictedMessage},
		{
			name:      "Evicted=False is not a pending reason",
			status:    workloadConditions(workloadCondition(workloadConditionEvicted, "False", "not evicted")),
			queueName: testLocalQueue,
			want:      testQueuedReason,
		},
		{
			name: "newer Evicted wins over QuotaReserved",
			status: workloadConditions(
				workloadConditionAt(workloadConditionQuotaReserved, "False", testQuotaMessage, testEarlier),
				workloadConditionAt(workloadConditionEvicted, "True", testEvictedMessage, testLater)),
			want: testEvictedMessage,
		},
		{
			name: "newer QuotaReserved wins over Evicted",
			status: workloadConditions(
				workloadConditionAt(workloadConditionEvicted, "True", testEvictedMessage, testEarlier),
				workloadConditionAt(workloadConditionQuotaReserved, "False", testQuotaMessage, testLater)),
			want: testQuotaMessage,
		},
		{
			name: "QuotaReserved wins a tie",
			status: workloadConditions(
				workloadConditionAt(workloadConditionEvicted, "True", testEvictedMessage, testLater),
				workloadConditionAt(workloadConditionQuotaReserved, "False", testQuotaMessage, testLater)),
			want: testQuotaMessage,
		},
		{name: "QuotaReserved wins without transition times", status: workloadConditions(evicted, quotaFalse), want: testQuotaMessage},
		{
			name:   "Evicted with a time wins over QuotaReserved without one",
			status: workloadConditions(workloadConditionAt(workloadConditionEvicted, "True", testEvictedMessage, testEarlier), quotaFalse),
			want:   testEvictedMessage,
		},
		{
			name:   "Evicted without a message falls back to QuotaReserved",
			status: workloadConditions(workloadConditionAt(workloadConditionEvicted, "True", "", testLater), quotaFalse),
			want:   testQuotaMessage,
		},
		{
			name:   "reserved Workload shows the pending admission check message",
			status: withAdmissionChecks(workloadConditions(quotaTrue), provisioningCheck),
			want:   testProvisioningCheckName + ": " + testProvisioningMessage,
		},
		{
			name: "reserved Workload joins the pending admission check messages",
			status: withAdmissionChecks(workloadConditions(quotaTrue),
				admissionCheck("a", admissionCheckStatePending, "first"),
				admissionCheck("b", "Ready", "ready check"),
				admissionCheck("c", admissionCheckStatePending, "second")),
			want: "a: first; c: second",
		},
		{
			name:   "reserved Workload with a pending check without a message",
			status: withAdmissionChecks(workloadConditions(quotaTrue), emptyCheck),
			want:   "waiting for " + testProvisioningCheckName,
		},
		{
			name: "reserved Workload joins the pending checks without a message",
			status: withAdmissionChecks(workloadConditions(quotaTrue),
				admissionCheck("a", admissionCheckStatePending, ""),
				admissionCheck("b", "Ready", ""),
				admissionCheck("c", admissionCheckStatePending, " ")),
			want: "waiting for a, c",
		},
		{
			name:      "queued Workload ignores a pending check without a message",
			status:    withAdmissionChecks(map[string]any{}, emptyCheck),
			queueName: testLocalQueue,
			want:      testQueuedReason,
		},
		{
			name:   "unreserved Workload ignores pending check messages",
			status: withAdmissionChecks(workloadConditions(quotaFalse), provisioningCheck),
			want:   testQuotaMessage,
		},
		{
			name: "preempted Workload shows the eviction, not the reset admission check",
			status: withAdmissionChecks(workloadConditions(
				workloadConditionAt(workloadConditionQuotaReserved, "False", testQuotaMessage, testEarlier),
				workloadConditionAt(workloadConditionEvicted, "True", testEvictedMessage, testLater)),
				resetCheck),
			want: testEvictedMessage,
		},
		{
			name:   "multi-line message is put on one line",
			status: workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", "line one\n  line two ")),
			want:   "line one line two",
		},
		{name: "unexpected status shape is ignored", status: map[string]any{"conditions": "not-a-list"}, queueName: testLocalQueue, want: testQueuedReason},
		{
			name: "deactivated Workload shows Evicted instead of the reset admission check",
			status: withAdmissionChecks(workloadConditions(
				workloadConditionAt(workloadConditionQuotaReserved, "False", testQuotaMessage, testLater),
				workloadConditionAt(workloadConditionEvicted, "True", testDeactivatedMessage, testEarlier)),
				resetCheck),
			spec: deactivatedSpec(),
			want: testDeactivatedMessage,
		},
		{
			name:   "deactivated Workload without Evicted shows QuotaReserved",
			status: withAdmissionChecks(workloadConditions(quotaFalse), resetCheck),
			spec:   deactivatedSpec(),
			want:   testQuotaMessage,
		},
		{
			name:      "deactivated Workload without a message has no reason",
			status:    withAdmissionChecks(map[string]any{}, resetCheck),
			spec:      deactivatedSpec(),
			queueName: testLocalQueue,
			want:      "",
		},
		{
			name:   "admitted Workload has no reason",
			status: workloadConditions(workloadCondition(workloadConditionAdmitted, "True", "The workload is admitted"), quotaFalse),
			want:   "",
		},
		{name: "queued in the Workload queueName without a JobSet label", spec: map[string]any{"queueName": "wl-queue"}, want: "queued in wl-queue"},
		{name: "no LocalQueue has no reason", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wl := newTestWorkload("wl", testJobSetUID, tc.status)
			if tc.spec != nil {
				wl.Object["spec"] = tc.spec
			}
			if got := kueuePendingReason(wl, tc.queueName); got != tc.want {
				t.Errorf("kueuePendingReason() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDesiredPendingStatus(t *testing.T) {
	const jobSetName = "slurm-job-" + reconJobIDStr
	unheld := unheldStatus(jobSetName)
	kueueQuota := pendingStatus(kueueReasonPrefix + testQuotaMessage)
	quotaWorkload := newTestWorkload("wl", testJobSetUID,
		workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", testQuotaMessage)))
	queuedWorkload := newTestWorkload("wl", testJobSetUID, nil)

	tests := []struct {
		name       string
		comment    string
		suspended  bool
		wl         *unstructured.Unstructured
		wantStatus string
		wantOK     bool
	}{
		{name: "suspended JobSet shows the Kueue reason", comment: unheld, suspended: true, wl: quotaWorkload, wantStatus: kueueQuota, wantOK: true},
		{
			name: "suspended JobSet with a Workload without a message is queued", comment: unheld, suspended: true, wl: queuedWorkload,
			wantStatus: pendingStatus(kueueReasonPrefix + testQueuedReason), wantOK: true,
		},
		{name: "suspended JobSet without a Workload keeps the comment", comment: unheld, suspended: true},
		{name: "unsuspended JobSet restores the Unheld status", comment: "my run, " + kueueQuota, wl: quotaWorkload, wantStatus: unheld, wantOK: true},
		{name: "unsuspended JobSet without a Pending status keeps the comment", comment: "my run, " + unheld},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := newTranslatedPendingReconJob(tc.comment)
			js := newReconTestJobSet().withUID(testJobSetUID).withSuspend(tc.suspended).build()
			status, ok := desiredPendingStatus(job, js, tc.wl)
			if status != tc.wantStatus || ok != tc.wantOK {
				t.Errorf("desiredPendingStatus() = (%q, %v), want (%q, %v)", status, ok, tc.wantStatus, tc.wantOK)
			}
		})
	}
}

func newTranslatedPendingReconJob(comment string) *slurmclienttypes.V0044JobInfo {
	return newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(false).
		withStateReason("Resources").
		withTranslatedJobMarkers().
		withComment(comment).
		build()
}

func failWorkloadList(err error) interceptor.Funcs {
	return interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if u, ok := list.(*unstructured.UnstructuredList); ok && u.GroupVersionKind() == workloadListGVK {
				return err
			}
			return c.List(ctx, list, opts...)
		},
	}
}

func TestReconcile_KueuePendingReason(t *testing.T) {
	unheld := unheldStatus("slurm-job-" + reconJobIDStr)
	kueueQuota := pendingStatus(kueueReasonPrefix + testQuotaMessage)
	quotaWorkload := newTestWorkload("wl-quota", testJobSetUID,
		workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", testQuotaMessage)))

	tests := []struct {
		name        string
		comment     string
		workloads   []client.Object
		readerFuncs *interceptor.Funcs
		// wantComment is the comment written to Slurm; empty means no update.
		wantComment string
	}{
		{
			name:        "quota reason replaces the Unheld status",
			comment:     unheld,
			workloads:   []client.Object{quotaWorkload},
			wantComment: kueueQuota,
		},
		{
			name:        "user text is kept",
			comment:     "my run, " + unheld,
			workloads:   []client.Object{quotaWorkload},
			wantComment: "my run, " + kueueQuota,
		},
		{
			name:    "admission check reason",
			comment: unheld,
			workloads: []client.Object{newTestWorkload("wl", testJobSetUID, withAdmissionChecks(
				workloadConditions(workloadCondition(workloadConditionQuotaReserved, "True", "")),
				admissionCheck(testProvisioningCheckName, admissionCheckStatePending, testProvisioningMessage)))},
			wantComment: pendingStatus(kueueReasonPrefix + testProvisioningCheckName + ": " + testProvisioningMessage),
		},
		{
			name:    "eviction reason replaces an older reason",
			comment: kueueQuota,
			workloads: []client.Object{newTestWorkload("wl", testJobSetUID,
				workloadConditions(workloadCondition(workloadConditionEvicted, "True", testEvictedMessage)))},
			wantComment: pendingStatus(kueueReasonPrefix + testEvictedMessage),
		},
		{
			name:      "unchanged reason is not written again",
			comment:   kueueQuota,
			workloads: []client.Object{quotaWorkload},
		},
		{
			name:    "no Workload",
			comment: unheld,
		},
		{
			name:      "Workload of another JobSet is ignored",
			comment:   unheld,
			workloads: []client.Object{newTestWorkload("other", "other-uid", workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", testQuotaMessage)))},
		},
		{
			name:        "Workload without a message is queued in the LocalQueue",
			comment:     unheld,
			workloads:   []client.Object{newTestWorkload("wl", testJobSetUID, nil)},
			wantComment: pendingStatus(kueueReasonPrefix + testQueuedReason),
		},
		{
			name:    "Workload CRD not installed",
			comment: unheld,
			readerFuncs: new(failWorkloadList(&meta.NoKindMatchError{
				GroupKind:        workloadListGVK.GroupVersion().WithKind("Workload").GroupKind(),
				SearchedVersions: []string{workloadListGVK.Version},
			})),
		},
		{
			name:        "Workload list error",
			comment:     unheld,
			readerFuncs: new(failWorkloadList(errors.New("connection refused"))),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			js := newReconTestJobSet().withUID(testJobSetUID).withSuspend(true).build()
			objs := append([]client.Object{js}, tc.workloads...)
			env := newReconcileTestEnv(t, withPendingSlurmJobs(newTranslatedPendingReconJob(tc.comment)), withExtraObjects(objs...))
			if tc.readerFuncs != nil {
				env.reconciler.APIReader = interceptor.NewClient(env.baseClient.(client.WithWatch), *tc.readerFuncs)
			}

			if _, err := env.reconcile(); err != nil {
				t.Fatalf("Reconcile() returned unexpected error: %v", err)
			}

			if tc.wantComment == "" {
				env.verifyNoSlurmUpdates()
				return
			}
			env.verifyCommentOnlyUpdate(tc.wantComment)
		})
	}
}

func TestReconcile_KueuePendingReason_JobHeldForBadConstraints(t *testing.T) {
	// After the release slurmctld reports the job as held (Priority=0, reason BadConstraints)
	// until a node has its k8s-id feature, which only happens once Kueue admits the JobSet.
	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		withStateReason("BadConstraints").
		withTranslatedJobMarkers().
		withComment(unheldStatus("slurm-job-" + reconJobIDStr)).
		build()
	const jobSetUID = "jobset-uid-held"
	js := newReconTestJobSet().withUID(jobSetUID).withSuspend(true).build()
	wl := newTestWorkload("wl", jobSetUID,
		workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", testQuotaMessage)))
	env := newReconcileTestEnv(t, withPendingSlurmJobs(job), withExtraObjects(js, wl))

	if _, err := env.reconcile(); err != nil {
		t.Fatalf("Reconcile() returned unexpected error: %v", err)
	}
	env.verifyCommentOnlyUpdate(pendingStatus(kueueReasonPrefix + testQuotaMessage))
}

func TestReconcile_KueuePendingReason_SlurmUpdateError(t *testing.T) {
	updateErr := errors.New("slurmrestd unavailable")
	tests := []struct {
		name      string
		updateErr error
		wantErr   error
	}{
		{name: "update error is returned", updateErr: updateErr, wantErr: updateErr},
		{name: "partition configuration unavailable is ignored", updateErr: newPartConfigUnavailable422Error()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			js := newReconTestJobSet().withUID(testJobSetUID).withSuspend(true).build()
			wl := newTestWorkload("wl", testJobSetUID,
				workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", testQuotaMessage)))
			env := newReconcileTestEnv(t,
				withPendingSlurmJobs(newTranslatedPendingReconJob(unheldStatus("slurm-job-"+reconJobIDStr))),
				withExtraObjects(js, wl),
				withSlurmUpdateError(tc.updateErr, false))

			_, err := env.reconcile()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Reconcile() error = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErr != nil && !strings.Contains(err.Error(), "failed to update comment of Slurm job") {
				t.Errorf("Reconcile() error = %q, want it wrapped with the comment update context", err)
			}
		})
	}
}

func TestReconcile_JobSetDeletedWhileWaitingInKueue_CancelsJob(t *testing.T) {
	// A Pending status marks the job as translated, so a missing JobSet cancels the job instead
	// of creating a new JobSet.
	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		withStateReason("BadConstraints").
		withTranslatedJobMarkers().
		withComment(pendingStatus(kueueReasonPrefix + testQueuedReason)).
		build()
	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	if _, err := env.reconcile(); err != nil {
		t.Fatalf("Reconcile() returned unexpected error: %v", err)
	}
	env.verifySlurmCancellation(ReasonJobSetMissing)
	env.verifyJobSetNotFound()
}

func TestReconcile_PendingStatusCleared_HeldForBadConstraints(t *testing.T) {
	// A translated job is held for BadConstraints until its JobSet is admitted, so the Pending status
	// is cleared by releaseHold when the JobSet is no longer suspended.
	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		withStateReason("BadConstraints").
		withTranslatedJobMarkers().
		withComment("my run, " + pendingStatus(kueueReasonPrefix+testQuotaMessage)).
		build()
	js := newReconTestJobSet().withUID(testJobSetUID).withSuspend(false).build()
	env := newReconcileTestEnv(t, withPendingSlurmJobs(job), withExtraObjects(js))

	if _, err := env.reconcile(); err != nil {
		t.Fatalf("Reconcile() returned unexpected error: %v", err)
	}
	env.verifySlurmUnhold()
	if got, want := env.firstUpdatedComment(), "my run, "+unheldStatus("slurm-job-"+reconJobIDStr); got != want {
		t.Errorf("Slurm job comment = %q, want %q", got, want)
	}
}

func TestReconcile_ExistingJobSetOfAutoHeldJob_ReleasesHold(t *testing.T) {
	// The Create of the JobSet returned an error after the JobSet was stored, so the job is still
	// auto-held with the creation retry status while its JobSet waits in Kueue.
	job := newReconTestJob().
		withAutoHold().
		withComment("my run, " + pendingStatus(jobSetCreateRetryReason+"timeout") + commentSeparator + AutoHoldMarker).
		build()
	js := newReconTestJobSet().withUID(testJobSetUID).withSuspend(true).build()
	env := newReconcileTestEnv(t, withPendingSlurmJobs(job), withExtraObjects(js))

	if _, err := env.reconcile(); err != nil {
		t.Fatalf("Reconcile() returned unexpected error: %v", err)
	}
	env.verifySlurmUnhold()
	if got, want := env.firstUpdatedComment(), "my run, "+unheldStatus("slurm-job-"+reconJobIDStr); got != want {
		t.Errorf("Slurm job comment = %q, want %q", got, want)
	}
}

func TestReconcile_KueueWorkloadAPIUnavailable_LoggedOnce(t *testing.T) {
	const unavailableLog = "Kueue Workload API is not available, Kueue pending reasons are not reported"
	workloadGR := schema.GroupResource{Group: workloadListGVK.Group, Resource: "workloads"}
	tests := []struct {
		name string
		err  error
	}{
		{name: "CRD not installed", err: &meta.NoKindMatchError{GroupKind: workloadListGVK.GroupVersion().WithKind("Workload").GroupKind()}},
		{name: "resource not served", err: apierrors.NewNotFound(workloadGR, "")},
		{name: "Forbidden", err: apierrors.NewForbidden(workloadGR, "", errors.New("RBAC denied"))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			js := newReconTestJobSet().withUID(testJobSetUID).withSuspend(true).build()
			env := newReconcileTestEnv(t,
				withPendingSlurmJobs(newTranslatedPendingReconJob(unheldStatus("slurm-job-"+reconJobIDStr))),
				withExtraObjects(js))
			env.reconciler.APIReader = interceptor.NewClient(env.baseClient.(client.WithWatch), failWorkloadList(tc.err))
			ctx, recorded := newObservedLoggerContext(t.Context())

			for i := range 2 {
				if _, err := env.reconcile(ctx); err != nil {
					t.Fatalf("Reconcile() #%d returned unexpected error: %v", i, err)
				}
			}
			if got := recorded.FilterMessage(unavailableLog).Len(); got != 1 {
				t.Errorf("Logged %q %d times, want 1", unavailableLog, got)
			}
			env.verifyNoSlurmUpdates()
		})
	}
}

func TestReconcile_KueueWorkloadAPIUnavailable_LogLatch(t *testing.T) {
	const unavailableLog = "Kueue Workload API is not available, Kueue pending reasons are not reported"
	noMatch := &meta.NoKindMatchError{GroupKind: workloadListGVK.GroupVersion().WithKind("Workload").GroupKind()}

	tests := []struct {
		name string
		// errs are the Workload LIST errors of successive reconciles; nil means the LIST succeeds.
		errs        []error
		wantInfoLog int
	}{
		{name: "API back and gone again is logged again", errs: []error{noMatch, nil, noMatch}, wantInfoLog: 2},
		{name: "transient error is not logged at Info", errs: []error{errors.New("connection refused")}, wantInfoLog: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			js := newReconTestJobSet().withUID(testJobSetUID).withSuspend(true).build()
			env := newReconcileTestEnv(t,
				withPendingSlurmJobs(newTranslatedPendingReconJob(unheldStatus("slurm-job-"+reconJobIDStr))),
				withExtraObjects(js))
			var listErr error
			env.reconciler.APIReader = interceptor.NewClient(env.baseClient.(client.WithWatch), interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if listErr != nil {
						return listErr
					}
					return c.List(ctx, list, opts...)
				},
			})
			ctx, recorded := newObservedLoggerContext(t.Context())

			for i, err := range tc.errs {
				listErr = err
				if _, err := env.reconcile(ctx); err != nil {
					t.Fatalf("Reconcile() #%d returned unexpected error: %v", i, err)
				}
			}
			if got := recorded.FilterMessage(unavailableLog).Len(); got != tc.wantInfoLog {
				t.Errorf("Logged %q %d times, want %d", unavailableLog, got, tc.wantInfoLog)
			}
			for _, entry := range recorded.FilterLevelExact(zapcore.InfoLevel).All() {
				if strings.Contains(entry.Message, "Kueue") && entry.Message != unavailableLog {
					t.Errorf("Unexpected Info log %q", entry.Message)
				}
			}
		})
	}
}

func TestReconcile_PendingStatusCleared(t *testing.T) {
	jobSetName := "slurm-job-" + reconJobIDStr
	unheld := unheldStatus(jobSetName)
	kueueQuota := pendingStatus(kueueReasonPrefix + testQuotaMessage)

	tests := []struct {
		name        string
		state       slurmapi.V0044JobInfoJobState
		comment     string
		wantComment string
	}{
		{name: "PENDING job", state: slurmapi.V0044JobInfoJobStatePENDING, comment: kueueQuota, wantComment: unheld},
		{name: "RUNNING job", state: slurmapi.V0044JobInfoJobStateRUNNING, comment: kueueQuota, wantComment: unheld},
		{name: "user text is kept", state: slurmapi.V0044JobInfoJobStateRUNNING, comment: "my run, " + kueueQuota, wantComment: "my run, " + unheld},
		{name: "clean comment is not written", state: slurmapi.V0044JobInfoJobStateRUNNING, comment: unheld},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := newReconTestJob().
				withJobState(tc.state).
				withHold(false).
				withTranslatedJobMarkers().
				withComment(tc.comment).
				build()
			js := newReconTestJobSet().withUID(testJobSetUID).withSuspend(false).build()
			env := newReconcileTestEnv(t, withPendingSlurmJobs(job), withExtraObjects(js), withSlurmUpdateError(nil, true))

			if _, err := env.reconcile(); err != nil {
				t.Fatalf("Reconcile() returned unexpected error: %v", err)
			}
			if tc.wantComment == "" {
				env.verifyNoSlurmUpdates()
				return
			}
			if got := env.firstUpdatedComment(); got != tc.wantComment {
				t.Errorf("Slurm job comment = %q, want %q", got, tc.wantComment)
			}

			// The restored comment is stable: a second reconcile writes nothing.
			if _, err := env.reconcile(); err != nil {
				t.Fatalf("Second Reconcile() returned unexpected error: %v", err)
			}
			if updates := env.mockSlurm.GetUpdatedJobs(); len(updates) != 1 {
				t.Errorf("Expected exactly 1 Slurm job update after two reconciles, got %d", len(updates))
			}
		})
	}
}

func TestReconcile_TransientJobSetCreateError(t *testing.T) {
	createErr := errors.New("connection refused")
	retryComment := pendingStatus(jobSetCreateRetryReason+"failed to create Kubernetes resource slurm-job-"+reconJobIDStr+": connection refused") +
		commentSeparator + AutoHoldMarker

	t.Run("comment is written once and keeps the AutoHoldMarker", func(t *testing.T) {
		job := newReconTestJob().withAutoHold().withComment("my run, " + AutoHoldMarker).build()
		env := newReconcileTestEnv(t, withPendingSlurmJobs(job), withInterceptors(rejectJobSetCreate(createErr)), withSlurmUpdateError(nil, true))

		for i := range 2 {
			if _, err := env.reconcile(); !errors.Is(err, createErr) {
				t.Fatalf("Reconcile() #%d error = %v, want the JobSet create error", i, err)
			}
		}
		env.verifyCommentOnlyUpdate("my run, " + retryComment)
		env.verifyJobSetNotFound()
	})

	t.Run("multi-line error is put on one line", func(t *testing.T) {
		job := newReconTestJob().withAutoHold().build()
		env := newReconcileTestEnv(t, withPendingSlurmJobs(job), withInterceptors(rejectJobSetCreate(errors.New("line one\n  line two"))))

		if _, err := env.reconcile(); err == nil {
			t.Fatal("Reconcile() returned nil error, want the JobSet create error")
		}
		env.verifyCommentOnlyUpdate(pendingStatus(jobSetCreateRetryReason+"failed to create Kubernetes resource slurm-job-"+reconJobIDStr+": line one line two") +
			commentSeparator + AutoHoldMarker)
	})

	t.Run("comment update failure still returns the create error", func(t *testing.T) {
		job := newReconTestJob().withAutoHold().build()
		env := newReconcileTestEnv(t, withPendingSlurmJobs(job), withInterceptors(rejectJobSetCreate(createErr)),
			withSlurmUpdateError(errors.New("slurmrestd unavailable"), false))

		if _, err := env.reconcile(); !errors.Is(err, createErr) {
			t.Fatalf("Reconcile() error = %v, want the JobSet create error", err)
		}
	})

	t.Run("held job without AutoHoldMarker gets no status and is not cancelled", func(t *testing.T) {
		job := newReconTestJob().
			withJobState(slurmapi.V0044JobInfoJobStatePENDING).
			withHold(true).
			withStateReason("Resources").
			withComment("my run").
			build()
		env := newReconcileTestEnv(t, withPendingSlurmJobs(job), withInterceptors(rejectJobSetCreate(createErr)), withSlurmUpdateError(nil, true))

		for i := range 2 {
			if _, err := env.reconcile(); !errors.Is(err, createErr) {
				t.Fatalf("Reconcile() #%d error = %v, want the JobSet create error", i, err)
			}
		}
		env.verifyNoSlurmUpdates()
		env.verifyJobSetNotFound()
	})

	t.Run("successful retry replaces the status with Unheld", func(t *testing.T) {
		job := newReconTestJob().withAutoHold().withComment(retryComment).build()
		env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

		if _, err := env.reconcile(); err != nil {
			t.Fatalf("Reconcile() returned unexpected error: %v", err)
		}
		env.verifySlurmUnhold()
		if got, want := env.firstUpdatedComment(), unheldStatus("slurm-job-"+reconJobIDStr); got != want {
			t.Errorf("Slurm job comment = %q, want %q", got, want)
		}
	})
}

func TestIsJobTranslated_PendingStatus(t *testing.T) {
	pending := pendingStatus(kueueReasonPrefix + testQuotaMessage)
	tests := []struct {
		name    string
		comment string
		want    bool
	}{
		{name: "Pending status without AutoHoldMarker", comment: pending, want: true},
		{name: "Pending status with AutoHoldMarker", comment: pending + commentSeparator + AutoHoldMarker, want: false},
		{
			name:    "JobSet creation retry status with AutoHoldMarker",
			comment: "my run, " + pendingStatus(jobSetCreateRetryReason+"etcd unavailable") + commentSeparator + AutoHoldMarker,
			want:    false,
		},
		{name: "Unheld status", comment: unheldStatus("slurm-job-1"), want: true},
		{name: "user comment only", comment: "my run", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := newEmptyTestJob().withJobID(1).withComment(tc.comment).build()
			if got := isJobTranslated(job); got != tc.want {
				t.Errorf("isJobTranslated(comment=%q) = %v, want %v", tc.comment, got, tc.want)
			}
		})
	}
}

func TestPendingStatus_CapsReason(t *testing.T) {
	tests := []struct {
		name   string
		reason string
		want   string
	}{
		{name: "short reason", reason: "Kueue: queued in q", want: PendingCommentPrefix + " Kueue: queued in q"},
		{name: "reason at the cap", reason: strings.Repeat("r", maxPendingReasonLength), want: PendingCommentPrefix + " " + strings.Repeat("r", maxPendingReasonLength)},
		{
			name:   "long reason is cut",
			reason: strings.Repeat("r", 2000),
			want:   PendingCommentPrefix + " " + strings.Repeat("r", maxPendingReasonLength-len(truncatedCommentSuffix)) + truncatedCommentSuffix,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := pendingStatus(tc.reason); got != tc.want {
				t.Errorf("pendingStatus() = %q, want %q", got, tc.want)
			}
		})
	}

	t.Run("multi-byte reason is not split", func(t *testing.T) {
		got := pendingStatus(strings.Repeat("ł", 2000))
		reason := strings.TrimPrefix(got, PendingCommentPrefix+" ")
		if len(reason) > maxPendingReasonLength || !utf8.ValidString(reason) {
			t.Errorf("pendingStatus() reason has %d bytes, valid UTF-8 %v; want at most %d valid bytes", len(reason), utf8.ValidString(reason), maxPendingReasonLength)
		}
	})
}

func TestReconcile_LongKueueMessage_KeepsUserComment(t *testing.T) {
	longMessage := strings.Repeat("m", 2000)
	status := pendingStatus(kueueReasonPrefix + longMessage)
	// The longest user comment that fits next to the status.
	user := strings.Repeat("u", maxSlurmCommentLength-len(commentSeparator)-len(status))
	js := newReconTestJobSet().withUID(testJobSetUID).withSuspend(true).build()
	wl := newTestWorkload("wl", testJobSetUID,
		workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", longMessage)))
	env := newReconcileTestEnv(t,
		withPendingSlurmJobs(newTranslatedPendingReconJob(user+commentSeparator+unheldStatus("slurm-job-"+reconJobIDStr))),
		withExtraObjects(js, wl))

	if _, err := env.reconcile(); err != nil {
		t.Fatalf("Reconcile() returned unexpected error: %v", err)
	}
	if got, want := env.firstUpdatedComment(), user+commentSeparator+status; got != want {
		t.Errorf("Slurm job comment =\n%q\nwant\n%q", got, want)
	}
}

func TestReconcile_LongTransientCreateError_KeepsUserComment(t *testing.T) {
	createErr := errors.New(strings.Repeat("e", 2000))
	status := pendingStatus(jobSetCreateRetryReason + "failed to create Kubernetes resource slurm-job-" + reconJobIDStr + ": " + createErr.Error())
	markerSuffix := commentSeparator + AutoHoldMarker
	userBudget := maxSlurmCommentLength - len(markerSuffix) - len(commentSeparator) - len(status)

	tests := []struct {
		name     string
		userLen  int
		wantUser string
	}{
		{name: "user comment that fits is kept", userLen: userBudget, wantUser: strings.Repeat("u", userBudget)},
		{
			name:     "user comment of about 1000 bytes is cut only to fit",
			userLen:  1000,
			wantUser: strings.Repeat("u", userBudget-len(truncatedCommentSuffix)) + truncatedCommentSuffix,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := newReconTestJob().withAutoHold().withComment(strings.Repeat("u", tc.userLen) + markerSuffix).build()
			env := newReconcileTestEnv(t, withPendingSlurmJobs(job), withInterceptors(rejectJobSetCreate(createErr)))

			if _, err := env.reconcile(); !errors.Is(err, createErr) {
				t.Fatalf("Reconcile() error = %v, want the JobSet create error", err)
			}
			if got, want := env.firstUpdatedComment(), tc.wantUser+commentSeparator+status+markerSuffix; got != want {
				t.Errorf("Slurm job comment =\n%q\nwant\n%q", got, want)
			}
		})
	}
}

func TestListWorkloads_Index(t *testing.T) {
	older := metav1.NewTime(time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC))
	newer := metav1.NewTime(older.Add(time.Minute))
	workload := func(name string, uid types.UID, created metav1.Time, deleting bool) client.Object {
		wl := newTestWorkload(name, uid, nil)
		wl.SetCreationTimestamp(created)
		if deleting {
			wl.SetFinalizers([]string{testFinalizer})
			wl.SetDeletionTimestamp(&newer)
		}
		return wl
	}

	tests := []struct {
		name     string
		objs     []client.Object
		wantName string
	}{
		{name: "deleting Workload is skipped", objs: []client.Object{workload("deleting", testJobSetUID, newer, true), workload("live", testJobSetUID, older, false)}, wantName: "live"},
		// The fake client lists by name, so both list orders are covered.
		{name: "newest live Workload listed first wins", objs: []client.Object{workload("a-newer", testJobSetUID, newer, false), workload("b-older", testJobSetUID, older, false)}, wantName: "a-newer"},
		{name: "newest live Workload listed last wins", objs: []client.Object{workload("a-older", testJobSetUID, older, false), workload("b-newer", testJobSetUID, newer, false)}, wantName: "b-newer"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			addTestWorkloadTypes(scheme)
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objs...).Build()

			workloads, err := listWorkloads(t.Context(), c, reconNamespace)
			if err != nil {
				t.Fatalf("listWorkloads() returned unexpected error: %v", err)
			}
			if len(workloads) != 1 || workloads[testJobSetUID] == nil {
				t.Fatalf("listWorkloads() = %v, want one Workload for %s", workloads, testJobSetUID)
			}
			if got := workloads[testJobSetUID].GetName(); got != tc.wantName {
				t.Errorf("Workload for %s = %q, want %q", testJobSetUID, got, tc.wantName)
			}
		})
	}
}
