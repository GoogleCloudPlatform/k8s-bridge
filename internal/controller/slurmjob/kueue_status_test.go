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

	"go.uber.org/zap/zapcore"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
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
)

// addTestWorkloadTypes registers the Workload kinds, as if the Kueue CRDs were installed.
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
		{name: "Evicted=True without QuotaReserved", status: workloadConditions(evicted), queueName: testLocalQueue, want: testEvictedMessage},
		{name: "QuotaReserved=False wins over Evicted=True", status: workloadConditions(evicted, quotaFalse), want: testQuotaMessage},
		{name: "Evicted=False is not a pending reason", status: workloadConditions(workloadCondition(workloadConditionEvicted, "False", "not evicted")), queueName: testLocalQueue, want: testQueuedReason},
		{name: "reserved Workload shows the pending admission check message", status: withAdmissionChecks(workloadConditions(quotaTrue), provisioningCheck), want: testProvisioningCheckName + ": " + testProvisioningMessage},
		{
			name: "reserved Workload joins the pending admission check messages",
			status: withAdmissionChecks(workloadConditions(quotaTrue),
				admissionCheck("a", admissionCheckStatePending, "first"),
				admissionCheck("b", "Ready", "ready check"),
				admissionCheck("c", admissionCheckStatePending, "second")),
			want: "a: first; c: second",
		},
		{name: "reserved Workload with a pending check without a message", status: withAdmissionChecks(workloadConditions(quotaTrue), emptyCheck), want: "waiting for " + testProvisioningCheckName},
		{
			name: "reserved Workload joins the pending checks without a message",
			status: withAdmissionChecks(workloadConditions(quotaTrue),
				admissionCheck("a", admissionCheckStatePending, ""),
				admissionCheck("b", "Ready", ""),
				admissionCheck("c", admissionCheckStatePending, " ")),
			want: "waiting for a, c",
		},
		{name: "queued Workload ignores a pending check without a message", status: withAdmissionChecks(map[string]any{}, emptyCheck), queueName: testLocalQueue, want: testQueuedReason},
		{name: "unreserved Workload ignores pending check messages", status: withAdmissionChecks(workloadConditions(quotaFalse), provisioningCheck), want: testQuotaMessage},
		{
			name:   "deactivated Workload shows the deactivation message, not the reset admission check",
			status: withAdmissionChecks(workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", testDeactivatedMessage)), resetCheck),
			want:   testDeactivatedMessage,
		},
		{name: "multi-line message is put on one line", status: workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", "line one\n  line two ")), want: "line one line two"},
		{name: "unexpected status shape is ignored", status: map[string]any{"conditions": "not-a-list"}, queueName: testLocalQueue, want: testQueuedReason},
		{name: "admitted Workload has no reason", status: workloadConditions(workloadCondition(workloadConditionAdmitted, "True", "The workload is admitted"), quotaFalse), want: ""},
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
	unheld := unheldStatus("slurm-job-" + reconJobIDStr)
	kueueQuota := pendingStatus(kueueReasonPrefix + testQuotaMessage)
	quotaWorkload := newTestWorkload("wl", testJobSetUID,
		workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", testQuotaMessage)))

	tests := []struct {
		name       string
		comment    string
		suspended  bool
		wl         *unstructured.Unstructured
		wantStatus string
		wantOK     bool
	}{
		{name: "suspended JobSet shows the Kueue reason", comment: unheld, suspended: true, wl: quotaWorkload, wantStatus: kueueQuota, wantOK: true},
		{name: "suspended JobSet without a Workload keeps the comment", comment: unheld, suspended: true},
		{name: "unsuspended JobSet restores the Unheld status", comment: "my run, " + kueueQuota, wl: quotaWorkload, wantStatus: unheld, wantOK: true},
		{name: "unsuspended JobSet without a Pending status keeps the comment", comment: "my run, " + unheld},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := newTranslatedReconJob(slurmapi.V0044JobInfoJobStatePENDING, false, tc.comment)
			js := newReconTestJobSet().withUID(testJobSetUID).withSuspend(tc.suspended).build()
			status, ok := desiredPendingStatus(job, js, tc.wl)
			if status != tc.wantStatus || ok != tc.wantOK {
				t.Errorf("desiredPendingStatus() = (%q, %v), want (%q, %v)", status, ok, tc.wantStatus, tc.wantOK)
			}
		})
	}
}

// newTranslatedReconJob returns a translated job; held jobs are held for BadConstraints.
func newTranslatedReconJob(state slurmapi.V0044JobInfoJobState, held bool, comment string) *slurmclienttypes.V0044JobInfo {
	reason := "Resources"
	if held {
		reason = "BadConstraints"
	}
	return newReconTestJob().
		withJobState(state).
		withHold(held).
		withStateReason(reason).
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
	updateErr := errors.New("slurmrestd unavailable")
	longMessage := strings.Repeat("m", 2000)
	longStatus := pendingStatus(kueueReasonPrefix + longMessage)
	// longUser is the longest user comment that fits next to longStatus.
	longUser := strings.Repeat("u", maxSlurmCommentLength-len(commentSeparator)-len(longStatus))

	tests := []struct {
		name        string
		comment     string
		held        bool
		noJobSet    bool
		workloads   []client.Object
		readerFuncs *interceptor.Funcs
		updateErr   error
		wantErr     error
		wantCancel  bool
		// wantComment is the comment written to Slurm; empty means no update.
		wantComment string
	}{
		{name: "quota reason replaces the Unheld status", comment: unheld, workloads: []client.Object{quotaWorkload}, wantComment: kueueQuota},
		{name: "user text is kept", comment: "my run, " + unheld, workloads: []client.Object{quotaWorkload}, wantComment: "my run, " + kueueQuota},
		{
			name:    "long Kueue message is cut and the user text is kept",
			comment: longUser + commentSeparator + unheld,
			workloads: []client.Object{newTestWorkload("wl", testJobSetUID,
				workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", longMessage)))},
			wantComment: longUser + commentSeparator + longStatus,
		},
		{name: "job held for BadConstraints gets the reason", comment: unheld, held: true, workloads: []client.Object{quotaWorkload}, wantComment: kueueQuota},
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
		{name: "unchanged reason is not written again", comment: kueueQuota, workloads: []client.Object{quotaWorkload}},
		{name: "no Workload", comment: unheld},
		{name: "Workload of another JobSet is ignored", comment: unheld, workloads: []client.Object{newTestWorkload("other", "other-uid", workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", testQuotaMessage)))}},
		{name: "Workload without a message is queued in the LocalQueue", comment: unheld, workloads: []client.Object{newTestWorkload("wl", testJobSetUID, nil)}, wantComment: pendingStatus(kueueReasonPrefix + testQueuedReason)},
		{
			name:    "Workload CRD not installed",
			comment: unheld,
			readerFuncs: new(failWorkloadList(&meta.NoKindMatchError{
				GroupKind:        workloadListGVK.GroupVersion().WithKind("Workload").GroupKind(),
				SearchedVersions: []string{workloadListGVK.Version},
			})),
		},
		{name: "Workload list error", comment: unheld, readerFuncs: new(failWorkloadList(errors.New("connection refused")))},
		{name: "Slurm update error is returned", comment: unheld, workloads: []client.Object{quotaWorkload}, updateErr: updateErr, wantErr: updateErr},
		{name: "Slurm partition configuration unavailable is ignored", comment: unheld, workloads: []client.Object{quotaWorkload}, updateErr: newPartConfigUnavailable422Error(), wantComment: kueueQuota},
		{
			name:       "JobSet deleted while waiting in Kueue cancels the job",
			comment:    pendingStatus(kueueReasonPrefix + testQueuedReason),
			held:       true,
			noJobSet:   true,
			wantCancel: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objs := tc.workloads
			if !tc.noJobSet {
				objs = append(objs, newReconTestJobSet().withUID(testJobSetUID).withSuspend(true).build())
			}
			env := newReconcileTestEnv(t,
				withPendingSlurmJobs(newTranslatedReconJob(slurmapi.V0044JobInfoJobStatePENDING, tc.held, tc.comment)),
				withExtraObjects(objs...),
				withSlurmUpdateError(tc.updateErr, false))
			if tc.readerFuncs != nil {
				env.reconciler.APIReader = interceptor.NewClient(env.baseClient.(client.WithWatch), *tc.readerFuncs)
			}

			_, err := env.reconcile()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Reconcile() error = %v, want %v", err, tc.wantErr)
			}
			switch {
			case tc.wantErr != nil:
				if !strings.Contains(err.Error(), "failed to update comment of Slurm job") {
					t.Errorf("Reconcile() error = %q, want it wrapped with the comment update context", err)
				}
			case tc.wantCancel:
				env.verifySlurmCancellation(ReasonJobSetMissing)
				env.verifyJobSetNotFound()
			case tc.wantComment == "":
				env.verifyNoSlurmUpdates()
			default:
				env.verifyCommentOnlyUpdate(tc.wantComment)
			}
		})
	}
}

func TestReconcile_StatusRestoredToUnheld(t *testing.T) {
	unheld := unheldStatus("slurm-job-" + reconJobIDStr)
	kueueQuota := pendingStatus(kueueReasonPrefix + testQuotaMessage)
	retryComment := pendingStatus(jobSetCreateRetryReason+"timeout") + commentSeparator + AutoHoldMarker
	pending := slurmapi.V0044JobInfoJobStatePENDING
	running := slurmapi.V0044JobInfoJobStateRUNNING

	tests := []struct {
		name string
		job  *slurmclienttypes.V0044JobInfo
		// jobSetSuspended is the state of the existing JobSet; nil means there is none.
		jobSetSuspended *bool
		// wantUnhold expects releaseHold instead of a comment-only update.
		wantUnhold bool
		// wantComment is the comment written to Slurm; empty means no update.
		wantComment string
	}{
		{name: "PENDING job", job: newTranslatedReconJob(pending, false, kueueQuota), jobSetSuspended: ptr.To(false), wantComment: unheld},
		{name: "RUNNING job", job: newTranslatedReconJob(running, false, kueueQuota), jobSetSuspended: ptr.To(false), wantComment: unheld},
		{name: "user text is kept", job: newTranslatedReconJob(running, false, "my run, "+kueueQuota), jobSetSuspended: ptr.To(false), wantComment: "my run, " + unheld},
		{name: "clean comment is not written", job: newTranslatedReconJob(running, false, unheld), jobSetSuspended: ptr.To(false)},
		{
			name:            "job held for BadConstraints with an admitted JobSet",
			job:             newTranslatedReconJob(pending, true, "my run, "+kueueQuota),
			jobSetSuspended: ptr.To(false),
			wantUnhold:      true,
			wantComment:     "my run, " + unheld,
		},
		{
			name:            "auto-held job with a JobSet creation retry status and an existing JobSet",
			job:             newReconTestJob().withAutoHold().withComment("my run, " + retryComment).build(),
			jobSetSuspended: ptr.To(true),
			wantUnhold:      true,
			wantComment:     "my run, " + unheld,
		},
		{name: "auto-held job with a JobSet creation retry status whose Create succeeds", job: newReconTestJob().withAutoHold().withComment(retryComment).build(), wantUnhold: true, wantComment: unheld},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var objs []client.Object
			if tc.jobSetSuspended != nil {
				objs = append(objs, newReconTestJobSet().withUID(testJobSetUID).withSuspend(*tc.jobSetSuspended).build())
			}
			env := newReconcileTestEnv(t, withPendingSlurmJobs(tc.job), withExtraObjects(objs...), withSlurmUpdateError(nil, true))

			if _, err := env.reconcile(); err != nil {
				t.Fatalf("Reconcile() returned unexpected error: %v", err)
			}
			switch {
			case tc.wantComment == "":
				env.verifyNoSlurmUpdates()
			case tc.wantUnhold:
				env.verifySlurmUnhold()
				if got := env.firstUpdatedComment(); got != tc.wantComment {
					t.Errorf("Slurm job comment = %q, want %q", got, tc.wantComment)
				}
			default:
				env.verifyCommentOnlyUpdate(tc.wantComment)
			}

			if _, err := env.reconcile(); err != nil {
				t.Fatalf("Second Reconcile() returned unexpected error: %v", err)
			}
			if got, want := len(env.mockSlurm.GetUpdatedJobs()), min(1, len(tc.wantComment)); got != want {
				t.Errorf("Slurm job updates after a second reconcile = %d, want %d", got, want)
			}
		})
	}
}

func TestReconcile_KueueWorkloadAPIUnavailable_Logging(t *testing.T) {
	const unavailableLog = "Kueue Workload API is not available, Kueue pending reasons are not reported"
	workloadGR := schema.GroupResource{Group: workloadListGVK.Group, Resource: "workloads"}
	noMatch := &meta.NoKindMatchError{GroupKind: workloadListGVK.GroupVersion().WithKind("Workload").GroupKind()}
	notFound := apierrors.NewNotFound(workloadGR, "")
	forbidden := apierrors.NewForbidden(workloadGR, "", errors.New("RBAC denied"))

	tests := []struct {
		name string
		// errs are the Workload LIST errors of successive reconciles; nil means the LIST succeeds.
		errs        []error
		wantInfoLog int
	}{
		{name: "CRD not installed is logged once", errs: []error{noMatch, noMatch}, wantInfoLog: 1},
		{name: "resource not served is logged once", errs: []error{notFound, notFound}, wantInfoLog: 1},
		{name: "Forbidden is logged once", errs: []error{forbidden, forbidden}, wantInfoLog: 1},
		{name: "API back and gone again is logged again", errs: []error{noMatch, nil, noMatch}, wantInfoLog: 2},
		{name: "transient error is not logged at Info", errs: []error{errors.New("connection refused")}, wantInfoLog: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			js := newReconTestJobSet().withUID(testJobSetUID).withSuspend(true).build()
			env := newReconcileTestEnv(t,
				withPendingSlurmJobs(newTranslatedReconJob(slurmapi.V0044JobInfoJobStatePENDING, false, unheldStatus("slurm-job-"+reconJobIDStr))),
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
			env.verifyNoSlurmUpdates()
		})
	}
}

func TestReconcile_TransientJobSetCreateError(t *testing.T) {
	createErr := errors.New("connection refused")
	retryStatus := func(apiErr string) string {
		return pendingStatus(jobSetCreateRetryReason + "failed to create Kubernetes resource slurm-job-" + reconJobIDStr + ": " + apiErr)
	}
	marker := commentSeparator + AutoHoldMarker
	longErr := errors.New(strings.Repeat("e", 2000))
	// longUser is the longest user comment that still fits.
	longUser := strings.Repeat("u", maxSlurmCommentLength-len(marker)-len(commentSeparator)-len(retryStatus(longErr.Error())))

	tests := []struct {
		name      string
		job       *slurmclienttypes.V0044JobInfo
		createErr error
		updateErr error
		// wantComment is written once over two failed creates; empty means no update.
		wantComment string
	}{
		{
			name:        "comment is written once and keeps the AutoHoldMarker",
			job:         newReconTestJob().withAutoHold().withComment("my run, " + AutoHoldMarker).build(),
			createErr:   createErr,
			wantComment: "my run, " + retryStatus("connection refused") + marker,
		},
		{name: "multi-line error is put on one line", job: newReconTestJob().withAutoHold().build(), createErr: errors.New("line one\n  line two"), wantComment: retryStatus("line one line two") + marker},
		{
			name:        "user comment that fits next to a long error is kept",
			job:         newReconTestJob().withAutoHold().withComment(longUser + marker).build(),
			createErr:   longErr,
			wantComment: longUser + commentSeparator + retryStatus(longErr.Error()) + marker,
		},
		{
			name:        "longer user comment is cut only as much as needed",
			job:         newReconTestJob().withAutoHold().withComment(strings.Repeat("u", 1000) + marker).build(),
			createErr:   longErr,
			wantComment: longUser[len(truncatedCommentSuffix):] + truncatedCommentSuffix + commentSeparator + retryStatus(longErr.Error()) + marker,
		},
		{
			name:        "comment update failure still returns the create error",
			job:         newReconTestJob().withAutoHold().build(),
			createErr:   createErr,
			updateErr:   errors.New("slurmrestd unavailable"),
			wantComment: retryStatus("connection refused") + marker,
		},
		{
			name:      "held job without AutoHoldMarker gets no status and is not cancelled",
			job:       newReconTestJob().withJobState(slurmapi.V0044JobInfoJobStatePENDING).withHold(true).withStateReason("Resources").withComment("my run").build(),
			createErr: createErr,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// The Slurm mock applies the comment even when it returns updateErr.
			env := newReconcileTestEnv(t, withPendingSlurmJobs(tc.job), withInterceptors(rejectJobSetCreate(tc.createErr)),
				withSlurmUpdateError(tc.updateErr, true))

			for i := range 2 {
				if _, err := env.reconcile(); !errors.Is(err, tc.createErr) {
					t.Fatalf("Reconcile() #%d error = %v, want the JobSet create error", i, err)
				}
			}
			if tc.wantComment == "" {
				env.verifyNoSlurmUpdates()
			} else {
				env.verifyCommentOnlyUpdate(tc.wantComment)
			}
			env.verifyJobSetNotFound()
		})
	}
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
		{name: "JobSet creation retry status with AutoHoldMarker", comment: "my run, " + pendingStatus(jobSetCreateRetryReason+"etcd unavailable") + commentSeparator + AutoHoldMarker, want: false},
		{name: "Unheld status", comment: unheldStatus("slurm-job-1"), want: true},
		{name: "user comment only", comment: "my run", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isJobTranslated(newEmptyTestJob().withJobID(1).withComment(tc.comment).build()); got != tc.want {
				t.Errorf("isJobTranslated(comment=%q) = %v, want %v", tc.comment, got, tc.want)
			}
		})
	}
}

func TestPendingStatus_CapsReason(t *testing.T) {
	cut := maxPendingReasonLength - len(truncatedCommentSuffix)
	tests := []struct {
		name   string
		reason string
		want   string
	}{
		{name: "short reason", reason: "Kueue: queued in q", want: PendingCommentPrefix + " Kueue: queued in q"},
		{name: "reason at the cap", reason: strings.Repeat("r", maxPendingReasonLength), want: PendingCommentPrefix + " " + strings.Repeat("r", maxPendingReasonLength)},
		{name: "long reason is cut", reason: strings.Repeat("r", 2000), want: PendingCommentPrefix + " " + strings.Repeat("r", cut) + truncatedCommentSuffix},
		// "ł" has 2 bytes, so the cut backs off to a character boundary.
		{name: "multi-byte reason is not split", reason: "x" + strings.Repeat("ł", 2000), want: PendingCommentPrefix + " x" + strings.Repeat("ł", (cut-1)/2) + truncatedCommentSuffix},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := pendingStatus(tc.reason); got != tc.want {
				t.Errorf("pendingStatus() = %q, want %q", got, tc.want)
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
