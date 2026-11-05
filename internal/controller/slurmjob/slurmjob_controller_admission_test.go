// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package slurmjob

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus/testutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
)

const (
	testKueueWebhookDenial = `admission webhook "vjobset.kb.io" denied the request: ` +
		`spec.replicatedJobs[0].template.spec.template.metadata.annotations: Invalid value: ` +
		`must not contain more than one topology annotation`
	// testPolicyWebhookDenial is the message the API server builds for a webhook that denies a
	// request without an explicit status code, which the API server reports as 400 Bad Request.
	testPolicyWebhookDenial = `admission webhook "policy.example.com" denied the request: jobsets are not allowed`
)

var (
	testJobSetGroupKind     = schema.GroupKind{Group: jobsetv1alpha2.GroupVersion.Group, Kind: "JobSet"}
	testJobSetGroupResource = schema.GroupResource{Group: jobsetv1alpha2.GroupVersion.Group, Resource: "jobsets"}
)

func newWebhookDeniedError(message string) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusForbidden,
		Reason:  metav1.StatusReasonForbidden,
		Message: message,
	}}
}

func newBadRequestWebhookDeniedError(message string) error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusBadRequest,
		Reason:  metav1.StatusReasonBadRequest,
		Message: message,
	}}
}

func newInvalidJobSetError() error {
	return apierrors.NewInvalid(testJobSetGroupKind, "slurm-job-"+reconJobIDStr, field.ErrorList{
		field.Invalid(field.NewPath("metadata", "annotations").Key(testAnnotationSliceSize), "0", "must be greater than or equal to 1"),
	})
}

func rejectJobSetCreate(createErr error) interceptor.Funcs {
	return interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*jobsetv1alpha2.JobSet); ok {
				return createErr
			}
			return c.Create(ctx, obj, opts...)
		},
	}
}

func TestIsPermanentAdmissionRejection(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "invalid", err: newInvalidJobSetError(), want: true},
		{name: "wrapped invalid", err: fmt.Errorf("failed to create: %w", newInvalidJobSetError()), want: true},
		{name: "validating webhook denial", err: newWebhookDeniedError(testKueueWebhookDenial), want: true},
		{name: "wrapped validating webhook denial", err: fmt.Errorf("failed to create: %w", newWebhookDeniedError(testKueueWebhookDenial)), want: true},
		{name: "400 validating webhook denial", err: newBadRequestWebhookDeniedError(testPolicyWebhookDenial), want: true},
		{name: "400 without webhook denial", err: apierrors.NewBadRequest("the server could not read the request body"), want: false},
		{
			name: "rbac forbidden",
			err:  apierrors.NewForbidden(testJobSetGroupResource, "slurm-job-1", errors.New(`User "system:serviceaccount:slurm:k8s-bridge" cannot create resource "jobsets"`)),
			want: false,
		},
		{name: "internal error", err: apierrors.NewInternalError(errors.New("etcd unavailable")), want: false},
		{name: "service unavailable", err: apierrors.NewServiceUnavailable("webhook unreachable"), want: false},
		{name: "timeout", err: apierrors.NewTimeoutError("webhook timed out", 1), want: false},
		{name: "plain error", err: errors.New(`admission webhook "x" denied the request`), want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isPermanentAdmissionRejection(tc.err); got != tc.want {
				t.Errorf("isPermanentAdmissionRejection(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestTruncateComment(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		want    string
	}{
		{name: "short comment is unchanged", comment: "short", want: "short"},
		{name: "max length comment is unchanged", comment: strings.Repeat("a", maxSlurmCommentLength), want: strings.Repeat("a", maxSlurmCommentLength)},
		{
			name:    "long comment is truncated with suffix",
			comment: strings.Repeat("a", maxSlurmCommentLength+1),
			want:    strings.Repeat("a", maxSlurmCommentLength-len(truncatedCommentSuffix)) + truncatedCommentSuffix,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := truncateComment(tc.comment, maxSlurmCommentLength); got != tc.want {
				t.Errorf("truncateComment() = %q, want %q", got, tc.want)
			}
		})
	}

	t.Run("does not split multi-byte characters", func(t *testing.T) {
		got := truncateComment(strings.Repeat("ż", maxSlurmCommentLength), maxSlurmCommentLength)
		if len(got) > maxSlurmCommentLength {
			t.Errorf("truncateComment() length = %d, want <= %d", len(got), maxSlurmCommentLength)
		}
		if !utf8.ValidString(got) {
			t.Errorf("truncateComment() returned invalid UTF-8: %q", got)
		}
	})
}

func TestBuildJobComment(t *testing.T) {
	const (
		reason    = "Translation failed: bad request"
		cancelled = CancelledCommentPrefix + " " + reason
		unheld    = UnheldCommentPrefix + " Kubernetes resource slurm-job-1 created successfully"
		marker    = commentSeparator + AutoHoldMarker
	)
	tests := []struct {
		name    string
		current string
		status  string
		want    string
	}{
		{name: "empty comment", current: "", status: cancelled, want: cancelled},
		{name: "user comment is kept", current: "my run", status: cancelled, want: "my run, " + cancelled},
		{name: "marker alone is kept", current: AutoHoldMarker, status: cancelled, want: cancelled + marker},
		{name: "user comment and marker are kept", current: "my run, " + AutoHoldMarker, status: cancelled, want: "my run, " + cancelled + marker},
		{name: "unheld status is replaced", current: "my run, " + unheld, status: CancelledCommentPrefix + " " + ReasonJobSetCompleted, want: "my run, " + CancelledCommentPrefix + " " + ReasonJobSetCompleted},
		{name: "unheld status alone is replaced", current: unheld, status: cancelled, want: cancelled},
		{name: "cancelled status is replaced by a newer one", current: "my run, " + CancelledCommentPrefix + " old reason" + marker, status: cancelled, want: "my run, " + cancelled + marker},
		{name: "repeated call is a no-op", current: "my run, " + cancelled + marker, status: cancelled, want: "my run, " + cancelled + marker},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := buildJobComment(tc.current, tc.status); got != tc.want {
				t.Errorf("buildJobComment(%q, %q) = %q, want %q", tc.current, tc.status, got, tc.want)
			}
		})
	}
}

func TestBuildJobComment_FitsSlurmLimit(t *testing.T) {
	longStatus := CancelledCommentPrefix + " " + strings.Repeat("r", 2*maxSlurmCommentLength)
	longUser := strings.Repeat("u", 2*maxSlurmCommentLength)
	tests := []struct {
		name          string
		current       string
		status        string
		wantMarker    bool
		wantUserPart  string
		wantStatusLen int
	}{
		{name: "long status without user comment uses the whole limit", current: AutoHoldMarker, status: longStatus, wantMarker: true, wantStatusLen: maxSlurmCommentLength - len(commentSeparator+AutoHoldMarker)},
		{name: "long status keeps room for a short user comment", current: "my run, " + AutoHoldMarker, status: longStatus, wantMarker: true, wantUserPart: "my run, "},
		{name: "long user comment is truncated before a short status", current: longUser + ", " + AutoHoldMarker, status: CancelledCommentPrefix + " short", wantMarker: true, wantUserPart: "uuu"},
		{name: "long user comment and long status share the limit", current: longUser, status: longStatus, wantUserPart: "uuu", wantStatusLen: minStatusLength},
		{name: "multi-byte characters are not split", current: strings.Repeat("ż", maxSlurmCommentLength) + ", " + AutoHoldMarker, status: CancelledCommentPrefix + " " + strings.Repeat("ł", maxSlurmCommentLength), wantMarker: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := buildJobComment(tc.current, tc.status)
			if len(got) > maxSlurmCommentLength {
				t.Errorf("len(buildJobComment()) = %d, want <= %d", len(got), maxSlurmCommentLength)
			}
			if !utf8.ValidString(got) {
				t.Errorf("buildJobComment() returned invalid UTF-8: %q", got)
			}
			if hasMarker := strings.HasSuffix(got, commentSeparator+AutoHoldMarker); hasMarker != tc.wantMarker {
				t.Errorf("buildJobComment() ends with marker = %v, want %v (comment %q)", hasMarker, tc.wantMarker, got)
			}
			if !strings.HasPrefix(got, tc.wantUserPart) {
				t.Errorf("buildJobComment() = %q, want prefix %q", got, tc.wantUserPart)
			}
			if tc.wantStatusLen > 0 {
				status := got[strings.Index(got, CancelledCommentPrefix):]
				status = strings.TrimSuffix(status, commentSeparator+AutoHoldMarker)
				if len(status) != tc.wantStatusLen {
					t.Errorf("status length = %d, want %d (comment %q)", len(status), tc.wantStatusLen, got)
				}
			}
			if again := buildJobComment(got, tc.status); again != got {
				t.Errorf("buildJobComment() is not stable:\nfirst:  %q\nsecond: %q", got, again)
			}
		})
	}
}

func TestReconcile_JobSetAdmissionRejection(t *testing.T) {
	tests := []struct {
		name            string
		createErr       error
		wantCancelled   bool
		wantCommentPart string
	}{
		{
			name:            "422 invalid cancels Slurm job",
			createErr:       newInvalidJobSetError(),
			wantCancelled:   true,
			wantCommentPart: admissionRejectedCommentPrefix + `JobSet.jobset.x-k8s.io "slurm-job-` + reconJobIDStr + `" is invalid`,
		},
		{
			name:            "403 validating webhook denial cancels Slurm job",
			createErr:       newWebhookDeniedError(testKueueWebhookDenial),
			wantCancelled:   true,
			wantCommentPart: admissionRejectedCommentPrefix + testKueueWebhookDenial,
		},
		{
			name:            "400 validating webhook denial cancels Slurm job",
			createErr:       newBadRequestWebhookDeniedError(testPolicyWebhookDenial),
			wantCancelled:   true,
			wantCommentPart: admissionRejectedCommentPrefix + testPolicyWebhookDenial,
		},
		{
			name:      "403 rbac forbidden is retried",
			createErr: apierrors.NewForbidden(testJobSetGroupResource, "slurm-job-"+reconJobIDStr, errors.New("RBAC denied")),
		},
		{
			name:      "500 internal error is retried",
			createErr: apierrors.NewInternalError(errors.New("etcd unavailable")),
		},
		{
			name:      "503 service unavailable is retried",
			createErr: apierrors.NewServiceUnavailable("webhook unreachable"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := newReconTestJob().
				withNodes(2).
				withAutoHold().
				build()
			env := newReconcileTestEnv(t, withPendingSlurmJobs(job), withInterceptors(rejectJobSetCreate(tc.createErr)))

			rejectedMetric := metrics.JobsSync.WithLabelValues(metrics.StatusError, metrics.ReasonJobSetAdmissionRejected, reconWmName, reconPartitionName)
			initRejected := testutil.ToFloat64(rejectedMetric)

			_, err := env.reconcile()
			env.verifyJobSetNotFound()

			if !tc.wantCancelled {
				if err == nil {
					t.Fatal("Reconcile() returned nil error, want a retriable error")
				}
				env.verifyNoSlurmUpdates()
				if got := testutil.ToFloat64(rejectedMetric); got != initRejected {
					t.Errorf("JobsSync (%s) = %f, want %f", metrics.ReasonJobSetAdmissionRejected, got, initRejected)
				}
				return
			}

			if err != nil {
				t.Fatalf("Reconcile() returned unexpected error: %v", err)
			}
			env.verifySlurmCancellation(tc.wantCommentPart)
			if got := testutil.ToFloat64(rejectedMetric); got != initRejected+1 {
				t.Errorf("JobsSync (%s) = %f, want %f", metrics.ReasonJobSetAdmissionRejected, got, initRejected+1)
			}

			// A later reconcile of the now cancelled job must not cancel it again.
			cancelled := newReconTestJob().
				withNodes(2).
				withJobState(slurmapi.V0044JobInfoJobStateCANCELLED).
				build()
			env.mockSlurm.SetPendingJobs(reconPartitionName, []slurmclienttypes.V0044JobInfo{*cancelled})
			if _, err := env.reconcile(); err != nil {
				t.Fatalf("Second Reconcile() returned unexpected error: %v", err)
			}
			if deleted := env.mockSlurm.GetDeletedJobs(); len(deleted) != 1 {
				t.Errorf("Deleted Slurm jobs = %v, want exactly one cancellation", deleted)
			}
		})
	}
}

func TestReconcile_InvalidExtra_CancelsWithTranslationError(t *testing.T) {
	job := newReconTestJob().
		withNodes(2).
		withAutoHold().
		withExtra(testAnnotationRequiredTopology + "=" + testTopologyBlock + ",oops").
		build()
	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	if _, err := env.reconcile(); err != nil {
		t.Fatalf("Reconcile() returned unexpected error: %v", err)
	}
	env.verifyJobSetNotFound()
	env.verifySlurmCancellation(`Translation failed: invalid --extra: "oops" is not in key=value format`)
}

func TestReconcile_LongTranslationError_TruncatesComment(t *testing.T) {
	job := newReconTestJob().
		withNodes(2).
		withAutoHold().
		withExtra(strings.Repeat("x", maxSlurmCommentLength)).
		build()
	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	if _, err := env.reconcile(); err != nil {
		t.Fatalf("Reconcile() returned unexpected error: %v", err)
	}
	env.verifyJobSetNotFound()
	env.verifySlurmCancellation("Translation failed: invalid --extra: ")

	reqMsg, ok := env.mockSlurm.GetUpdatedJobs()[0].Req.(slurmapi.V0044JobDescMsg)
	if !ok || reqMsg.Comment == nil {
		t.Fatalf("Expected a comment update, got %+v", env.mockSlurm.GetUpdatedJobs()[0].Req)
	}
	if got := len(*reqMsg.Comment); got != maxSlurmCommentLength {
		t.Errorf("Comment length = %d, want %d", got, maxSlurmCommentLength)
	}
	if !strings.HasSuffix(*reqMsg.Comment, truncatedCommentSuffix+commentSeparator+AutoHoldMarker) {
		t.Errorf("Comment = %q, want the truncated status followed by %q", *reqMsg.Comment, AutoHoldMarker)
	}
}

// TestReconcile_FailedCancelOfAutoHeldJob_IsRetried checks that when cancelling an auto-held job
// fails after its comment was updated, the job keeps the AutoHoldMarker, so the next reconcile does
// not mistake it for a job held by the user and retries the cancellation.
func TestReconcile_FailedCancelOfAutoHeldJob_IsRetried(t *testing.T) {
	const testRetryWebhookDenial = `admission webhook "vjobset.kb.io" denied the request: invalid topology request`
	tests := []struct {
		name       string
		extra      string
		createErr  error
		wantReason string
	}{
		{
			name:       "JobSet admission rejected",
			createErr:  newWebhookDeniedError(testRetryWebhookDenial),
			wantReason: admissionRejectedCommentPrefix + testRetryWebhookDenial,
		},
		{
			name:       "translation failed",
			extra:      "oops",
			wantReason: `Translation failed: invalid --extra: "oops" is not in key=value format`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := newReconTestJob().
				withNodes(2).
				withAutoHold().
				withExtra(tc.extra).
				build()
			opts := []reconcileEnvOpt{withPendingSlurmJobs(job), withSlurmUpdateError(nil, true)}
			if tc.createErr != nil {
				opts = append(opts, withInterceptors(rejectJobSetCreate(tc.createErr)))
			}
			env := newReconcileTestEnv(t, opts...)
			env.mockSlurm.DeleteError = errors.New("slurmrestd unavailable")

			if _, err := env.reconcile(); err == nil {
				t.Fatal("First Reconcile() returned nil error, want the cancellation error")
			}
			if deleted := env.mockSlurm.GetDeletedJobs(); len(deleted) != 0 {
				t.Fatalf("Deleted Slurm jobs = %v, want none after a failed cancellation", deleted)
			}
			wantComment := CancelledCommentPrefix + " " + tc.wantReason + commentSeparator + AutoHoldMarker
			if got := env.updatedComment(0); got != wantComment {
				t.Errorf("Comment after failed cancellation = %q, want %q", got, wantComment)
			}

			env.mockSlurm.DeleteError = nil
			if _, err := env.reconcile(); err != nil {
				t.Fatalf("Second Reconcile() returned unexpected error: %v", err)
			}
			// The second reconcile retries the cancellation without rewriting the unchanged comment.
			env.verifySlurmCancellation(tc.wantReason)
			env.verifyJobSetNotFound()
		})
	}
}

func TestReconcile_ValidExtra_AnnotatesJobSetPodTemplate(t *testing.T) {
	job := newReconTestJob().
		withNodes(2).
		withAutoHold().
		withExtra(testAnnotationRequiredTopology + "=" + testTopologySubblock).
		build()
	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	if _, err := env.reconcile(); err != nil {
		t.Fatalf("Reconcile() returned unexpected error: %v", err)
	}
	js := env.verifyJobSetCreated("some-queue", reconJobIDStr, reconWmName)
	got := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Annotations[testAnnotationRequiredTopology]
	if got != testTopologySubblock {
		t.Errorf("PodTemplate annotation %s = %q, want %q", testAnnotationRequiredTopology, got, testTopologySubblock)
	}
	env.verifySlurmUnhold()
}
