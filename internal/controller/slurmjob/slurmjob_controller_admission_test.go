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
		maxLen  int
		want    string
	}{
		{name: "short comment is unchanged", comment: "short", maxLen: maxSlurmCommentLength, want: "short"},
		{name: "max length comment is unchanged", comment: strings.Repeat("a", maxSlurmCommentLength), maxLen: maxSlurmCommentLength, want: strings.Repeat("a", maxSlurmCommentLength)},
		{
			name:    "long comment is truncated with suffix",
			comment: strings.Repeat("a", maxSlurmCommentLength+1),
			maxLen:  maxSlurmCommentLength,
			want:    strings.Repeat("a", maxSlurmCommentLength-len(truncatedCommentSuffix)) + truncatedCommentSuffix,
		},
		{name: "small limit keeps the suffix", comment: "abcdefgh", maxLen: 5, want: "ab..."},
		{name: "limit equal to the suffix length", comment: "abcdef", maxLen: 3, want: "..."},
		{name: "limit below the suffix length cuts without the suffix", comment: "abcdef", maxLen: 2, want: "ab"},
		{name: "zero limit", comment: "abc", maxLen: 0, want: ""},
		{name: "negative limit", comment: "abc", maxLen: -1, want: ""},
		{name: "multi-byte character is not split without the suffix", comment: "żż", maxLen: 2, want: "ż"},
		{name: "multi-byte character that does not fit is dropped", comment: "ż", maxLen: 1, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := truncateComment(tc.comment, tc.maxLen); got != tc.want {
				t.Errorf("truncateComment(%q, %d) = %q, want %q", tc.comment, tc.maxLen, got, tc.want)
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
	const marker = commentSeparator + AutoHoldMarker
	cancelled := cancelledStatus("Translation failed: bad request")
	unheld := UnheldCommentPrefix + " Kubernetes resource slurm-job-1 created successfully"
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
		{name: "marker before the user comment is moved to the end", current: AutoHoldMarker + ", my run", status: cancelled, want: "my run, " + cancelled + marker},
		{name: "unheld status is replaced", current: "my run, " + unheld, status: cancelledStatus(ReasonJobSetCompleted), want: "my run, " + cancelledStatus(ReasonJobSetCompleted)},
		{name: "unheld status alone is replaced", current: unheld, status: cancelled, want: cancelled},
		{name: "cancelled status is replaced by a newer one", current: "my run, " + cancelledStatus("old reason") + marker, status: cancelled, want: "my run, " + cancelled + marker},
		{name: "repeated call is a no-op", current: "my run, " + cancelled + marker, status: cancelled, want: "my run, " + cancelled + marker},
		{name: "text after a k8s-bridge status is dropped", current: "note " + UnheldCommentPrefix + " x, more", status: cancelled, want: "note, " + cancelled},
		{name: "commas and whitespace around the user comment are trimmed", current: "  my run ,, ", status: cancelled, want: "my run, " + cancelled},
		{name: "bare reason from older k8s-bridge versions is kept as user text", current: "JobSet completed", status: cancelled, want: "JobSet completed, " + cancelled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, truncated := buildJobComment(tc.current, tc.status)
			if got != tc.want {
				t.Errorf("buildJobComment(%q, %q) = %q, want %q", tc.current, tc.status, got, tc.want)
			}
			if truncated {
				t.Errorf("buildJobComment(%q, %q) reported truncation, want none", tc.current, tc.status)
			}
		})
	}
}

func TestBuildJobComment_FitsSlurmLimit(t *testing.T) {
	const marker = commentSeparator + AutoHoldMarker
	longStatus := cancelledStatus(strings.Repeat("r", 2*maxSlurmCommentLength))
	longUser := strings.Repeat("u", 2*maxSlurmCommentLength)
	shortStatus := cancelledStatus("short")
	// mediumStatus is 525 bytes: longer than minStatusBudget, short enough to be cut by only a few bytes.
	mediumStatus := cancelledStatus(strings.Repeat("r", 500))
	// multiByteStatus has its multi-byte characters at even offsets from 26, so a cut at an odd
	// offset has to back off.
	multiByteStatus := cancelledStatus("x" + strings.Repeat("ł", maxSlurmCommentLength))
	tests := []struct {
		name    string
		current string
		status  string
		want    string
	}{
		{
			name:    "long status without user comment uses the whole limit",
			current: AutoHoldMarker,
			status:  longStatus,
			want:    longStatus[:maxSlurmCommentLength-len(marker)-3] + "..." + marker,
		},
		{
			name:    "long status leaves room for a short user comment",
			current: "my run, " + AutoHoldMarker,
			status:  longStatus,
			want:    "my run, " + longStatus[:991] + "..." + marker,
		},
		{
			name:    "long user comment is truncated before a short status",
			current: longUser + ", " + AutoHoldMarker,
			status:  shortStatus,
			want:    strings.Repeat("u", 1000-len(shortStatus)-3) + "..., " + shortStatus + marker,
		},
		{
			name:    "long user comment and long status share the limit",
			current: longUser,
			status:  longStatus,
			want:    strings.Repeat("u", 507) + "..., " + longStatus[:509] + "...",
		},
		{
			name:    "bytes freed by a multi-byte cut of the user comment go to the status",
			current: strings.Repeat("ż", 512),
			status:  mediumStatus,
			want:    strings.Repeat("ż", 253) + "..., " + mediumStatus[:510] + "...",
		},
		{
			name:    "multi-byte character in the status is not split",
			current: longUser,
			status:  multiByteStatus,
			want:    strings.Repeat("u", 508) + "..., " + multiByteStatus[:508] + "...",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, truncated := buildJobComment(tc.current, tc.status)
			if got != tc.want {
				t.Errorf("buildJobComment() =\n%q\nwant\n%q", got, tc.want)
			}
			if !truncated {
				t.Error("buildJobComment() reported no truncation, want truncation")
			}
			if again, _ := buildJobComment(got, tc.status); again != got {
				t.Errorf("buildJobComment() is not stable:\nfirst:  %q\nsecond: %q", got, again)
			}
		})
	}
}

// TestBuildJobComment_StableWithinLimit checks the invariants of buildJobComment over many
// combinations of user comment and status lengths and character widths.
func TestBuildJobComment_StableWithinLimit(t *testing.T) {
	for _, userChar := range []string{"u", "ż", "€"} {
		for userLen := 0; userLen <= 700; userLen += 13 {
			for _, statusChar := range []string{"r", "ł"} {
				for _, reasonLen := range []int{10, 300, 487, 488, 489, 700, 1100} {
					for _, withMarker := range []bool{false, true} {
						user := strings.Repeat(userChar, userLen)
						current := user
						if withMarker {
							current = strings.TrimPrefix(user+commentSeparator+AutoHoldMarker, commentSeparator)
						}
						status := cancelledStatus(strings.Repeat(statusChar, reasonLen))

						got, _ := buildJobComment(current, status)
						if len(got) > maxSlurmCommentLength || !utf8.ValidString(got) {
							t.Fatalf("buildJobComment(%d x %q, %d x %q, marker=%v): length %d, valid UTF-8 %v",
								userLen, userChar, reasonLen, statusChar, withMarker, len(got), utf8.ValidString(got))
						}
						if hasMarker := strings.HasSuffix(got, commentSeparator+AutoHoldMarker); hasMarker != withMarker {
							t.Fatalf("buildJobComment(%d x %q, %d x %q, marker=%v) ends with marker = %v",
								userLen, userChar, reasonLen, statusChar, withMarker, hasMarker)
						}
						if again, _ := buildJobComment(got, status); again != got {
							t.Fatalf("buildJobComment(%d x %q, %d x %q, marker=%v) is not stable:\nfirst:  %q\nsecond: %q",
								userLen, userChar, reasonLen, statusChar, withMarker, got, again)
						}
					}
				}
			}
		}
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

	comment := env.firstUpdatedComment()
	if got := len(comment); got != maxSlurmCommentLength {
		t.Errorf("Comment length = %d, want %d", got, maxSlurmCommentLength)
	}
	if !strings.HasSuffix(comment, truncatedCommentSuffix+commentSeparator+AutoHoldMarker) {
		t.Errorf("Comment = %q, want the truncated status followed by %q", comment, AutoHoldMarker)
	}
}

// TestReconcile_FailedCancelOfAutoHeldJob_IsRetried checks that when cancelling an auto-held job
// fails, the job keeps the AutoHoldMarker, so the next reconcile does not mistake it for a job held
// by the user and retries the cancellation.
func TestReconcile_FailedCancelOfAutoHeldJob_IsRetried(t *testing.T) {
	const testRetryWebhookDenial = `admission webhook "vjobset.kb.io" denied the request: invalid topology request`
	const invalidExtraReason = `Translation failed: invalid --extra: "oops" is not in key=value format`
	tests := []struct {
		name  string
		extra string
		// opts are applied after the defaults: comment updates succeed and job cancellations fail.
		opts        []reconcileEnvOpt
		wantReason  string
		wantUpdates int
	}{
		{
			name:        "JobSet admission rejected",
			opts:        []reconcileEnvOpt{withInterceptors(rejectJobSetCreate(newWebhookDeniedError(testRetryWebhookDenial)))},
			wantReason:  admissionRejectedCommentPrefix + testRetryWebhookDenial,
			wantUpdates: 1,
		},
		{
			name:        "translation failed",
			extra:       "oops",
			wantReason:  invalidExtraReason,
			wantUpdates: 1,
		},
		{
			// The comment update fails without being applied, so the retry sends it again.
			name:        "translation failed and the comment update failed too",
			extra:       "oops",
			opts:        []reconcileEnvOpt{withSlurmUpdateError(errors.New("connection reset by peer"), false)},
			wantReason:  invalidExtraReason,
			wantUpdates: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := newReconTestJob().
				withNodes(2).
				withAutoHold().
				withExtra(tc.extra).
				build()
			opts := append([]reconcileEnvOpt{
				withPendingSlurmJobs(job),
				withSlurmUpdateError(nil, true),
				withSlurmDeleteError(errors.New("slurmrestd unavailable")),
			}, tc.opts...)
			env := newReconcileTestEnv(t, opts...)
			wantComment := cancelledStatus(tc.wantReason) + commentSeparator + AutoHoldMarker

			if _, err := env.reconcile(); err == nil {
				t.Fatal("First Reconcile() returned nil error, want the cancellation error")
			}
			if deleted := env.mockSlurm.GetDeletedJobs(); len(deleted) != 0 {
				t.Fatalf("Deleted Slurm jobs = %v, want none after a failed cancellation", deleted)
			}

			// Slurm recovers.
			env.mockSlurm.UpdateError = nil
			env.mockSlurm.MutateOnUpdate = true
			env.mockSlurm.DeleteJobError = nil
			if _, err := env.reconcile(); err != nil {
				t.Fatalf("Second Reconcile() returned unexpected error: %v", err)
			}

			updates := env.mockSlurm.GetUpdatedJobs()
			if len(updates) != tc.wantUpdates {
				t.Errorf("Slurm job updates = %d, want %d", len(updates), tc.wantUpdates)
			}
			for i, update := range updates {
				reqMsg, ok := update.Req.(slurmapi.V0044JobDescMsg)
				if !ok || reqMsg.Comment == nil || *reqMsg.Comment != wantComment {
					t.Errorf("Slurm job update %d = %+v, want comment %q", i, update.Req, wantComment)
				}
			}
			if deleted := env.mockSlurm.GetDeletedJobs(); len(deleted) != 1 || deleted[0] != reconJobID {
				t.Errorf("Deleted Slurm jobs = %v, want [%d]", deleted, reconJobID)
			}
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
