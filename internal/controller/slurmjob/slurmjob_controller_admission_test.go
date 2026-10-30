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

const testKueueWebhookDenial = `admission webhook "vjobset.kb.io" denied the request: ` +
	`spec.replicatedJobs[0].template.spec.template.metadata.annotations: Invalid value: ` +
	`must not contain more than one topology annotation`

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
			if got := truncateComment(tc.comment); got != tc.want {
				t.Errorf("truncateComment() = %q, want %q", got, tc.want)
			}
		})
	}

	t.Run("does not split multi-byte characters", func(t *testing.T) {
		got := truncateComment(strings.Repeat("ż", maxSlurmCommentLength))
		if len(got) > maxSlurmCommentLength {
			t.Errorf("truncateComment() length = %d, want <= %d", len(got), maxSlurmCommentLength)
		}
		if !utf8.ValidString(got) {
			t.Errorf("truncateComment() returned invalid UTF-8: %q", got)
		}
	})
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
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
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
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
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
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
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
	if !strings.HasSuffix(*reqMsg.Comment, truncatedCommentSuffix) {
		t.Errorf("Comment = %q, want suffix %q", *reqMsg.Comment, truncatedCommentSuffix)
	}
}

func TestReconcile_ValidExtra_AnnotatesJobSetPodTemplate(t *testing.T) {
	job := newReconTestJob().
		withNodes(2).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
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
