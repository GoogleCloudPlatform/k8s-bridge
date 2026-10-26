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
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/clientmap"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/utils/slurmtypes"
	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	slurmclientinterceptor "github.com/SlinkyProject/slurm-client/pkg/client/interceptor"
	"github.com/SlinkyProject/slurm-client/pkg/object"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/prometheus/client_golang/prometheus/testutil"
	uberzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

func TestReconcile_Success(t *testing.T) {
	job := newReconTestJob().
		withNodes(2).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		build()

	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	res, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("Expected empty result, got %v", res)
	}

	env.verifyJobSetCreated("some-queue", reconJobIDStr, reconWmName)
	env.verifySlurmUnhold()

	if len(env.mockSlurm.GetDeletedJobs()) != 0 {
		t.Errorf("Expected no jobs to be deleted, but got %v", env.mockSlurm.GetDeletedJobs())
	}
}

func TestReconcile_PermanentFailure_TranslationError(t *testing.T) {
	job := newReconTestJob().
		withNodes(2).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		withTresPerNode("gres/gpu:a100=1").
		build()

	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	res, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("Expected empty result for permanent failure, got %v", res)
	}

	env.verifyJobSetNotFound()
	env.verifySlurmCancellation("missing GRES mapping for gpu:a100")
}

func TestReconcile_TemporaryFailure_GetJobError(t *testing.T) {
	env := newReconcileTestEnv(t)
	env.mockSlurm.GetError = errors.New("connection refused")

	_, err := env.reconcile()
	verifyError(t, err, "connection refused")

	env.verifyJobSetNotFound()
	env.verifyNoSlurmUpdates()
}

func TestReconcile_JobNotFound_Skipped(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "slurm rest not found error",
			err:  errors.New("[Not Found, Invalid job id specified]"),
		},
		{
			name: "http 404 not found",
			err:  errors.New("Not Found"),
		},
		{
			name: "invalid job id specified",
			err:  errors.New("Invalid job id specified"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := newReconcileTestEnv(t)
			env.mockSlurm.GetError = tc.err

			res, err := env.reconcile()
			if err != nil {
				t.Fatalf("Expected no error when Slurm job is not found, got %v", err)
			}
			if res != (ctrl.Result{}) {
				t.Errorf("Expected empty result, got %v", res)
			}

			env.verifyJobSetNotFound()
			env.verifyNoSlurmUpdates()
		})
	}
}

// TestReconcile_SlurmUnavailable_KeepsExistingJobSet checks that the reconciler does not change
// the JobSet, and does not cancel the job, when it cannot read the job from Slurm. The failed
// JobSet case matters most: with the job data, the reconciler would cancel the job, delete its
// dynamic nodes, and delete the JobSet.
func TestReconcile_SlurmUnavailable_KeepsExistingJobSet(t *testing.T) {
	slurmStates := []struct {
		name        string
		opts        []reconcileEnvOpt
		getError    error
		wantErr     string
		wantRequeue time.Duration
	}{
		{
			name:     "Slurm API returns an error",
			getError: errors.New("connection refused"),
			wantErr:  "connection refused",
		},
		{
			name:        "Slurm client not ready",
			opts:        []reconcileEnvOpt{withWithoutSlurmClient()},
			wantRequeue: 5 * time.Second,
		},
	}
	jobSets := []struct {
		name  string
		build func() *jobsetv1alpha2.JobSet
	}{
		{
			name: "running JobSet",
			build: func() *jobsetv1alpha2.JobSet {
				return newReconTestJobSet().withSuspend(false).withSlurmdReplicas(1).build()
			},
		},
		{
			name: "failed JobSet",
			build: func() *jobsetv1alpha2.JobSet {
				return newReconTestJobSet().
					withSuspend(false).
					withSlurmdReplicas(1).
					withCondition(string(jobsetv1alpha2.JobSetFailed), metav1.ConditionTrue).
					build()
			},
		},
	}

	for _, slurm := range slurmStates {
		t.Run(slurm.name, func(t *testing.T) {
			for _, js := range jobSets {
				t.Run(js.name, func(t *testing.T) {
					opts := append([]reconcileEnvOpt{withExtraObjects(js.build())}, slurm.opts...)
					env := newReconcileTestEnv(t, opts...)
					env.mockSlurm.GetError = slurm.getError
					before := env.getJobSet()

					res, err := env.reconcile()
					switch {
					case slurm.wantErr == "" && err != nil:
						t.Errorf("Expected no error, got %v", err)
					case slurm.wantErr != "" && (err == nil || !strings.Contains(err.Error(), slurm.wantErr)):
						t.Errorf("Expected error containing %q, got %v", slurm.wantErr, err)
					}
					if res.RequeueAfter != slurm.wantRequeue {
						t.Errorf("Expected requeue after %v, got %v", slurm.wantRequeue, res.RequeueAfter)
					}

					env.verifyJobSetUnchanged(before)
					env.verifyNoSlurmUpdates()
				})
			}
		})
	}
}

func TestReconcile_Idempotency(t *testing.T) {
	existingJobSet := newTestJobSet("slurm-job-" + reconJobIDStr).build()

	job := newReconTestJob().
		withNodes(2).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		build()

	env := newReconcileTestEnv(t,
		withExtraObjects(existingJobSet),
		withPendingSlurmJobs(job),
	)

	res, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("Expected empty result, got %v", res)
	}

	env.verifySlurmUnhold()
}

func TestReconcile_MissingJobSet_NotPending(t *testing.T) {
	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStateRUNNING).
		withHold(true).
		build()

	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	_, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	env.verifySlurmCancellation(ReasonJobSetMissing)
}

func TestReconcile_MissingJobSet_NotHeld(t *testing.T) {
	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(false).
		build()

	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	_, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	env.verifySlurmCancellation(ReasonJobSetMissing)
}

func TestReconcile_MissingJobSet_ReheldTranslatedJob_CancelsInsteadOfRecreating(t *testing.T) {
	tests := []struct {
		name       string
		job        *slurmclienttypes.V0044JobInfo
		wantCancel bool
	}{
		{
			name: "re-held with BadConstraints and single k8s-id feature",
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason("BadConstraints").
				withTranslatedJobMarkers().
				build(),
			wantCancel: true,
		},
		{
			name: "re-held with BadConstraints and compound features",
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason("BadConstraints").
				withTranslatedJobMarkers().
				withFeatures("(gpu)&k8s-id-12345").
				build(),
			wantCancel: true,
		},
		{
			name: "re-held with BadConstraints and user-overwritten comment",
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason("BadConstraints").
				withFeatures("k8s-id-12345").
				withComment("custom-user-comment").
				build(),
			wantCancel: true,
		},
		{
			name: "explicitly held by user after translation",
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				withTranslatedJobMarkers().
				build(),
			wantCancel: true,
		},
		{
			name: "explicitly held by admin after translation",
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason(ReasonJobHeldAdmin).
				withTranslatedJobMarkers().
				build(),
			wantCancel: true,
		},
		{
			name: "untranslated job with prefix-colliding feature k8s-id-123456",
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				withFeatures("k8s-id-123456").
				withComment(AutoHoldMarker).
				build(),
			wantCancel: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := newReconcileTestEnv(t, withPendingSlurmJobs(tc.job))

			initDiscovered := env.jobsDiscoveredCount()
			initCreated := env.jobsSyncCount(metrics.StatusSuccess, metrics.ReasonJobSetCreated)
			initFailed := env.jobsSyncCount(metrics.StatusError, metrics.ReasonJobSetFailed)

			res, err := env.reconcile()
			if err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}
			if res != (ctrl.Result{}) {
				t.Errorf("Expected empty result, got %v", res)
			}

			if tc.wantCancel {
				env.verifyJobSetNotFound()
				env.verifySlurmCancellation(ReasonJobSetMissing)

				if got := env.jobsDiscoveredCount(); got != initDiscovered {
					t.Errorf("Expected JobsDiscovered delta 0, got %f", got-initDiscovered)
				}
				if got := env.jobsSyncCount(metrics.StatusSuccess, metrics.ReasonJobSetCreated); got != initCreated {
					t.Errorf("Expected JobsSync(created) delta 0, got %f", got-initCreated)
				}
				if got := env.jobsSyncCount(metrics.StatusError, metrics.ReasonJobSetFailed); got != initFailed+1 {
					t.Errorf("Expected JobsSync(jobset_failed) delta +1, got %f", got-initFailed)
				}
			} else {
				env.verifyJobSetCreated("some-queue", reconJobIDStr, reconWmName)
				env.verifySlurmUnhold()
			}
		})
	}
}

func TestReconcile_DeletingJobSet_CancelsSlurmJob(t *testing.T) {
	tests := []struct {
		name               string
		existingJobSet     *jobsetv1alpha2.JobSet
		job                *slurmclienttypes.V0044JobInfo
		wantCancelReason   string
		wantNoSlurmUpdates bool
		wantFailedDelta    float64
	}{
		{
			name: "deleting unsuspended JobSet with re-held PENDING job (BadConstraints)",
			existingJobSet: newReconTestJobSet().
				withSuspend(false).
				withDeletionTimestamp().
				build(),
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason("BadConstraints").
				withTranslatedJobMarkers().
				build(),
			wantCancelReason: ReasonJobSetMissing,
			wantFailedDelta:  1,
		},
		{
			name: "deleting suspended JobSet with re-held PENDING job (BadConstraints)",
			existingJobSet: newReconTestJobSet().
				withSuspend(true).
				withDeletionTimestamp().
				build(),
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason("BadConstraints").
				withTranslatedJobMarkers().
				build(),
			wantCancelReason: ReasonJobSetMissing,
			wantFailedDelta:  1,
		},
		{
			name: "deleting unsuspended JobSet with explicitly held PENDING job (JobHeldUser)",
			existingJobSet: newReconTestJobSet().
				withSuspend(false).
				withDeletionTimestamp().
				build(),
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				withTranslatedJobMarkers().
				build(),
			wantCancelReason: ReasonJobSetMissing,
			wantFailedDelta:  1,
		},
		{
			name: "deleting unsuspended JobSet with RUNNING job",
			existingJobSet: newReconTestJobSet().
				withSuspend(false).
				withDeletionTimestamp().
				build(),
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStateRUNNING).
				withHold(false).
				withTranslatedJobMarkers().
				build(),
			wantCancelReason: ReasonJobSetMissing,
			wantFailedDelta:  1,
		},
		{
			name: "deleting unsuspended JobSet when Slurm job is CANCELLED with Hold=true",
			existingJobSet: newReconTestJobSet().
				withSuspend(false).
				withDeletionTimestamp().
				build(),
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStateCANCELLED).
				withHold(true).
				withTranslatedJobMarkers().
				build(),
			wantNoSlurmUpdates: true,
			wantFailedDelta:    0,
		},
		{
			name: "deleting JobSet when Slurm job is already COMPLETED",
			existingJobSet: newReconTestJobSet().
				withSuspend(false).
				withDeletionTimestamp().
				build(),
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStateCOMPLETED).
				withHold(false).
				withTranslatedJobMarkers().
				build(),
			wantNoSlurmUpdates: true,
			wantFailedDelta:    0,
		},
		{
			name: "deleting JobSet that already has JobSetCompleted=True condition",
			existingJobSet: newReconTestJobSet().
				withSuspend(false).
				withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
				withDeletionTimestamp().
				build(),
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStateRUNNING).
				withHold(false).
				withTranslatedJobMarkers().
				build(),
			wantCancelReason: ReasonJobSetCompleted,
			wantFailedDelta:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := newReconcileTestEnv(t,
				withExtraObjects(tc.existingJobSet),
				withPendingSlurmJobs(tc.job),
			)

			initFailed := env.jobsSyncCount(metrics.StatusError, metrics.ReasonJobSetFailed)

			res, err := env.reconcile()
			if err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}
			if res != (ctrl.Result{}) {
				t.Errorf("Expected empty result, got %v", res)
			}

			if tc.wantNoSlurmUpdates {
				env.verifyNoSlurmUpdates()
			} else {
				env.verifySlurmCancellation(tc.wantCancelReason)
			}

			env.verifyJobSetNotFound()

			if got := env.jobsSyncCount(metrics.StatusError, metrics.ReasonJobSetFailed); got != initFailed+tc.wantFailedDelta {
				t.Errorf("Expected JobsSync(jobset_failed) delta %f, got %f", tc.wantFailedDelta, got-initFailed)
			}
		})
	}
}

func TestReconcile_DeletingJobSet_FinalizerAndDynamicNodes(t *testing.T) {
	const (
		dynamicNode0 = "slurm-job-12345-slurmd-0-0"
		dynamicNode1 = "slurm-job-12345-slurmd-1-0"
	)

	t.Run("JobSet deleted during slurmrestd outage cleans up dynamic nodes and removes finalizer once job is COMPLETED", func(t *testing.T) {
		deletingJS := newReconTestJobSet().
			withSuspend(false).
			withSlurmdReplicas(2).
			withDeletionTimestamp().
			build()

		completedJob := newReconTestJob().
			withJobState(slurmapi.V0044JobInfoJobStateCOMPLETED).
			withHold(false).
			withTranslatedJobMarkers().
			build()

		env := newReconcileTestEnv(t,
			withExtraObjects(deletingJS),
			withPendingSlurmJobs(completedJob),
		)

		// 1. Simulate slurmrestd outage when the JobSet deletion watch event first arrives
		env.mockSlurm.GetError = errors.New("connection refused")
		if _, err := env.reconcile(); err == nil {
			t.Fatal("Expected error during slurmrestd outage, got nil")
		}

		// Finalizer must keep the JobSet in Kubernetes so cleanup is not lost
		env.verifyJobSetHasFinalizer()
		env.verifyNoSlurmUpdates()

		// 2. slurmrestd recovers and reports the Slurm job is now COMPLETED
		env.mockSlurm.GetError = nil
		res, err := env.reconcile()
		if err != nil {
			t.Fatalf("Expected second reconcile to succeed after slurmrestd recovery, got: %v", err)
		}
		if res != (ctrl.Result{}) {
			t.Errorf("Expected empty result, got %v", res)
		}

		env.verifySlurmNodesDeleted(dynamicNode0, dynamicNode1)
		env.verifyJobSetNotFound()
	})

	t.Run("Transient dynamic node deletion error on deleting JobSet retains finalizer for retry", func(t *testing.T) {
		deletingJS := newReconTestJobSet().
			withSuspend(false).
			withSlurmdReplicas(2).
			withDeletionTimestamp().
			build()

		runningJob := newReconTestJob().
			withJobState(slurmapi.V0044JobInfoJobStateRUNNING).
			withHold(false).
			withTranslatedJobMarkers().
			build()

		env := newReconcileTestEnv(t,
			withExtraObjects(deletingJS),
			withPendingSlurmJobs(runningJob),
		)
		env.mockSlurm.DeleteNodeError = errors.New("Nodes busy")

		initFailed := env.jobsSyncCount(metrics.StatusError, metrics.ReasonJobSetFailed)

		// 1. First reconcile cancels the RUNNING Slurm job, fails node deletion, and retains the finalizer
		if _, err := env.reconcile(); err == nil {
			t.Fatal("Expected error when dynamic node deletion fails with Nodes busy, got nil")
		}
		env.verifySlurmCancellation(ReasonJobSetMissing)
		env.verifyJobSetHasFinalizer()

		// 2. On retry, the Slurm job is now CANCELLED and nodes are no longer busy
		cancelledJob := newReconTestJob().
			withJobState(slurmapi.V0044JobInfoJobStateCANCELLED).
			withHold(false).
			withTranslatedJobMarkers().
			build()
		env.mockSlurm.SetPendingJobs(reconPartitionName, []slurmclienttypes.V0044JobInfo{*cancelledJob})
		env.mockSlurm.DeleteNodeError = nil

		if _, err := env.reconcile(); err != nil {
			t.Fatalf("Expected retry reconcile to succeed, got: %v", err)
		}

		env.verifySlurmNodesDeleted(dynamicNode0, dynamicNode1, dynamicNode0, dynamicNode1)
		env.verifyJobSetNotFound()

		if got := env.jobsSyncCount(metrics.StatusError, metrics.ReasonJobSetFailed); got != initFailed+1 {
			t.Errorf("Expected JobsSync(jobset_failed) delta 1 across retries, got %f", got-initFailed)
		}
	})

	t.Run("Deleting JobSet when Slurm job is purged (NotFound) cleans up dynamic nodes and removes finalizer", func(t *testing.T) {
		deletingJS := newReconTestJobSet().
			withSuspend(false).
			withSlurmdReplicas(2).
			withDeletionTimestamp().
			build()

		env := newReconcileTestEnv(t, withExtraObjects(deletingJS))
		env.mockSlurm.GetError = errors.New("[Not Found, Invalid job id specified]")

		res, err := env.reconcile()
		if err != nil {
			t.Fatalf("Expected no error when Slurm job is NotFound for deleting JobSet, got: %v", err)
		}
		if res != (ctrl.Result{}) {
			t.Errorf("Expected empty result, got %v", res)
		}

		env.verifySlurmNodesDeleted(dynamicNode0, dynamicNode1)
		env.verifyJobSetNotFound()
	})

	t.Run("Deleting JobSet when parent WorkloadMixing is NotFound removes finalizer", func(t *testing.T) {
		deletingJS := newReconTestJobSet().
			withSuspend(false).
			withSlurmdReplicas(1).
			withDeletionTimestamp().
			build()

		env := newReconcileTestEnv(t, withExtraObjects(deletingJS))
		if err := env.baseClient.Delete(t.Context(), env.wm); err != nil {
			t.Fatalf("Failed to delete WorkloadMixing: %v", err)
		}

		res, err := env.reconcile()
		if err != nil {
			t.Fatalf("Expected no error when WorkloadMixing is NotFound for deleting JobSet, got: %v", err)
		}
		if res != (ctrl.Result{}) {
			t.Errorf("Expected empty result, got %v", res)
		}

		env.verifyJobSetNotFound()
		env.verifyNoSlurmUpdates()
	})

	t.Run("Live JobSet keeps finalizer when Slurm job is purged (NotFound)", func(t *testing.T) {
		liveJS := newReconTestJobSet().
			withSuspend(false).
			withSlurmdReplicas(1).
			build()

		env := newReconcileTestEnv(t, withExtraObjects(liveJS))
		env.mockSlurm.GetError = errors.New("[Not Found, Invalid job id specified]")

		if _, err := env.reconcile(); err != nil {
			t.Fatalf("Expected no error when Slurm job is NotFound for live JobSet, got: %v", err)
		}

		want := jobSetState{Exists: true, Deleting: false, Finalizers: []string{FinalizerJobSet}}
		if diff := cmp.Diff(want, getJobSetState(t, env.baseClient, env.jobSetKey)); diff != "" {
			t.Errorf("JobSet state mismatch (-want +got):\n%s", diff)
		}
		env.verifySlurmNodesDeleted()
	})

	t.Run("Finalizer patch failure returns error and keeps the finalizer", func(t *testing.T) {
		deletingJS := newReconTestJobSet().
			withSuspend(false).
			withSlurmdReplicas(1).
			withDeletionTimestamp().
			build()

		env := newReconcileTestEnv(t,
			withExtraObjects(deletingJS),
			withInterceptors(interceptor.Funcs{
				Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
					return errors.New("apiserver unavailable")
				},
			}),
		)
		env.mockSlurm.GetError = errors.New("[Not Found, Invalid job id specified]")

		if _, err := env.reconcile(); err == nil {
			t.Fatal("Expected error when the finalizer patch fails, got nil")
		}

		want := jobSetState{Exists: true, Deleting: true, Finalizers: []string{FinalizerJobSet}}
		if diff := cmp.Diff(want, getJobSetState(t, env.baseClient, env.jobSetKey)); diff != "" {
			t.Errorf("JobSet state mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestReconcile_InformerCacheLag_DoesNotCancelNewlyTranslatedJob(t *testing.T) {
	tests := []struct {
		name                 string
		reconcile1UpdateErr  error
		reholdAfterReconcile bool
	}{
		{
			name: "cache lag after successful unhold while Hold=false (PartitionConfig)",
		},
		{
			name:                "cache lag after 422 in-place update while Hold=false (PartitionConfig)",
			reconcile1UpdateErr: newPartConfigUnavailable422Error(),
		},
		{
			name:                 "cache lag after Slurm re-holds admitted job with BadConstraints (Hold=true)",
			reholdAfterReconcile: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var simulateCacheLag bool
			interceptors := interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
					if simulateCacheLag && key.Name == "slurm-job-"+reconJobIDStr {
						return apierrors.NewNotFound(jobsetv1alpha2.Resource("jobsets"), key.Name)
					}
					return c.Get(ctx, key, obj, opts...)
				},
			}

			job := newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				withComment(AutoHoldMarker).
				build()

			env := newReconcileTestEnv(t,
				withInterceptors(interceptors),
				withPendingSlurmJobs(job),
				withSlurmUpdateError(tc.reconcile1UpdateErr, true),
			)

			initDiscovered := env.jobsDiscoveredCount()
			initFailed := env.jobsSyncCount(metrics.StatusError, metrics.ReasonJobSetFailed)

			res1, err1 := env.reconcile()
			if err1 != nil {
				t.Errorf("First reconcile returned unexpected error: %v", err1)
			}
			if res1 != (ctrl.Result{}) {
				t.Errorf("First reconcile expected empty result, got %v", res1)
			}

			if tc.reholdAfterReconcile {
				var mutatedJob slurmclienttypes.V0044JobInfo
				if err := env.mockSlurm.Get(context.Background(), slurmclient.ObjectKey(reconJobIDStr), &mutatedJob); err != nil {
					t.Fatalf("Failed to get job from mockSlurm: %v", err)
				}
				mutatedJob.Hold = ptr.To(true)
				mutatedJob.StateReason = ptr.To("BadConstraints")
				env.mockSlurm.SetPendingJobs(reconPartitionName, []slurmclienttypes.V0044JobInfo{mutatedJob})
			}

			simulateCacheLag = true
			env.mockSlurm.UpdateError = nil

			res2, err2 := env.reconcile()
			if err2 != nil {
				t.Fatalf("Second reconcile returned unexpected error: %v", err2)
			}
			if res2 != (ctrl.Result{}) {
				t.Errorf("Second reconcile expected empty result, got %v", res2)
			}

			if len(env.mockSlurm.GetDeletedJobs()) != 0 {
				t.Errorf("Expected newly translated Slurm job not to be cancelled, got deleted jobs: %v", env.mockSlurm.GetDeletedJobs())
			}
			if len(env.mockSlurm.GetUpdatedJobs()) != 1 {
				t.Errorf("Expected 1 Slurm job update across both reconciles, got %d", len(env.mockSlurm.GetUpdatedJobs()))
			}

			env.verifyJobSetCreated("some-queue", reconJobIDStr, reconWmName)

			if got := env.jobsDiscoveredCount(); got != initDiscovered+1 {
				t.Errorf("Expected JobsDiscovered delta 1 across both reconciles, got %f", got-initDiscovered)
			}
			if got := env.jobsSyncCount(metrics.StatusError, metrics.ReasonJobSetFailed); got != initFailed {
				t.Errorf("Expected JobsSync(jobset_failed) delta 0, got %f", got-initFailed)
			}
		})
	}
}

var terminalSlurmJobStates = []struct {
	name   string
	states []slurmapi.V0044JobInfoJobState
}{
	{name: "COMPLETED", states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateCOMPLETED}},
	{name: "CANCELLED", states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateCANCELLED}},
	{name: "FAILED", states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateFAILED}},
	{name: "TIMEOUT", states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateTIMEOUT}},
	{name: "NODEFAIL", states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateNODEFAIL}},
	{name: "PREEMPTED", states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStatePREEMPTED}},
	{name: "BOOTFAIL", states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateBOOTFAIL}},
	{name: "DEADLINE", states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateDEADLINE}},
	{name: "OUTOFMEMORY", states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateOUTOFMEMORY}},
	{name: "LAUNCHFAILED", states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateLAUNCHFAILED}},
	{name: "RECONFIGFAIL", states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateRECONFIGFAIL}},
	{name: "REVOKED", states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateREVOKED}},
	{name: "SPECIALEXIT", states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateSPECIALEXIT}},
	{
		name: "COMPLETED+COMPLETING",
		states: []slurmapi.V0044JobInfoJobState{
			slurmapi.V0044JobInfoJobStateCOMPLETED,
			slurmapi.V0044JobInfoJobStateCOMPLETING,
		},
	},
	{
		name: "CANCELLED+COMPLETING",
		states: []slurmapi.V0044JobInfoJobState{
			slurmapi.V0044JobInfoJobStateCANCELLED,
			slurmapi.V0044JobInfoJobStateCOMPLETING,
		},
	},
	{
		name: "FAILED+COMPLETING",
		states: []slurmapi.V0044JobInfoJobState{
			slurmapi.V0044JobInfoJobStateFAILED,
			slurmapi.V0044JobInfoJobStateCOMPLETING,
		},
	},
	{
		name: "TIMEOUT+COMPLETING",
		states: []slurmapi.V0044JobInfoJobState{
			slurmapi.V0044JobInfoJobStateTIMEOUT,
			slurmapi.V0044JobInfoJobStateCOMPLETING,
		},
	},
}

func TestReconcile_MissingJobSet_TerminalSlurmJob_Ignored(t *testing.T) {
	type testCase struct {
		name   string
		states []slurmapi.V0044JobInfoJobState
		hold   bool
	}
	tests := make([]testCase, 0, len(terminalSlurmJobStates)+1)
	for _, s := range terminalSlurmJobStates {
		tests = append(tests, testCase{name: s.name, states: s.states})
	}
	tests = append(tests, testCase{
		name:   "CANCELLED with Hold=true",
		states: []slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateCANCELLED},
		hold:   true,
	})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := newReconTestJob().
				withJobState(tc.states...).
				withHold(tc.hold).
				withTranslatedJobMarkers().
				build()

			env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

			initDiscovered := env.jobsDiscoveredCount()
			initFailed := env.jobsSyncCount(metrics.StatusError, metrics.ReasonJobSetFailed)

			res, err := env.reconcile()
			if err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}
			if res != (ctrl.Result{}) {
				t.Errorf("Expected empty result, got %v", res)
			}

			env.verifyNoSlurmUpdates()
			env.verifyJobSetNotFound()

			if got := env.jobsSyncCount(metrics.StatusError, metrics.ReasonJobSetFailed); got != initFailed {
				t.Errorf("Expected JobsSync(jobset_failed) delta 0, got %f", got-initFailed)
			}
			if got := env.jobsDiscoveredCount(); got != initDiscovered {
				t.Errorf("Expected JobsDiscovered delta 0, got %f", got-initDiscovered)
			}
		})
	}
}

func TestReconcile_Cancelled_MultiPartition(t *testing.T) {
	multiPart := fmt.Sprintf("%s,unmapped-partition", reconPartitionName)
	job := newReconTestJob().
		withPartition(multiPart).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		withStateReason(ReasonJobHeldUser).
		build()

	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	var buf bytes.Buffer
	testLogger := zap.New(zap.WriteTo(&buf), zap.UseDevMode(true))
	ctx := logf.IntoContext(context.Background(), testLogger)

	_, err := env.reconcile(ctx)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	logOutput := buf.String()
	if !strings.Contains(logOutput, "Job requests multiple partitions and at least one is mapped, cancelling job") {
		t.Errorf("Expected log message about cancellation not found: %s", logOutput)
	}

	env.verifySlurmCancellation("Multi-partition jobs with multiple mapped partitions are not supported")
	env.verifyJobSetNotFound()
}

func TestReconcile_Graceful_MissingClient(t *testing.T) {
	env := newReconcileTestEnv(t, withWithoutSlurmClient())

	var buf bytes.Buffer
	testLogger := zap.New(zap.WriteTo(&buf), zap.UseDevMode(true))
	ctx := logf.IntoContext(context.Background(), testLogger)

	res, err := env.reconcile(ctx)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if res.RequeueAfter != 5*time.Second {
		t.Errorf("Expected requeue after 5s, got %v", res.RequeueAfter)
	}

	logOutput := buf.String()
	if !strings.Contains(logOutput, "Slurm client not found in ClientMap") {
		t.Errorf("Expected log message about missing client not found: %s", logOutput)
	}
}

func TestReconcile_MissingClient_DeletingJobSet(t *testing.T) {
	tests := []struct {
		name             string
		opts             []reconcileEnvOpt
		wantRequeueAfter time.Duration
		wantJobSet       jobSetState
	}{
		{
			name:             "retains finalizer while Slurm Controller exists",
			wantRequeueAfter: 5 * time.Second,
			wantJobSet:       jobSetState{Exists: true, Deleting: true, Finalizers: []string{FinalizerJobSet}},
		},
		{
			name:             "releases finalizer without requeue when Slurm Controller is gone",
			opts:             []reconcileEnvOpt{withWithoutController()},
			wantRequeueAfter: 0,
			wantJobSet:       jobSetState{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]reconcileEnvOpt{
				withWithoutSlurmClient(),
				withExtraObjects(newReconTestJobSet().withDeletionTimestamp().build()),
			}, tc.opts...)
			env := newReconcileTestEnv(t, opts...)

			res, err := env.reconcile()
			if err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}
			if res.RequeueAfter != tc.wantRequeueAfter {
				t.Errorf("Expected requeue after %v, got %v", tc.wantRequeueAfter, res.RequeueAfter)
			}
			if diff := cmp.Diff(tc.wantJobSet, getJobSetState(t, env.baseClient, env.jobSetKey), cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("JobSet state mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSlurmJobReconciler_Start(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	t.Run("Event is forwarded to genericEvents channel", func(t *testing.T) {
		var buf bytes.Buffer
		testLogger := zap.New(zap.WriteTo(&buf), zap.UseDevMode(true))
		logf.SetLogger(testLogger)
		ctx := t.Context()

		ctx = logf.IntoContext(ctx, testLogger)

		cfg := &rest.Config{Host: "http://localhost:0"}
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:                 scheme,
			Metrics:                metricsserver.Options{BindAddress: "0"},
			HealthProbeBindAddress: "0",
		})
		if err != nil {
			t.Fatalf("Failed to create manager: %v", err)
		}

		eventsChan := make(chan slurmtypes.SlurmJobAdmissionEvent, 10)
		reconciler := &SlurmJobReconciler{
			Client:              fake.NewClientBuilder().WithScheme(scheme).Build(),
			Scheme:              scheme,
			AdmissionEventsChan: eventsChan,
			ClientMap:           clientmap.NewClientMap(),
		}

		const testWMName = "test-wm"

		if err := reconciler.SetupWithManager(mgr); err != nil {
			t.Fatalf("SetupWithManager failed: %v", err)
		}

		go func() {
			if err := reconciler.Start(ctx); err != nil {
				logf.Log.Error(err, "Reconciler failed to start")
			}
		}()

		ev := slurmtypes.SlurmJobAdmissionEvent{
			JobID: "54321",
			WorkloadMixing: types.NamespacedName{
				Namespace: reconNamespace,
				Name:      testWMName,
			},
		}
		eventsChan <- ev

		select {
		case ge := <-reconciler.genericEvents:
			if ge.Object.GetName() != "slurm-job-test-wm-54321" {
				t.Errorf("Expected generic event name 'slurm-job-test-wm-54321', got %s", ge.Object.GetName())
			}
		case <-time.After(2 * time.Second):
			t.Errorf("Timeout waiting for generic event")
		}
	})
}

func TestSlurmJobReconciler_MapJobSetToRequest(t *testing.T) {
	reconciler := &SlurmJobReconciler{}

	t.Run("Valid JobSet", func(t *testing.T) {
		js := newTestJobSet("").
			withLabels(map[string]string{
				LabelSlurmJobID: "12345",
			}).
			withOwnerReference(metav1.OwnerReference{
				Kind: KindWorkloadMixing,
				Name: testWMName,
			}).
			build()

		reqs := reconciler.mapJobSetToRequest(context.Background(), js)
		if len(reqs) != 1 {
			t.Fatalf("Expected 1 request, got %d", len(reqs))
		}
		expectedName := "slurm-job-test-wm-12345"
		if reqs[0].Name != expectedName || reqs[0].Namespace != reconNamespace {
			t.Errorf("Expected request %s/%s, got %s/%s", reconNamespace, expectedName, reqs[0].Namespace, reqs[0].Name)
		}
	})

	t.Run("Missing SlurmJobID Label", func(t *testing.T) {
		var buf bytes.Buffer
		testLogger := zap.New(zap.WriteTo(&buf), zap.UseDevMode(true))
		ctx := logf.IntoContext(context.Background(), testLogger)

		js := newTestJobSet("test-jobset").
			withOwnerReference(metav1.OwnerReference{
				Kind: KindWorkloadMixing,
				Name: reconWmName,
			}).
			build()

		reqs := reconciler.mapJobSetToRequest(ctx, js)
		if len(reqs) != 0 {
			t.Errorf("Expected 0 requests, got %d", len(reqs))
		}

		logOutput := buf.String()
		if !strings.Contains(logOutput, "JobSet missing SlurmJobID label, ignoring") {
			t.Errorf("Expected log message about missing label not found: %s", logOutput)
		}
	})

	t.Run("Missing WorkloadMixing OwnerReference", func(t *testing.T) {
		var buf bytes.Buffer
		testLogger := zap.New(zap.WriteTo(&buf), zap.UseDevMode(true))
		ctx := logf.IntoContext(context.Background(), testLogger)

		js := newTestJobSet("test-jobset").
			withLabels(map[string]string{
				LabelSlurmJobID: "12345",
			}).
			build()

		reqs := reconciler.mapJobSetToRequest(ctx, js)
		if len(reqs) != 0 {
			t.Errorf("Expected 0 requests, got %d", len(reqs))
		}

		logOutput := buf.String()
		if !strings.Contains(logOutput, "JobSet missing WorkloadMixing OwnerReference, ignoring") {
			t.Errorf("Expected log message about missing owner reference not found: %s", logOutput)
		}
	})
}

func TestReconcile_JobSetCompleted(t *testing.T) {
	completedJobSet := newReconTestJobSet().
		withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
		build()

	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		build()

	env := newReconcileTestEnv(t,
		withExtraObjects(completedJobSet),
		withPendingSlurmJobs(job),
	)

	_, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	env.verifySlurmCancellation(ReasonJobSetCompleted)
	env.verifyJobSetNotFound()
}

func TestReconcile_JobSetFailed(t *testing.T) {
	failedJobSet := newReconTestJobSet().
		withCondition(string(jobsetv1alpha2.JobSetFailed), metav1.ConditionTrue).
		build()

	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		build()

	env := newReconcileTestEnv(t,
		withExtraObjects(failedJobSet),
		withPendingSlurmJobs(job),
	)

	_, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	env.verifySlurmCancellation(ReasonJobSetFailed)
	env.verifyJobSetNotFound()
}

func TestReconcile_JobSetTerminal_CleansUpDynamicNodes(t *testing.T) {
	const (
		dynamicNode0 = "slurm-job-12345-slurmd-0-0"
		dynamicNode1 = "slurm-job-12345-slurmd-1-0"
	)
	tests := []struct {
		name            string
		conditionType   string
		replicas        int32
		deleteNodeError error
		wantReason      string
		wantDeletedNode []string
		wantErr         bool
	}{
		{
			name:            "JobSetCompleted cleans up dynamic nodes and deletes JobSet",
			conditionType:   string(jobsetv1alpha2.JobSetCompleted),
			replicas:        2,
			wantReason:      ReasonJobSetCompleted,
			wantDeletedNode: []string{dynamicNode0, dynamicNode1},
		},
		{
			name:            "JobSetFailed cleans up dynamic nodes and deletes JobSet",
			conditionType:   string(jobsetv1alpha2.JobSetFailed),
			replicas:        2,
			wantReason:      ReasonJobSetFailed,
			wantDeletedNode: []string{dynamicNode0, dynamicNode1},
		},
		{
			name:            "already-deleted dynamic node (NotFound) is ignored and JobSet is deleted",
			conditionType:   string(jobsetv1alpha2.JobSetCompleted),
			replicas:        1,
			deleteNodeError: errors.New("[Not Found, Invalid node name specified]"),
			wantReason:      ReasonJobSetCompleted,
			wantDeletedNode: []string{dynamicNode0},
		},
		{
			name:            "transient dynamic node deletion error retains JobSet for retry",
			conditionType:   string(jobsetv1alpha2.JobSetFailed),
			replicas:        1,
			deleteNodeError: errors.New("Nodes busy"),
			wantErr:         true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			terminalJobSet := newReconTestJobSet().
				withSlurmdReplicas(tc.replicas).
				withCondition(tc.conditionType, metav1.ConditionTrue).
				build()

			job := newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStateRUNNING).
				withHold(false).
				withTranslatedJobMarkers().
				build()

			env := newReconcileTestEnv(t,
				withExtraObjects(terminalJobSet),
				withPendingSlurmJobs(job),
			)
			env.mockSlurm.DeleteNodeError = tc.deleteNodeError

			res, err := env.reconcile()
			if tc.wantErr {
				if err == nil {
					t.Fatal("Expected error when dynamic node deletion fails, got nil")
				}
				env.verifyJobSetCreated("some-queue", reconJobIDStr, reconWmName)
				return
			}

			if err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}
			if res != (ctrl.Result{}) {
				t.Errorf("Expected empty result, got %v", res)
			}

			env.verifySlurmNodesDeleted(tc.wantDeletedNode...)
			env.verifySlurmCancellation(tc.wantReason)
			env.verifyJobSetNotFound()
		})
	}
}

func TestCancelJobWithComment_Idempotent(t *testing.T) {
	for _, tc := range terminalSlurmJobStates {
		t.Run(tc.name, func(t *testing.T) {
			mockSlurm := &utils.SlurmClient{}
			jobInfo := newTestJob().
				withJobState(tc.states...).
				build()

			r := &SlurmJobReconciler{}

			err := r.cancelJobWithComment(context.Background(), mockSlurm, jobInfo, "test comment")
			if err != nil {
				t.Errorf("Expected nil error for terminal job, got %v", err)
			}
			if len(mockSlurm.GetUpdatedJobs()) != 0 {
				t.Errorf("Expected 0 updates, got %d", len(mockSlurm.GetUpdatedJobs()))
			}
			if len(mockSlurm.GetDeletedJobs()) != 0 {
				t.Errorf("Expected 0 deletes, got %d", len(mockSlurm.GetDeletedJobs()))
			}
		})
	}
}

func TestCancelJobWithComment_PartConfigUnavailable422_DoesNotLogError(t *testing.T) {
	tests := []struct {
		name           string
		updateErr      error
		mutateOnUpdate bool
		wantErrorLog   bool
	}{
		{
			name:           "tolerated 422 on comment update does not log ERROR and deletes job",
			updateErr:      newPartConfigUnavailable422Error(),
			mutateOnUpdate: true,
			wantErrorLog:   false,
		},
		{
			name:           "genuine Update error logs ERROR and deletes job",
			updateErr:      errors.New("connection reset by peer"),
			mutateOnUpdate: false,
			wantErrorLog:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jobInfo := newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				build()

			env := newReconcileTestEnv(t,
				withPendingSlurmJobs(jobInfo),
				withSlurmUpdateError(tc.updateErr, tc.mutateOnUpdate),
			)

			var buf bytes.Buffer
			testLogger := zap.New(zap.WriteTo(&buf), zap.UseDevMode(true))
			ctx := logf.IntoContext(context.Background(), testLogger)

			err := env.reconciler.cancelJobWithComment(ctx, env.mockSlurm, jobInfo, ReasonJobSetCompleted)
			if err != nil {
				t.Fatalf("Expected nil error from cancelJobWithComment, got %v", err)
			}

			env.verifySlurmCancellation(ReasonJobSetCompleted)

			hasErrLog := strings.Contains(buf.String(), "Failed to set failure comment on Slurm job")
			if hasErrLog != tc.wantErrorLog {
				t.Errorf("Error log presence = %v, want %v (logs: %s)", hasErrLog, tc.wantErrorLog, buf.String())
			}
		})
	}
}

func TestReconcile_ExplicitHold_DoesNotMutateExistingJobSetOrUnhold(t *testing.T) {
	tests := []struct {
		name        string
		stateReason string
		comment     string
	}{
		{
			name:        "user hold without auto-hold marker",
			stateReason: ReasonJobHeldUser,
		},
		{
			name:        "admin hold",
			stateReason: ReasonJobHeldAdmin,
		},
		{
			name:        "admin hold with auto-hold marker",
			stateReason: ReasonJobHeldAdmin,
			comment:     AutoHoldMarker,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			js := newReconTestJobSet().
				withSuspend(false).
				build()

			job := newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason(tc.stateReason).
				withComment(tc.comment).
				build()

			env := newReconcileTestEnv(t,
				withExtraObjects(js),
				withPendingSlurmJobs(job),
			)

			before := env.getJobSet()
			if _, err := env.reconcile(); err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}

			env.verifyJobSetUnchanged(before)
			env.verifyNoSlurmUpdates()
		})
	}
}

func TestReconcile_SuspendedJobSet_DoesNotReleaseHold(t *testing.T) {
	js := newReconTestJobSet().
		withSuspend(true).
		build()

	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		withStateReason("None").
		build()

	env := newReconcileTestEnv(t,
		withExtraObjects(js),
		withPendingSlurmJobs(job),
	)

	_, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	env.verifyNoSlurmUpdates()
}

func TestReconcile_Preempted_PreservesJobSet(t *testing.T) {
	js := newReconTestJobSet().
		withLabels(map[string]string{
			LabelKueuePriorityClass: "prod-high",
		}).
		withSuspend(true).
		build()

	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		build()

	env := newReconcileTestEnv(t,
		withExtraObjects(js),
		withPendingSlurmJobs(job),
	)

	_, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	fetchedJS := &jobsetv1alpha2.JobSet{}
	if err := env.fakeClient.Get(context.Background(), env.jobSetKey, fetchedJS); err != nil {
		t.Fatalf("Expected JobSet to be preserved during preemption, got err: %v", err)
		return
	}
	if fetchedJS.Labels[LabelKueuePriorityClass] != "prod-high" {
		t.Errorf("Expected priority class label to be preserved as 'prod-high', got %q", fetchedJS.Labels[LabelKueuePriorityClass])
	}
}

func TestReconcile_SuspendedJobSet_UnheldJob_NotResumedByBridge(t *testing.T) {
	js := newReconTestJobSet().
		withSuspend(true).
		build()

	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(false).
		withStateReason("None").
		build()

	env := newReconcileTestEnv(t,
		withExtraObjects(js),
		withPendingSlurmJobs(job),
	)

	before := env.getJobSet()
	if _, err := env.reconcile(); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	env.verifyJobSetUnchanged(before)
	env.verifyNoSlurmUpdates()
}

func TestReconcile_ReleaseHold_PreservesExistingComment(t *testing.T) {
	job := newReconTestJob().
		withNodes(1).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		withComment("user-comment, k8s-bridge-auto-hold").
		build()

	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	res, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("Expected empty result, got %v", res)
	}

	updatedJobs := env.mockSlurm.GetUpdatedJobs()
	if len(updatedJobs) != 1 {
		t.Fatalf("Expected 1 job update, got %d", len(updatedJobs))
		return
	}
	reqMsg, ok := updatedJobs[0].Req.(slurmapi.V0044JobDescMsg)
	if !ok {
		t.Fatalf("Expected update request to be V0044JobDescMsg, got %T", updatedJobs[0].Req)
		return
	}
	if reqMsg.Comment == nil {
		t.Fatalf("Expected Comment to be set")
		return
	}
	expectedComment := "user-comment, Unheld by k8s-bridge: Kubernetes resource slurm-job-12345 created successfully"
	if *reqMsg.Comment != expectedComment {
		t.Errorf("Expected comment %q, got %q", expectedComment, *reqMsg.Comment)
	}
}

func TestReconcile_ReleaseHold_IdempotentCommentAndConstraints(t *testing.T) {
	existingJobSet := newReconTestJobSet().
		withSuspend(false).
		build()
	unheldComment := "user-comment, Unheld by k8s-bridge: Kubernetes resource slurm-job-12345 created successfully, k8s-bridge-auto-hold"
	existingConstraints := "(user-feature)&k8s-id-12345"

	job := newReconTestJob().
		withNodes(1).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		withComment(unheldComment).
		withFeatures(existingConstraints).
		build()

	env := newReconcileTestEnv(t,
		withExtraObjects(existingJobSet),
		withPendingSlurmJobs(job),
	)

	res, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("Expected empty result, got %v", res)
	}

	updatedJobs := env.mockSlurm.GetUpdatedJobs()
	if len(updatedJobs) != 1 {
		t.Fatalf("Expected 1 job update, got %d", len(updatedJobs))
		return
	}
	reqMsg, ok := updatedJobs[0].Req.(slurmapi.V0044JobDescMsg)
	if !ok {
		t.Fatalf("Expected update request to be V0044JobDescMsg, got %T", updatedJobs[0].Req)
		return
	}
	if reqMsg.Comment == nil {
		t.Fatalf("Expected Comment to be set")
		return
	}
	expectedComment := "user-comment, Unheld by k8s-bridge: Kubernetes resource slurm-job-12345 created successfully"
	if *reqMsg.Comment != expectedComment {
		t.Errorf("Comment = %q, want %q", *reqMsg.Comment, expectedComment)
	}

	if reqMsg.Constraints == nil {
		t.Fatalf("Expected Constraints to be set")
		return
	}
	if *reqMsg.Constraints != existingConstraints {
		t.Errorf("Constraints = %q, want %q", *reqMsg.Constraints, existingConstraints)
	}
}

func TestReconcile_ReleaseHold_PartConfigUnavailable422_AppliedInPlace(t *testing.T) {
	tests := []struct {
		name              string
		existingJobSet    *jobsetv1alpha2.JobSet
		job               *slurmclienttypes.V0044JobInfo
		updateErr         error
		mutateOnUpdate    bool
		wantErr           bool
		wantErrMetricIncr float64
	}{
		{
			name: "reconcileNewJob tolerates 422 aggregate error when mutation applied in-place",
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				withComment(AutoHoldMarker).
				build(),
			updateErr:      newPartConfigUnavailable422Error(),
			mutateOnUpdate: true,
		},
		{
			name: "reconcileActiveJob StatusShouldUnhold tolerates 422 string error when mutation applied in-place",
			existingJobSet: newReconTestJobSet().
				withSuspend(false).
				build(),
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason("BadConstraints").
				withTranslatedJobMarkers().
				build(),
			updateErr:      errors.New("[Unprocessable Entity, Requested partition configuration not available now]"),
			mutateOnUpdate: true,
		},
		{
			name: "422 Invalid feature specification without in-place mutation fails",
			job: newReconTestJob().
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				withComment(AutoHoldMarker).
				build(),
			updateErr:         errors.New("[Unprocessable Entity, Invalid feature specification]"),
			mutateOnUpdate:    false,
			wantErr:           true,
			wantErrMetricIncr: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var opts []reconcileEnvOpt
			if tc.existingJobSet != nil {
				opts = append(opts, withExtraObjects(tc.existingJobSet))
			}
			opts = append(opts,
				withPendingSlurmJobs(tc.job),
				withSlurmUpdateError(tc.updateErr, tc.mutateOnUpdate),
			)
			env := newReconcileTestEnv(t, opts...)

			initUpdateErr := env.jobsSyncCount(metrics.StatusError, metrics.ReasonSlurmUpdateError)

			res, err := env.reconcile()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Reconcile error = %v, wantErr %v", err, tc.wantErr)
			}

			if got := env.jobsSyncCount(metrics.StatusError, metrics.ReasonSlurmUpdateError); got != initUpdateErr+tc.wantErrMetricIncr {
				t.Errorf("Expected JobsSync(slurm_update_error) delta %f, got %f", tc.wantErrMetricIncr, got-initUpdateErr)
			}

			if !tc.wantErr {
				if res != (ctrl.Result{}) {
					t.Errorf("Expected empty result, got %v", res)
				}
				env.verifyJobSetCreated("some-queue", reconJobIDStr, reconWmName)
				env.verifySlurmUnhold()

				var updatedJob slurmclienttypes.V0044JobInfo
				if err := env.mockSlurm.Get(context.Background(), slurmclient.ObjectKey(reconJobIDStr), &updatedJob); err != nil {
					t.Fatalf("Failed to get job from mockSlurm: %v", err)
				}
				if ptr.Deref(updatedJob.Hold, true) {
					t.Errorf("Expected Slurm job Hold to be false after in-place update")
				}
				if !strings.Contains(ptr.Deref(updatedJob.Features, ""), "k8s-id-12345") {
					t.Errorf("Expected Slurm job Features to contain k8s-id-12345, got %q", ptr.Deref(updatedJob.Features, ""))
				}
				if strings.Contains(ptr.Deref(updatedJob.Comment, ""), AutoHoldMarker) {
					t.Errorf("Expected Slurm job Comment to strip %q, got %q", AutoHoldMarker, ptr.Deref(updatedJob.Comment, ""))
				}
				if len(env.mockSlurm.GetDeletedJobs()) != 0 {
					t.Errorf("Expected 0 deleted jobs, got %v", env.mockSlurm.GetDeletedJobs())
				}
			}
		})
	}
}

func TestReconcile_GetJobSetError_ReturnsError(t *testing.T) {
	interceptors := interceptor.Funcs{
		Get: func(ctx context.Context, client client.WithWatch, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
			if strings.HasPrefix(key.Name, "slurm-job-") {
				return apierrors.NewInternalError(fmt.Errorf("injected error on JobSet Get"))
			}
			return client.Get(ctx, key, obj, opts...)
		},
	}

	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		build()

	env := newReconcileTestEnv(t,
		withInterceptors(interceptors),
		withPendingSlurmJobs(job),
	)

	_, err := env.reconcile()
	verifyError(t, err, "injected error on JobSet Get")
}

func TestReconcile_MissingController(t *testing.T) {
	job := newReconTestJob().
		withNodes(2).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		build()

	env := newReconcileTestEnv(t,
		withWithoutController(),
		withPendingSlurmJobs(job),
	)

	res, err := env.reconcile()
	if err == nil {
		t.Fatal("Expected error when Controller CR is missing, got nil")
		return
	}
	if !apierrors.IsNotFound(err) {
		t.Errorf("Expected NotFound error, got: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("Expected empty result, got %v", res)
	}

	env.verifyJobSetNotFound()
	env.verifyNoSlurmUpdates()
}

func TestReconcile_EmptyControllerRefName(t *testing.T) {
	job := newReconTestJob().
		withNodes(2).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		build()

	env := newReconcileTestEnv(t,
		withEmptyControllerRefName(),
		withPendingSlurmJobs(job),
	)

	res, err := env.reconcile()
	if err == nil {
		t.Fatal("Expected error when ControllerRef.Name is empty, got nil")
	}
	if !strings.Contains(err.Error(), "ControllerRef.Name is empty") {
		t.Errorf("Expected error to contain 'ControllerRef.Name is empty', got: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("Expected empty result, got %v", res)
	}
}

func TestHasFeatureConstraint(t *testing.T) {
	const testFeature123 = "k8s-id-123"
	tests := []struct {
		name          string
		features      string
		targetFeature string
		want          bool
	}{
		{
			name:          "Empty features",
			features:      "",
			targetFeature: testFeature123,
			want:          false,
		},
		{
			name:          "Empty target feature",
			features:      testFeature123,
			targetFeature: "",
			want:          false,
		},
		{
			name:          "Exact single match",
			features:      testFeature123,
			targetFeature: testFeature123,
			want:          true,
		},
		{
			name:          "Substring prefix does not falsely match",
			features:      "k8s-id-1234",
			targetFeature: testFeature123,
			want:          false,
		},
		{
			name:          "Substring suffix does not falsely match",
			features:      "ak8s-id-123",
			targetFeature: testFeature123,
			want:          false,
		},
		{
			name:          "Joined with ampersand at end",
			features:      "(gpu)&k8s-id-123",
			targetFeature: testFeature123,
			want:          true,
		},
		{
			name:          "Joined with ampersand and similar prefix earlier",
			features:      "(k8s-id-1234)&k8s-id-123",
			targetFeature: testFeature123,
			want:          true,
		},
		{
			name:          "Joined with comma",
			features:      "gpu,k8s-id-123,fast",
			targetFeature: testFeature123,
			want:          true,
		},
		{
			name:          "Joined with pipe",
			features:      "k8s-id-123|other",
			targetFeature: testFeature123,
			want:          true,
		},
		{
			name:          "Inside parentheses",
			features:      "(k8s-id-123)&fast",
			targetFeature: testFeature123,
			want:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasFeatureConstraint(tt.features, tt.targetFeature)
			if got != tt.want {
				t.Errorf("hasFeatureConstraint(%q, %q) = %v, want %v", tt.features, tt.targetFeature, got, tt.want)
			}
		})
	}
}

func TestReconcile_UnholdJob_ConstraintSubstringNoFalsePositive(t *testing.T) {
	existingConstraints := "k8s-id-123456"
	job := newReconTestJob().
		withJobID(12345).
		withFeatures(existingConstraints).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		build()

	env := newReconcileTestEnv(t,
		withPendingSlurmJobs(job),
	)

	res, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("Expected empty result, got %v", res)
	}

	updatedJobs := env.mockSlurm.GetUpdatedJobs()
	if len(updatedJobs) != 1 {
		t.Fatalf("Expected 1 job update, got %d", len(updatedJobs))
		return
	}
	reqMsg, ok := updatedJobs[0].Req.(slurmapi.V0044JobDescMsg)
	if !ok {
		t.Fatalf("Expected update request to be V0044JobDescMsg, got %T", updatedJobs[0].Req)
		return
	}
	if reqMsg.Constraints == nil {
		t.Fatalf("Expected Constraints to be set")
		return
	}
	wantConstraints := "(k8s-id-123456)&k8s-id-12345"
	if *reqMsg.Constraints != wantConstraints {
		t.Errorf("Constraints = %q, want %q", *reqMsg.Constraints, wantConstraints)
	}
}

func TestReconcile_Metrics_CreationAndCompletion(t *testing.T) {
	job := newReconTestJob().
		withNodes(2).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		build()

	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	initDiscovered := testutil.ToFloat64(metrics.JobsDiscovered.WithLabelValues(reconWmName, reconPartitionName))
	initCreated := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusSuccess, metrics.ReasonJobSetCreated, reconWmName, reconPartitionName))

	// 1. Reconcile new job creation
	res, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("Expected empty result, got %v", res)
	}

	if val := testutil.ToFloat64(metrics.JobsDiscovered.WithLabelValues(reconWmName, reconPartitionName)); val != initDiscovered+1 {
		t.Errorf("Expected JobsDiscovered %f, got %f", initDiscovered+1, val)
	}
	if val := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusSuccess, metrics.ReasonJobSetCreated, reconWmName, reconPartitionName)); val != initCreated+1 {
		t.Errorf("Expected JobsSync (created) %f, got %f", initCreated+1, val)
	}
	if val := testutil.ToFloat64(metrics.ActiveJobs.WithLabelValues(reconWmName, reconPartitionName)); val != 1.0 {
		t.Errorf("Expected ActiveJobs 1.0, got %f", val)
	}

	// 2. JobSet completes
	completedJobSet := newReconTestJobSet().
		withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
		build()

	envCompleted := newReconcileTestEnv(t,
		withExtraObjects(completedJobSet),
		withPendingSlurmJobs(job),
	)

	initCompleted := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusSuccess, metrics.ReasonJobSetCompleted, reconWmName, reconPartitionName))
	_, err = envCompleted.reconcile()
	if err != nil {
		t.Fatalf("Reconcile on completed JobSet failed: %v", err)
	}

	if val := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusSuccess, metrics.ReasonJobSetCompleted, reconWmName, reconPartitionName)); val != initCompleted+1 {
		t.Errorf("Expected JobsSync (completed) %f, got %f", initCompleted+1, val)
	}
	if val := testutil.ToFloat64(metrics.ActiveJobs.WithLabelValues(reconWmName, reconPartitionName)); val != 0.0 {
		t.Errorf("Expected ActiveJobs 0.0 after completion, got %f", val)
	}
}

func TestReconcile_Metrics_TranslationError(t *testing.T) {
	job := newReconTestJob().
		withNodes(2).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		withTresPerNode("gres/gpu:a100=1").
		build()

	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	initErrors := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusError, metrics.ReasonTranslationError, reconWmName, reconPartitionName))

	_, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if val := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusError, metrics.ReasonTranslationError, reconWmName, reconPartitionName)); val != initErrors+1 {
		t.Errorf("Expected JobsSync (translation_error) %f, got %f", initErrors+1, val)
	}
}

func TestReconcile_Metrics_JobSetFailed(t *testing.T) {
	failedJobSet := newReconTestJobSet().
		withCondition(string(jobsetv1alpha2.JobSetFailed), metav1.ConditionTrue).
		build()

	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		build()

	env := newReconcileTestEnv(t,
		withExtraObjects(failedJobSet),
		withPendingSlurmJobs(job),
	)

	initFailed := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusError, metrics.ReasonJobSetFailed, reconWmName, reconPartitionName))

	_, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if val := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusError, metrics.ReasonJobSetFailed, reconWmName, reconPartitionName)); val != initFailed+1 {
		t.Errorf("Expected JobsSync (jobset_failed) %f, got %f", initFailed+1, val)
	}
}

func TestReconcile_Metrics_JobSetMissing(t *testing.T) {
	// Job is in RUNNING state (not PENDING+Hold) and has no JobSet
	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStateRUNNING).
		withHold(false).
		build()

	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	initActive := testutil.ToFloat64(metrics.ActiveJobs.WithLabelValues(reconWmName, reconPartitionName))
	initFailed := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusError, metrics.ReasonJobSetFailed, reconWmName, reconPartitionName))

	_, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if val := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusError, metrics.ReasonJobSetFailed, reconWmName, reconPartitionName)); val != initFailed+1 {
		t.Errorf("Expected JobsSync (jobset_failed) %f, got %f", initFailed+1, val)
	}
	if val := testutil.ToFloat64(metrics.ActiveJobs.WithLabelValues(reconWmName, reconPartitionName)); val != initActive {
		t.Errorf("Expected ActiveJobs to remain %f, got %f", initActive, val)
	}
}

func TestReconcile_Metrics_ExistingJobSet_NoDoubleCount(t *testing.T) {
	existingJobSet := newReconTestJobSet().build()
	job := newReconTestJob().
		withNodes(2).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		build()

	env := newReconcileTestEnv(t,
		withExtraObjects(existingJobSet),
		withPendingSlurmJobs(job),
	)

	initCreated := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusSuccess, metrics.ReasonJobSetCreated, reconWmName, reconPartitionName))
	initActive := testutil.ToFloat64(metrics.ActiveJobs.WithLabelValues(reconWmName, reconPartitionName))

	_, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if val := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusSuccess, metrics.ReasonJobSetCreated, reconWmName, reconPartitionName)); val != initCreated {
		t.Errorf("Expected JobsSync (created) to remain %f, got %f", initCreated, val)
	}
	if val := testutil.ToFloat64(metrics.ActiveJobs.WithLabelValues(reconWmName, reconPartitionName)); val != initActive {
		t.Errorf("Expected ActiveJobs to remain %f, got %f", initActive, val)
	}
}

func TestCountActiveJobSets(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	const activeUID = "test-active-wm-uid"

	wm := &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconWmName,
			Namespace: reconNamespace,
			UID:       activeUID,
		},
	}

	activeJS := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "active-js",
			Namespace: reconNamespace,
			Labels:    map[string]string{LabelSlurmPartition: reconPartitionName},
			OwnerReferences: []metav1.OwnerReference{
				{Kind: KindWorkloadMixing, Name: reconWmName, UID: activeUID},
			},
		},
	}

	mismatchedUIDJS := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mismatched-uid-js",
			Namespace: reconNamespace,
			Labels:    map[string]string{LabelSlurmPartition: reconPartitionName},
			OwnerReferences: []metav1.OwnerReference{
				{Kind: KindWorkloadMixing, Name: reconWmName, UID: "stale-uid"},
			},
		},
	}

	completedJS := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "completed-js",
			Namespace: reconNamespace,
			Labels:    map[string]string{LabelSlurmPartition: reconPartitionName},
			OwnerReferences: []metav1.OwnerReference{
				{Kind: KindWorkloadMixing, Name: reconWmName, UID: activeUID},
			},
		},
		Status: jobsetv1alpha2.JobSetStatus{
			Conditions: []metav1.Condition{
				{Type: string(jobsetv1alpha2.JobSetCompleted), Status: metav1.ConditionTrue},
			},
		},
	}

	now := metav1.Now()
	deletedJS := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "deleted-js",
			Namespace:         reconNamespace,
			Labels:            map[string]string{LabelSlurmPartition: reconPartitionName},
			DeletionTimestamp: &now,
			Finalizers:        []string{testFinalizer},
			OwnerReferences: []metav1.OwnerReference{
				{Kind: KindWorkloadMixing, Name: reconWmName, UID: activeUID},
			},
		},
	}

	t.Run("Counts only active owned JobSets", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(activeJS, mismatchedUIDJS, completedJS, deletedJS).Build()
		count, err := CountActiveJobSets(context.Background(), fakeClient, wm, reconPartitionName)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected count 1, got %d", count)
		}
	})

	t.Run("Respects excludedNames", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(activeJS).Build()
		count, err := CountActiveJobSets(context.Background(), fakeClient, wm, reconPartitionName, "active-js")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected count 0 with excludedNames, got %d", count)
		}
	})

	t.Run("Returns error when List fails", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithInterceptorFuncs(interceptor.Funcs{
				List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					return fmt.Errorf("simulated apiserver list error")
				},
			}).
			Build()
		count, err := CountActiveJobSets(context.Background(), fakeClient, wm, reconPartitionName)
		if err == nil {
			t.Fatal("Expected error from CountActiveJobSets when client.List fails, got nil")
		}
		if count != 0 {
			t.Errorf("Expected count 0 on error, got %d", count)
		}
		if !strings.Contains(err.Error(), "simulated apiserver list error") {
			t.Errorf("Expected error to wrap list error, got: %v", err)
		}
	})
}

func newObservedLoggerContext(ctx context.Context) (context.Context, *observer.ObservedLogs) {
	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(zap.UseDevMode(true), zap.RawZapOpts(uberzap.WrapCore(func(zapcore.Core) zapcore.Core {
		return core
	})))
	return logf.IntoContext(ctx, logger), recorded
}

func TestRequestLogger(t *testing.T) {
	tests := []struct {
		name               string
		req                *reconcile.Request
		wantJobID          any
		wantWorkloadMixing bool
		wantRequestFields  bool
	}{
		{
			name:              "nil request keeps base logger",
			req:               nil,
			wantRequestFields: false,
		},
		{
			name: "valid request adds jobID and workloadMixing",
			req: &reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: reconNamespace, Name: "slurm-job-" + reconWmName + "-" + reconJobIDStr,
			}},
			wantJobID:          reconJobIDStr,
			wantWorkloadMixing: true,
			wantRequestFields:  true,
		},
		{
			name: "unparsable request keeps request fields only",
			req: &reconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: reconNamespace, Name: "unrelated-name",
			}},
			wantRequestFields: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, recorded := newObservedLoggerContext(t.Context())
			requestLogger(logf.FromContext(ctx), tc.req).Info("test")

			entries := recorded.All()
			if len(entries) != 1 {
				t.Fatalf("Expected 1 log entry, got %d", len(entries))
			}
			fields := entries[0].ContextMap()

			if got := fields["jobID"]; got != tc.wantJobID {
				t.Errorf("Expected jobID %v, got %v", tc.wantJobID, got)
			}
			got, hasWM := fields["workloadMixing"]
			if hasWM != tc.wantWorkloadMixing {
				t.Errorf("Expected workloadMixing present=%v, got fields %v", tc.wantWorkloadMixing, fields)
			}
			if hasWM && (!strings.Contains(fmt.Sprint(got), reconWmName) || !strings.Contains(fmt.Sprint(got), reconNamespace)) {
				t.Errorf("Expected workloadMixing %s/%s, got %v", reconNamespace, reconWmName, got)
			}
			_, hasName := fields["name"]
			_, hasNamespace := fields["namespace"]
			_, hasObject := fields["object"]
			if hasName != tc.wantRequestFields || hasNamespace != tc.wantRequestFields || hasObject != tc.wantRequestFields {
				t.Errorf("Expected request fields present=%v, got fields %v", tc.wantRequestFields, fields)
			}
		})
	}
}

func TestReconcile_NewJobLogOnlyOnInitialAdmission(t *testing.T) {
	job := newReconTestJob().
		withNodes(2).
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		withStateReason(ReasonPriority).
		build()

	env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

	ctx, recorded := newObservedLoggerContext(t.Context())

	res, err := env.reconcile(ctx)
	if err != nil {
		t.Fatalf("First reconcile failed: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("Expected empty result, got %v", res)
	}

	js := env.verifyJobSetCreated("some-queue", reconJobIDStr, reconWmName)
	if js == nil {
		t.Fatal("Expected non-nil JobSet")
	}

	if got := recorded.FilterMessage("New Slurm job appeared").Len(); got != 1 {
		t.Errorf("Expected 1 'New Slurm job appeared' log entry on first reconcile, got %d", got)
	}
	if got := recorded.FilterMessage("Created Kubernetes JobSet for Slurm job").Len(); got != 1 {
		t.Errorf("Expected 1 'Created Kubernetes JobSet for Slurm job' log entry on first reconcile, got %d", got)
	}
	if got := recorded.FilterMessage("Released hold on Slurm job").Len(); got != 1 {
		t.Errorf("Expected 1 'Released hold on Slurm job' log entry on first reconcile, got %d", got)
	}
	for _, entry := range recorded.All() {
		if entry.ContextMap()["jobID"] != reconJobIDStr {
			t.Errorf("Expected log entry %q to carry jobID=%q, got %v", entry.Message, reconJobIDStr, entry.ContextMap())
		}
	}

	recorded.TakeAll()

	res, err = env.reconcile(ctx)
	if err != nil {
		t.Fatalf("Second reconcile failed: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("Expected empty result on second reconcile, got %v", res)
	}

	if got := recorded.FilterMessage("New Slurm job appeared").Len(); got != 0 {
		t.Errorf("Did not expect 'New Slurm job appeared' on subsequent reconcile when JobSet exists, got %d", got)
	}
}

func TestReconcile_NewJob_ExplicitHold_DefersWithoutLoggingOrMetrics(t *testing.T) {
	tests := []struct {
		name        string
		stateReason string
		comment     string
	}{
		{
			name:        "user hold without auto-hold marker",
			stateReason: ReasonJobHeldUser,
			comment:     "held by user",
		},
		{
			name:        "admin hold",
			stateReason: ReasonJobHeldAdmin,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := newReconTestJob().
				withNodes(2).
				withJobState(slurmapi.V0044JobInfoJobStatePENDING).
				withHold(true).
				withStateReason(tc.stateReason).
				withComment(tc.comment).
				build()

			env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

			ctx, recorded := newObservedLoggerContext(t.Context())

			initDiscovered := testutil.ToFloat64(metrics.JobsDiscovered.WithLabelValues(reconWmName, reconPartitionName))

			res, err := env.reconcile(ctx)
			if err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}
			if res != (ctrl.Result{}) {
				t.Errorf("Expected empty result, got %v", res)
			}

			env.verifyJobSetNotFound()
			env.verifyNoSlurmUpdates()

			if got := recorded.FilterMessage("Job is explicitly held by user or admin, deferring reconciliation").Len(); got != 1 {
				t.Errorf("Expected 1 'Job is explicitly held by user or admin, deferring reconciliation' log entry, got %d", got)
			}
			if got := recorded.FilterMessage("New Slurm job appeared").Len(); got != 0 {
				t.Errorf("Did not expect 'New Slurm job appeared' for explicitly held job, got %d", got)
			}

			if val := testutil.ToFloat64(metrics.JobsDiscovered.WithLabelValues(reconWmName, reconPartitionName)); val != initDiscovered {
				t.Errorf("Expected JobsDiscovered to remain %f, got %f", initDiscovered, val)
			}
		})
	}
}

func TestReconcile_EmptyWMName_SkipsWithoutNewJobLog(t *testing.T) {
	env := newReconcileTestEnv(t)
	env.req.Name = "slurm-job-" + reconJobIDStr

	ctx, recorded := newObservedLoggerContext(t.Context())

	res, err := env.reconcile(ctx)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Errorf("Expected empty result, got %v", res)
	}

	if got := recorded.FilterMessage("Skipping Slurm job reconciliation").Len(); got != 1 {
		t.Errorf("Expected 1 'Skipping Slurm job reconciliation' log entry, got %d", got)
	}
	if got := recorded.FilterMessage("New Slurm job appeared").Len(); got != 0 {
		t.Errorf("Did not expect 'New Slurm job appeared' when WorkloadMixing name is empty, got %d", got)
	}
}

func assertContextBoundedBySlurmTimeout(t *testing.T, ctx context.Context, op string) {
	t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Errorf("Expected context passed to %s to have a deadline", op)
		return
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > defaultSlurmCallTimeout {
		t.Errorf("Expected %s context deadline within (0, %v], got %v", op, defaultSlurmCallTimeout, remaining)
	}
}

func TestReconcile_SlurmCallContextDeadlines(t *testing.T) {
	t.Run("fetchState Get and releaseHold Update set per-call timeout", func(t *testing.T) {
		job := newReconTestJob().
			withNodes(2).
			withJobState(slurmapi.V0044JobInfoJobStatePENDING).
			withHold(true).
			build()

		env := newReconcileTestEnv(t, withPendingSlurmJobs(job))

		var getCalled, updateCalled bool
		wrappedClient := slurmclientinterceptor.NewClient(env.mockSlurm, slurmclientinterceptor.Funcs{
			Get: func(ctx context.Context, key object.ObjectKey, obj object.Object, opts ...slurmclient.GetOption) error {
				getCalled = true
				assertContextBoundedBySlurmTimeout(t, ctx, "Get")
				return env.mockSlurm.Get(ctx, key, obj, opts...)
			},
			Update: func(ctx context.Context, obj object.Object, req any, opts ...slurmclient.UpdateOption) error {
				updateCalled = true
				assertContextBoundedBySlurmTimeout(t, ctx, "releaseHold Update")
				return env.mockSlurm.Update(ctx, obj, req, opts...)
			},
		})
		env.clientMap.Add(t.Context(), types.NamespacedName{Namespace: reconNamespace, Name: reconControllerName}, wrappedClient)

		if _, err := env.reconcile(); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}
		if !getCalled {
			t.Error("Expected Slurm Get to be called")
		}
		if !updateCalled {
			t.Error("Expected Slurm Update to be called for releaseHold")
		}
	})

	t.Run("cancelJobWithComment Update and Delete set independent per-call timeouts", func(t *testing.T) {
		completedJobSet := newReconTestJobSet().
			withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
			build()

		job := newReconTestJob().
			withJobState(slurmapi.V0044JobInfoJobStatePENDING).
			withHold(true).
			build()

		env := newReconcileTestEnv(t,
			withExtraObjects(completedJobSet),
			withPendingSlurmJobs(job),
		)

		var updateCalled, deleteCalled bool
		var updateCtxCaptured context.Context
		wrappedClient := slurmclientinterceptor.NewClient(env.mockSlurm, slurmclientinterceptor.Funcs{
			Update: func(ctx context.Context, obj object.Object, req any, opts ...slurmclient.UpdateOption) error {
				updateCalled = true
				updateCtxCaptured = ctx
				assertContextBoundedBySlurmTimeout(t, ctx, "cancelJobWithComment Update")
				return env.mockSlurm.Update(ctx, obj, req, opts...)
			},
			Delete: func(ctx context.Context, obj object.Object, opts ...slurmclient.DeleteOption) error {
				deleteCalled = true
				if updateCtxCaptured == nil || updateCtxCaptured.Err() == nil {
					t.Error("Expected Update context to be cancelled before Delete is invoked")
				}
				assertContextBoundedBySlurmTimeout(t, ctx, "cancelJobWithComment Delete")
				return env.mockSlurm.Delete(ctx, obj, opts...)
			},
		})
		env.clientMap.Add(t.Context(), types.NamespacedName{Namespace: reconNamespace, Name: reconControllerName}, wrappedClient)

		if _, err := env.reconcile(); err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}
		if !updateCalled {
			t.Error("Expected Slurm Update to be called for comment")
		}
		if !deleteCalled {
			t.Error("Expected Slurm Delete to be called for job cancellation")
		}
	})
}

func TestReconcile_SlurmCallTimeout_BlockedCallsAbort(t *testing.T) {
	const testCallTimeout = 10 * time.Millisecond

	t.Run("fetchState Get aborts with DeadlineExceeded when server hangs", func(t *testing.T) {
		env := newReconcileTestEnv(t)
		env.reconciler.slurmCallTimeout = testCallTimeout
		wrappedClient := slurmclientinterceptor.NewClient(env.mockSlurm, slurmclientinterceptor.Funcs{
			Get: func(ctx context.Context, key object.ObjectKey, obj object.Object, opts ...slurmclient.GetOption) error {
				<-ctx.Done()
				return ctx.Err()
			},
		})
		env.clientMap.Add(t.Context(), types.NamespacedName{Namespace: reconNamespace, Name: reconControllerName}, wrappedClient)

		_, err := env.reconcile()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expected error wrapping context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("releaseHold Update aborts with DeadlineExceeded when server hangs", func(t *testing.T) {
		job := newReconTestJob().
			withNodes(1).
			withJobState(slurmapi.V0044JobInfoJobStatePENDING).
			withHold(true).
			build()

		env := newReconcileTestEnv(t, withPendingSlurmJobs(job))
		env.reconciler.slurmCallTimeout = testCallTimeout
		wrappedClient := slurmclientinterceptor.NewClient(env.mockSlurm, slurmclientinterceptor.Funcs{
			Update: func(ctx context.Context, obj object.Object, req any, opts ...slurmclient.UpdateOption) error {
				<-ctx.Done()
				return ctx.Err()
			},
		})
		env.clientMap.Add(t.Context(), types.NamespacedName{Namespace: reconNamespace, Name: reconControllerName}, wrappedClient)

		_, err := env.reconcile()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expected error wrapping context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("cancelJobWithComment Update timeout still allows Delete to succeed", func(t *testing.T) {
		completedJobSet := newReconTestJobSet().
			withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
			build()

		job := newReconTestJob().
			withJobState(slurmapi.V0044JobInfoJobStatePENDING).
			withHold(true).
			build()

		env := newReconcileTestEnv(t,
			withExtraObjects(completedJobSet),
			withPendingSlurmJobs(job),
		)
		env.reconciler.slurmCallTimeout = testCallTimeout

		var deleteCalled bool
		wrappedClient := slurmclientinterceptor.NewClient(env.mockSlurm, slurmclientinterceptor.Funcs{
			Update: func(ctx context.Context, obj object.Object, req any, opts ...slurmclient.UpdateOption) error {
				<-ctx.Done()
				return ctx.Err()
			},
			Delete: func(ctx context.Context, obj object.Object, opts ...slurmclient.DeleteOption) error {
				deleteCalled = true
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > testCallTimeout {
					t.Errorf("Expected Delete context to have fresh deadline within (0, %v]", testCallTimeout)
				}
				return env.mockSlurm.Delete(ctx, obj, opts...)
			},
		})
		env.clientMap.Add(t.Context(), types.NamespacedName{Namespace: reconNamespace, Name: reconControllerName}, wrappedClient)

		if _, err := env.reconcile(); err != nil {
			t.Fatalf("Expected Reconcile to succeed when only comment Update times out, got: %v", err)
		}
		if !deleteCalled {
			t.Fatal("Expected Delete to be called after Update timed out")
		}
	})

	t.Run("cancelJobWithComment Delete aborts with DeadlineExceeded when server hangs", func(t *testing.T) {
		completedJobSet := newReconTestJobSet().
			withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
			build()

		job := newReconTestJob().
			withJobState(slurmapi.V0044JobInfoJobStatePENDING).
			withHold(true).
			build()

		env := newReconcileTestEnv(t,
			withExtraObjects(completedJobSet),
			withPendingSlurmJobs(job),
		)
		env.reconciler.slurmCallTimeout = testCallTimeout

		wrappedClient := slurmclientinterceptor.NewClient(env.mockSlurm, slurmclientinterceptor.Funcs{
			Delete: func(ctx context.Context, obj object.Object, opts ...slurmclient.DeleteOption) error {
				<-ctx.Done()
				return ctx.Err()
			},
		})
		env.clientMap.Add(t.Context(), types.NamespacedName{Namespace: reconNamespace, Name: reconControllerName}, wrappedClient)

		_, err := env.reconcile()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expected error wrapping context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("real slurmclient aborts over HTTP when server hangs", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}))
		defer srv.Close()

		realClient, err := slurmclient.NewClient(&slurmclient.Config{
			Server:    srv.URL,
			AuthToken: "test-token",
		}, &slurmclient.ClientOptions{
			DisableFor: []object.Object{
				&slurmclienttypes.V0044JobInfo{},
			},
		})
		if err != nil {
			t.Fatalf("Failed to create real slurmclient: %v", err)
		}

		env := newReconcileTestEnv(t)
		env.reconciler.slurmCallTimeout = 50 * time.Millisecond
		env.clientMap.Add(t.Context(), types.NamespacedName{Namespace: reconNamespace, Name: reconControllerName}, realClient)

		_, err = env.reconcile()
		if err == nil {
			t.Fatal("Expected Reconcile to fail when HTTP server hangs, got nil")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expected error wrapping context.DeadlineExceeded, got: %v", err)
		}
	})

	t.Run("reconciliationTimeout short-circuits multi-node cleanupDynamicNodes when server hangs", func(t *testing.T) {
		const totalNodes = 20
		completedJobSet := newReconTestJobSet().
			withSlurmdReplicas(totalNodes).
			withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
			build()

		job := newReconTestJob().
			withJobState(slurmapi.V0044JobInfoJobStateRUNNING).
			withHold(false).
			build()

		env := newReconcileTestEnv(t,
			withExtraObjects(completedJobSet),
			withPendingSlurmJobs(job),
		)
		env.reconciler.slurmCallTimeout = 15 * time.Millisecond
		env.reconciler.reconciliationTimeout = 35 * time.Millisecond

		var nodeDeleteAttempts int
		wrappedClient := slurmclientinterceptor.NewClient(env.mockSlurm, slurmclientinterceptor.Funcs{
			Delete: func(ctx context.Context, obj object.Object, opts ...slurmclient.DeleteOption) error {
				if _, ok := obj.(*slurmclienttypes.V0044Node); ok {
					nodeDeleteAttempts++
					<-ctx.Done()
					return ctx.Err()
				}
				return env.mockSlurm.Delete(ctx, obj, opts...)
			},
		})
		env.clientMap.Add(t.Context(), types.NamespacedName{Namespace: reconNamespace, Name: reconControllerName}, wrappedClient)

		_, err := env.reconcile()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expected error wrapping context.DeadlineExceeded, got: %v", err)
		}
		if nodeDeleteAttempts == 0 || nodeDeleteAttempts >= totalNodes {
			t.Errorf("Expected reconciliationTimeout to short-circuit node cleanup before all %d nodes were attempted, got %d attempts", totalNodes, nodeDeleteAttempts)
		}
	})
}

func TestJobSetWatchPredicate(t *testing.T) {
	pred := jobSetWatchPredicate()

	t.Run("Create", func(t *testing.T) {
		tests := []struct {
			name string
			obj  *jobsetv1alpha2.JobSet
			want bool
		}{
			{
				name: "Nil object is ignored",
				obj:  nil,
				want: false,
			},
			{
				name: "Newly created suspended non-terminal JobSet is ignored",
				obj:  newReconTestJobSet().withSuspend(true).build(),
				want: false,
			},
			{
				name: "Unsuspended JobSet is allowed",
				obj:  newReconTestJobSet().withSuspend(false).build(),
				want: true,
			},
			{
				name: "Completed JobSet is allowed",
				obj: newReconTestJobSet().
					withSuspend(true).
					withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
					build(),
				want: true,
			},
			{
				name: "Failed JobSet is allowed",
				obj: newReconTestJobSet().
					withSuspend(true).
					withCondition(string(jobsetv1alpha2.JobSetFailed), metav1.ConditionTrue).
					build(),
				want: true,
			},
			{
				name: "Deleting suspended JobSet is allowed",
				obj: newReconTestJobSet().
					withSuspend(true).
					withDeletionTimestamp().
					build(),
				want: true,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got := pred.Create(event.TypedCreateEvent[*jobsetv1alpha2.JobSet]{Object: tc.obj})
				if got != tc.want {
					t.Errorf("Create() = %v, want %v", got, tc.want)
				}
			})
		}
	})

	t.Run("Update", func(t *testing.T) {
		tests := []struct {
			name   string
			oldObj *jobsetv1alpha2.JobSet
			newObj *jobsetv1alpha2.JobSet
			want   bool
		}{
			{
				name:   "Nil ObjectOld is ignored",
				oldObj: nil,
				newObj: newReconTestJobSet().build(),
				want:   false,
			},
			{
				name:   "Nil ObjectNew is ignored",
				oldObj: newReconTestJobSet().build(),
				newObj: nil,
				want:   false,
			},
			{
				name:   "Non-terminal status replica update is ignored",
				oldObj: newReconTestJobSet().withSuspend(false).build(),
				newObj: func() *jobsetv1alpha2.JobSet {
					js := newReconTestJobSet().withSuspend(false).build()
					js.Status.ReplicatedJobsStatus = []jobsetv1alpha2.ReplicatedJobStatus{
						{Name: ContainerSlurmd, Ready: 2, Active: 2},
					}
					return js
				}(),
				want: false,
			},
			{
				name: "Transition to JobSetCompleted condition is allowed",
				oldObj: newReconTestJobSet().
					withSuspend(false).
					build(),
				newObj: newReconTestJobSet().
					withSuspend(false).
					withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
					build(),
				want: true,
			},
			{
				name: "Transition to JobSetFailed condition is allowed",
				oldObj: newReconTestJobSet().
					withSuspend(false).
					build(),
				newObj: newReconTestJobSet().
					withSuspend(false).
					withCondition(string(jobsetv1alpha2.JobSetFailed), metav1.ConditionTrue).
					build(),
				want: true,
			},
			{
				name:   "Transition in spec.suspend is allowed",
				oldObj: newReconTestJobSet().withSuspend(true).build(),
				newObj: newReconTestJobSet().withSuspend(false).build(),
				want:   true,
			},
			{
				name:   "Transition in deletionTimestamp on non-terminal JobSet is allowed",
				oldObj: newReconTestJobSet().withSuspend(false).build(),
				newObj: newReconTestJobSet().
					withSuspend(false).
					withDeletionTimestamp().
					build(),
				want: true,
			},
			{
				name: "Transition in deletionTimestamp on terminal JobSet carrying FinalizerJobSet is allowed",
				oldObj: newReconTestJobSet().
					withSuspend(false).
					withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
					build(),
				newObj: newReconTestJobSet().
					withSuspend(false).
					withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
					withDeletionTimestamp().
					build(),
				want: true,
			},
			{
				name: "Transition in deletionTimestamp on terminal JobSet without FinalizerJobSet is ignored",
				oldObj: newReconTestJobSet().
					withFinalizers(testFinalizer).
					withSuspend(false).
					withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
					build(),
				newObj: newReconTestJobSet().
					withFinalizers(testFinalizer).
					withSuspend(false).
					withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
					withDeletionTimestamp().
					build(),
				want: false,
			},
			{
				name: "Subsequent status update on already-deleting JobSet is ignored",
				oldObj: newReconTestJobSet().
					withSuspend(false).
					withDeletionTimestamp().
					build(),
				newObj: func() *jobsetv1alpha2.JobSet {
					js := newReconTestJobSet().
						withSuspend(false).
						withDeletionTimestamp().
						build()
					js.Status.ReplicatedJobsStatus = []jobsetv1alpha2.ReplicatedJobStatus{
						{Name: ContainerSlurmd, Ready: 0, Active: 1},
					}
					return js
				}(),
				want: false,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got := pred.Update(event.TypedUpdateEvent[*jobsetv1alpha2.JobSet]{
					ObjectOld: tc.oldObj,
					ObjectNew: tc.newObj,
				})
				if got != tc.want {
					t.Errorf("Update() = %v, want %v", got, tc.want)
				}
			})
		}
	})

	t.Run("Delete", func(t *testing.T) {
		tests := []struct {
			name string
			obj  *jobsetv1alpha2.JobSet
			want bool
		}{
			{
				name: "Nil Object is ignored",
				obj:  nil,
				want: false,
			},
			{
				name: "Non-terminal JobSet is allowed",
				obj:  newReconTestJobSet().build(),
				want: true,
			},
			{
				name: "Completed JobSet is ignored",
				obj: newReconTestJobSet().
					withCondition(string(jobsetv1alpha2.JobSetCompleted), metav1.ConditionTrue).
					build(),
				want: false,
			},
			{
				name: "Failed JobSet is ignored",
				obj: newReconTestJobSet().
					withCondition(string(jobsetv1alpha2.JobSetFailed), metav1.ConditionTrue).
					build(),
				want: false,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got := pred.Delete(event.TypedDeleteEvent[*jobsetv1alpha2.JobSet]{Object: tc.obj})
				if got != tc.want {
					t.Errorf("Delete() = %v, want %v", got, tc.want)
				}
			})
		}
	})

	t.Run("Generic", func(t *testing.T) {
		js := newReconTestJobSet().build()
		if pred.Generic(event.TypedGenericEvent[*jobsetv1alpha2.JobSet]{Object: js}) {
			t.Errorf("Generic() = true, want false")
		}
	})
}
