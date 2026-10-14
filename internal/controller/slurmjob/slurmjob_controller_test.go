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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/clientmap"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/utils/slurmtypes"
	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

func TestReconcile_ExplicitHold_SuspendsExistingJobSet(t *testing.T) {
	js := newReconTestJobSet().
		withSuspend(false).
		build()

	job := newReconTestJob().
		withJobState(slurmapi.V0044JobInfoJobStatePENDING).
		withHold(true).
		withStateReason("JobHeldUser").
		build()

	env := newReconcileTestEnv(t,
		withExtraObjects(js),
		withPendingSlurmJobs(job),
	)

	_, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	env.verifyJobSetSuspended(true, true)
	env.verifyNoSlurmUpdates()
}

func TestReconcile_ExplicitHold_Removed_ResumesJobSet(t *testing.T) {
	js := newReconTestJobSet().
		withAnnotations(map[string]string{
			AnnotationSuspendedBySlurmHold: ValueTrue,
		}).
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

	_, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	env.verifyJobSetSuspended(false, false)
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

func TestReconcile_SuspendedJobSet_WithoutAnnotation_NotResumed(t *testing.T) {
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

	_, err := env.reconcile()
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	env.verifyJobSetSuspended(true, false)
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
