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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/clientmap"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/utils/slurmtypes"
	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	slurmclientfake "github.com/SlinkyProject/slurm-client/pkg/client/fake"
	slurmclientinterceptor "github.com/SlinkyProject/slurm-client/pkg/client/interceptor"
	"github.com/SlinkyProject/slurm-client/pkg/object"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
)

const (
	testBatchPartition  = "batch"
	testJobSet101Name   = "slurm-job-101"
	testJobSet102Name   = "slurm-job-102"
	testJobSet103Name   = "slurm-job-103"
	testWMUID           = "wm-uid"
	testFinalizer       = "test-finalizer"
	testNode101Slurmd00 = "slurm-job-101-slurmd-0-0"
	testNode101Slurmd10 = "slurm-job-101-slurmd-1-0"
)

func TestJobTeardownReconciler_Interfaces(t *testing.T) {
	var r any = &JobTeardownReconciler{}
	if _, ok := r.(manager.Runnable); !ok {
		t.Fatalf("JobTeardownReconciler must implement manager.Runnable")
	}
	if l, ok := r.(manager.LeaderElectionRunnable); !ok {
		t.Fatalf("JobTeardownReconciler must implement manager.LeaderElectionRunnable")
	} else if !l.NeedLeaderElection() {
		t.Fatalf("NeedLeaderElection() must return true")
	}
}

func TestJobTeardownReconciler_Start_ContextCancelled(t *testing.T) {
	teardownChan := make(chan slurmtypes.SlurmJobTeardownEvent)
	reconciler := &JobTeardownReconciler{
		Client:         fake.NewClientBuilder().Build(),
		ClientMap:      clientmap.NewClientMap(),
		Recorder:       record.NewFakeRecorder(10),
		TeardownEvents: teardownChan,
	}

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- reconciler.Start(ctx)
	}()

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Expected nil error on context cancel, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not stop after context cancel")
	}
}

func TestJobTeardownReconciler_Start_ChannelClosed(t *testing.T) {
	teardownChan := make(chan slurmtypes.SlurmJobTeardownEvent)
	reconciler := &JobTeardownReconciler{
		Client:         fake.NewClientBuilder().Build(),
		ClientMap:      clientmap.NewClientMap(),
		Recorder:       record.NewFakeRecorder(10),
		TeardownEvents: teardownChan,
	}

	ctx := context.Background()

	errCh := make(chan error, 1)
	go func() {
		errCh <- reconciler.Start(ctx)
	}()

	close(teardownChan)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Expected nil error on channel close, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not stop after channel close")
	}
}

func TestJobTeardownReconciler_ConsumeEvent(t *testing.T) {
	teardownChan := make(chan slurmtypes.SlurmJobTeardownEvent, 1)
	handled := make(chan struct{})
	reconciler := &JobTeardownReconciler{
		Client:         fake.NewClientBuilder().Build(),
		ClientMap:      clientmap.NewClientMap(),
		Recorder:       record.NewFakeRecorder(10),
		TeardownEvents: teardownChan,
		onEventHandled: func(ev slurmtypes.SlurmJobTeardownEvent) {
			close(handled)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- reconciler.Start(ctx)
	}()

	teardownChan <- slurmtypes.SlurmJobTeardownEvent{
		JobID: "101",
		WorkloadMixing: types.NamespacedName{
			Namespace: testNamespace,
			Name:      reconWmName,
		},
		Partition: testBatchPartition,
		Reason:    metrics.ReasonJobCompleted,
	}

	select {
	case <-handled:
	case <-time.After(2 * time.Second):
		t.Fatal("Start timed out waiting for event to be handled")
	}
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Expected nil error, got: %v", err)
		}
	case <-time.After(2 * time.Second):
	}
}

func TestJobTeardownReconciler_HandleTeardown_Success(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	wm := &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconWmName,
			Namespace: testNamespace,
		},
		Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
			PartitionsMapping: []k8sbridgev1alpha1.PartitionMapping{
				{Partition: testBatchPartition, LocalQueue: testNamespace},
			},
		},
	}

	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testJobSet101Name,
			Namespace: testNamespace,
			Labels: map[string]string{
				LabelSlurmJobID:     "101",
				LabelSlurmPartition: testBatchPartition,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
					Kind:       KindWorkloadMixing,
					Name:       reconWmName,
					UID:        testWMUID,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
	fakeRecorder := record.NewFakeRecorder(10)

	reconciler := &JobTeardownReconciler{
		Client:   fakeClient,
		Recorder: fakeRecorder,
	}

	metrics.SetActiveJobs(wm.Name, testBatchPartition, 1)
	initialSync := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusSuccess, metrics.ReasonJobCompleted, wm.Name, testBatchPartition))

	ev := slurmtypes.SlurmJobTeardownEvent{
		JobID: "101",
		WorkloadMixing: types.NamespacedName{
			Namespace: testNamespace,
			Name:      reconWmName,
		},
		Partition: testBatchPartition,
		Reason:    metrics.ReasonJobCompleted,
	}

	err := reconciler.handleTeardown(context.Background(), ev)
	if err != nil {
		t.Fatalf("handleTeardown failed: %v", err)
	}

	// Verify JobSet is deleted
	checkJS := &jobsetv1alpha2.JobSet{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testJobSet101Name}, checkJS)
	if !apierrors.IsNotFound(err) {
		t.Errorf("Expected JobSet to be deleted (NotFound), got: %v", err)
	}

	// Verify Kubernetes audit event recorded on WorkloadMixing
	select {
	case event := <-fakeRecorder.Events:
		if !strings.Contains(event, "JobCompleted") {
			t.Errorf("Expected JobCompleted event, got: %s", event)
		}
	default:
		t.Error("Expected an audit event on WorkloadMixing, but none received")
	}

	// Verify JobsSync metric incremented
	newSync := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusSuccess, metrics.ReasonJobCompleted, wm.Name, testBatchPartition))
	if newSync != initialSync+1 {
		t.Errorf("Expected JobsSync %f, got %f", initialSync+1, newSync)
	}

	// Verify ActiveJobs gauge updated to 0
	activeJobs := testutil.ToFloat64(metrics.ActiveJobs.WithLabelValues(wm.Name, testBatchPartition))
	if activeJobs != 0 {
		t.Errorf("Expected ActiveJobs gauge 0, got %f", activeJobs)
	}
}

func TestJobTeardownReconciler_HandleTeardown_Reasons(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	testCases := []struct {
		reason      string
		eventReason string
	}{
		{reason: metrics.ReasonJobFailed, eventReason: "JobFailed"},
		{reason: metrics.ReasonJobCancelled, eventReason: "JobCancelled"},
		{reason: metrics.ReasonJobOrphaned, eventReason: "JobOrphaned"},
	}

	for _, tc := range testCases {
		t.Run(tc.reason, func(t *testing.T) {
			wm := &k8sbridgev1alpha1.WorkloadMixing{
				ObjectMeta: metav1.ObjectMeta{
					Name:      reconWmName,
					Namespace: testNamespace,
				},
			}
			js := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testJobSet102Name,
					Namespace: testNamespace,
					Labels: map[string]string{
						LabelSlurmJobID:     "102",
						LabelSlurmPartition: testBatchPartition,
					},
				},
			}
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
			fakeRecorder := record.NewFakeRecorder(10)
			reconciler := &JobTeardownReconciler{
				Client:   fakeClient,
				Recorder: fakeRecorder,
			}

			ev := slurmtypes.SlurmJobTeardownEvent{
				JobID: "102",
				WorkloadMixing: types.NamespacedName{
					Namespace: testNamespace,
					Name:      reconWmName,
				},
				Partition: testBatchPartition,
				Reason:    tc.reason,
			}

			if err := reconciler.handleTeardown(context.Background(), ev); err != nil {
				t.Fatalf("handleTeardown failed: %v", err)
			}

			select {
			case event := <-fakeRecorder.Events:
				if !strings.Contains(event, tc.eventReason) {
					t.Errorf("Expected event to contain %s, got: %s", tc.eventReason, event)
				}
			default:
				t.Errorf("Expected %s event, got none", tc.eventReason)
			}
		})
	}
}

func TestJobTeardownReconciler_HandleTeardown_AlreadyDeleted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	now := metav1.Now()
	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testJobSet103Name,
			Namespace:         testNamespace,
			DeletionTimestamp: &now,
			Finalizers:        []string{testFinalizer},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(js).Build()
	fakeRecorder := record.NewFakeRecorder(10)
	reconciler := &JobTeardownReconciler{
		Client:   fakeClient,
		Recorder: fakeRecorder,
	}

	ev := slurmtypes.SlurmJobTeardownEvent{
		JobID: "103",
		WorkloadMixing: types.NamespacedName{
			Namespace: testNamespace,
			Name:      reconWmName,
		},
		Partition: testBatchPartition,
		Reason:    metrics.ReasonJobCompleted,
	}

	if err := reconciler.handleTeardown(context.Background(), ev); err != nil {
		t.Fatalf("handleTeardown failed: %v", err)
	}

	// Ensure no events were emitted for already terminating JobSet
	select {
	case event := <-fakeRecorder.Events:
		t.Errorf("Unexpected event emitted for already terminating JobSet: %s", event)
	default:
	}
}

func TestJobTeardownReconciler_HandleTeardown_NotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = jobsetv1alpha2.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	reconciler := &JobTeardownReconciler{
		Client: fakeClient,
	}

	ev := slurmtypes.SlurmJobTeardownEvent{
		JobID: "999",
		WorkloadMixing: types.NamespacedName{
			Namespace: testNamespace,
			Name:      reconWmName,
		},
		Partition: testBatchPartition,
		Reason:    metrics.ReasonJobCompleted,
	}

	if err := reconciler.handleTeardown(context.Background(), ev); err != nil {
		t.Fatalf("Expected nil error on not found, got: %v", err)
	}
}

func TestJobTeardownReconciler_HandleTeardown_ParentWorkloadMixingNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "slurm-job-104",
			Namespace: testNamespace,
			Labels: map[string]string{
				LabelSlurmJobID:     "104",
				LabelSlurmPartition: testBatchPartition,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(js).Build()
	fakeRecorder := record.NewFakeRecorder(10)

	reconciler := &JobTeardownReconciler{
		Client:   fakeClient,
		Recorder: fakeRecorder,
	}

	ev := slurmtypes.SlurmJobTeardownEvent{
		JobID: "104",
		WorkloadMixing: types.NamespacedName{
			Namespace: testNamespace,
			Name:      "non-existent-wm",
		},
		Partition: testBatchPartition,
		Reason:    metrics.ReasonJobCompleted,
	}

	// Should not return an error when parent WorkloadMixing is NotFound
	err := reconciler.handleTeardown(context.Background(), ev)
	if err != nil {
		t.Fatalf("handleTeardown failed unexpectedly on parent NotFound: %v", err)
	}

	// Verify JobSet was still deleted
	checkJS := &jobsetv1alpha2.JobSet{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "slurm-job-104"}, checkJS)
	if !apierrors.IsNotFound(err) {
		t.Errorf("Expected JobSet to be deleted, got: %v", err)
	}

	// Verify no events were recorded since parent WorkloadMixing does not exist
	select {
	case event := <-fakeRecorder.Events:
		t.Errorf("Unexpected event recorded on missing WorkloadMixing: %s", event)
	default:
	}
}

func TestJobTeardownReconciler_HandleTeardown_ParentWorkloadMixingTransientError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "slurm-job-105",
			Namespace: testNamespace,
			Labels: map[string]string{
				LabelSlurmJobID:     "105",
				LabelSlurmPartition: testBatchPartition,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(js).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*k8sbridgev1alpha1.WorkloadMixing); ok {
					return fmt.Errorf("transient apiserver timeout")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	reconciler := &JobTeardownReconciler{
		Client: fakeClient,
	}

	ev := slurmtypes.SlurmJobTeardownEvent{
		JobID: "105",
		WorkloadMixing: types.NamespacedName{
			Namespace: testNamespace,
			Name:      reconWmName,
		},
		Partition: testBatchPartition,
		Reason:    metrics.ReasonJobCompleted,
	}

	err := reconciler.handleTeardown(context.Background(), ev)
	if err == nil {
		t.Fatal("Expected error on transient WorkloadMixing fetch failure, got nil")
	}
	if !strings.Contains(err.Error(), "transient apiserver timeout") {
		t.Errorf("Expected error to contain 'transient apiserver timeout', got: %v", err)
	}

	// Verify JobSet was NOT deleted so that the event can be retried on subsequent sync cycles
	checkJS := &jobsetv1alpha2.JobSet{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "slurm-job-105"}, checkJS); err != nil {
		t.Errorf("Expected JobSet to remain active (not deleted) on transient error, got err: %v", err)
	}
}

func TestDeriveNodeNames(t *testing.T) {
	testCases := []struct {
		name      string
		jobSet    *jobsetv1alpha2.JobSet
		wantNodes []string
	}{
		{
			name:      "nil JobSet",
			jobSet:    nil,
			wantNodes: nil,
		},
		{
			name: "empty ReplicatedJobs",
			jobSet: &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{Name: testJobSet101Name},
				Spec:       jobsetv1alpha2.JobSetSpec{},
			},
			wantNodes: nil,
		},
		{
			name: "single replica with default parallelism",
			jobSet: &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{Name: testJobSet101Name},
				Spec: jobsetv1alpha2.JobSetSpec{
					ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
						{
							Name:     ContainerSlurmd,
							Replicas: 1,
						},
					},
				},
			},
			wantNodes: []string{testNode101Slurmd00},
		},
		{
			name: "multiple replicas with default parallelism",
			jobSet: &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{Name: testJobSet101Name},
				Spec: jobsetv1alpha2.JobSetSpec{
					ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
						{
							Name:     ContainerSlurmd,
							Replicas: 3,
						},
					},
				},
			},
			wantNodes: []string{
				testNode101Slurmd00,
				testNode101Slurmd10,
				"slurm-job-101-slurmd-2-0",
			},
		},
		{
			name: "multiple replicas with multiple parallelism pods",
			jobSet: &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{Name: testJobSet101Name},
				Spec: jobsetv1alpha2.JobSetSpec{
					ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
						{
							Name:     ContainerSlurmd,
							Replicas: 2,
							Template: batchv1.JobTemplateSpec{
								Spec: batchv1.JobSpec{
									Parallelism: ptr.To(int32(2)),
								},
							},
						},
					},
				},
			},
			wantNodes: []string{
				testNode101Slurmd00,
				"slurm-job-101-slurmd-0-1",
				testNode101Slurmd10,
				"slurm-job-101-slurmd-1-1",
			},
		},
		{
			name: "multiple replicated jobs filters only slurmd",
			jobSet: &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{Name: testJobSet101Name},
				Spec: jobsetv1alpha2.JobSetSpec{
					ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
						{
							Name:     "sidecar-job",
							Replicas: 1,
						},
						{
							Name:     ContainerSlurmd,
							Replicas: 2,
						},
					},
				},
			},
			wantNodes: []string{
				testNode101Slurmd00,
				testNode101Slurmd10,
			},
		},
		{
			name: "replicated job with empty name defaults to slurmd",
			jobSet: &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{Name: testJobSet101Name},
				Spec: jobsetv1alpha2.JobSetSpec{
					ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
						{
							Replicas: 1,
						},
					},
				},
			},
			wantNodes: []string{
				testNode101Slurmd00,
			},
		},
		{
			name: "single replicated job with custom name uses custom replicated job name",
			jobSet: &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{Name: testJobSet101Name},
				Spec: jobsetv1alpha2.JobSetSpec{
					ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
						{
							Name:     "worker",
							Replicas: 1,
						},
					},
				},
			},
			wantNodes: []string{"slurm-job-101-worker-0-0"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveNodeNames(tc.jobSet)
			if len(got) != len(tc.wantNodes) {
				t.Fatalf("deriveNodeNames() returned %d nodes, want %d: %v", len(got), len(tc.wantNodes), got)
			}
			for i := range got {
				if got[i] != tc.wantNodes[i] {
					t.Errorf("deriveNodeNames()[%d] = %q, want %q", i, got[i], tc.wantNodes[i])
				}
			}
		})
	}
}

func TestCleanupDynamicNodes_Success(t *testing.T) {
	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{Name: testJobSet101Name},
		Spec: jobsetv1alpha2.JobSetSpec{
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
				{
					Name:     ContainerSlurmd,
					Replicas: 2,
				},
			},
		},
	}

	var mu sync.Mutex
	var deletedNodes []string

	fakeSlurm := slurmclientfake.NewClientBuilder().WithInterceptorFuncs(slurmclientinterceptor.Funcs{
		Delete: func(ctx context.Context, obj object.Object, opts ...slurmclient.DeleteOption) error {
			mu.Lock()
			defer mu.Unlock()
			if node, ok := obj.(*slurmclienttypes.V0044Node); ok {
				deletedNodes = append(deletedNodes, ptr.Deref(node.Name, ""))
			}
			return nil
		},
	}).Build()

	r := &JobTeardownReconciler{}
	err := r.cleanupDynamicNodes(context.Background(), fakeSlurm, js)
	if err != nil {
		t.Fatalf("cleanupDynamicNodes failed unexpectedly: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	expectedNodes := []string{testNode101Slurmd00, testNode101Slurmd10}
	if len(deletedNodes) != len(expectedNodes) {
		t.Fatalf("Expected %d deleted nodes, got %d: %v", len(expectedNodes), len(deletedNodes), deletedNodes)
	}
	for i := range deletedNodes {
		if deletedNodes[i] != expectedNodes[i] {
			t.Errorf("Deleted node [%d] = %s, want %s", i, deletedNodes[i], expectedNodes[i])
		}
	}
}

func TestCleanupDynamicNodes_NotFoundIgnored(t *testing.T) {
	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{Name: testJobSet101Name},
		Spec: jobsetv1alpha2.JobSetSpec{
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
				{
					Name:     ContainerSlurmd,
					Replicas: 1,
				},
			},
		},
	}

	fakeSlurm := slurmclientfake.NewClientBuilder().WithInterceptorFuncs(slurmclientinterceptor.Funcs{
		Delete: func(ctx context.Context, obj object.Object, opts ...slurmclient.DeleteOption) error {
			return errors.New("Not Found")
		},
	}).Build()

	r := &JobTeardownReconciler{}
	err := r.cleanupDynamicNodes(context.Background(), fakeSlurm, js)
	if err != nil {
		t.Fatalf("Expected NotFound error to be safely ignored, got: %v", err)
	}
}

func TestCleanupDynamicNodes_ErrorPropagated(t *testing.T) {
	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{Name: testJobSet101Name},
		Spec: jobsetv1alpha2.JobSetSpec{
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
				{
					Name:     ContainerSlurmd,
					Replicas: 1,
				},
			},
		},
	}

	fakeSlurm := slurmclientfake.NewClientBuilder().WithInterceptorFuncs(slurmclientinterceptor.Funcs{
		Delete: func(ctx context.Context, obj object.Object, opts ...slurmclient.DeleteOption) error {
			return errors.New("connection reset by peer")
		},
	}).Build()

	r := &JobTeardownReconciler{}
	err := r.cleanupDynamicNodes(context.Background(), fakeSlurm, js)
	if err == nil {
		t.Fatal("Expected error on network disconnect, got nil")
	}
	if !strings.Contains(err.Error(), "connection reset by peer") {
		t.Errorf("Expected error to contain 'connection reset by peer', got: %v", err)
	}
}

func TestJobTeardownReconciler_HandleTeardown_DynamicNodeCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	testCtrlName := "slurmctld-controller"
	wm := &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconWmName,
			Namespace: testNamespace,
		},
		Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testCtrlName,
			},
			PartitionsMapping: []k8sbridgev1alpha1.PartitionMapping{
				{Partition: testBatchPartition, LocalQueue: testNamespace},
			},
		},
	}

	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testJobSet101Name,
			Namespace: testNamespace,
			Labels: map[string]string{
				LabelSlurmJobID:     "101",
				LabelSlurmPartition: testBatchPartition,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
					Kind:       KindWorkloadMixing,
					Name:       reconWmName,
					UID:        testWMUID,
				},
			},
		},
		Spec: jobsetv1alpha2.JobSetSpec{
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
				{
					Name:     ContainerSlurmd,
					Replicas: 2,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
	fakeRecorder := record.NewFakeRecorder(10)

	var mu sync.Mutex
	var deletedNodes []string
	fakeSlurm := slurmclientfake.NewClientBuilder().WithInterceptorFuncs(slurmclientinterceptor.Funcs{
		Delete: func(ctx context.Context, obj object.Object, opts ...slurmclient.DeleteOption) error {
			mu.Lock()
			defer mu.Unlock()
			if node, ok := obj.(*slurmclienttypes.V0044Node); ok {
				deletedNodes = append(deletedNodes, ptr.Deref(node.Name, ""))
			}
			return nil
		},
	}).Build()

	cm := clientmap.NewClientMap()
	ctrlKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}
	cm.Add(t.Context(), ctrlKey, fakeSlurm)

	reconciler := &JobTeardownReconciler{
		Client:    fakeClient,
		ClientMap: cm,
		Recorder:  fakeRecorder,
	}

	ev := slurmtypes.SlurmJobTeardownEvent{
		JobID: "101",
		WorkloadMixing: types.NamespacedName{
			Namespace: testNamespace,
			Name:      reconWmName,
		},
		Partition: testBatchPartition,
		Reason:    metrics.ReasonJobCompleted,
	}

	err := reconciler.handleTeardown(context.Background(), ev)
	if err != nil {
		t.Fatalf("handleTeardown failed: %v", err)
	}

	// Verify JobSet is deleted
	checkJS := &jobsetv1alpha2.JobSet{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testJobSet101Name}, checkJS)
	if !apierrors.IsNotFound(err) {
		t.Errorf("Expected JobSet to be deleted, got: %v", err)
	}

	// Verify dynamic slurmd nodes were deleted from Slurm
	mu.Lock()
	defer mu.Unlock()
	expectedNodes := []string{testNode101Slurmd00, testNode101Slurmd10}
	if len(deletedNodes) != len(expectedNodes) {
		t.Fatalf("Expected %d deleted nodes in Slurm, got %d: %v", len(expectedNodes), len(deletedNodes), deletedNodes)
	}
	for i := range deletedNodes {
		if deletedNodes[i] != expectedNodes[i] {
			t.Errorf("Expected deleted node %q, got %q", expectedNodes[i], deletedNodes[i])
		}
	}
}

func TestJobTeardownReconciler_HandleTeardown_DynamicNodeCleanup_NotFoundIgnored(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	testCtrlName := "slurmctld-controller-nf"
	wm := &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconWmName,
			Namespace: testNamespace,
		},
		Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testCtrlName,
			},
			PartitionsMapping: []k8sbridgev1alpha1.PartitionMapping{
				{Partition: testBatchPartition, LocalQueue: testNamespace},
			},
		},
	}

	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testJobSet101Name,
			Namespace: testNamespace,
			Labels: map[string]string{
				LabelSlurmJobID:     "101",
				LabelSlurmPartition: testBatchPartition,
			},
		},
		Spec: jobsetv1alpha2.JobSetSpec{
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
				{
					Name:     ContainerSlurmd,
					Replicas: 1,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
	fakeRecorder := record.NewFakeRecorder(10)

	fakeSlurm := slurmclientfake.NewClientBuilder().WithInterceptorFuncs(slurmclientinterceptor.Funcs{
		Delete: func(ctx context.Context, obj object.Object, opts ...slurmclient.DeleteOption) error {
			return errors.New("Not Found")
		},
	}).Build()

	cm := clientmap.NewClientMap()
	ctrlKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}
	cm.Add(t.Context(), ctrlKey, fakeSlurm)

	reconciler := &JobTeardownReconciler{
		Client:    fakeClient,
		ClientMap: cm,
		Recorder:  fakeRecorder,
	}

	ev := slurmtypes.SlurmJobTeardownEvent{
		JobID: "101",
		WorkloadMixing: types.NamespacedName{
			Namespace: testNamespace,
			Name:      reconWmName,
		},
		Partition: testBatchPartition,
		Reason:    metrics.ReasonJobCompleted,
	}

	err := reconciler.handleTeardown(context.Background(), ev)
	if err != nil {
		t.Fatalf("Expected NotFound during dynamic node deletion to be safely ignored, got: %v", err)
	}

	// Verify JobSet was still deleted
	checkJS := &jobsetv1alpha2.JobSet{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testJobSet101Name}, checkJS)
	if !apierrors.IsNotFound(err) {
		t.Errorf("Expected JobSet to be deleted, got: %v", err)
	}
}

func TestJobTeardownReconciler_HandleTeardown_DynamicNodeCleanup_ErrorPropagated(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	testCtrlName := "slurmctld-controller-err"
	wm := &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconWmName,
			Namespace: testNamespace,
		},
		Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testCtrlName,
			},
			PartitionsMapping: []k8sbridgev1alpha1.PartitionMapping{
				{Partition: testBatchPartition, LocalQueue: testNamespace},
			},
		},
	}

	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testJobSet101Name,
			Namespace: testNamespace,
			Labels: map[string]string{
				LabelSlurmJobID:     "101",
				LabelSlurmPartition: testBatchPartition,
			},
		},
		Spec: jobsetv1alpha2.JobSetSpec{
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
				{
					Name:     ContainerSlurmd,
					Replicas: 1,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
	fakeRecorder := record.NewFakeRecorder(10)

	fakeSlurm := slurmclientfake.NewClientBuilder().WithInterceptorFuncs(slurmclientinterceptor.Funcs{
		Delete: func(ctx context.Context, obj object.Object, opts ...slurmclient.DeleteOption) error {
			return errors.New("slurmctld internal error")
		},
	}).Build()

	cm := clientmap.NewClientMap()
	ctrlKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}
	cm.Add(t.Context(), ctrlKey, fakeSlurm)

	reconciler := &JobTeardownReconciler{
		Client:    fakeClient,
		ClientMap: cm,
		Recorder:  fakeRecorder,
	}

	ev := slurmtypes.SlurmJobTeardownEvent{
		JobID: "101",
		WorkloadMixing: types.NamespacedName{
			Namespace: testNamespace,
			Name:      reconWmName,
		},
		Partition: testBatchPartition,
		Reason:    metrics.ReasonJobCompleted,
	}

	err := reconciler.handleTeardown(context.Background(), ev)
	if err == nil {
		t.Fatal("Expected error on slurmctld node deletion failure, got nil")
	}
	if !strings.Contains(err.Error(), "slurmctld internal error") {
		t.Errorf("Expected error to contain 'slurmctld internal error', got: %v", err)
	}

	// Verify JobSet was NOT deleted because node cleanup failed first (enabling self-healing retry)
	checkJS := &jobsetv1alpha2.JobSet{}
	getErr := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testJobSet101Name}, checkJS)
	if getErr != nil {
		t.Errorf("Expected JobSet to remain in cluster when node cleanup fails, got error: %v", getErr)
	}
}

func TestJobTeardownReconciler_HandleTeardown_DynamicNodeCleanup_WhenAlreadyDeleting(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	testCtrlName := "slurmctld-controller-del"
	wm := &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconWmName,
			Namespace: testNamespace,
		},
		Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testCtrlName,
			},
		},
	}

	now := metav1.Now()
	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testJobSet103Name,
			Namespace:         testNamespace,
			DeletionTimestamp: &now,
			Finalizers:        []string{FinalizerJobSet},
		},
		Spec: jobsetv1alpha2.JobSetSpec{
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
				{
					Name:     ContainerSlurmd,
					Replicas: 2,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
	fakeRecorder := record.NewFakeRecorder(10)

	nodeDeleteCalls := 0
	fakeSlurm := slurmclientfake.NewClientBuilder().WithInterceptorFuncs(slurmclientinterceptor.Funcs{
		Delete: func(ctx context.Context, obj object.Object, opts ...slurmclient.DeleteOption) error {
			nodeDeleteCalls++
			return nil
		},
	}).Build()

	cm := clientmap.NewClientMap()
	ctrlKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}
	cm.Add(t.Context(), ctrlKey, fakeSlurm)

	reconciler := &JobTeardownReconciler{
		Client:    fakeClient,
		ClientMap: cm,
		Recorder:  fakeRecorder,
	}

	ev := slurmtypes.SlurmJobTeardownEvent{
		JobID: "103",
		WorkloadMixing: types.NamespacedName{
			Namespace: testNamespace,
			Name:      reconWmName,
		},
		Partition: testBatchPartition,
		Reason:    metrics.ReasonJobCompleted,
	}

	err := reconciler.handleTeardown(context.Background(), ev)
	if err != nil {
		t.Fatalf("handleTeardown failed: %v", err)
	}

	// Dynamic nodes must still be cleaned up in Slurm even if JobSet has DeletionTimestamp
	if nodeDeleteCalls != 2 {
		t.Errorf("Expected 2 node delete calls for already deleting JobSet, got %d", nodeDeleteCalls)
	}

	// Verify FinalizerJobSet was removed and the terminating JobSet is now fully deleted
	checkJS := &jobsetv1alpha2.JobSet{}
	getErr := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testJobSet103Name}, checkJS)
	if !apierrors.IsNotFound(getErr) {
		t.Errorf("Expected terminating JobSet to be deleted after finalizer removal, got: %v", getErr)
	}
}

func TestJobTeardownReconciler_HandleTeardown_MissingSlurmClient(t *testing.T) {
	const controllerName = "missing-client-controller"

	tests := []struct {
		name              string
		controllerExists  bool
		wantErr           bool
		wantJobSetDeleted bool
	}{
		{
			name:             "retains finalizer while Slurm Controller exists",
			controllerExists: true,
			wantErr:          true,
		},
		{
			name:              "releases finalizer and deletes JobSet when Slurm Controller is gone",
			controllerExists:  false,
			wantJobSetDeleted: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = k8sbridgev1alpha1.AddToScheme(scheme)
			_ = jobsetv1alpha2.AddToScheme(scheme)
			_ = slinkyv1beta1.AddToScheme(scheme)

			wm := &k8sbridgev1alpha1.WorkloadMixing{
				ObjectMeta: metav1.ObjectMeta{
					Name:      reconWmName,
					Namespace: testNamespace,
				},
				Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
					ControllerRef: corev1.LocalObjectReference{
						Name: controllerName,
					},
				},
			}

			js := newTestJobSet(testJobSet101Name).
				withNamespace(testNamespace).
				withFinalizers(FinalizerJobSet).
				withSlurmdReplicas(1).
				build()

			objs := []client.Object{wm, js}
			if tc.controllerExists {
				objs = append(objs, &slinkyv1beta1.Controller{
					ObjectMeta: metav1.ObjectMeta{Name: controllerName, Namespace: testNamespace},
				})
			}

			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
			reconciler := &JobTeardownReconciler{
				Client:    fakeClient,
				ClientMap: clientmap.NewClientMap(),
			}

			ev := slurmtypes.SlurmJobTeardownEvent{
				JobID: "101",
				WorkloadMixing: types.NamespacedName{
					Namespace: testNamespace,
					Name:      reconWmName,
				},
				Partition: testBatchPartition,
				Reason:    metrics.ReasonJobCompleted,
			}

			err := reconciler.handleTeardown(context.Background(), ev)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("handleTeardown() error = %v, wantErr %v", err, tc.wantErr)
			}

			checkJS := &jobsetv1alpha2.JobSet{}
			getErr := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testJobSet101Name}, checkJS)
			if tc.wantJobSetDeleted {
				if !apierrors.IsNotFound(getErr) {
					t.Errorf("Expected JobSet to be deleted, got: %v", getErr)
				}
				return
			}
			if getErr != nil {
				t.Fatalf("Expected JobSet to remain, got: %v", getErr)
			}
			if !slices.Equal(checkJS.Finalizers, []string{FinalizerJobSet}) {
				t.Errorf("Expected FinalizerJobSet to be retained on JobSet, got: %v", checkJS.Finalizers)
			}
		})
	}
}

func TestIsSlurmNotFound(t *testing.T) {
	testCases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "slurm rest Not Found error",
			err:  errors.New("Not Found"),
			want: true,
		},
		{
			name: "slurm rest aggregate error containing Not Found",
			err:  utilerrors.NewAggregate([]error{errors.New("Not Found"), errors.New("node not registered")}),
			want: true,
		},
		{
			name: "connection refused with 404 in port should not match",
			err:  errors.New("dial tcp 10.0.0.1:4040: connect: connection refused"),
			want: false,
		},
		{
			name: "node name containing 404 in connection timeout should not match",
			err:  errors.New("failed to connect to host for slurm-job-404-slurmd-0-0: timeout"),
			want: false,
		},
		{
			name: "slurm rest Unprocessable Entity with invalid node name",
			err:  errors.New("[Unprocessable Entity, Invalid node name specified]"),
			want: true,
		},
		{
			name: "slurm rest Invalid job id specified error",
			err:  errors.New("[Not Found, Invalid job id specified]"),
			want: true,
		},
		{
			name: "slurm rest raw Invalid job id specified",
			err:  errors.New("Invalid job id specified"),
			want: true,
		},
		{
			name: "slurm rest Node not found error",
			err:  errors.New("Node not found"),
			want: true,
		},
		{
			name: "account not found configuration error should not match",
			err:  errors.New("Account 'finance' Not Found"),
			want: false,
		},
		{
			name: "qos not found configuration error should not match",
			err:  errors.New("QOS 'high-priority' Not Found"),
			want: false,
		},
		{
			name: "token file not found error should not match",
			err:  errors.New("Token file /var/run/secrets/slurm/token: Not Found"),
			want: false,
		},
		{
			name: "proxy route not found error should not match",
			err:  errors.New("Upstream route Not Found"),
			want: false,
		},
		{
			name: "general internal error",
			err:  errors.New("slurmctld error 500"),
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := isSlurmNotFound(tc.err)
			if got != tc.want {
				t.Errorf("isSlurmNotFound(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestCleanupDynamicNodes_ContextCancelled(t *testing.T) {
	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{Name: testJobSet101Name},
		Spec: jobsetv1alpha2.JobSetSpec{
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
				{
					Name:     ContainerSlurmd,
					Replicas: 3,
				},
			},
		},
	}

	callCount := 0
	fakeSlurm := slurmclientfake.NewClientBuilder().WithInterceptorFuncs(slurmclientinterceptor.Funcs{
		Delete: func(ctx context.Context, obj object.Object, opts ...slurmclient.DeleteOption) error {
			callCount++
			return nil
		},
	}).Build()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel context

	r := &JobTeardownReconciler{}
	err := r.cleanupDynamicNodes(ctx, fakeSlurm, js)
	if err == nil {
		t.Fatal("Expected error when context is cancelled, got nil")
	}
	if callCount > 0 {
		t.Errorf("Expected 0 delete calls when context is pre-cancelled, got %d", callCount)
	}
}

func TestJobTeardownReconciler_HandleTeardown_ReconciliationTimeoutShortCircuitsNodeCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	const (
		testCtrlName = "slurmctld-controller-timeout"
		totalNodes   = 20
	)
	wm := &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconWmName,
			Namespace: testNamespace,
		},
		Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testCtrlName,
			},
		},
	}

	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testJobSet101Name,
			Namespace: testNamespace,
			Labels: map[string]string{
				LabelSlurmJobID:     "101",
				LabelSlurmPartition: testBatchPartition,
			},
		},
		Spec: jobsetv1alpha2.JobSetSpec{
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
				{
					Name:     ContainerSlurmd,
					Replicas: totalNodes,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()

	var nodeDeleteAttempts int
	fakeSlurm := slurmclientfake.NewClientBuilder().WithInterceptorFuncs(slurmclientinterceptor.Funcs{
		Delete: func(ctx context.Context, obj object.Object, opts ...slurmclient.DeleteOption) error {
			nodeDeleteAttempts++
			<-ctx.Done()
			return ctx.Err()
		},
	}).Build()

	cm := clientmap.NewClientMap()
	ctrlKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}
	cm.Add(t.Context(), ctrlKey, fakeSlurm)

	reconciler := &JobTeardownReconciler{
		Client:                fakeClient,
		ClientMap:             cm,
		slurmCallTimeout:      15 * time.Millisecond,
		reconciliationTimeout: 35 * time.Millisecond,
	}

	ev := slurmtypes.SlurmJobTeardownEvent{
		JobID: "101",
		WorkloadMixing: types.NamespacedName{
			Namespace: testNamespace,
			Name:      reconWmName,
		},
		Partition: testBatchPartition,
		Reason:    metrics.ReasonJobCompleted,
	}

	err := reconciler.handleTeardown(context.Background(), ev)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Expected handleTeardown error wrapping context.DeadlineExceeded, got: %v", err)
	}
	if nodeDeleteAttempts == 0 || nodeDeleteAttempts >= totalNodes {
		t.Errorf("Expected reconciliationTimeout to short-circuit node cleanup before all %d nodes were attempted, got %d attempts", totalNodes, nodeDeleteAttempts)
	}
}
