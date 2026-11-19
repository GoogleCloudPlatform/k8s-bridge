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
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	"github.com/SlinkyProject/slurm-client/pkg/object"
	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/clientmap"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/utils/slurmtypes"
	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

const (
	testPartitionName   = "workloadmixing-partition"
	testWMName          = reconWmName
	testNamespace       = "default"
	testControllerName  = "test-controller"
	testQueueName       = "test-queue"
	testMetricsWMName   = "wm-metrics"
	testMetricsCtrlName = "ctrl-metrics"
)

// errorClient implements slurmclient.Client, returning an error on List.
type errorClient struct {
	slurmclient.Client
}

func (e *errorClient) Start(ctx context.Context) {}
func (e *errorClient) Stop()                     {}

func (e *errorClient) List(ctx context.Context, list object.ObjectList, opts ...slurmclient.ListOption) error {
	return fmt.Errorf("mock slurm error")
}

func newPendingJob(id int32, partition string) slurmclienttypes.V0044JobInfo {
	return slurmclienttypes.V0044JobInfo{
		V0044JobInfo: slurmapi.V0044JobInfo{
			JobId:     ptr.To[int32](id),
			Partition: ptr.To(partition),
			JobState:  ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStatePENDING}),
		},
	}
}

func newTestWorkloadMixing(name, controller, partition, queue string) *k8sbridgev1alpha1.WorkloadMixing {
	var pm []k8sbridgev1alpha1.PartitionMapping
	if partition != "" {
		pm = append(pm, k8sbridgev1alpha1.PartitionMapping{
			Partition:  partition,
			LocalQueue: queue,
		})
	}
	return &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: controller,
			},
			PartitionsMapping: pm,
		},
	}
}

func testPollSingleWorkloadMixing(t *testing.T, scheme *runtime.Scheme) {
	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm).Build()

	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}
	testJobs := []slurmclienttypes.V0044JobInfo{
		newPendingJob(123, testPartitionName),
		newPendingJob(456, testPartitionName),
	}
	mockSlurm.SetPendingJobs(testPartitionName, testJobs)
	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(context.Background(), controllerKey, mockSlurm)

	eventsChan := make(chan slurmtypes.SlurmJobAdmissionEvent, 10)
	poller := &SlurmPoller{
		Client:          fakeK8sClient,
		ClientMap:       cMap,
		AdmissionEvents: eventsChan,
		PollInterval:    10 * time.Second,
	}

	err := poller.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	close(eventsChan)
	var receivedEvents []slurmtypes.SlurmJobAdmissionEvent
	for ev := range eventsChan {
		receivedEvents = append(receivedEvents, ev)
	}

	if len(receivedEvents) != len(testJobs) {
		t.Fatalf("Expected %d events, got %d", len(testJobs), len(receivedEvents))
	}

	for i, ev := range receivedEvents {
		expectedJob := testJobs[i]
		expectedJobIDStr := fmt.Sprintf("%d", ptr.Deref(expectedJob.JobId, 0))
		if ev.JobID != expectedJobIDStr {
			t.Errorf("Event %d JobID mismatch. Expected %s, got %s", i, expectedJobIDStr, ev.JobID)
		}

		var jobInfo slurmclienttypes.V0044JobInfo
		if err := json.Unmarshal([]byte(ev.RawJobSpecJSON), &jobInfo); err != nil {
			t.Fatalf("Failed to unmarshal RawJobSpecJSON: %v", err)
		}
		if diff := cmp.Diff(expectedJob, jobInfo); diff != "" {
			t.Errorf("Event %d content mismatch (-want +got):\n%s", i, diff)
		}

		expectedWM := types.NamespacedName{Namespace: wm.Namespace, Name: wm.Name}
		if ev.WorkloadMixing != expectedWM {
			t.Errorf("Event %d WorkloadMixing ref mismatch. Expected %v, got %v", i, expectedWM, ev.WorkloadMixing)
		}
	}

	if status := testutil.ToFloat64(metrics.SlurmClientStatus.WithLabelValues(testControllerName)); status != 1.0 {
		t.Errorf("Expected SlurmClientStatus 1.0, got %f", status)
	}
}

func testPollMultipleWorkloadMixing(t *testing.T, scheme *runtime.Scheme) {
	wm1 := newTestWorkloadMixing("wm-1", "controller-1", testPartitionName, testQueueName)
	wm2 := newTestWorkloadMixing("wm-2", "controller-2", testPartitionName, testQueueName) // Client not ready
	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm1, wm2).Build()

	cMap := clientmap.NewClientMap()
	mockSlurm1 := &utils.SlurmClient{}
	testJobs := []slurmclienttypes.V0044JobInfo{
		newPendingJob(111, testPartitionName),
	}
	mockSlurm1.SetPendingJobs(testPartitionName, testJobs)
	cMap.Add(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: "controller-1"}, mockSlurm1)

	eventsChan := make(chan slurmtypes.SlurmJobAdmissionEvent, 10)
	poller := &SlurmPoller{
		Client:          fakeK8sClient,
		ClientMap:       cMap,
		AdmissionEvents: eventsChan,
		PollInterval:    10 * time.Second,
	}

	err := poller.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	close(eventsChan)
	var receivedEvents []slurmtypes.SlurmJobAdmissionEvent
	for ev := range eventsChan {
		receivedEvents = append(receivedEvents, ev)
	}

	// Only events from wm1 should be processed
	if len(receivedEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(receivedEvents))
	}
	if receivedEvents[0].JobID != "111" {
		t.Errorf("Expected JobID 111, got %s", receivedEvents[0].JobID)
	}

	if status := testutil.ToFloat64(metrics.SlurmClientStatus.WithLabelValues("controller-1")); status != 1.0 {
		t.Errorf("Expected SlurmClientStatus 1.0 for controller-1, got %f", status)
	}
	if status := testutil.ToFloat64(metrics.SlurmClientStatus.WithLabelValues("controller-2")); status != 0.0 {
		t.Errorf("Expected SlurmClientStatus 0.0 for controller-2, got %f", status)
	}
}

func testPollSlurmQueryFailure(t *testing.T, scheme *runtime.Scheme) {
	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm).Build()

	cMap := clientmap.NewClientMap()
	// Add error-returning client
	cMap.Add(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testControllerName}, &errorClient{})

	poller := &SlurmPoller{
		Client:          fakeK8sClient,
		ClientMap:       cMap,
		AdmissionEvents: make(chan slurmtypes.SlurmJobAdmissionEvent, 10),
		PollInterval:    10 * time.Second,
	}

	initialErrors := testutil.ToFloat64(metrics.SlurmPollerErrors.WithLabelValues(testControllerName))
	err := poller.Poll(context.Background())
	if err != nil {
		t.Fatalf("Expected nil error (Poll should continue on error), got %v", err)
	}
	newErrors := testutil.ToFloat64(metrics.SlurmPollerErrors.WithLabelValues(testControllerName))
	if newErrors != initialErrors+1 {
		t.Errorf("Expected SlurmPollerErrors to increment by 1 (got %f, was %f)", newErrors, initialErrors)
	}
	if status := testutil.ToFloat64(metrics.SlurmClientStatus.WithLabelValues(testControllerName)); status != 0.0 {
		t.Errorf("Expected SlurmClientStatus 0.0 on error, got %f", status)
	}
}

func testPollChannelFull(t *testing.T, scheme *runtime.Scheme) {
	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm).Build()

	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}
	testJobs := []slurmclienttypes.V0044JobInfo{
		newPendingJob(123, testPartitionName),
		newPendingJob(456, testPartitionName), // This one should be dropped
	}
	mockSlurm.SetPendingJobs(testPartitionName, testJobs)
	cMap.Add(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testControllerName}, mockSlurm)

	initialDropped := testutil.ToFloat64(metrics.DroppedEvents.WithLabelValues(testWMName))

	// Channel capacity is 1, but we have 2 jobs
	eventsChan := make(chan slurmtypes.SlurmJobAdmissionEvent, 1)
	poller := &SlurmPoller{
		Client:          fakeK8sClient,
		ClientMap:       cMap,
		AdmissionEvents: eventsChan,
		PollInterval:    10 * time.Second,
	}

	// Run Poll. It should complete without blocking/deadlocking.
	pollDone := make(chan error, 1)
	go func() {
		pollDone <- poller.Poll(context.Background())
	}()

	select {
	case err := <-pollDone:
		if err != nil {
			t.Fatalf("Poll failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Poll blocked, indicating deadlock on channel send")
	}

	close(eventsChan)
	var receivedEvents []slurmtypes.SlurmJobAdmissionEvent
	for ev := range eventsChan {
		receivedEvents = append(receivedEvents, ev)
	}

	// Channel capacity was 1, so we should have received exactly 1 event, and the other dropped.
	if len(receivedEvents) != 1 {
		t.Fatalf("Expected 1 event (channel full), got %d", len(receivedEvents))
	}
	if receivedEvents[0].JobID != "123" {
		t.Errorf("Expected first job ID 123, got %s", receivedEvents[0].JobID)
	}

	newDropped := testutil.ToFloat64(metrics.DroppedEvents.WithLabelValues(testWMName))
	if newDropped != initialDropped+1 {
		t.Errorf("Expected DroppedEvents to increment by 1 (got %f, was %f)", newDropped, initialDropped)
	}
}

func testPollFilteringByPartitionsMapping(t *testing.T, scheme *runtime.Scheme) {
	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm).Build()

	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}

	// Job in observed partition
	jobObserved := newPendingJob(1, testPartitionName)
	// Job in unobserved partition
	jobBatch := newPendingJob(2, "dynamic-non-workload-mixing")

	mockSlurm.SetPendingJobs(testPartitionName, []slurmclienttypes.V0044JobInfo{jobObserved})
	mockSlurm.SetPendingJobs("dynamic-non-workload-mixing", []slurmclienttypes.V0044JobInfo{jobBatch})

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(context.Background(), controllerKey, mockSlurm)

	eventsChan := make(chan slurmtypes.SlurmJobAdmissionEvent, 10)
	poller := &SlurmPoller{
		Client:          fakeK8sClient,
		ClientMap:       cMap,
		AdmissionEvents: eventsChan,
		PollInterval:    10 * time.Second,
	}

	err := poller.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	close(eventsChan)
	var receivedEvents []slurmtypes.SlurmJobAdmissionEvent
	for ev := range eventsChan {
		receivedEvents = append(receivedEvents, ev)
	}

	// Should only receive event for debug partition
	if len(receivedEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(receivedEvents))
	}

	if receivedEvents[0].JobID != "1" {
		t.Errorf("Expected JobID 1, got %s", receivedEvents[0].JobID)
	}
}

func testPollEmptyPartitionsMapping(t *testing.T, scheme *runtime.Scheme) {
	wm := newTestWorkloadMixing(testWMName, testControllerName, "", "")
	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm).Build()

	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}

	jobDebug := newPendingJob(1, "workloadmixing-debug-partition")
	mockSlurm.SetPendingJobs("workloadmixing-debug-partition", []slurmclienttypes.V0044JobInfo{jobDebug})

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(context.Background(), controllerKey, mockSlurm)

	eventsChan := make(chan slurmtypes.SlurmJobAdmissionEvent, 10)
	poller := &SlurmPoller{
		Client:          fakeK8sClient,
		ClientMap:       cMap,
		AdmissionEvents: eventsChan,
		PollInterval:    10 * time.Second,
	}

	err := poller.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	close(eventsChan)
	var receivedEvents []slurmtypes.SlurmJobAdmissionEvent
	for ev := range eventsChan {
		receivedEvents = append(receivedEvents, ev)
	}

	// Should receive NO events
	if len(receivedEvents) != 0 {
		t.Fatalf("Expected 0 events, got %d", len(receivedEvents))
	}
}

func testPollContextCancelled(t *testing.T, scheme *runtime.Scheme) {
	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm).Build()

	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}
	testJobs := []slurmclienttypes.V0044JobInfo{
		newPendingJob(123, testPartitionName),
	}
	mockSlurm.SetPendingJobs(testPartitionName, testJobs)
	cMap.Add(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testControllerName}, mockSlurm)

	// Create a channel that is already blocked (size 0) so we force the select block to evaluate
	eventsChan := make(chan slurmtypes.SlurmJobAdmissionEvent)
	poller := &SlurmPoller{
		Client:          fakeK8sClient,
		ClientMap:       cMap,
		AdmissionEvents: eventsChan,
		PollInterval:    10 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel context immediately

	err := poller.Poll(ctx)
	if err == nil {
		t.Fatal("Expected context cancelled error, got nil")
	}
}

func TestSlurmPoller_Poll(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	t.Run("Success - Single WorkloadMixing with jobs", func(t *testing.T) {
		testPollSingleWorkloadMixing(t, scheme)
	})

	t.Run("Success - Multiple WorkloadMixing, one client not ready", func(t *testing.T) {
		testPollMultipleWorkloadMixing(t, scheme)
	})

	t.Run("Tolerate - Slurm query failure", func(t *testing.T) {
		testPollSlurmQueryFailure(t, scheme)
	})

	t.Run("Success - Channel full, events dropped without blocking", func(t *testing.T) {
		testPollChannelFull(t, scheme)
	})

	t.Run("Success - Filtering by PartitionsMapping", func(t *testing.T) {
		testPollFilteringByPartitionsMapping(t, scheme)
	})

	t.Run("Success - Empty PartitionsMapping skips processing", func(t *testing.T) {
		testPollEmptyPartitionsMapping(t, scheme)
	})

	t.Run("Error - Context cancelled during poll", func(t *testing.T) {
		testPollContextCancelled(t, scheme)
	})
}

func TestSlurmPoller_Poll_MultiPartition(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm).Build()

	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}

	// Job with multiple partitions, one is mapped
	jobMapped := newPendingJob(1, fmt.Sprintf("%s,unmapped-partition", testPartitionName))
	// Job with multiple partitions, none are mapped
	jobUnmapped := newPendingJob(2, "unmapped-1,unmapped-2")

	mockSlurm.SetPendingJobs("some-key", []slurmclienttypes.V0044JobInfo{jobMapped, jobUnmapped})
	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(context.Background(), controllerKey, mockSlurm)

	eventsChan := make(chan slurmtypes.SlurmJobAdmissionEvent, 10)
	poller := &SlurmPoller{
		Client:          fakeK8sClient,
		ClientMap:       cMap,
		AdmissionEvents: eventsChan,
		PollInterval:    10 * time.Second,
	}

	err := poller.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	close(eventsChan)
	var receivedEvents []slurmtypes.SlurmJobAdmissionEvent
	for ev := range eventsChan {
		receivedEvents = append(receivedEvents, ev)
	}

	// Should only receive event for jobMapped (JobID 1)
	if len(receivedEvents) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(receivedEvents))
	}

	if receivedEvents[0].JobID != "1" {
		t.Errorf("Expected JobID 1, got %s", receivedEvents[0].JobID)
	}
}

func TestSlurmPoller_Metrics(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	wm := newTestWorkloadMixing(testMetricsWMName, testMetricsCtrlName, testPartitionName, testQueueName)
	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm).Build()

	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}
	testJobs := []slurmclienttypes.V0044JobInfo{
		newPendingJob(10, testPartitionName),
	}
	mockSlurm.SetPendingJobs(testPartitionName, testJobs)
	cMap.Add(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testMetricsCtrlName}, mockSlurm)

	eventsChan := make(chan slurmtypes.SlurmJobAdmissionEvent, 10)
	poller := &SlurmPoller{
		Client:          fakeK8sClient,
		ClientMap:       cMap,
		AdmissionEvents: eventsChan,
		PollInterval:    10 * time.Second,
	}

	// 1. Initial successful poll
	err := poller.Poll(context.Background())
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	if status := testutil.ToFloat64(metrics.SlurmClientStatus.WithLabelValues(testMetricsCtrlName)); status != 1.0 {
		t.Errorf("Expected SlurmClientStatus 1.0, got %f", status)
	}

	if queueDepth := testutil.ToFloat64(metrics.EventsQueueDepth); queueDepth != 1.0 {
		t.Errorf("Expected EventsQueueDepth 1.0, got %f", queueDepth)
	}

	// 2. Controller failure (replace client with errorClient)
	cMap.Add(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testMetricsCtrlName}, &errorClient{})
	initialErrors := testutil.ToFloat64(metrics.SlurmPollerErrors.WithLabelValues(testMetricsCtrlName))
	_ = poller.Poll(context.Background())

	if status := testutil.ToFloat64(metrics.SlurmClientStatus.WithLabelValues(testMetricsCtrlName)); status != 0.0 {
		t.Errorf("Expected SlurmClientStatus 0.0 after error, got %f", status)
	}
	if newErrors := testutil.ToFloat64(metrics.SlurmPollerErrors.WithLabelValues(testMetricsCtrlName)); newErrors != initialErrors+1 {
		t.Errorf("Expected SlurmPollerErrors to increment by 1, got %f", newErrors)
	}

	// 3. Controller recovery (restore mockSlurm)
	cMap.Add(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testMetricsCtrlName}, mockSlurm)
	_ = poller.Poll(context.Background())

	if status := testutil.ToFloat64(metrics.SlurmClientStatus.WithLabelValues(testMetricsCtrlName)); status != 1.0 {
		t.Errorf("Expected SlurmClientStatus 1.0 after recovery, got %f", status)
	}
}

func newPollerTestJobSet(jobID string) *testJobSetBuilder {
	return newTestJobSet("slurm-job-" + jobID).
		withNamespace(testNamespace).
		withLabels(map[string]string{
			LabelSlurmJobID:     jobID,
			LabelSlurmPartition: testPartitionName,
		}).
		withOwnerReference(metav1.OwnerReference{
			APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
			Kind:       KindWorkloadMixing,
			Name:       testWMName,
			UID:        testWMUID,
		})
}

func TestPoll_TerminalStateDetection(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	js := newPollerTestJobSet("101").build()

	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}

	completedJob := slurmclienttypes.V0044JobInfo{
		V0044JobInfo: slurmapi.V0044JobInfo{
			JobId:     ptr.To[int32](101),
			Partition: ptr.To(testPartitionName),
			JobState:  ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateCOMPLETED}),
		},
	}
	mockSlurm.SetPendingJobs(testPartitionName, []slurmclienttypes.V0044JobInfo{completedJob})

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(context.Background(), controllerKey, mockSlurm)

	teardownChan := make(chan slurmtypes.SlurmJobTeardownEvent, 10)
	poller := &SlurmPoller{
		Client:         fakeK8sClient,
		ClientMap:      cMap,
		TeardownEvents: teardownChan,
		PollInterval:   10 * time.Second,
	}

	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	select {
	case ev := <-teardownChan:
		if ev.JobID != "101" {
			t.Errorf("Expected teardown JobID 101, got %s", ev.JobID)
		}
		if ev.Reason != metrics.ReasonJobCompleted {
			t.Errorf("Expected reason %s, got %s", metrics.ReasonJobCompleted, ev.Reason)
		}
		if ev.Partition != testPartitionName {
			t.Errorf("Expected partition %s, got %s", testPartitionName, ev.Partition)
		}
	default:
		t.Fatal("Expected teardown event for completed job, got none")
	}
}

func TestPoll_NonTerminalPreservation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	js := newPollerTestJobSet("102").build()

	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}

	runningJob := slurmclienttypes.V0044JobInfo{
		V0044JobInfo: slurmapi.V0044JobInfo{
			JobId:     ptr.To[int32](102),
			Partition: ptr.To(testPartitionName),
			JobState:  ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateRUNNING}),
		},
	}
	mockSlurm.SetPendingJobs(testPartitionName, []slurmclienttypes.V0044JobInfo{runningJob})

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(context.Background(), controllerKey, mockSlurm)

	teardownChan := make(chan slurmtypes.SlurmJobTeardownEvent, 10)
	poller := &SlurmPoller{
		Client:         fakeK8sClient,
		ClientMap:      cMap,
		TeardownEvents: teardownChan,
		PollInterval:   10 * time.Second,
	}

	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	select {
	case ev := <-teardownChan:
		t.Fatalf("Expected no teardown event for running job, got: %v", ev)
	default:
		// Success: non-terminal job preserved
	}
}

func TestPoll_OwnerUIDMismatch_Ignored(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	wm.UID = "current-wm-uid"
	js := newTestJobSet(testJobSet101Name).
		withNamespace(testNamespace).
		withLabels(map[string]string{
			LabelSlurmJobID:     "101",
			LabelSlurmPartition: testPartitionName,
		}).
		withOwnerReference(metav1.OwnerReference{
			APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
			Kind:       KindWorkloadMixing,
			Name:       testWMName,
			UID:        "stale-wm-uid",
		}).
		build()

	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}

	completedJob := slurmclienttypes.V0044JobInfo{
		V0044JobInfo: slurmapi.V0044JobInfo{
			JobId:     ptr.To[int32](101),
			Partition: ptr.To(testPartitionName),
			JobState:  ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateCOMPLETED}),
		},
	}
	mockSlurm.SetPendingJobs(testPartitionName, []slurmclienttypes.V0044JobInfo{completedJob})

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(context.Background(), controllerKey, mockSlurm)

	teardownChan := make(chan slurmtypes.SlurmJobTeardownEvent, 10)
	poller := &SlurmPoller{
		Client:         fakeK8sClient,
		ClientMap:      cMap,
		TeardownEvents: teardownChan,
		PollInterval:   10 * time.Second,
	}

	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	select {
	case ev := <-teardownChan:
		t.Fatalf("Expected no teardown event for JobSet with mismatched owner UID, got: %v", ev)
	default:
		// Success: ignored due to UID mismatch
	}
}

func newRunningJob(id int32, partition string) slurmclienttypes.V0044JobInfo {
	return slurmclienttypes.V0044JobInfo{
		V0044JobInfo: slurmapi.V0044JobInfo{
			JobId:     ptr.To[int32](id),
			Partition: ptr.To(partition),
			JobState:  ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateRUNNING}),
		},
	}
}

func TestPoll_OrphanedJobSet_DisappearanceGracePeriod(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	wm.UID = testWMUID

	js := newPollerTestJobSet("202").build()

	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}
	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(context.Background(), controllerKey, mockSlurm)

	teardownChan := make(chan slurmtypes.SlurmJobTeardownEvent, 10)
	currentTime := now
	poller := &SlurmPoller{
		Client:            fakeK8sClient,
		ClientMap:         cMap,
		TeardownEvents:    teardownChan,
		PollInterval:      10 * time.Second,
		OrphanGracePeriod: 1 * time.Minute,
		nowFn:             func() time.Time { return currentTime },
	}

	// Poll cycle 1: First observation of missing job, no teardown event
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 1 failed: %v", err)
	}
	select {
	case ev := <-teardownChan:
		t.Fatalf("Expected no teardown event on first missing observation, got: %v", ev)
	default:
	}

	// Poll cycle 2: Within grace period (30s elapsed < 60s), no teardown event
	currentTime = now.Add(30 * time.Second)
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 2 failed: %v", err)
	}
	select {
	case ev := <-teardownChan:
		t.Fatalf("Expected no teardown event while within disappearance grace period, got: %v", ev)
	default:
	}

	// Poll cycle 3: Grace period exceeded (70s elapsed >= 60s), teardown event emitted
	currentTime = now.Add(70 * time.Second)
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 3 failed: %v", err)
	}
	select {
	case ev := <-teardownChan:
		if ev.JobID != "202" {
			t.Errorf("Expected JobID 202, got %s", ev.JobID)
		}
		if ev.Reason != metrics.ReasonJobOrphaned {
			t.Errorf("Expected reason %s, got %s", metrics.ReasonJobOrphaned, ev.Reason)
		}
		if ev.Partition != testPartitionName {
			t.Errorf("Expected partition %s, got %s", testPartitionName, ev.Partition)
		}
	default:
		t.Fatal("Expected teardown event after grace period elapsed, got none")
	}

	// Poll cycle 4: Retry on subsequent poll while JobSet remains in K8s
	currentTime = now.Add(80 * time.Second)
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 4 failed: %v", err)
	}
	select {
	case ev := <-teardownChan:
		if ev.JobID != "202" {
			t.Errorf("Expected retry teardown event with JobID 202, got %s", ev.JobID)
		}
	default:
		t.Fatal("Expected retry teardown event on subsequent poll cycle, got none")
	}
}

func TestPoll_OrphanedJobSet_PartitionFallback(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	wm.UID = testWMUID

	js := newTestJobSet("slurm-job-209").
		withNamespace(testNamespace).
		withLabels(map[string]string{
			LabelSlurmJobID: "209",
		}).
		withOwnerReference(metav1.OwnerReference{
			APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
			Kind:       KindWorkloadMixing,
			Name:       testWMName,
			UID:        testWMUID,
		}).
		build()

	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}
	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(context.Background(), controllerKey, mockSlurm)

	teardownChan := make(chan slurmtypes.SlurmJobTeardownEvent, 10)
	currentTime := now
	poller := &SlurmPoller{
		Client:            fakeK8sClient,
		ClientMap:         cMap,
		TeardownEvents:    teardownChan,
		PollInterval:      10 * time.Second,
		OrphanGracePeriod: 1 * time.Minute,
		nowFn:             func() time.Time { return currentTime },
	}

	// Poll cycle 1: First missing observation
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	// Poll cycle 2: After grace period
	currentTime = now.Add(70 * time.Second)
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	select {
	case ev := <-teardownChan:
		if ev.Partition != testPartitionName {
			t.Errorf("Expected fallback partition %s, got %s", testPartitionName, ev.Partition)
		}
	default:
		t.Fatal("Expected teardown event with fallback partition, got none")
	}
}

func TestPoll_OrphanedJobSet_ReappearanceClearsTracking(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	wm.UID = testWMUID

	js := newPollerTestJobSet("204").build()

	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}
	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(context.Background(), controllerKey, mockSlurm)

	teardownChan := make(chan slurmtypes.SlurmJobTeardownEvent, 10)
	currentTime := now
	poller := &SlurmPoller{
		Client:            fakeK8sClient,
		ClientMap:         cMap,
		TeardownEvents:    teardownChan,
		PollInterval:      10 * time.Second,
		OrphanGracePeriod: 1 * time.Minute,
		nowFn:             func() time.Time { return currentTime },
	}

	// 1. Initial observation: absent in Slurm
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 1 failed: %v", err)
	}
	select {
	case ev := <-teardownChan:
		t.Fatalf("Expected no teardown event on first observation, got: %v", ev)
	default:
	}

	// 2. Reappears in Slurm 30s later (should clear disappearance tracking)
	currentTime = now.Add(30 * time.Second)
	mockSlurm.SetPendingJobs(testPartitionName, []slurmclienttypes.V0044JobInfo{newRunningJob(204, testPartitionName)})
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 2 failed: %v", err)
	}
	select {
	case ev := <-teardownChan:
		t.Fatalf("Expected no teardown event for active running job, got: %v", ev)
	default:
	}

	// 3. Disappears from Slurm again at 70s (70s from initial now, but newly disappeared!).
	// If tracking was NOT cleared, elapsed time (70s > 60s) would erroneously trigger teardown.
	currentTime = now.Add(70 * time.Second)
	mockSlurm.SetPendingJobs(testPartitionName, nil)
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 3 failed: %v", err)
	}
	select {
	case ev := <-teardownChan:
		t.Fatalf("Expected no teardown event because grace period should reset on reappearance, got: %v", ev)
	default:
	}

	// 4. Finally, 70s after the SECOND disappearance (140s total), teardown should trigger.
	currentTime = now.Add(140 * time.Second)
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 4 failed: %v", err)
	}
	select {
	case ev := <-teardownChan:
		if ev.JobID != "204" {
			t.Errorf("Expected teardown for JobID 204, got %s", ev.JobID)
		}
	default:
		t.Fatal("Expected teardown event after second grace period elapsed, got none")
	}
}

type pollerTestEnv struct {
	poller        *SlurmPoller
	teardownChan  chan slurmtypes.SlurmJobTeardownEvent
	clientMap     *clientmap.ClientMap
	mockSlurm     *utils.SlurmClient
	controllerKey types.NamespacedName
	now           time.Time
}

func newPollerTestEnv(t *testing.T, slurmJobs []slurmclienttypes.V0044JobInfo, objs ...client.Object) *pollerTestEnv {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	wm.UID = testWMUID

	mockSlurm := &utils.SlurmClient{}
	mockSlurm.SetPendingJobs(testPartitionName, slurmJobs)
	cMap := clientmap.NewClientMap()
	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(t.Context(), controllerKey, mockSlurm)

	env := &pollerTestEnv{
		teardownChan:  make(chan slurmtypes.SlurmJobTeardownEvent, 10),
		clientMap:     cMap,
		mockSlurm:     mockSlurm,
		controllerKey: controllerKey,
		now:           time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC),
	}
	env.poller = &SlurmPoller{
		Client:            fake.NewClientBuilder().WithScheme(scheme).WithObjects(append([]client.Object{wm}, objs...)...).Build(),
		ClientMap:         cMap,
		TeardownEvents:    env.teardownChan,
		PollInterval:      10 * time.Second,
		OrphanGracePeriod: time.Minute,
		nowFn:             func() time.Time { return env.now },
	}
	return env
}

func (e *pollerTestEnv) poll(t *testing.T) {
	t.Helper()
	if err := e.poller.Poll(t.Context()); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}
}

func (e *pollerTestEnv) drainTeardownEvents() []slurmtypes.SlurmJobTeardownEvent {
	var events []slurmtypes.SlurmJobTeardownEvent
	for {
		select {
		case ev := <-e.teardownChan:
			events = append(events, ev)
		default:
			return events
		}
	}
}

// expectNoTeardownEvent fails the test if the poller sent a teardown event. step names the poll
// cycle in the failure message.
func (e *pollerTestEnv) expectNoTeardownEvent(t *testing.T, step string) {
	t.Helper()
	if events := e.drainTeardownEvents(); len(events) != 0 {
		t.Fatalf("%s: expected no teardown event, got %v", step, events)
	}
}

// expectTeardownEvent fails the test if the poller did not send exactly one teardown event, or if
// a non-empty field of want differs from that event.
func (e *pollerTestEnv) expectTeardownEvent(t *testing.T, want slurmtypes.SlurmJobTeardownEvent) {
	t.Helper()
	events := e.drainTeardownEvents()
	if len(events) != 1 {
		t.Fatalf("Expected 1 teardown event, got %d: %v", len(events), events)
	}
	got := events[0]
	if got.JobID != want.JobID {
		t.Errorf("Teardown event JobID = %q, want %q", got.JobID, want.JobID)
	}
	if want.Reason != "" && got.Reason != want.Reason {
		t.Errorf("Teardown event Reason = %q, want %q", got.Reason, want.Reason)
	}
	if want.Partition != "" && got.Partition != want.Partition {
		t.Errorf("Teardown event Partition = %q, want %q", got.Partition, want.Partition)
	}
}

func TestPoll_DeletingJobSet_TeardownEvents(t *testing.T) {
	completedJob := func(id int32) []slurmclienttypes.V0044JobInfo {
		return []slurmclienttypes.V0044JobInfo{
			*newEmptyTestJob().
				withJobID(id).
				withPartition(testPartitionName).
				withJobState(slurmapi.V0044JobInfoJobStateCOMPLETED).
				build(),
		}
	}

	tests := []struct {
		name      string
		jobSet    *jobsetv1alpha2.JobSet
		slurmJobs []slurmclienttypes.V0044JobInfo
		// afterGracePeriod polls a second time after the orphan grace period has elapsed.
		afterGracePeriod bool
		// wantReason is the expected teardown reason; empty means no teardown event.
		wantReason string
	}{
		{
			name:       "Terminal Slurm job with deleting JobSet carrying FinalizerJobSet emits teardown event",
			jobSet:     newPollerTestJobSet("210").withFinalizers(FinalizerJobSet).withDeletionTimestamp().build(),
			slurmJobs:  completedJob(210),
			wantReason: metrics.ReasonJobCompleted,
		},
		{
			name:      "Terminal Slurm job with deleting JobSet without FinalizerJobSet is ignored",
			jobSet:    newPollerTestJobSet("211").withFinalizers(testFinalizer).withDeletionTimestamp().build(),
			slurmJobs: completedJob(211),
		},
		{
			name:             "Orphaned deleting JobSet carrying FinalizerJobSet emits teardown event after grace period",
			jobSet:           newPollerTestJobSet("212").withFinalizers(FinalizerJobSet).withDeletionTimestamp().build(),
			afterGracePeriod: true,
			wantReason:       metrics.ReasonJobOrphaned,
		},
		{
			name:             "Orphaned deleting JobSet without FinalizerJobSet is ignored",
			jobSet:           newPollerTestJobSet("205").withFinalizers(testFinalizer).withDeletionTimestamp().build(),
			afterGracePeriod: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := newPollerTestEnv(t, tc.slurmJobs, tc.jobSet)
			env.poll(t)
			if tc.afterGracePeriod {
				env.now = env.now.Add(70 * time.Second)
				env.poll(t)
			}

			events := env.drainTeardownEvents()
			if tc.wantReason == "" {
				if len(events) != 0 {
					t.Fatalf("Expected no teardown events, got %+v", events)
				}
				return
			}
			wantJobID := tc.jobSet.Labels[LabelSlurmJobID]
			if len(events) != 1 || events[0].JobID != wantJobID || events[0].Reason != tc.wantReason {
				t.Fatalf("Expected one teardown event for JobID %s with reason %s, got %+v", wantJobID, tc.wantReason, events)
			}
		})
	}
}

func TestPoll_OrphanedJobSet_ChannelFull_RetriesOnNextPoll(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	wm.UID = testWMUID

	js := newPollerTestJobSet("206").build()

	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}
	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(context.Background(), controllerKey, mockSlurm)

	// Pre-fill channel with capacity 1 so it is full
	teardownChan := make(chan slurmtypes.SlurmJobTeardownEvent, 1)
	teardownChan <- slurmtypes.SlurmJobTeardownEvent{JobID: "blocking-event"}

	currentTime := now
	poller := &SlurmPoller{
		Client:            fakeK8sClient,
		ClientMap:         cMap,
		TeardownEvents:    teardownChan,
		PollInterval:      10 * time.Second,
		OrphanGracePeriod: 1 * time.Minute,
		nowFn:             func() time.Time { return currentTime },
	}

	// Cycle 1: initial observation
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 1 failed: %v", err)
	}

	// Cycle 2: grace period elapsed, but channel is full -> dropped via default case
	currentTime = now.Add(70 * time.Second)
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 2 failed: %v", err)
	}

	// Drain blocking event
	<-teardownChan

	// Cycle 3: next poll tick (shortly after, 75s), channel has space -> event dispatched immediately
	// Proves that dropping an event does NOT reset the grace timer back to zero.
	currentTime = now.Add(75 * time.Second)
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 3 failed: %v", err)
	}

	select {
	case ev := <-teardownChan:
		if ev.JobID != "206" {
			t.Errorf("Expected JobID 206, got %s", ev.JobID)
		}
	default:
		t.Fatal("Expected teardown event on retry poll, got none")
	}
}

func TestPoll_OrphanedJobSet_NilTeardownChannel_PreservesTracking(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	wm.UID = testWMUID

	js := newPollerTestJobSet("207").build()

	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, js).Build()
	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}
	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(context.Background(), controllerKey, mockSlurm)

	currentTime := now
	poller := &SlurmPoller{
		Client:            fakeK8sClient,
		ClientMap:         cMap,
		TeardownEvents:    nil,
		PollInterval:      10 * time.Second,
		OrphanGracePeriod: 1 * time.Minute,
		nowFn:             func() time.Time { return currentTime },
	}

	// Cycle 1: Poll with nil TeardownEvents at now
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	// Cycle 2: Poll past grace period with nil channel (should drop event gracefully without crashing)
	currentTime = now.Add(70 * time.Second)
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	// Assign teardown channel and poll again (shortly after, 75s)
	teardownChan := make(chan slurmtypes.SlurmJobTeardownEvent, 10)
	poller.TeardownEvents = teardownChan
	currentTime = now.Add(75 * time.Second)
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	select {
	case ev := <-teardownChan:
		if ev.JobID != "207" {
			t.Errorf("Expected JobID 207, got %s", ev.JobID)
		}
	default:
		t.Fatal("Expected teardown event after channel was assigned, got none")
	}
}

func TestPoll_OrphanedJobSet_PrunesDeletedJobSetsFromTracking(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	wm.UID = testWMUID

	jsA := newPollerTestJobSet("208a").build()
	jsB := newPollerTestJobSet("208b").build()

	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, jsA, jsB).Build()
	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}
	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(context.Background(), controllerKey, mockSlurm)

	teardownChan := make(chan slurmtypes.SlurmJobTeardownEvent, 10)
	currentTime := now
	poller := &SlurmPoller{
		Client:            fakeK8sClient,
		ClientMap:         cMap,
		TeardownEvents:    teardownChan,
		PollInterval:      10 * time.Second,
		OrphanGracePeriod: 1 * time.Minute,
		nowFn:             func() time.Time { return currentTime },
	}

	// 1. Initial poll: tracks both jsA and jsB
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 1 failed: %v", err)
	}

	// 2. Delete jsB from Kubernetes at 30s
	currentTime = now.Add(30 * time.Second)
	if err := fakeK8sClient.Delete(context.Background(), jsB); err != nil {
		t.Fatalf("Failed to delete jsB: %v", err)
	}
	// Poll prunes jsB because it no longer exists in K8s
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 2 failed: %v", err)
	}

	// 3. Re-create jsB in Kubernetes at 30s
	jsBNew := jsB.DeepCopy()
	jsBNew.ResourceVersion = ""
	if err := fakeK8sClient.Create(context.Background(), jsBNew); err != nil {
		t.Fatalf("Failed to recreate jsB: %v", err)
	}

	// 4. Poll at 70s (70s elapsed for jsA, but jsB was pruned and re-tracked!)
	currentTime = now.Add(70 * time.Second)
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll cycle 3 failed: %v", err)
	}

	// jsA must have triggered teardown (70s >= 60s)
	select {
	case ev := <-teardownChan:
		if ev.JobID != "208a" {
			t.Errorf("Expected teardown event for jsA (208a), got %s", ev.JobID)
		}
	default:
		t.Fatal("Expected teardown event for jsA, got none")
	}

	// jsB must NOT have triggered teardown yet because its tracking was pruned on deletion!
	select {
	case ev := <-teardownChan:
		t.Fatalf("Expected no teardown event for recreated jsB yet, got: %v", ev)
	default:
	}
}

// TestPoll_OrphanedJobSet_SlurmUnavailable_NoTeardown checks that the poller does not tear down
// a JobSet when it cannot read the Slurm job list. If the poller used an empty job list instead,
// a Slurm outage would tear down all running workloads. When Slurm answers again and the job is
// still missing, the poller tears down the JobSet at once: two successful polls that are more than
// the grace period apart did not find the job.
func TestPoll_OrphanedJobSet_SlurmUnavailable_NoTeardown(t *testing.T) {
	tests := []struct {
		name            string
		makeUnavailable func(t *testing.T, env *pollerTestEnv)
	}{
		{
			name: "Slurm job list fails",
			makeUnavailable: func(t *testing.T, env *pollerTestEnv) {
				env.clientMap.Add(t.Context(), env.controllerKey, &errorClient{})
			},
		},
		{
			name: "Slurm client not ready",
			makeUnavailable: func(_ *testing.T, env *pollerTestEnv) {
				env.clientMap.Remove(env.controllerKey)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := newPollerTestEnv(t, nil, newPollerTestJobSet("210").build())
			start := env.now

			// Poll cycle 1: Slurm answers without job 210, so the grace period starts.
			env.poll(t)
			env.expectNoTeardownEvent(t, "Poll cycle 1")

			// Poll cycle 2: the grace period is over, but Slurm is unavailable. The poller
			// cannot confirm that job 210 is still missing, so it must keep the JobSet.
			tc.makeUnavailable(t, env)
			env.now = start.Add(70 * time.Second)
			env.poll(t)
			env.expectNoTeardownEvent(t, "Poll cycle 2")

			// Poll cycle 3: Slurm answers again, and job 210 is still missing. This also proves
			// that poll cycle 1 started the grace period, so poll cycle 2 checked a real case.
			env.clientMap.Add(t.Context(), env.controllerKey, env.mockSlurm)
			env.now = start.Add(80 * time.Second)
			env.poll(t)
			env.expectTeardownEvent(t, slurmtypes.SlurmJobTeardownEvent{JobID: "210", Reason: metrics.ReasonJobOrphaned})
		})
	}
}

// TestPoll_OrphanedJobSet_SlurmOutage_GracePeriodStartsAfterRecovery checks that a Slurm outage
// does not start the orphan grace period, also when the outage is longer than the grace period.
// The job is in Slurm before the outage, so the grace period starts at the first successful poll
// after the outage that does not find the job.
func TestPoll_OrphanedJobSet_SlurmOutage_GracePeriodStartsAfterRecovery(t *testing.T) {
	env := newPollerTestEnv(t,
		[]slurmclienttypes.V0044JobInfo{newRunningJob(211, testPartitionName)},
		newPollerTestJobSet("211").build(),
	)
	start := env.now

	// Poll cycle 1: job 211 runs in Slurm.
	env.poll(t)
	env.expectNoTeardownEvent(t, "Poll cycle 1")

	// Poll cycles 2 to 4: Slurm is unavailable for 3 minutes, which is 3 times the grace period.
	env.clientMap.Add(t.Context(), env.controllerKey, &errorClient{})
	for _, elapsed := range []time.Duration{10 * time.Second, 70 * time.Second, 3 * time.Minute} {
		env.now = start.Add(elapsed)
		env.poll(t)
		env.expectNoTeardownEvent(t, fmt.Sprintf("Poll at %s with Slurm unavailable", elapsed))
	}

	// Poll cycle 5: Slurm is available again, but job 211 is gone, for example because slurmctld
	// lost its state. This poll is the first one that does not find the job, so the grace period
	// starts now.
	env.mockSlurm.SetPendingJobs(testPartitionName, nil)
	env.clientMap.Add(t.Context(), env.controllerKey, env.mockSlurm)
	recovery := start.Add(3*time.Minute + 10*time.Second)
	env.now = recovery
	env.poll(t)
	env.expectNoTeardownEvent(t, "Poll cycle 5")

	// Poll cycle 6: the grace period that started at recovery is over.
	env.now = recovery.Add(70 * time.Second)
	env.poll(t)
	env.expectTeardownEvent(t, slurmtypes.SlurmJobTeardownEvent{JobID: "211", Reason: metrics.ReasonJobOrphaned})
}

func TestShouldEmitAdmissionEvent(t *testing.T) {
	const jobID int32 = 301
	const jobSetName = "slurm-job-301"

	newPendingJobBuilder := func() *testJobBuilder {
		return newEmptyTestJob().
			withJobID(jobID).
			withPartition(testPartitionName).
			withJobState(slurmapi.V0044JobInfoJobStatePENDING)
	}
	translatedFeature := fmt.Sprintf("%s%d", UniqueFeaturePrefix, jobID)
	unheld := unheldStatus(jobSetName)
	pending := pendingStatus(kueueReasonPrefix + testQuotaMessage)
	newRunningJobBuilder := func() *testJobBuilder {
		return newEmptyTestJob().
			withJobID(jobID).
			withPartition(testPartitionName).
			withJobState(slurmapi.V0044JobInfoJobStateRUNNING).
			withHold(false).
			withFeatures(translatedFeature)
	}

	tests := []struct {
		name       string
		job        *slurmclienttypes.V0044JobInfo
		existingJS *jobsetv1alpha2.JobSet
		want       bool
		// wantStatus, if set, is the expected activeJobStatus.
		wantStatus K8sbridgeJobStatus
		// workloads replaces the default index, which maps the JobSet to a Workload waiting for quota.
		workloads workloadsByJobUID
		// workloadsUnavailable simulates a failed Workload LIST.
		workloadsUnavailable bool
	}{
		{
			name: "Skips when PENDING job has existing unsuspended JobSet and is already unheld (StatusNoAction)",
			job: newPendingJobBuilder().
				withHold(false).
				withStateReason("Resources").
				build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(false).build(),
			want:       false,
		},
		{
			name: "Skips when PENDING job has existing JobSet suspended by Kueue preemption (StatusNoAction)",
			job: newPendingJobBuilder().
				withHold(false).
				withStateReason("Resources").
				build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(true).build(),
			want:       false,
		},
		{
			name: "Skips when PENDING job has no JobSet and is explicitly held by user",
			job: newPendingJobBuilder().
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				withComment("user hold comment").
				build(),
			existingJS: nil,
			want:       false,
		},
		{
			name: "Skips when PENDING job has no JobSet and is explicitly held by admin",
			job: newPendingJobBuilder().
				withHold(true).
				withStateReason(ReasonJobHeldAdmin).
				withComment(AutoHoldMarker).
				build(),
			existingJS: nil,
			want:       false,
		},
		{
			name: "Emits when PENDING job has no JobSet, is explicitly held, and requests multiple partitions",
			job: newPendingJobBuilder().
				withPartition(testPartitionName + ",other-partition").
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				build(),
			existingJS: nil,
			want:       true,
		},
		{
			name: "Emits when PENDING job has no JobSet and is auto-held",
			job: newPendingJobBuilder().
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				withComment(AutoHoldMarker).
				build(),
			existingJS: nil,
			want:       true,
		},
		{
			name: "Skips when PENDING job has existing unsuspended JobSet and becomes explicitly held (StatusNoAction)",
			job: newPendingJobBuilder().
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(false).build(),
			want:       false,
		},
		{
			name: "Emits when PENDING job has existing unsuspended JobSet and is still auto-held (StatusShouldUnhold)",
			job: newPendingJobBuilder().
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				withComment(AutoHoldMarker).
				build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(false).build(),
			want:       true,
		},
		{
			name: "Emits when translated unheld PENDING job has a suspended JobSet (StatusPendingInKubernetes)",
			job: newPendingJobBuilder().
				withHold(false).
				withFeatures(translatedFeature).
				withComment(unheld).
				build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(true).build(),
			want:       true,
		},
		{
			name: "Emits when translated PENDING job held by Slurm for BadConstraints has a suspended JobSet (StatusPendingInKubernetes)",
			job: newPendingJobBuilder().
				withHold(true).
				withStateReason("BadConstraints").
				withFeatures(translatedFeature).
				withComment(unheld).
				build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(true).build(),
			want:       true,
			wantStatus: StatusPendingInKubernetes,
		},
		{
			name: "Skips when translated PENDING job with a suspended JobSet is explicitly held by user (StatusNoAction)",
			job: newPendingJobBuilder().
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				withFeatures(translatedFeature).
				withComment(unheld).
				build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(true).build(),
			want:       false,
			wantStatus: StatusNoAction,
		},
		{
			name: "Emits when translated PENDING job with a Pending status has an unsuspended JobSet",
			job: newPendingJobBuilder().
				withHold(false).
				withFeatures(translatedFeature).
				withComment(pending).
				build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(false).build(),
			want:       true,
		},
		{
			name: "Skips when explicitly held translated PENDING job has a suspended JobSet",
			job: newPendingJobBuilder().
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				withFeatures(translatedFeature).
				withComment(unheld).
				build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(true).build(),
			want:       false,
		},
		{
			name: "Emits when auto-held PENDING job with a JobSet creation retry status has no JobSet (StatusNewJob)",
			job: newPendingJobBuilder().
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				withComment("my run, " + pendingStatus(jobSetCreateRetryReason+"etcd unavailable") + commentSeparator + AutoHoldMarker).
				build(),
			existingJS: nil,
			want:       true,
			wantStatus: StatusNewJob,
		},
		{
			name:       "Skips when RUNNING job has a clean comment",
			job:        newRunningJobBuilder().withComment(unheld).build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(false).build(),
			want:       false,
		},
		{
			name:       "Emits when RUNNING job still has a Pending status",
			job:        newRunningJobBuilder().withComment("my run, " + pending).build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(false).build(),
			want:       true,
		},
		{
			name:       "Skips when RUNNING job with a Pending status has a suspended JobSet",
			job:        newRunningJobBuilder().withComment(pending).build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(true).build(),
			want:       false,
		},
		{
			name:       "Skips when RUNNING job with a Pending status has no JobSet",
			job:        newRunningJobBuilder().withComment(pending).build(),
			existingJS: nil,
			want:       false,
		},
		{
			name:       "Skips when the comment already shows the Kueue reason",
			job:        newPendingJobBuilder().withHold(false).withFeatures(translatedFeature).withComment(pending).build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(true).build(),
			want:       false,
			wantStatus: StatusPendingInKubernetes,
		},
		{
			name:       "Emits when the Kueue reason changed",
			job:        newPendingJobBuilder().withHold(false).withFeatures(translatedFeature).withComment(pendingStatus(kueueReasonPrefix + "old reason")).build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(true).build(),
			want:       true,
		},
		{
			name:       "Skips when the suspended JobSet has no Workload",
			job:        newPendingJobBuilder().withHold(false).withFeatures(translatedFeature).withComment(unheld).build(),
			existingJS: newTestJobSet(jobSetName).withSuspend(true).build(),
			workloads:  workloadsByJobUID{},
			want:       false,
		},
		{
			name:                 "Skips the Kueue reason when the Workloads cannot be read",
			job:                  newPendingJobBuilder().withHold(false).withFeatures(translatedFeature).withComment(unheld).build(),
			existingJS:           newTestJobSet(jobSetName).withSuspend(true).build(),
			workloadsUnavailable: true,
			want:                 false,
		},
		{
			name:                 "Emits the Unheld restore when the Workloads cannot be read",
			job:                  newPendingJobBuilder().withHold(false).withFeatures(translatedFeature).withComment(pending).build(),
			existingJS:           newTestJobSet(jobSetName).withSuspend(false).build(),
			workloadsUnavailable: true,
			want:                 true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			workloads := tc.workloads
			if workloads == nil && tc.existingJS != nil {
				workloads = workloadsByJobUID{tc.existingJS.UID: newTestWorkload("wl", tc.existingJS.UID,
					workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", testQuotaMessage)))}
			}
			if tc.workloadsUnavailable {
				workloads = nil
			}
			if got := shouldEmitAdmissionEvent(tc.job, tc.existingJS, func() workloadsByJobUID { return workloads }); got != tc.want {
				t.Errorf("shouldEmitAdmissionEvent() = %v, want %v", got, tc.want)
			}
			if tc.wantStatus != "" {
				if got := activeJobStatus(tc.job, tc.existingJS); got != tc.wantStatus {
					t.Errorf("activeJobStatus() = %q, want %q", got, tc.wantStatus)
				}
			}
		})
	}
}

const pollJobSetUID types.UID = "poll-jobset-uid"

func TestPoll_FilterRedundantAdmissionEvents(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)
	addTestWorkloadTypes(scheme)

	const jobID int32 = 301
	const jobSetName = "slurm-job-301"

	newPendingJobBuilder := func() *testJobBuilder {
		return newEmptyTestJob().
			withJobID(jobID).
			withPartition(testPartitionName).
			withJobState(slurmapi.V0044JobInfoJobStatePENDING)
	}

	newOwnedJobSetBuilder := func(ownerUID types.UID) *testJobSetBuilder {
		return newTestJobSet(jobSetName).
			withNamespace(testNamespace).
			withUID(pollJobSetUID).
			withLabels(map[string]string{
				LabelSlurmJobID:     "301",
				LabelSlurmPartition: testPartitionName,
			}).
			withOwnerReference(metav1.OwnerReference{
				APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
				Kind:       KindWorkloadMixing,
				Name:       testWMName,
				UID:        ownerUID,
			})
	}

	tests := []struct {
		name       string
		job        slurmclienttypes.V0044JobInfo
		existingJS *jobsetv1alpha2.JobSet
		wantEmit   bool
	}{
		{
			name: "Skips when PENDING job has existing unsuspended JobSet and is already unheld (StatusNoAction)",
			job: *newPendingJobBuilder().
				withHold(false).
				withStateReason("Resources").
				build(),
			existingJS: newOwnedJobSetBuilder(testWMUID).withSuspend(false).build(),
			wantEmit:   false,
		},
		{
			name: "Emits when PENDING job has existing JobSet with mismatched owner UID (not owned by WM)",
			job: *newPendingJobBuilder().
				withHold(true).
				withStateReason(ReasonJobHeldUser).
				withComment(AutoHoldMarker).
				build(),
			existingJS: newOwnedJobSetBuilder("stale-wm-uid").withSuspend(true).build(),
			wantEmit:   true,
		},
		{
			name: "Emits when translated unheld PENDING job has a suspended JobSet",
			job: *newPendingJobBuilder().
				withHold(false).
				withStateReason("Resources").
				withFeatures(fmt.Sprintf("%s%d", UniqueFeaturePrefix, jobID)).
				withComment(unheldStatus(jobSetName)).
				build(),
			existingJS: newOwnedJobSetBuilder(testWMUID).withSuspend(true).build(),
			wantEmit:   true,
		},
		{
			name: "Emits when RUNNING job still has a Pending status",
			job: *newEmptyTestJob().
				withJobID(jobID).
				withPartition(testPartitionName).
				withJobState(slurmapi.V0044JobInfoJobStateRUNNING).
				withHold(false).
				withFeatures(fmt.Sprintf("%s%d", UniqueFeaturePrefix, jobID)).
				withComment(pendingStatus(kueueReasonPrefix + testQuotaMessage)).
				build(),
			existingJS: newOwnedJobSetBuilder(testWMUID).withSuspend(false).build(),
			wantEmit:   true,
		},
		{
			name: "Skips when RUNNING job has a clean comment",
			job: *newEmptyTestJob().
				withJobID(jobID).
				withPartition(testPartitionName).
				withJobState(slurmapi.V0044JobInfoJobStateRUNNING).
				withHold(false).
				withFeatures(fmt.Sprintf("%s%d", UniqueFeaturePrefix, jobID)).
				withComment(unheldStatus(jobSetName)).
				build(),
			existingJS: newOwnedJobSetBuilder(testWMUID).withSuspend(false).build(),
			wantEmit:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
			wm.UID = testWMUID

			wl := newTestWorkload("wl", pollJobSetUID,
				workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", testQuotaMessage)))
			wl.SetNamespace(testNamespace)
			objs := []client.Object{wm, wl}
			if tc.existingJS != nil {
				objs = append(objs, tc.existingJS)
			}
			fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

			cMap := clientmap.NewClientMap()
			mockSlurm := &utils.SlurmClient{}
			mockSlurm.SetPendingJobs(testPartitionName, []slurmclienttypes.V0044JobInfo{tc.job})
			controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
			cMap.Add(ctx, controllerKey, mockSlurm)

			admissionChan := make(chan slurmtypes.SlurmJobAdmissionEvent, 10)
			teardownChan := make(chan slurmtypes.SlurmJobTeardownEvent, 10)
			poller := &SlurmPoller{
				Client:          fakeK8sClient,
				APIReader:       fakeK8sClient,
				ClientMap:       cMap,
				AdmissionEvents: admissionChan,
				TeardownEvents:  teardownChan,
				PollInterval:    10 * time.Second,
			}

			if err := poller.Poll(ctx); err != nil {
				t.Fatalf("Poll failed: %v", err)
			}

			want := 0
			if tc.wantEmit {
				want = 1
			}
			if got := len(admissionChan); got != want {
				t.Fatalf("Poll() emitted %d admission events, want %d", got, want)
			}
		})
	}
}

// pendingReasonPollEnv is a poller with translated jobs waiting in Kueue, whose Workload LISTs are
// counted.
type pendingReasonPollEnv struct {
	poller        *SlurmPoller
	admissionChan chan slurmtypes.SlurmJobAdmissionEvent
	workloadLists *atomic.Int32
	// workloadSelectors are the label selectors of the Workload LISTs.
	workloadSelectors []string
	k8sClient         client.WithWatch
	reader            client.Client
	mockSlurm         *utils.SlurmClient
	clientMap         *clientmap.ClientMap
}

func newPendingReasonPollEnv(t *testing.T, jobs []slurmclienttypes.V0044JobInfo, objs []client.Object, listErr error) *pendingReasonPollEnv {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)
	addTestWorkloadTypes(scheme)

	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	wm.UID = testWMUID
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(append([]client.Object{wm}, objs...)...).Build()

	lists := &atomic.Int32{}
	env := &pendingReasonPollEnv{
		admissionChan: make(chan slurmtypes.SlurmJobAdmissionEvent, 20),
		workloadLists: lists,
		k8sClient:     k8sClient,
	}
	reader := interceptor.NewClient(k8sClient, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if u, ok := list.(*unstructured.UnstructuredList); ok && u.GroupVersionKind() == workloadListGVK {
				lists.Add(1)
				listOpts := &client.ListOptions{}
				listOpts.ApplyOptions(opts)
				selector := ""
				if listOpts.LabelSelector != nil {
					selector = listOpts.LabelSelector.String()
				}
				env.workloadSelectors = append(env.workloadSelectors, selector)
				if listErr != nil {
					return listErr
				}
			}
			return c.List(ctx, list, opts...)
		},
	})

	mockSlurm := &utils.SlurmClient{MutateOnUpdate: true}
	mockSlurm.SetPendingJobs(testPartitionName, jobs)
	cMap := clientmap.NewClientMap()
	cMap.Add(t.Context(), types.NamespacedName{Namespace: testNamespace, Name: testControllerName}, mockSlurm)

	env.reader = reader
	env.mockSlurm = mockSlurm
	env.clientMap = cMap
	env.poller = &SlurmPoller{
		Client:          k8sClient,
		APIReader:       reader,
		ClientMap:       cMap,
		AdmissionEvents: env.admissionChan,
		TeardownEvents:  make(chan slurmtypes.SlurmJobTeardownEvent, 20),
		PollInterval:    10 * time.Second,
	}
	return env
}

// emittedJobIDs polls once and returns the job IDs of the admission events, sorted.
func (e *pendingReasonPollEnv) emittedJobIDs(t *testing.T) []string {
	t.Helper()
	if err := e.poller.Poll(t.Context()); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}
	var ids []string
	for len(e.admissionChan) > 0 {
		ids = append(ids, (<-e.admissionChan).JobID)
	}
	slices.Sort(ids)
	return ids
}

// reconcile runs the SlurmJobReconciler for the job, against the same Kubernetes and Slurm state.
func (e *pendingReasonPollEnv) reconcile(t *testing.T, jobID string) error {
	t.Helper()
	r := &SlurmJobReconciler{
		Client:    e.k8sClient,
		APIReader: e.reader,
		Scheme:    e.k8sClient.Scheme(),
		ClientMap: e.clientMap,
	}
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{
		Namespace: testNamespace,
		Name:      fmt.Sprintf("slurm-job-%s-%s", testWMName, jobID),
	}})
	return err
}

func TestPoll_PendingReasons(t *testing.T) {
	// A translated PENDING job is reported as held for BadConstraints until it has a node.
	translatedJob := func(id int32, state slurmapi.V0044JobInfoJobState, comment string) slurmclienttypes.V0044JobInfo {
		pending := state == slurmapi.V0044JobInfoJobStatePENDING
		reason := ""
		if pending {
			reason = "BadConstraints"
		}
		return *newEmptyTestJob().
			withJobID(id).
			withPartition(testPartitionName).
			withJobState(state).
			withHold(pending).
			withStateReason(reason).
			withFeatures(fmt.Sprintf("%s%d", UniqueFeaturePrefix, id)).
			withComment(comment).
			build()
	}
	ownedJobSet := func(id int32, suspended bool) *jobsetv1alpha2.JobSet {
		return newTestJobSet(fmt.Sprintf("slurm-job-%d", id)).
			withNamespace(testNamespace).
			withUID(types.UID(fmt.Sprintf("uid-%d", id))).
			withLabels(map[string]string{LabelSlurmJobID: fmt.Sprint(id), LabelKueueQueue: testQueueName}).
			withOwnerReference(metav1.OwnerReference{Kind: KindWorkloadMixing, Name: testWMName, UID: testWMUID}).
			withSuspend(suspended).
			build()
	}
	quotaWorkload := func(id int32) *unstructured.Unstructured {
		wl := newTestWorkload(fmt.Sprintf("wl-%d", id), types.UID(fmt.Sprintf("uid-%d", id)),
			workloadConditions(workloadCondition(workloadConditionQuotaReserved, "False", testQuotaMessage)))
		wl.SetNamespace(testNamespace)
		return wl
	}
	kueueQuota := pendingStatus(kueueReasonPrefix + testQuotaMessage)

	jobs := []slurmclienttypes.V0044JobInfo{
		// Waits for quota and still shows the Unheld status.
		translatedJob(401, slurmapi.V0044JobInfoJobStatePENDING, unheldStatus("slurm-job-401")),
		// Waits in the queue without a Kueue message.
		translatedJob(402, slurmapi.V0044JobInfoJobStatePENDING, unheldStatus("slurm-job-402")),
		// Already shows the current reason.
		translatedJob(403, slurmapi.V0044JobInfoJobStatePENDING, kueueQuota),
		// Admitted, but still shows a Pending status.
		translatedJob(404, slurmapi.V0044JobInfoJobStateRUNNING, kueueQuota),
		// New auto-held job without a JobSet.
		*newEmptyTestJob().withJobID(405).withPartition(testPartitionName).withAutoHold().build(),
	}
	objs := []client.Object{
		ownedJobSet(401, true), quotaWorkload(401),
		ownedJobSet(402, true), func() client.Object {
			wl := newTestWorkload("wl-402", "uid-402", nil)
			wl.SetNamespace(testNamespace)
			return wl
		}(),
		ownedJobSet(403, true), quotaWorkload(403),
		ownedJobSet(404, false),
	}
	workloadGR := schema.GroupResource{Group: workloadListGVK.Group, Resource: "workloads"}

	const wantSelector = kueueJobUIDLabel + " in (uid-401,uid-402,uid-403)"
	tests := []struct {
		name      string
		listErr   error
		wantIDs   []string
		wantLists int32
	}{
		{name: "one LIST, emits only changed comments", wantIDs: []string{"401", "402", "404", "405"}, wantLists: 1},
		{name: "LIST error skips the Kueue reasons only", listErr: errors.New("connection refused"), wantIDs: []string{"404", "405"}, wantLists: 1},
		{name: "unavailable API skips the Kueue reasons only", listErr: apierrors.NewForbidden(workloadGR, "", errors.New("RBAC denied")), wantIDs: []string{"404", "405"}, wantLists: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env := newPendingReasonPollEnv(t, jobs, objs, tc.listErr)
			if got := env.emittedJobIDs(t); !slices.Equal(got, tc.wantIDs) {
				t.Errorf("Poll() emitted events for jobs %v, want %v", got, tc.wantIDs)
			}
			if got := env.workloadLists.Load(); got != tc.wantLists {
				t.Errorf("Poll() listed Workloads %d times, want %d", got, tc.wantLists)
			}
			if want := []string{wantSelector}; !slices.Equal(env.workloadSelectors, want) {
				t.Errorf("Workload LIST selectors = %q, want %q", env.workloadSelectors, want)
			}
		})
	}

	t.Run("converges after the emitted jobs are reconciled", func(t *testing.T) {
		env := newPendingReasonPollEnv(t, jobs, objs, nil)
		for _, id := range env.emittedJobIDs(t) {
			// The new job 405 cannot be translated here (no Slurm Controller); it stays new.
			if err := env.reconcile(t, id); err != nil && id != "405" {
				t.Fatalf("Reconcile(%s) returned unexpected error: %v", id, err)
			}
		}
		if got, want := env.emittedJobIDs(t), []string{"405"}; !slices.Equal(got, want) {
			t.Errorf("second Poll() emitted events for jobs %v, want only the untranslated job %v", got, want)
		}
	})

	t.Run("no LIST without a job waiting in Kueue", func(t *testing.T) {
		env := newPendingReasonPollEnv(t, jobs[3:], []client.Object{ownedJobSet(404, false)}, nil)
		if got, want := env.emittedJobIDs(t), []string{"404", "405"}; !slices.Equal(got, want) {
			t.Errorf("Poll() emitted events for jobs %v, want %v", got, want)
		}
		if got := env.workloadLists.Load(); got != 0 {
			t.Errorf("Poll() listed Workloads %d times, want 0", got)
		}
	})
}
