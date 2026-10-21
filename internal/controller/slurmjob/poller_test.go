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
	"fmt"
	"testing"
	"time"

	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	"github.com/SlinkyProject/slurm-client/pkg/object"
	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

func TestPoll_TerminalStateDetection(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testJobSet101Name,
			Namespace: testNamespace,
			Labels: map[string]string{
				LabelSlurmJobID:     "101",
				LabelSlurmPartition: testPartitionName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
					Kind:       KindWorkloadMixing,
					Name:       testWMName,
					UID:        testWMUID,
				},
			},
		},
	}

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
	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testJobSet102Name,
			Namespace: testNamespace,
			Labels: map[string]string{
				LabelSlurmJobID:     "102",
				LabelSlurmPartition: testPartitionName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
					Kind:       KindWorkloadMixing,
					Name:       testWMName,
					UID:        testWMUID,
				},
			},
		},
	}

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
	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testJobSet101Name,
			Namespace: testNamespace,
			Labels: map[string]string{
				LabelSlurmJobID:     "101",
				LabelSlurmPartition: testPartitionName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
					Kind:       KindWorkloadMixing,
					Name:       testWMName,
					UID:        "stale-wm-uid",
				},
			},
		},
	}

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

	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "slurm-job-202",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.NewTime(now.Add(-5 * time.Minute)),
			Labels: map[string]string{
				LabelSlurmJobID:     "202",
				LabelSlurmPartition: testPartitionName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
					Kind:       KindWorkloadMixing,
					Name:       testWMName,
					UID:        testWMUID,
				},
			},
		},
	}

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

	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "slurm-job-209",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.NewTime(now.Add(-5 * time.Minute)),
			Labels: map[string]string{
				LabelSlurmJobID: "209",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
					Kind:       KindWorkloadMixing,
					Name:       testWMName,
					UID:        testWMUID,
				},
			},
		},
	}

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

	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "slurm-job-204",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.NewTime(now.Add(-5 * time.Minute)),
			Labels: map[string]string{
				LabelSlurmJobID:     "204",
				LabelSlurmPartition: testPartitionName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
					Kind:       KindWorkloadMixing,
					Name:       testWMName,
					UID:        testWMUID,
				},
			},
		},
	}

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

func TestPoll_OrphanedJobSet_AlreadyDeletingIgnored(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	wm.UID = testWMUID

	deletionTime := metav1.NewTime(now.Add(-10 * time.Second))
	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "slurm-job-205",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.NewTime(now.Add(-5 * time.Minute)),
			DeletionTimestamp: &deletionTime,
			Finalizers:        []string{testFinalizer},
			Labels: map[string]string{
				LabelSlurmJobID:     "205",
				LabelSlurmPartition: testPartitionName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
					Kind:       KindWorkloadMixing,
					Name:       testWMName,
					UID:        testWMUID,
				},
			},
		},
	}

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

	// Poll cycle 1 at now
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	// Poll cycle 2 past grace period
	currentTime = now.Add(70 * time.Second)
	if err := poller.Poll(context.Background()); err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	select {
	case ev := <-teardownChan:
		t.Fatalf("Expected no teardown event for JobSet with DeletionTimestamp, got: %v", ev)
	default:
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

	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "slurm-job-206",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.NewTime(now.Add(-5 * time.Minute)),
			Labels: map[string]string{
				LabelSlurmJobID:     "206",
				LabelSlurmPartition: testPartitionName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
					Kind:       KindWorkloadMixing,
					Name:       testWMName,
					UID:        testWMUID,
				},
			},
		},
	}

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

	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "slurm-job-207",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.NewTime(now.Add(-5 * time.Minute)),
			Labels: map[string]string{
				LabelSlurmJobID:     "207",
				LabelSlurmPartition: testPartitionName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
					Kind:       KindWorkloadMixing,
					Name:       testWMName,
					UID:        testWMUID,
				},
			},
		},
	}

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

	jsA := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "slurm-job-208a",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.NewTime(now.Add(-5 * time.Minute)),
			Labels: map[string]string{
				LabelSlurmJobID:     "208a",
				LabelSlurmPartition: testPartitionName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
					Kind:       KindWorkloadMixing,
					Name:       testWMName,
					UID:        testWMUID,
				},
			},
		},
	}

	jsB := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "slurm-job-208b",
			Namespace:         testNamespace,
			CreationTimestamp: metav1.NewTime(now.Add(-5 * time.Minute)),
			Labels: map[string]string{
				LabelSlurmJobID:     "208b",
				LabelSlurmPartition: testPartitionName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sbridgev1alpha1.GroupVersion.String(),
					Kind:       KindWorkloadMixing,
					Name:       testWMName,
					UID:        testWMUID,
				},
			},
		},
	}

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

// newOwnedJobSet returns a JobSet for Slurm job jobID that the test WorkloadMixing owns.
func newOwnedJobSet(jobID string) *jobsetv1alpha2.JobSet {
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
		}).
		build()
}

// expectNoTeardownEvent fails the test if the poller sent a teardown event.
func expectNoTeardownEvent(t *testing.T, teardownChan <-chan slurmtypes.SlurmJobTeardownEvent, step string) {
	t.Helper()
	select {
	case ev := <-teardownChan:
		t.Fatalf("%s: expected no teardown event, got: %v", step, ev)
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
		makeUnavailable func(ctx context.Context, cMap *clientmap.ClientMap, key types.NamespacedName)
	}{
		{
			name: "Slurm job list fails",
			makeUnavailable: func(ctx context.Context, cMap *clientmap.ClientMap, key types.NamespacedName) {
				cMap.Add(ctx, key, &errorClient{})
			},
		},
		{
			name: "Slurm client not ready",
			makeUnavailable: func(_ context.Context, cMap *clientmap.ClientMap, key types.NamespacedName) {
				cMap.Remove(key)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = k8sbridgev1alpha1.AddToScheme(scheme)
			_ = jobsetv1alpha2.AddToScheme(scheme)

			now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
			wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
			wm.UID = testWMUID

			fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, newOwnedJobSet("210")).Build()
			cMap := clientmap.NewClientMap()
			controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
			cMap.Add(ctx, controllerKey, &utils.SlurmClient{})

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

			// Poll cycle 1: Slurm answers without job 210, so the grace period starts.
			if err := poller.Poll(ctx); err != nil {
				t.Fatalf("Poll cycle 1 failed: %v", err)
			}
			expectNoTeardownEvent(t, teardownChan, "Poll cycle 1")

			// Poll cycle 2: the grace period is over, but Slurm is unavailable. The poller
			// cannot confirm that job 210 is still missing, so it must keep the JobSet.
			tc.makeUnavailable(ctx, cMap, controllerKey)
			currentTime = now.Add(70 * time.Second)
			if err := poller.Poll(ctx); err != nil {
				t.Fatalf("Poll cycle 2 failed: %v", err)
			}
			expectNoTeardownEvent(t, teardownChan, "Poll cycle 2")

			// Poll cycle 3: Slurm answers again, and job 210 is still missing. This also proves
			// that poll cycle 1 started the grace period, so poll cycle 2 checked a real case.
			cMap.Add(ctx, controllerKey, &utils.SlurmClient{})
			currentTime = now.Add(80 * time.Second)
			if err := poller.Poll(ctx); err != nil {
				t.Fatalf("Poll cycle 3 failed: %v", err)
			}
			select {
			case ev := <-teardownChan:
				if ev.JobID != "210" {
					t.Errorf("Expected JobID 210, got %s", ev.JobID)
				}
			default:
				t.Fatal("Expected teardown event when Slurm answers again without job 210, got none")
			}
		})
	}
}

// TestPoll_OrphanedJobSet_SlurmOutage_GracePeriodStartsAfterRecovery checks that a Slurm outage
// does not start the orphan grace period, also when the outage is longer than the grace period.
// The job is in Slurm before the outage, so the grace period starts at the first successful poll
// after the outage that does not find the job.
func TestPoll_OrphanedJobSet_SlurmOutage_GracePeriodStartsAfterRecovery(t *testing.T) {
	ctx := t.Context()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

	now := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
	wm.UID = testWMUID

	fakeK8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(wm, newOwnedJobSet("211")).Build()
	cMap := clientmap.NewClientMap()
	mockSlurm := &utils.SlurmClient{}
	mockSlurm.SetPendingJobs(testPartitionName, []slurmclienttypes.V0044JobInfo{newRunningJob(211, testPartitionName)})
	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testControllerName}
	cMap.Add(ctx, controllerKey, mockSlurm)

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

	// Poll cycle 1: job 211 runs in Slurm.
	if err := poller.Poll(ctx); err != nil {
		t.Fatalf("Poll cycle 1 failed: %v", err)
	}
	expectNoTeardownEvent(t, teardownChan, "Poll cycle 1")

	// Poll cycles 2 to 4: Slurm is unavailable for 3 minutes, which is 3 times the grace period.
	cMap.Add(ctx, controllerKey, &errorClient{})
	for _, elapsed := range []time.Duration{10 * time.Second, 70 * time.Second, 3 * time.Minute} {
		currentTime = now.Add(elapsed)
		if err := poller.Poll(ctx); err != nil {
			t.Fatalf("Poll at %s failed: %v", elapsed, err)
		}
		expectNoTeardownEvent(t, teardownChan, fmt.Sprintf("Poll at %s with Slurm unavailable", elapsed))
	}

	// Poll cycle 5: Slurm is available again, but job 211 is gone, for example because slurmctld
	// lost its state. This poll is the first one that does not find the job, so the grace period
	// starts now.
	mockSlurm.SetPendingJobs(testPartitionName, nil)
	cMap.Add(ctx, controllerKey, mockSlurm)
	recovery := now.Add(3*time.Minute + 10*time.Second)
	currentTime = recovery
	if err := poller.Poll(ctx); err != nil {
		t.Fatalf("Poll cycle 5 failed: %v", err)
	}
	expectNoTeardownEvent(t, teardownChan, "Poll cycle 5")

	// Poll cycle 6: the grace period that started at recovery is over.
	currentTime = recovery.Add(70 * time.Second)
	if err := poller.Poll(ctx); err != nil {
		t.Fatalf("Poll cycle 6 failed: %v", err)
	}
	select {
	case ev := <-teardownChan:
		if ev.JobID != "211" {
			t.Errorf("Expected JobID 211, got %s", ev.JobID)
		}
		if ev.Reason != metrics.ReasonJobOrphaned {
			t.Errorf("Expected reason %s, got %s", metrics.ReasonJobOrphaned, ev.Reason)
		}
	default:
		t.Fatal("Expected teardown event after the grace period that started at recovery, got none")
	}
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

	tests := []struct {
		name       string
		job        *slurmclienttypes.V0044JobInfo
		existingJS *jobsetv1alpha2.JobSet
		want       bool
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldEmitAdmissionEvent(tc.job, tc.existingJS); got != tc.want {
				t.Errorf("shouldEmitAdmissionEvent() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPoll_FilterRedundantAdmissionEvents(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)

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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			wm := newTestWorkloadMixing(testWMName, testControllerName, testPartitionName, testQueueName)
			wm.UID = testWMUID

			objs := []client.Object{wm}
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
