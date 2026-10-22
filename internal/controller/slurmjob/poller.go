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
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/clientmap"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/utils/slurmtypes"
	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

const (
	// defaultOrphanGracePeriod defines the default duration an absent Slurm workload's
	// corresponding JobSet must remain missing before being flagged as orphaned and torn down.
	defaultOrphanGracePeriod = 1 * time.Minute
)

// SlurmPoller periodically queries the Slurm REST API for pending jobs
// and audits active workloads for termination and orphan cleanup.
// It implements the manager.Runnable interface.
type SlurmPoller struct {
	client.Client
	ClientMap         *clientmap.ClientMap
	AdmissionEvents   chan<- slurmtypes.SlurmJobAdmissionEvent
	TeardownEvents    chan<- slurmtypes.SlurmJobTeardownEvent
	PollInterval      time.Duration
	OrphanGracePeriod time.Duration
	// firstSeenMissing tracks the timestamp when a Kubernetes JobSet was first observed
	// to be absent from the active Slurm job list. Protected by mu.
	firstSeenMissing map[types.NamespacedName]time.Time
	mu               sync.Mutex
	// nowFn is a clock abstraction hook used exclusively in unit tests to simulate time progression.
	nowFn func() time.Time
}

// NeedLeaderElection implements the manager.LeaderElectionRunnable interface
// to explicitly require leadership before running the poller.
func (p *SlurmPoller) NeedLeaderElection() bool {
	return true
}

// Start runs the polling loop.
func (p *SlurmPoller) Start(ctx context.Context) error {

	logger := log.FromContext(ctx).WithName("slurm-poller")
	logger.Info("Starting Slurm Poller", "interval", p.PollInterval)

	ticker := time.NewTicker(p.PollInterval)
	defer ticker.Stop()

	for {
		if err := p.Poll(ctx); err != nil {
			if ctx.Err() != nil {
				logger.Info("Stopping Slurm Poller")
				return nil
			}
			logger.Error(err, "Failed to poll Slurm jobs")
		}
		select {
		case <-ctx.Done():
			logger.Info("Stopping Slurm Poller")
			return nil
		case <-ticker.C:
		}
	}
}

// Poll queries Slurm REST API for all active WorkloadMixing configurations.
func (p *SlurmPoller) Poll(ctx context.Context) error {
	metrics.SetEventsQueueDepth(len(p.AdmissionEvents))
	defer func() {
		metrics.SetEventsQueueDepth(len(p.AdmissionEvents))
	}()

	logger := log.FromContext(ctx).WithName("slurm-poller")

	wmList := &k8sbridgev1alpha1.WorkloadMixingList{}
	// p.Client uses the manager's in-memory cache (backed by informers) for WorkloadMixing resources
	// watched by the manager, which avoids direct kube-apiserver queries on every poll tick.
	if err := p.List(ctx, wmList); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to list WorkloadMixing resources: %w", err)
	}

	// Group WorkloadMixing by ControllerRef
	wmByController := make(map[types.NamespacedName][]*k8sbridgev1alpha1.WorkloadMixing)
	for i := range wmList.Items {
		wm := &wmList.Items[i]
		if len(wm.Spec.PartitionsMapping) == 0 {
			logger.V(1).Info("PartitionsMapping is empty, skipping WorkloadMixing", "workloadMixing", client.ObjectKeyFromObject(wm))
			continue
		}

		controllerKey := types.NamespacedName{
			Namespace: wm.Namespace,
			Name:      wm.Spec.ControllerRef.Name,
		}
		wmByController[controllerKey] = append(wmByController[controllerKey], wm)
	}

	// Query Slurm REST API once per unique controller
	for controllerKey, wms := range wmByController {
		cli := p.ClientMap.Get(controllerKey)
		if cli == nil {
			logger.V(1).Info("Slurm client not ready yet, skipping controller", "controller", controllerKey)
			metrics.SetSlurmClientStatus(controllerKey.Name, false)
			continue
		}

		start := time.Now()
		timeoutCtx, cancel := context.WithTimeout(ctx, defaultSlurmCallTimeout)
		var jobList slurmclienttypes.V0044JobInfoList
		err := cli.List(timeoutCtx, &jobList)
		duration := time.Since(start)
		cancel()

		metrics.RecordPollerDuration(controllerKey.Name, duration)

		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			metrics.RecordPollerError(controllerKey.Name)
			metrics.SetSlurmClientStatus(controllerKey.Name, false)
			logger.Error(err, "Failed to list jobs for controller", "controller", controllerKey)
			continue
		}

		metrics.SetSlurmClientStatus(controllerKey.Name, true)

		// Index Slurm batch jobs by Job ID once per controller poll cycle
		jobsByID := make(map[string]*slurmclienttypes.V0044JobInfo, len(jobList.Items))
		for i := range jobList.Items {
			job := &jobList.Items[i]
			if job.JobId != nil {
				jobsByID[strconv.Itoa(int(*job.JobId))] = job
			}
		}

		jsList := &jobsetv1alpha2.JobSetList{}
		if err := p.List(ctx, jsList, client.InNamespace(controllerKey.Namespace)); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.Error(err, "Failed to list JobSets for controller namespace", "controller", controllerKey)
			continue
		}

		for _, wm := range wms {
			if err := p.processPendingJobsForWM(ctx, &jobList, wm, jsList.Items); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				logger.Error(err, "Failed to process pending jobs for WorkloadMixing", "controller", controllerKey, "workloadMixing", client.ObjectKeyFromObject(wm))
				continue
			}

			if err := p.reconcileWorkloadsForWM(ctx, jobsByID, wm, jsList.Items); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				logger.Error(err, "Failed to reconcile workloads for WorkloadMixing", "controller", controllerKey, "workloadMixing", client.ObjectKeyFromObject(wm))
				continue
			}
		}
	}

	return nil
}

// reconcileWorkloadsForWM reconciles active workloads against Slurm state for terminal and orphaned jobs.
func (p *SlurmPoller) reconcileWorkloadsForWM(
	ctx context.Context,
	jobsByID map[string]*slurmclienttypes.V0044JobInfo,
	wm *k8sbridgev1alpha1.WorkloadMixing,
	jobSets []jobsetv1alpha2.JobSet,
) error {
	logger := log.FromContext(ctx).WithName("slurm-poller")

	for i := range jobSets {
		event := getTeardownEvent(&jobSets[i], wm, jobsByID)
		if event == nil {
			continue
		}

		select {
		case p.TeardownEvents <- *event:
			logger.V(1).Info("Sent SlurmJobTeardownEvent", "jobID", event.JobID, "workloadMixing", event.WorkloadMixing, "reason", event.Reason)
		case <-ctx.Done():
			return ctx.Err()
		default:
			metrics.RecordDroppedEvent(wm.Name)
			logger.Info("Teardown event channel is full, dropping event", "jobID", event.JobID, "workloadMixing", event.WorkloadMixing)
		}
	}

	return p.auditOrphanedJobSets(ctx, wm, jobSets, jobsByID)
}

// getOrphanGracePeriod returns the configured grace period or the default.
func (p *SlurmPoller) getOrphanGracePeriod() time.Duration {
	if p.OrphanGracePeriod > 0 {
		return p.OrphanGracePeriod
	}
	return defaultOrphanGracePeriod
}

// now returns the current time or the simulated time if nowFn is provided.
func (p *SlurmPoller) now() time.Time {
	if p.nowFn != nil {
		return p.nowFn()
	}
	return time.Now()
}

type teardownCandidate struct {
	key             types.NamespacedName
	event           slurmtypes.SlurmJobTeardownEvent
	missingDuration time.Duration
}

// auditOrphanedJobSets audits cached JobSets against the Slurm batch job list,
// tracking disappeared jobs and emitting teardown events once the orphan grace period expires.
func (p *SlurmPoller) auditOrphanedJobSets(
	ctx context.Context,
	wm *k8sbridgev1alpha1.WorkloadMixing,
	jobSets []jobsetv1alpha2.JobSet,
	slurmJobs map[string]*slurmclienttypes.V0044JobInfo,
) error {
	logger := log.FromContext(ctx).WithName("slurm-poller")
	gracePeriod := p.getOrphanGracePeriod()
	now := p.now()

	existingJobSets := make(map[string]struct{}, len(jobSets))
	for i := range jobSets {
		existingJobSets[jobSets[i].Name] = struct{}{}
	}

	var toDispatch []teardownCandidate

	p.mu.Lock()
	if p.firstSeenMissing == nil {
		p.firstSeenMissing = make(map[types.NamespacedName]time.Time)
	}

	// Prune tracked JobSets that no longer exist in Kubernetes
	for key := range p.firstSeenMissing {
		if key.Namespace == wm.Namespace {
			if _, exists := existingJobSets[key.Name]; !exists {
				delete(p.firstSeenMissing, key)
			}
		}
	}
	p.mu.Unlock()

	for i := range jobSets {
		js := &jobSets[i]
		key := types.NamespacedName{Namespace: js.Namespace, Name: js.Name}

		if !isJobSetOwnedByWM(js, wm) {
			continue
		}

		if isJobSetFinalized(js) {
			p.mu.Lock()
			delete(p.firstSeenMissing, key)
			p.mu.Unlock()
			continue
		}

		jobID := js.Labels[LabelSlurmJobID]
		if jobID == "" {
			continue
		}

		// If job is present in Slurm, clear any missing tracking
		if _, exists := slurmJobs[jobID]; exists {
			p.mu.Lock()
			delete(p.firstSeenMissing, key)
			p.mu.Unlock()
			continue
		}

		// Track disappearance immediately when observed missing
		var (
			firstSeen time.Time
			isNew     bool
		)
		p.mu.Lock()
		if ts, tracked := p.firstSeenMissing[key]; tracked {
			firstSeen = ts
		} else {
			p.firstSeenMissing[key] = now
			isNew = true
		}
		p.mu.Unlock()

		if isNew {
			logger.V(1).Info("Recorded missing Slurm job for orphan grace period",
				"jobSet", key, "jobID", jobID, "gracePeriod", gracePeriod)
			continue
		}

		// JobSet must exceed disappearance grace period (protects transient API misses)
		missingDuration := now.Sub(firstSeen)
		if missingDuration < gracePeriod {
			logger.V(1).Info("Slurm job missing from Slurm within disappearance grace period, skipped orphan teardown",
				"jobSet", key, "jobID", jobID, "missingDuration", missingDuration, "gracePeriod", gracePeriod)
			continue
		}

		partition := js.Labels[LabelSlurmPartition]
		if partition == "" {
			if len(wm.Spec.PartitionsMapping) == 1 {
				partition = wm.Spec.PartitionsMapping[0].Partition
			} else {
				logger.Info("Orphaned JobSet missing partition label", "jobSet", key, "jobID", jobID)
			}
		}

		toDispatch = append(toDispatch, teardownCandidate{
			key: key,
			event: slurmtypes.SlurmJobTeardownEvent{
				JobID: jobID,
				WorkloadMixing: types.NamespacedName{
					Namespace: wm.Namespace,
					Name:      wm.Name,
				},
				Partition: partition,
				Reason:    metrics.ReasonJobOrphaned,
			},
			missingDuration: missingDuration,
		})
	}

	for _, cand := range toDispatch {
		select {
		case p.TeardownEvents <- cand.event:
			logger.Info("Sent SlurmJobTeardownEvent for orphaned JobSet",
				"jobID", cand.event.JobID, "workloadMixing", cand.event.WorkloadMixing, "reason", cand.event.Reason, "missingDuration", cand.missingDuration)
		case <-ctx.Done():
			return ctx.Err()
		default:
			metrics.RecordDroppedEvent(wm.Name)
			logger.Info("Teardown event channel is full, dropped orphan event",
				"jobID", cand.event.JobID, "workloadMixing", cand.event.WorkloadMixing)
		}
	}

	return nil
}

// getTeardownEvent evaluates a JobSet against known Slurm jobs and returns a teardown event if terminal.
// Returns nil if the JobSet should not be torn down.
func getTeardownEvent(
	js *jobsetv1alpha2.JobSet,
	wm *k8sbridgev1alpha1.WorkloadMixing,
	jobsByID map[string]*slurmclienttypes.V0044JobInfo,
) *slurmtypes.SlurmJobTeardownEvent {
	if isJobSetFinalized(js) {
		return nil
	}

	// Verify ownership by this WorkloadMixing
	if !isJobSetOwnedByWM(js, wm) {
		return nil
	}

	jobID := js.Labels[LabelSlurmJobID]
	if jobID == "" {
		return nil
	}

	jobInfo, exists := jobsByID[jobID]
	if !exists {
		// Orphaned JobSet handling will be done in a later commit
		return nil
	}

	isTerminal, reason := isTerminalSlurmJob(jobInfo)
	if !isTerminal {
		// Workload in non-terminal state - preserve unmodified
		return nil
	}

	partition := js.Labels[LabelSlurmPartition]
	if partition == "" && jobInfo.Partition != nil {
		partition = *jobInfo.Partition
	}

	return &slurmtypes.SlurmJobTeardownEvent{
		JobID: jobID,
		WorkloadMixing: types.NamespacedName{
			Namespace: wm.Namespace,
			Name:      wm.Name,
		},
		Partition: partition,
		Reason:    reason,
	}
}

func shouldEmitAdmissionEvent(job *slurmclienttypes.V0044JobInfo, existingJS *jobsetv1alpha2.JobSet) bool {
	status := activeJobStatus(job, existingJS)
	if status == StatusNewJob {
		if strings.Contains(ptr.Deref(job.Partition, ""), ",") {
			return true
		}
		return !isJobExplicitlyHeld(job)
	}
	return status != StatusNoAction
}

func (p *SlurmPoller) processPendingJobsForWM(
	ctx context.Context,
	jobList *slurmclienttypes.V0044JobInfoList,
	wm *k8sbridgev1alpha1.WorkloadMixing,
	jobSets []jobsetv1alpha2.JobSet,
) error {
	logger := log.FromContext(ctx).WithName("slurm-poller")

	observedPartitions := make(map[string]bool, len(wm.Spec.PartitionsMapping))
	for _, pm := range wm.Spec.PartitionsMapping {
		observedPartitions[pm.Partition] = true
	}

	jobSetsByName := make(map[string]*jobsetv1alpha2.JobSet, len(jobSets))
	for i := range jobSets {
		if isJobSetOwnedByWM(&jobSets[i], wm) {
			jobSetsByName[jobSets[i].Name] = &jobSets[i]
		}
	}

	for i := range jobList.Items {
		job := &jobList.Items[i]
		if job.GetStateAsSet().Has(slurmapi.V0044JobInfoJobStatePENDING) {
			jobPartition := ptr.Deref(job.Partition, "")
			partitions := strings.Split(jobPartition, ",")
			matched := false
			for _, part := range partitions {
				part = strings.TrimSpace(part)
				if observedPartitions[part] {
					matched = true
					break
				}
			}

			if matched {
				jobID := ptr.Deref(job.JobId, 0)
				jobSetName := jobSetNameForJobID(strconv.Itoa(int(jobID)))
				existingJS := jobSetsByName[jobSetName]
				if !shouldEmitAdmissionEvent(job, existingJS) {
					logger.V(1).Info("Skipping redundant SlurmJobAdmissionEvent", "jobID", jobID, "workloadMixing", client.ObjectKeyFromObject(wm))
					continue
				}

				rawJSON, err := json.Marshal(*job)
				if err != nil {
					logger.Error(err, "Failed to marshal job info", "jobID", jobID)
					continue
				}
				event := slurmtypes.SlurmJobAdmissionEvent{
					JobID:          fmt.Sprintf("%d", jobID),
					RawJobSpecJSON: string(rawJSON),
					WorkloadMixing: types.NamespacedName{
						Namespace: wm.Namespace,
						Name:      wm.Name,
					},
				}

				select {
				case p.AdmissionEvents <- event:
					metrics.SetEventsQueueDepth(len(p.AdmissionEvents))
					logger.V(1).Info("Sent SlurmJobAdmissionEvent", "jobID", event.JobID, "workloadMixing", event.WorkloadMixing)
				case <-ctx.Done():
					return ctx.Err()
				default:
					metrics.RecordDroppedEvent(wm.Name)
					logger.Info("Event channel is full, dropping event", "jobID", event.JobID, "workloadMixing", event.WorkloadMixing)
				}
			}
		}
	}
	return nil
}
