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

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	// Metric namespace used across all k8s-bridge metrics.
	metricNamespace = "k8s_bridge"

	// Label names.
	labelController     = "controller"
	labelWorkloadMixing = "workload_mixing"
	labelPartition      = "partition"
	labelStatus         = "status"
	labelReason         = "reason"

	// Status constants for job synchronization outcomes.
	StatusSuccess = "success"
	StatusError   = "error"

	// Reason constants for job synchronization.
	ReasonJobSetCreated    = "jobset_created"
	ReasonTranslationError = "translation_error"
	// ReasonJobSetAdmissionRejected is recorded when the API server or an admission webhook
	// (for example Kueue's JobSet validating webhook) permanently rejects JobSet creation.
	ReasonJobSetAdmissionRejected = "jobset_admission_rejected"
	ReasonJobSetCompleted         = "jobset_completed"
	ReasonJobSetFailed            = "jobset_failed"
	ReasonSlurmUpdateError        = "slurm_update_error"
	ReasonJobCompleted            = "job_completed"
	ReasonJobFailed               = "job_failed"
	ReasonJobCancelled            = "job_cancelled"
	ReasonJobOrphaned             = "job_orphaned"
)

var (
	// SlurmPollerDuration tracks latency of Slurm REST API polling cycles.
	SlurmPollerDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricNamespace,
			Name:      "slurm_poller_duration_seconds",
			Help:      "Duration of Slurm poller cycles in seconds.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		},
		[]string{labelController},
	)

	// SlurmPollerErrors tracks cumulative errors encountered during Slurm poller cycles.
	SlurmPollerErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "slurm_poller_errors_total",
			Help:      "Total number of errors encountered during Slurm poller cycles.",
		},
		[]string{labelController},
	)

	// SlurmClientStatus reflects the connectivity and health status of the Slurm REST client.
	SlurmClientStatus = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "slurm_client_status",
			Help:      "Health and connectivity status of the Slurm REST client (1 for connected/healthy, 0 for disconnected/error).",
		},
		[]string{labelController},
	)

	// JobsDiscovered tracks cumulative incoming pending Slurm jobs discovered.
	JobsDiscovered = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "jobs_discovered_total",
			Help:      "Total number of pending Slurm jobs discovered.",
		},
		[]string{labelWorkloadMixing, labelPartition},
	)

	// JobsSync tracks cumulative job synchronization attempts and outcomes.
	JobsSync = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "jobs_sync_total",
			Help:      "Total number of job synchronization attempts and outcomes.",
		},
		[]string{labelStatus, labelReason, labelWorkloadMixing, labelPartition},
	)

	// JobTranslationDuration tracks latency of converting Slurm jobs to Kubernetes JobSets.
	JobTranslationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricNamespace,
			Name:      "job_translation_duration_seconds",
			Help:      "Duration of Slurm job to JobSet translation in seconds.",
			Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		},
		[]string{labelWorkloadMixing, labelPartition},
	)

	// ActiveJobs tracks the current number of in-flight managed Slurm jobs.
	ActiveJobs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "active_jobs",
			Help:      "Current number of in-flight managed Slurm jobs.",
		},
		[]string{labelWorkloadMixing, labelPartition},
	)

	// ActiveWorkloadMixings tracks the total count of active WorkloadMixing custom resources.
	ActiveWorkloadMixings = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "active_workload_mixings",
			Help:      "Total number of active WorkloadMixing custom resources.",
		},
	)

	// TrackedPartitions tracks the count of Slurm partitions configured and tracked per WorkloadMixing CR.
	TrackedPartitions = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "tracked_partitions",
			Help:      "Number of Slurm partitions actively configured and tracked per WorkloadMixing CR.",
		},
		[]string{labelWorkloadMixing},
	)

	// DroppedEvents tracks cumulative events dropped due to channel buffer saturation.
	DroppedEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricNamespace,
			Name:      "dropped_events_total",
			Help:      "Total number of events dropped due to channel buffer saturation.",
		},
		[]string{labelWorkloadMixing},
	)

	// EventsQueueDepth tracks the current depth of the poller event channel queue.
	EventsQueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: metricNamespace,
			Name:      "events_queue_depth",
			Help:      "Current depth of the poller event channel queue.",
		},
	)
)

func init() {
	crmetrics.Registry.MustRegister(
		SlurmPollerDuration,
		SlurmPollerErrors,
		SlurmClientStatus,
		JobsDiscovered,
		JobsSync,
		JobTranslationDuration,
		ActiveJobs,
		ActiveWorkloadMixings,
		TrackedPartitions,
		DroppedEvents,
		EventsQueueDepth,
	)
}

// RecordPollerDuration observes poller cycle duration in seconds.
func RecordPollerDuration(controller string, duration time.Duration) {
	SlurmPollerDuration.WithLabelValues(controller).Observe(duration.Seconds())
}

// RecordPollerError increments the poller error counter.
func RecordPollerError(controller string) {
	SlurmPollerErrors.WithLabelValues(controller).Inc()
}

// SetSlurmClientStatus sets the Slurm client connectivity gauge (1 for healthy, 0 for unhealthy).
func SetSlurmClientStatus(controller string, healthy bool) {
	val := 0.0
	if healthy {
		val = 1.0
	}
	SlurmClientStatus.WithLabelValues(controller).Set(val)
}

// RecordJobDiscovered increments the discovered jobs counter.
func RecordJobDiscovered(workloadMixing, partition string) {
	JobsDiscovered.WithLabelValues(workloadMixing, partition).Inc()
}

// RecordJobSync increments the job sync counter with status and reason.
func RecordJobSync(status, reason, workloadMixing, partition string) {
	JobsSync.WithLabelValues(status, reason, workloadMixing, partition).Inc()
}

// RecordJobTranslationDuration observes job translation latency.
func RecordJobTranslationDuration(workloadMixing, partition string, duration time.Duration) {
	JobTranslationDuration.WithLabelValues(workloadMixing, partition).Observe(duration.Seconds())
}

// SetActiveJobs sets the active jobs gauge for a workload mixing and partition.
func SetActiveJobs(workloadMixing, partition string, count int) {
	ActiveJobs.WithLabelValues(workloadMixing, partition).Set(float64(count))
}

// SetActiveWorkloadMixings sets the active WorkloadMixing resource count.
func SetActiveWorkloadMixings(count int) {
	ActiveWorkloadMixings.Set(float64(count))
}

// SetTrackedPartitions sets the number of tracked partitions for a WorkloadMixing CR.
func SetTrackedPartitions(workloadMixing string, count int) {
	TrackedPartitions.WithLabelValues(workloadMixing).Set(float64(count))
}

// DeleteWorkloadMixingMetrics cleans up partition gauges and resets WorkloadMixing-scoped metrics upon CR deletion.
func DeleteWorkloadMixingMetrics(workloadMixing string) {
	labels := prometheus.Labels{labelWorkloadMixing: workloadMixing}
	TrackedPartitions.DeletePartialMatch(labels)
	ActiveJobs.DeletePartialMatch(labels)
	DroppedEvents.DeletePartialMatch(labels)
	JobsDiscovered.DeletePartialMatch(labels)
	JobsSync.DeletePartialMatch(labels)
	JobTranslationDuration.DeletePartialMatch(labels)
}

// RecordDroppedEvent increments the channel saturation dropped events counter.
func RecordDroppedEvent(workloadMixing string) {
	DroppedEvents.WithLabelValues(workloadMixing).Inc()
}

// SetEventsQueueDepth updates the current event channel queue depth gauge.
func SetEventsQueueDepth(depth int) {
	EventsQueueDepth.Set(float64(depth))
}

// DeleteSlurmClientMetrics cleans up metric series for a Slurm controller upon deletion.
func DeleteSlurmClientMetrics(controller string) {
	labels := prometheus.Labels{labelController: controller}
	SlurmClientStatus.DeletePartialMatch(labels)
	SlurmPollerErrors.DeletePartialMatch(labels)
	SlurmPollerDuration.DeletePartialMatch(labels)
}
