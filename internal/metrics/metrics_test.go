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

package metrics_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	labelNamespace      = "namespace"
	labelController     = "controller"
	labelWorkloadMixing = "workload_mixing"
	labelPartition      = "partition"
	labelStatus         = "status"
	labelReason         = "reason"
)

func TestMetricsRegistration(t *testing.T) {
	// Verify that each collector is registered with crmetrics.Registry by attempting re-registration.
	// In Prometheus client_golang, re-registering an already registered collector returns prometheus.AlreadyRegisteredError.
	collectors := []struct {
		name      string
		collector prometheus.Collector
	}{
		{"SlurmPollerDuration", metrics.SlurmPollerDuration},
		{"SlurmPollerErrors", metrics.SlurmPollerErrors},
		{"SlurmClientStatus", metrics.SlurmClientStatus},
		{"JobsDiscovered", metrics.JobsDiscovered},
		{"JobsSync", metrics.JobsSync},
		{"JobTranslationDuration", metrics.JobTranslationDuration},
		{"ActiveJobs", metrics.ActiveJobs},
		{"ActiveWorkloadMixings", metrics.ActiveWorkloadMixings},
		{"WorkloadMixingAvailable", metrics.WorkloadMixingAvailable},
		{"TrackedPartitions", metrics.TrackedPartitions},
		{"DroppedEvents", metrics.DroppedEvents},
		{"EventsQueueDepth", metrics.EventsQueueDepth},
	}

	for _, tc := range collectors {
		err := crmetrics.Registry.Register(tc.collector)
		if err == nil {
			t.Errorf("expected collector %s to already be registered in crmetrics.Registry, but Register succeeded", tc.name)
		} else if _, ok := err.(prometheus.AlreadyRegisteredError); !ok {
			t.Errorf("expected AlreadyRegisteredError for %s, got: %v", tc.name, err)
		}
	}
}

func TestPollerMetrics(t *testing.T) {
	controller := "test-controller"

	// Test RecordPollerDuration
	metrics.RecordPollerDuration(controller, 150*time.Millisecond)

	// Test RecordPollerError
	initialErrors := testutil.ToFloat64(metrics.SlurmPollerErrors.WithLabelValues(controller))
	metrics.RecordPollerError(controller)
	newErrors := testutil.ToFloat64(metrics.SlurmPollerErrors.WithLabelValues(controller))
	if newErrors != initialErrors+1 {
		t.Errorf("expected error count %f, got %f", initialErrors+1, newErrors)
	}

	// Test SetSlurmClientStatus
	metrics.SetSlurmClientStatus(controller, true)
	if val := testutil.ToFloat64(metrics.SlurmClientStatus.WithLabelValues(controller)); val != 1.0 {
		t.Errorf("expected client status 1.0, got %f", val)
	}

	metrics.SetSlurmClientStatus(controller, false)
	if val := testutil.ToFloat64(metrics.SlurmClientStatus.WithLabelValues(controller)); val != 0.0 {
		t.Errorf("expected client status 0.0, got %f", val)
	}
}

func TestJobMetrics(t *testing.T) {
	wm := "wm-job-test"
	partition := "gpu-part"

	// Test RecordJobDiscovered
	initialDiscovered := testutil.ToFloat64(metrics.JobsDiscovered.WithLabelValues(wm, partition))
	metrics.RecordJobDiscovered(wm, partition)
	if val := testutil.ToFloat64(metrics.JobsDiscovered.WithLabelValues(wm, partition)); val != initialDiscovered+1 {
		t.Errorf("expected discovered %f, got %f", initialDiscovered+1, val)
	}

	// Test RecordJobSync
	initialSyncSuccess := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusSuccess, metrics.ReasonJobSetCreated, wm, partition))
	metrics.RecordJobSync(metrics.StatusSuccess, metrics.ReasonJobSetCreated, wm, partition)
	if val := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusSuccess, metrics.ReasonJobSetCreated, wm, partition)); val != initialSyncSuccess+1 {
		t.Errorf("expected sync success %f, got %f", initialSyncSuccess+1, val)
	}

	initialSyncError := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusError, metrics.ReasonTranslationError, wm, partition))
	metrics.RecordJobSync(metrics.StatusError, metrics.ReasonTranslationError, wm, partition)
	if val := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusError, metrics.ReasonTranslationError, wm, partition)); val != initialSyncError+1 {
		t.Errorf("expected sync error %f, got %f", initialSyncError+1, val)
	}

	// Test RecordJobTranslationDuration
	metrics.RecordJobTranslationDuration(wm, partition, 25*time.Millisecond)

	// Test SetActiveJobs
	metrics.SetActiveJobs(wm, partition, 5)
	if val := testutil.ToFloat64(metrics.ActiveJobs.WithLabelValues(wm, partition)); val != 5.0 {
		t.Errorf("expected active jobs 5.0, got %f", val)
	}

	metrics.SetActiveJobs(wm, partition, 0)
	if val := testutil.ToFloat64(metrics.ActiveJobs.WithLabelValues(wm, partition)); val != 0.0 {
		t.Errorf("expected active jobs 0.0, got %f", val)
	}
}

func TestTerminationReasonMetrics(t *testing.T) {
	wm := "wm-term-test"
	partition := "batch"

	reasons := []string{
		metrics.ReasonJobCompleted,
		metrics.ReasonJobFailed,
		metrics.ReasonJobCancelled,
		metrics.ReasonJobOrphaned,
	}

	for _, reason := range reasons {
		initial := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusSuccess, reason, wm, partition))
		metrics.RecordJobSync(metrics.StatusSuccess, reason, wm, partition)
		if val := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusSuccess, reason, wm, partition)); val != initial+1 {
			t.Errorf("expected sync count %f for reason %s, got %f", initial+1, reason, val)
		}
	}
}

func TestResourceAndEventMetrics(t *testing.T) {
	ns := "slurm"
	wm := "wm-res-test"

	// Test SetActiveWorkloadMixings
	metrics.SetActiveWorkloadMixings(4)
	if val := testutil.ToFloat64(metrics.ActiveWorkloadMixings); val != 4.0 {
		t.Errorf("expected active workload mixings 4.0, got %f", val)
	}

	// Test SetWorkloadMixingAvailable
	metrics.SetWorkloadMixingAvailable(ns, wm, false)
	if val := testutil.ToFloat64(metrics.WorkloadMixingAvailable.WithLabelValues(ns, wm)); val != 0.0 {
		t.Errorf("expected workload mixing available 0.0, got %f", val)
	}
	metrics.SetWorkloadMixingAvailable(ns, wm, true)
	if val := testutil.ToFloat64(metrics.WorkloadMixingAvailable.WithLabelValues(ns, wm)); val != 1.0 {
		t.Errorf("expected workload mixing available 1.0, got %f", val)
	}

	// Test SetTrackedPartitions
	metrics.SetTrackedPartitions(wm, 3)
	if val := testutil.ToFloat64(metrics.TrackedPartitions.WithLabelValues(wm)); val != 3.0 {
		t.Errorf("expected tracked partitions 3.0, got %f", val)
	}

	// Test RecordDroppedEvent
	initialDropped := testutil.ToFloat64(metrics.DroppedEvents.WithLabelValues(wm))
	metrics.RecordDroppedEvent(wm)
	if val := testutil.ToFloat64(metrics.DroppedEvents.WithLabelValues(wm)); val != initialDropped+1 {
		t.Errorf("expected dropped events %f, got %f", initialDropped+1, val)
	}

	// Test SetEventsQueueDepth
	metrics.SetEventsQueueDepth(128)
	if val := testutil.ToFloat64(metrics.EventsQueueDepth); val != 128.0 {
		t.Errorf("expected queue depth 128.0, got %f", val)
	}

	// Test DeleteWorkloadMixingMetrics across all workload_mixing labeled metric vectors
	otherNs := "other-slurm"
	metrics.SetWorkloadMixingAvailable(otherNs, wm, true)
	metrics.RecordJobDiscovered(wm, "p1")
	metrics.RecordJobSync(metrics.StatusSuccess, metrics.ReasonJobSetCreated, wm, "p1")
	metrics.RecordJobTranslationDuration(wm, "p1", 50*time.Millisecond)
	metrics.SetActiveJobs(wm, "p1", 2)

	metrics.DeleteWorkloadMixingMetrics(ns, wm)

	// After deletion, reading with label values returns new zero instances for the deleted namespace,
	// while preserving WorkloadMixingAvailable for identically named CRs in other namespaces.
	if val := testutil.ToFloat64(metrics.WorkloadMixingAvailable.WithLabelValues(ns, wm)); val != 0.0 {
		t.Errorf("expected deleted workload mixing available to reset, got %f", val)
	}
	if val := testutil.ToFloat64(metrics.WorkloadMixingAvailable.WithLabelValues(otherNs, wm)); val != 1.0 {
		t.Errorf("expected other namespace workload mixing available to remain 1.0, got %f", val)
	}
	metrics.DeleteWorkloadMixingMetrics(otherNs, wm)

	if val := testutil.ToFloat64(metrics.TrackedPartitions.WithLabelValues(wm)); val != 0.0 {
		t.Errorf("expected deleted tracked partitions to reset, got %f", val)
	}
	if val := testutil.ToFloat64(metrics.ActiveJobs.WithLabelValues(wm, "p1")); val != 0.0 {
		t.Errorf("expected deleted active jobs to reset, got %f", val)
	}
	if val := testutil.ToFloat64(metrics.DroppedEvents.WithLabelValues(wm)); val != 0.0 {
		t.Errorf("expected deleted dropped events to reset, got %f", val)
	}
	if val := testutil.ToFloat64(metrics.JobsDiscovered.WithLabelValues(wm, "p1")); val != 0.0 {
		t.Errorf("expected deleted jobs discovered to reset, got %f", val)
	}
	if val := testutil.ToFloat64(metrics.JobsSync.WithLabelValues(metrics.StatusSuccess, metrics.ReasonJobSetCreated, wm, "p1")); val != 0.0 {
		t.Errorf("expected deleted jobs sync to reset, got %f", val)
	}
}

func TestMetricsHTTPExposition(t *testing.T) {
	// Populate observations for all metrics collectors
	controller := "http-test-controller"
	ns := "http-test-ns"
	wm := "wm-http-test"
	partition := "batch-test"

	metrics.RecordPollerDuration(controller, 120*time.Millisecond)
	metrics.RecordPollerError(controller)
	metrics.SetSlurmClientStatus(controller, true)
	metrics.RecordJobDiscovered(wm, partition)
	metrics.RecordJobSync(metrics.StatusSuccess, metrics.ReasonJobSetCreated, wm, partition)
	metrics.RecordJobTranslationDuration(wm, partition, 45*time.Millisecond)
	metrics.SetActiveJobs(wm, partition, 1)
	metrics.SetActiveWorkloadMixings(3)
	metrics.SetWorkloadMixingAvailable(ns, wm, true)
	metrics.SetTrackedPartitions(wm, 2)
	metrics.RecordDroppedEvent(wm)
	metrics.SetEventsQueueDepth(16)

	// Create test HTTP server serving Prometheus metrics via promhttp handler
	handler := promhttp.HandlerFor(crmetrics.Registry, promhttp.HandlerOpts{})
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	res := rec.Result()
	defer func() {
		_ = res.Body.Close()
	}()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP status 200, got %d", res.StatusCode)
	}

	contentType := res.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/plain") {
		t.Errorf("expected text/plain Content-Type, got %q", contentType)
	}

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	bodyStr := string(bodyBytes)

	// Verify all registered k8s_bridge_* metric names appear in the exposition output
	expectedMetricNames := []string{
		"k8s_bridge_slurm_poller_duration_seconds",
		"k8s_bridge_slurm_poller_errors_total",
		"k8s_bridge_slurm_client_status",
		"k8s_bridge_jobs_discovered_total",
		"k8s_bridge_jobs_sync_total",
		"k8s_bridge_job_translation_duration_seconds",
		"k8s_bridge_active_jobs",
		"k8s_bridge_active_workload_mixings",
		"k8s_bridge_workload_mixing_available",
		"k8s_bridge_tracked_partitions",
		"k8s_bridge_dropped_events_total",
		"k8s_bridge_events_queue_depth",
	}

	for _, metricName := range expectedMetricNames {
		if !strings.Contains(bodyStr, metricName) {
			t.Errorf("expected exposition output to contain metric %q, but was missing", metricName)
		}
	}

	// Parse the exposition text to verify valid Prometheus exposition format
	parser := expfmt.NewTextParser(model.LegacyValidation)
	metricFamilies, err := parser.TextToMetricFamilies(strings.NewReader(bodyStr))
	if err != nil {
		t.Fatalf("failed to parse Prometheus exposition format: %v", err)
	}

	for _, metricName := range expectedMetricNames {
		mf, ok := metricFamilies[metricName]
		if !ok || mf == nil {
			t.Errorf("expected parsed metric family for %q, but not found", metricName)
		}
	}

	// Assert specific parsed metric values and labels on the metric families
	assertGaugeValue(t, metricFamilies["k8s_bridge_active_workload_mixings"], 3.0, nil)
	assertGaugeValue(t, metricFamilies["k8s_bridge_events_queue_depth"], 16.0, nil)
	assertGaugeValue(t, metricFamilies["k8s_bridge_slurm_client_status"], 1.0, map[string]string{labelController: controller})
	assertGaugeValue(t, metricFamilies["k8s_bridge_workload_mixing_available"], 1.0, map[string]string{
		labelNamespace:      ns,
		labelWorkloadMixing: wm,
	})
	assertGaugeValue(t, metricFamilies["k8s_bridge_tracked_partitions"], 2.0, map[string]string{labelWorkloadMixing: wm})
	assertCounterValue(t, metricFamilies["k8s_bridge_jobs_discovered_total"], 1.0, map[string]string{
		labelWorkloadMixing: wm,
		labelPartition:      partition,
	})
	assertCounterValue(t, metricFamilies["k8s_bridge_jobs_sync_total"], 1.0, map[string]string{
		labelStatus:         metrics.StatusSuccess,
		labelReason:         metrics.ReasonJobSetCreated,
		labelWorkloadMixing: wm,
		labelPartition:      partition,
	})
}

func assertGaugeValue(t *testing.T, mf *dto.MetricFamily, expected float64, labels map[string]string) {
	t.Helper()
	if mf == nil || len(mf.GetMetric()) == 0 {
		t.Errorf("missing metric data")
		return
	}
	for _, m := range mf.GetMetric() {
		if matchesLabels(m, labels) {
			if val := m.GetGauge().GetValue(); val != expected {
				t.Errorf("expected %s gauge %f, got %f", mf.GetName(), expected, val)
			}
			return
		}
	}
	t.Errorf("metric %s matching labels %v not found", mf.GetName(), labels)
}

func assertCounterValue(t *testing.T, mf *dto.MetricFamily, minExpected float64, labels map[string]string) {
	t.Helper()
	if mf == nil || len(mf.GetMetric()) == 0 {
		t.Errorf("missing metric data")
		return
	}
	for _, m := range mf.GetMetric() {
		if matchesLabels(m, labels) {
			if val := m.GetCounter().GetValue(); val < minExpected {
				t.Errorf("expected %s counter >= %f, got %f", mf.GetName(), minExpected, val)
			}
			return
		}
	}
	t.Errorf("metric %s matching labels %v not found", mf.GetName(), labels)
}

func matchesLabels(m *dto.Metric, expectedLabels map[string]string) bool {
	for k, v := range expectedLabels {
		if getMetricLabel(m, k) != v {
			return false
		}
	}
	return true
}

func getMetricLabel(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

func TestMetricsSecureAuthSimulation(t *testing.T) {
	// Simulate an authentication filter provider wrapping the metrics handler
	validToken := "Bearer secret-valid-token"
	handler := promhttp.HandlerFor(crmetrics.Registry, promhttp.HandlerOpts{})

	authMiddleware := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if authHeader != validToken {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		handler.ServeHTTP(w, r)
	})

	// 1. Request without token -> 401 Unauthorized
	reqUnauth := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recUnauth := httptest.NewRecorder()
	authMiddleware.ServeHTTP(recUnauth, reqUnauth)
	if recUnauth.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized without token, got %d", recUnauth.Code)
	}

	// 2. Request with invalid token -> 403 Forbidden
	reqInvalid := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	reqInvalid.Header.Set("Authorization", "Bearer invalid-token")
	recInvalid := httptest.NewRecorder()
	authMiddleware.ServeHTTP(recInvalid, reqInvalid)
	if recInvalid.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden with invalid token, got %d", recInvalid.Code)
	}

	// 3. Request with valid token -> 200 OK
	reqValid := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	reqValid.Header.Set("Authorization", validToken)
	recValid := httptest.NewRecorder()
	authMiddleware.ServeHTTP(recValid, reqValid)
	if recValid.Code != http.StatusOK {
		t.Errorf("expected 200 OK with valid token, got %d", recValid.Code)
	}
}

func TestCardinalitySafety(t *testing.T) {
	wm := "wm-cardinality"
	partition := "batch"

	// Simulate 10,000 job updates with fixed label sets
	for range 10000 {
		metrics.RecordJobDiscovered(wm, partition)
		metrics.RecordJobSync(metrics.StatusSuccess, metrics.ReasonJobSetCreated, wm, partition)
		metrics.RecordJobSync(metrics.StatusSuccess, metrics.ReasonJobSetCompleted, wm, partition)
	}

	// Verify that the number of series collected for JobsSync with this wm is bounded
	count := testutil.CollectAndCount(metrics.JobsSync)
	if count > 50 { // Reasonable threshold across all test runs
		t.Errorf("cardinality explosion detected: %d metric series present", count)
	}
}
