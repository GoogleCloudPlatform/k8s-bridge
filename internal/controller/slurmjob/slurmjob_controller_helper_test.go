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
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/clientmap"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

const (
	reconWmName          = "test-wm"
	reconNamespace       = "test-ns"
	reconControllerName  = "test-controller"
	reconPartitionName   = "test-partition"
	reconJobID           = 12345
	reconJobIDStr        = "12345"
	reconSlurmAuthSecret = "slurm-auth-slurm"
)

func newPartConfigUnavailable422Error() error {
	return utilerrors.NewAggregate([]error{
		errors.New(http.StatusText(http.StatusUnprocessableEntity)),
		errors.New("Requested partition configuration not available now"),
	})
}

func getReconTestController() *slinkyv1beta1.Controller {
	return &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconControllerName,
			Namespace: reconNamespace,
		},
		Spec: slinkyv1beta1.ControllerSpec{
			SlurmKeyRef: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: reconSlurmAuthSecret,
				},
				Key: SlurmKeyFile,
			},
		},
	}
}

func setupReconcileTest(t *testing.T) (*runtime.Scheme, *k8sbridgev1alpha1.WorkloadMixing, ctrl.Request) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = k8sbridgev1alpha1.AddToScheme(scheme)
	_ = jobsetv1alpha2.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = slinkyv1beta1.AddToScheme(scheme)

	wm := &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reconWmName,
			Namespace: reconNamespace,
		},
		Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: reconControllerName,
			},
			PartitionsMapping: []k8sbridgev1alpha1.PartitionMapping{
				{Partition: reconPartitionName, LocalQueue: "some-queue"},
			},
		},
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: reconNamespace,
			Name:      fmt.Sprintf("slurm-job-%s-%s", reconWmName, reconJobIDStr),
		},
	}
	return scheme, wm, req
}

type testJobSetBuilder struct {
	jobSet *jobsetv1alpha2.JobSet
}

func newTestJobSet(name string) *testJobSetBuilder {
	return &testJobSetBuilder{
		jobSet: &jobsetv1alpha2.JobSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: reconNamespace,
			},
		},
	}
}

func newReconTestJobSet() *testJobSetBuilder {
	return newTestJobSet("slurm-job-" + reconJobIDStr).
		withLabels(map[string]string{
			LabelKueueQueue:     "some-queue",
			LabelSlurmJobID:     reconJobIDStr,
			LabelSlurmPartition: reconPartitionName,
		}).
		withOwnerReference(metav1.OwnerReference{
			Kind: KindWorkloadMixing,
			Name: reconWmName,
		})
}

func (b *testJobBuilder) withDefaultReconFields() *testJobBuilder {
	return b.
		withJobID(reconJobID).
		withPartition(reconPartitionName).
		withNodes(testDefaultNodeCount)
}

func (b *testJobBuilder) withTranslatedJobMarkers() *testJobBuilder {
	jobID := ptr.Deref(b.job.JobId, reconJobID)
	return b.
		withFeatures(fmt.Sprintf("%s%d", UniqueFeaturePrefix, jobID)).
		withComment(fmt.Sprintf("Unheld by k8s-bridge: Kubernetes resource slurm-job-%d created successfully", jobID))
}

func newReconTestJob() *testJobBuilder {
	return newEmptyTestJob().withDefaultReconFields()
}

func (b *testJobSetBuilder) withLabels(labels map[string]string) *testJobSetBuilder {
	if b.jobSet.Labels == nil {
		b.jobSet.Labels = make(map[string]string)
	}
	maps.Copy(b.jobSet.Labels, labels)
	return b
}

func (b *testJobSetBuilder) withAnnotations(annotations map[string]string) *testJobSetBuilder {
	if b.jobSet.Annotations == nil {
		b.jobSet.Annotations = make(map[string]string)
	}
	maps.Copy(b.jobSet.Annotations, annotations)
	return b
}

func (b *testJobSetBuilder) withOwnerReference(owner metav1.OwnerReference) *testJobSetBuilder {
	b.jobSet.OwnerReferences = append(b.jobSet.OwnerReferences, owner)
	return b
}

func (b *testJobSetBuilder) withSuspend(suspend bool) *testJobSetBuilder {
	b.jobSet.Spec.Suspend = ptr.To(suspend)
	return b
}

func (b *testJobSetBuilder) withDeletionTimestamp() *testJobSetBuilder {
	now := metav1.Now()
	b.jobSet.DeletionTimestamp = &now
	if len(b.jobSet.Finalizers) == 0 {
		b.jobSet.Finalizers = []string{testFinalizer}
	}
	return b
}

//nolint:unparam
func (b *testJobSetBuilder) withCondition(conditionType string, status metav1.ConditionStatus) *testJobSetBuilder {
	b.jobSet.Status.Conditions = append(b.jobSet.Status.Conditions, metav1.Condition{
		Type:   conditionType,
		Status: status,
	})
	return b
}

func (b *testJobSetBuilder) build() *jobsetv1alpha2.JobSet {
	return b.jobSet
}

type reconcileTestEnv struct {
	t          *testing.T
	scheme     *runtime.Scheme
	wm         *k8sbridgev1alpha1.WorkloadMixing
	req        ctrl.Request
	fakeClient client.Client
	mockSlurm  *utils.SlurmClient
	clientMap  *clientmap.ClientMap
	reconciler *SlurmJobReconciler
	jobSetKey  types.NamespacedName
}

type reconcileEnvOpt func(c *reconcileEnvConfig)

type reconcileEnvConfig struct {
	withoutController      bool
	withoutSlurmClient     bool
	emptyControllerRefName bool
	extraObjects           []client.Object
	interceptors           *interceptor.Funcs
	pendingJobs            []slurmclienttypes.V0044JobInfo
	partition              string
	slurmUpdateError       error
	slurmMutateOnUpdate    bool
}

func withExtraObjects(objs ...client.Object) reconcileEnvOpt {
	return func(c *reconcileEnvConfig) {
		c.extraObjects = append(c.extraObjects, objs...)
	}
}

func withWithoutController() reconcileEnvOpt {
	return func(c *reconcileEnvConfig) {
		c.withoutController = true
	}
}

func withWithoutSlurmClient() reconcileEnvOpt {
	return func(c *reconcileEnvConfig) {
		c.withoutSlurmClient = true
	}
}

func withEmptyControllerRefName() reconcileEnvOpt {
	return func(c *reconcileEnvConfig) {
		c.emptyControllerRefName = true
	}
}

func withInterceptors(interceptors interceptor.Funcs) reconcileEnvOpt {
	return func(c *reconcileEnvConfig) {
		c.interceptors = &interceptors
	}
}

func withPendingSlurmJobs(jobs ...*slurmclienttypes.V0044JobInfo) reconcileEnvOpt {
	return func(c *reconcileEnvConfig) {
		for _, j := range jobs {
			if j != nil {
				c.pendingJobs = append(c.pendingJobs, *j)
			}
		}
	}
}

func withSlurmUpdateError(err error, mutateOnUpdate bool) reconcileEnvOpt {
	return func(c *reconcileEnvConfig) {
		c.slurmUpdateError = err
		c.slurmMutateOnUpdate = mutateOnUpdate
	}
}

func newReconcileTestEnv(t *testing.T, opts ...reconcileEnvOpt) *reconcileTestEnv {
	t.Helper()
	var cfg reconcileEnvConfig
	cfg.partition = reconPartitionName
	for _, opt := range opts {
		opt(&cfg)
	}

	scheme, wm, req := setupReconcileTest(t)
	if cfg.emptyControllerRefName {
		wm.Spec.ControllerRef.Name = ""
	}

	var initObjs []client.Object
	initObjs = append(initObjs, wm)
	if !cfg.withoutController {
		initObjs = append(initObjs, getReconTestController())
	}
	initObjs = append(initObjs, cfg.extraObjects...)

	clientBuilder := fake.NewClientBuilder().WithScheme(scheme).WithObjects(initObjs...)
	if cfg.interceptors != nil {
		clientBuilder = clientBuilder.WithInterceptorFuncs(*cfg.interceptors)
	}
	fakeClient := clientBuilder.Build()

	mockSlurm := &utils.SlurmClient{
		UpdateError:    cfg.slurmUpdateError,
		MutateOnUpdate: cfg.slurmMutateOnUpdate,
	}
	if len(cfg.pendingJobs) > 0 {
		mockSlurm.SetPendingJobs(cfg.partition, cfg.pendingJobs)
	}

	cMap := clientmap.NewClientMap()
	if !cfg.withoutSlurmClient {
		controllerName := reconControllerName
		if cfg.emptyControllerRefName {
			controllerName = ""
		}
		cMap.Add(context.Background(), types.NamespacedName{Namespace: reconNamespace, Name: controllerName}, mockSlurm)
	}

	reconciler := &SlurmJobReconciler{
		Client:    fakeClient,
		Scheme:    scheme,
		ClientMap: cMap,
	}

	return &reconcileTestEnv{
		t:          t,
		scheme:     scheme,
		wm:         wm,
		req:        req,
		fakeClient: fakeClient,
		mockSlurm:  mockSlurm,
		clientMap:  cMap,
		reconciler: reconciler,
		jobSetKey:  types.NamespacedName{Namespace: reconNamespace, Name: "slurm-job-" + reconJobIDStr},
	}
}

func (e *reconcileTestEnv) reconcile(ctx ...context.Context) (ctrl.Result, error) {
	e.t.Helper()
	c := context.Background()
	if len(ctx) > 0 && ctx[0] != nil {
		c = ctx[0]
	}
	return e.reconciler.Reconcile(c, e.req)
}

//nolint:unparam
func (e *reconcileTestEnv) verifyJobSetCreated(wantQueue string, wantJobID string, wantOwnerName string) *jobsetv1alpha2.JobSet {
	e.t.Helper()
	var js jobsetv1alpha2.JobSet
	if err := e.fakeClient.Get(context.Background(), e.jobSetKey, &js); err != nil {
		e.t.Fatalf("Expected JobSet to be created, but got error: %v", err)
		return nil
	}
	if wantQueue != "" && js.Labels[LabelKueueQueue] != wantQueue {
		e.t.Errorf("Expected JobSet queue label %q, got %q", wantQueue, js.Labels[LabelKueueQueue])
	}
	if wantJobID != "" && js.Labels[LabelSlurmJobID] != wantJobID {
		e.t.Errorf("Expected JobSet job ID label %q, got %q", wantJobID, js.Labels[LabelSlurmJobID])
	}
	if wantOwnerName != "" {
		if len(js.OwnerReferences) != 1 {
			e.t.Errorf("Expected 1 owner reference, got %d", len(js.OwnerReferences))
		} else {
			owner := js.OwnerReferences[0]
			if owner.Name != wantOwnerName || owner.Kind != KindWorkloadMixing {
				e.t.Errorf("Expected owner reference to be WorkloadMixing %q, got %s %q", wantOwnerName, owner.Kind, owner.Name)
			}
		}
	}
	return &js
}

func (e *reconcileTestEnv) verifyJobSetNotFound() {
	e.t.Helper()
	var js jobsetv1alpha2.JobSet
	err := e.fakeClient.Get(context.Background(), e.jobSetKey, &js)
	if !apierrors.IsNotFound(err) {
		e.t.Errorf("Expected JobSet to NOT exist (NotFound), got err: %v", err)
	}
}

func (e *reconcileTestEnv) verifyJobSetSuspended(wantSuspended bool, wantSuspendedBySlurmHoldAnnotation bool) {
	e.t.Helper()
	var js jobsetv1alpha2.JobSet
	if err := e.fakeClient.Get(context.Background(), e.jobSetKey, &js); err != nil {
		e.t.Fatalf("Failed to fetch JobSet: %v", err)
	}
	isSuspended := js.Spec.Suspend != nil && *js.Spec.Suspend
	if isSuspended != wantSuspended {
		e.t.Errorf("Expected JobSet.Spec.Suspend = %v, got %v", wantSuspended, isSuspended)
	}
	hasAnnotation := js.Annotations != nil && js.Annotations[AnnotationSuspendedBySlurmHold] == ValueTrue
	if hasAnnotation != wantSuspendedBySlurmHoldAnnotation {
		e.t.Errorf("Expected annotation %s = %v, got %v", AnnotationSuspendedBySlurmHold, wantSuspendedBySlurmHoldAnnotation, hasAnnotation)
	}
}

func (e *reconcileTestEnv) verifySlurmUnhold() {
	e.t.Helper()
	updatedJobs := e.mockSlurm.GetUpdatedJobs()
	if len(updatedJobs) != 1 {
		e.t.Fatalf("Expected 1 job update, got %d", len(updatedJobs))
	}
	update := updatedJobs[0]
	if update.JobID != reconJobID {
		e.t.Errorf("Expected update for job %d, got %d", reconJobID, update.JobID)
	}
	reqMsg, ok := update.Req.(slurmapi.V0044JobDescMsg)
	if !ok {
		e.t.Fatalf("Expected update request to be V0044JobDescMsg, got %T", update.Req)
	}
	if reqMsg.Hold == nil || *reqMsg.Hold {
		e.t.Errorf("Expected update request to set Hold to false, got %v", reqMsg.Hold)
	}
	if reqMsg.Constraints == nil || !strings.Contains(*reqMsg.Constraints, fmt.Sprintf("k8s-id-%d", reconJobID)) {
		e.t.Errorf("Expected update request to set Constraints containing k8s-id-%d, got %v", reconJobID, reqMsg.Constraints)
	}
}

func (e *reconcileTestEnv) verifySlurmCancellation(wantCommentSubstr string) {
	e.t.Helper()
	if wantCommentSubstr != "" {
		updatedJobs := e.mockSlurm.GetUpdatedJobs()
		if len(updatedJobs) != 1 {
			e.t.Fatalf("Expected 1 job update for comment, got %d", len(updatedJobs))
		}
		update := updatedJobs[0]
		if update.JobID != reconJobID {
			e.t.Errorf("Expected update for job %d, got %d", reconJobID, update.JobID)
		}
		reqMsg, ok := update.Req.(slurmapi.V0044JobDescMsg)
		if !ok {
			e.t.Fatalf("Expected update request to be V0044JobDescMsg, got %T", update.Req)
		}
		if reqMsg.Comment == nil {
			e.t.Errorf("Expected comment to be set")
		} else if !strings.Contains(*reqMsg.Comment, wantCommentSubstr) {
			e.t.Errorf("Expected comment to contain %q, got %q", wantCommentSubstr, *reqMsg.Comment)
		}
	}

	deletedJobs := e.mockSlurm.GetDeletedJobs()
	if len(deletedJobs) != 1 || deletedJobs[0] != reconJobID {
		e.t.Errorf("Expected job %d to be deleted, got %v", reconJobID, deletedJobs)
	}
}

func (e *reconcileTestEnv) verifyNoSlurmUpdates() {
	e.t.Helper()
	if len(e.mockSlurm.GetUpdatedJobs()) != 0 {
		e.t.Errorf("Expected no job updates, got %v", e.mockSlurm.GetUpdatedJobs())
	}
	if len(e.mockSlurm.GetDeletedJobs()) != 0 {
		e.t.Errorf("Expected no job deletions, got %v", e.mockSlurm.GetDeletedJobs())
	}
}

func (e *reconcileTestEnv) jobsDiscoveredCount() float64 {
	return testutil.ToFloat64(metrics.JobsDiscovered.WithLabelValues(reconWmName, reconPartitionName))
}

func (e *reconcileTestEnv) jobsSyncCount(status, reason string) float64 {
	return testutil.ToFloat64(metrics.JobsSync.WithLabelValues(status, reason, reconWmName, reconPartitionName))
}
