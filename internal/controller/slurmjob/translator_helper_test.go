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
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
)

const (
	testSlurmName           = "slurm"
	testSlurmAuthSecret     = "slurm-auth-slurm"
	testGpuResourceName     = "nvidia.com/gpu"
	testGkeAcceleratorLabel = "cloud.google.com/gke-accelerator"
	tresCPU                 = "cpu"
	tresMEM                 = "mem"
	tresGpuA3Ultra          = "gres/gpu:a3_ultra"
	argConf                 = "--conf"
	testConfServerDefault   = "slurm-controller.slurm:6817"
	testGresGpuL4           = "Gres=gpu:l4:2"
	testUUIDFeature         = "k8s-id-550e8400-e29b-41d4-a716-446655440000"
	testGpuArchFeature      = "gpu_arch"
	testGpuTypeL4           = "nvidia-l4"
	testGpuTypeA100         = "nvidia-tesla-a100"
	testCustomSlurmdImage   = "custom-slurmd:v1"
	testPartitionHighPrio   = "'Features=k8s-id-123'"
	testPartitionLowPrio    = "'Features=k8s-id-123'"
	testFeatureWithPart     = "'Features=k8s-id-123'"
	testExpectedGpuL4Args   = "'Features=k8s-id-123 Gres=gpu:l4:1'"
	testExpectedGpuL4_2Args = "'Features=k8s-id-123 Gres=gpu:l4:2'"
	testSidecarName         = "my-sidecar"
	testSidecarImage        = "sidecar:latest"
	testUserVolumeName      = "user-vol"
	testHighPrioQueue       = "high-prio-queue"
	testHighPrioClass       = "high-prio-class"

	wantLabelAppName      = "app.kubernetes.io/name"
	wantLabelAppComponent = "app.kubernetes.io/component"
	wantLabelAppManagedBy = "app.kubernetes.io/managed-by"
	wantLabelSlurmJobID   = "k8s-bridge.x-k8s.io/slurm-job-id"
	wantLabelSlurmJobName = "k8s-bridge.x-k8s.io/slurm-job-name"
	wantLabelSlurmPart    = "k8s-bridge.x-k8s.io/slurm-partition"
	wantLabelKueueQueue   = "kueue.x-k8s.io/queue-name"
	wantLabelKueueClass   = "kueue.x-k8s.io/priority-class"
	wantAppName           = "k8s-bridge"
	wantComponentJobSet   = "jobset"
	wantComponentWorker   = "worker"
)

func getTestController() *slinkyv1beta1.Controller {
	return &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testSlurmName,
			Namespace: testSlurmName,
		},
		Spec: slinkyv1beta1.ControllerSpec{
			SlurmKeyRef: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: testSlurmAuthSecret,
				},
				Key: SlurmKeyFile,
			},
		},
	}
}

type testWMSpecBuilder struct {
	spec *k8sbridgev1alpha1.WorkloadMixingSpec
}

func newTestWMSpec() *testWMSpecBuilder {
	return &testWMSpecBuilder{
		spec: &k8sbridgev1alpha1.WorkloadMixingSpec{
			PartitionsMapping: []k8sbridgev1alpha1.PartitionMapping{
				{
					Partition:             testDefaultPartition,
					LocalQueue:            testHighPrioQueue,
					WorkloadPriorityClass: testHighPrioClass,
				},
				{
					Partition:  "low-prio",
					LocalQueue: "low-prio-queue",
				},
			},
			MappedResources: []k8sbridgev1alpha1.GenericResourceMapping{
				{
					ResourceName:    GRESGPU,
					ResourceType:    "l4",
					K8sResourceName: testGpuResourceName,
					NodeSelectors: map[string]string{
						testGkeAcceleratorLabel: testGpuTypeL4,
					},
				},
				{
					ResourceName:    GRESGPU,
					ResourceType:    "a100",
					K8sResourceName: testGpuResourceName,
					NodeSelectors: map[string]string{
						testGkeAcceleratorLabel: testGpuTypeA100,
					},
				},
				{
					ResourceName:    GRESGPU,
					ResourceType:    "", // untyped
					K8sResourceName: testGpuResourceName,
				},
			},
		},
	}
}

func (b *testWMSpecBuilder) withContainers(containers ...corev1.Container) *testWMSpecBuilder {
	b.ensurePodTemplate()
	b.spec.PodTemplate.Spec.Containers = append(b.spec.PodTemplate.Spec.Containers, containers...)
	return b
}

func (b *testWMSpecBuilder) withNodeSelector(nodeSelector map[string]string) *testWMSpecBuilder {
	b.ensurePodTemplate()
	b.spec.PodTemplate.Spec.NodeSelector = nodeSelector
	return b
}

func (b *testWMSpecBuilder) withVolumes(volumes ...corev1.Volume) *testWMSpecBuilder {
	b.ensurePodTemplate()
	b.spec.PodTemplate.Spec.Volumes = append(b.spec.PodTemplate.Spec.Volumes, volumes...)
	return b
}

func (b *testWMSpecBuilder) withPodTemplateLabels(labels map[string]string) *testWMSpecBuilder {
	b.ensurePodTemplate()
	if b.spec.PodTemplate.Labels == nil {
		b.spec.PodTemplate.Labels = make(map[string]string)
	}
	maps.Copy(b.spec.PodTemplate.Labels, labels)
	return b
}

func (b *testWMSpecBuilder) ensurePodTemplate() {
	if b.spec.PodTemplate == nil {
		b.spec.PodTemplate = &corev1.PodTemplateSpec{}
	}
}

func (b *testWMSpecBuilder) build() *k8sbridgev1alpha1.WorkloadMixingSpec {
	return b.spec
}

func getTestWMSpec() *k8sbridgev1alpha1.WorkloadMixingSpec {
	return newTestWMSpec().build()
}

const (
	testDefaultJobID     = 123
	testDefaultPartition = "high-prio"
	testDefaultNodeCount = 1
)

type testJobBuilder struct {
	job *slurmclienttypes.V0044JobInfo
}

func newEmptyTestJob() *testJobBuilder {
	return &testJobBuilder{
		job: &slurmclienttypes.V0044JobInfo{},
	}
}

func (b *testJobBuilder) withDefaultTranslatorFields() *testJobBuilder {
	return b.
		withJobID(testDefaultJobID).
		withPartition(testDefaultPartition).
		withNodes(testDefaultNodeCount)
}

func newTestJob() *testJobBuilder {
	return newEmptyTestJob().withDefaultTranslatorFields()
}

func (b *testJobBuilder) withJobID(jobID int32) *testJobBuilder {
	b.job.JobId = ptr.To(jobID)
	return b
}

func (b *testJobBuilder) withPartition(partition string) *testJobBuilder {
	b.job.Partition = ptr.To(partition)
	return b
}

func (b *testJobBuilder) withName(name string) *testJobBuilder {
	b.job.Name = ptr.To(name)
	return b
}

func (b *testJobBuilder) withNodes(nodes int32) *testJobBuilder {
	if b.job.NodeCount != nil {
		b.job.NodeCount.Number = ptr.To(nodes)
		b.job.NodeCount.Set = ptr.To(true)
	} else {
		b.job.NodeCount = &slurmapi.V0044Uint32NoValStruct{
			Set:    ptr.To(true),
			Number: ptr.To(nodes),
		}
	}
	return b
}

func (b *testJobBuilder) withTasks(tasks int32) *testJobBuilder {
	b.job.Tasks = &slurmapi.V0044Uint32NoValStruct{
		Set:    ptr.To(true),
		Number: ptr.To(tasks),
	}
	return b
}

func (b *testJobBuilder) withTasksPerNode(tasksPerNode int32) *testJobBuilder {
	b.job.TasksPerNode = &slurmapi.V0044Uint16NoValStruct{
		Set:    ptr.To(true),
		Number: ptr.To(tasksPerNode),
	}
	return b
}

func (b *testJobBuilder) withTresReqStr(tresReqStr string) *testJobBuilder {
	b.job.TresReqStr = ptr.To(tresReqStr)
	return b
}

func (b *testJobBuilder) withCpusPerTask(cpus int32) *testJobBuilder {
	b.job.CpusPerTask = &slurmapi.V0044Uint16NoValStruct{
		Set:    ptr.To(true),
		Number: ptr.To(cpus),
	}
	return b
}

func (b *testJobBuilder) withCpusPerTres(cpusPerTres string) *testJobBuilder {
	b.job.CpusPerTres = ptr.To(cpusPerTres)
	return b
}

func (b *testJobBuilder) withTresPerNode(tresPerNode string) *testJobBuilder {
	b.job.TresPerNode = ptr.To(tresPerNode)
	return b
}

func (b *testJobBuilder) withTresPerTask(tresPerTask string) *testJobBuilder {
	b.job.TresPerTask = ptr.To(tresPerTask)
	return b
}

func (b *testJobBuilder) withTresPerJob(tresPerJob string) *testJobBuilder {
	b.job.TresPerJob = ptr.To(tresPerJob)
	return b
}

func (b *testJobBuilder) withMemoryPerNode(mb int64) *testJobBuilder {
	b.job.MemoryPerNode = &slurmapi.V0044Uint64NoValStruct{
		Set:    ptr.To(true),
		Number: ptr.To(mb),
	}
	return b
}

func (b *testJobBuilder) withMemoryPerCpu(mb int64) *testJobBuilder {
	b.job.MemoryPerCpu = &slurmapi.V0044Uint64NoValStruct{
		Set:    ptr.To(true),
		Number: ptr.To(mb),
	}
	return b
}

func (b *testJobBuilder) withMemoryPerTres(memoryPerTres string) *testJobBuilder {
	b.job.MemoryPerTres = ptr.To(memoryPerTres)
	return b
}

func (b *testJobBuilder) withUnsetNodeCount() *testJobBuilder {
	b.job.NodeCount = nil
	return b
}

func (b *testJobBuilder) withNodeCountSet(set bool) *testJobBuilder {
	if b.job.NodeCount != nil {
		b.job.NodeCount.Set = ptr.To(set)
	} else {
		b.job.NodeCount = &slurmapi.V0044Uint32NoValStruct{Set: ptr.To(set)}
	}
	return b
}

// withInfiniteNodeCount sets NodeCount to the value that slurmrestd sends for an infinite node count.
func (b *testJobBuilder) withInfiniteNodeCount() *testJobBuilder {
	b.job.NodeCount = &slurmapi.V0044Uint32NoValStruct{
		Set:      ptr.To(false),
		Infinite: ptr.To(true),
		Number:   ptr.To(int32(0)),
	}
	return b
}

func (b *testJobBuilder) withUnsetJobID() *testJobBuilder {
	b.job.JobId = nil
	return b
}

func (b *testJobBuilder) withHold(hold bool) *testJobBuilder {
	b.job.Hold = ptr.To(hold)
	return b
}

func (b *testJobBuilder) withJobState(states ...slurmapi.V0044JobInfoJobState) *testJobBuilder {
	b.job.JobState = ptr.To(states)
	return b
}

func (b *testJobBuilder) withStateReason(reason string) *testJobBuilder {
	b.job.StateReason = ptr.To(reason)
	return b
}

func (b *testJobBuilder) withComment(comment string) *testJobBuilder {
	b.job.Comment = ptr.To(comment)
	return b
}

func (b *testJobBuilder) withFeatures(features string) *testJobBuilder {
	b.job.Features = ptr.To(features)
	return b
}

// withRequeue sets the requeue flag that slurmrestd reports for the job:
// false for sbatch --no-requeue (or JobRequeue=0), true otherwise.
func (b *testJobBuilder) withRequeue(requeue bool) *testJobBuilder {
	b.job.Requeue = ptr.To(requeue)
	return b
}

func (b *testJobBuilder) build() *slurmclienttypes.V0044JobInfo {
	return b.job
}

func verifyError(t *testing.T, err error, wantErrSubstr string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Expected error containing %q, got nil", wantErrSubstr)
	}
	if !strings.Contains(err.Error(), wantErrSubstr) {
		t.Errorf("Error = %q, want error containing %q", err.Error(), wantErrSubstr)
	}
}

func verifyTranslationError(t *testing.T, obj client.Object, err error, wantErrSubstr string) {
	t.Helper()
	verifyError(t, err, wantErrSubstr)
	if obj != nil {
		t.Errorf("Translate() returned non-nil object %T, want nil", obj)
	}
}

func verifyTranslationSkip(t *testing.T, obj client.Object, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Translate() returned unexpected error: %v", err)
	}
	if obj != nil {
		t.Fatalf("Translate() returned %T, want nil (skipped)", obj)
	}
}

type jobSetVerifier struct {
	wantLabels       map[string]string
	wantArgs         []string
	verifyVolume     bool
	wantRequests     corev1.ResourceList
	wantLimits       corev1.ResourceList
	wantNodeSelector map[string]string
	hasNodeSelector  bool
	wantSlurmdImage  string
	wantPreStopHook  []string
	wantContainers   *int
	// wantEnv values must be present exactly once on the slurmd container.
	wantEnv map[string]string
}

type verifyJobSetOpt func(*jobSetVerifier)

func withEnv(wantEnv map[string]string) verifyJobSetOpt {
	return func(v *jobSetVerifier) {
		v.wantEnv = wantEnv
	}
}

func wantK8sBridgeLabels(component string) map[string]string {
	return map[string]string{
		wantLabelAppName:      wantAppName,
		wantLabelAppComponent: component,
		wantLabelAppManagedBy: wantAppName,
	}
}

func withLabels(wantLabels map[string]string) verifyJobSetOpt {
	return func(v *jobSetVerifier) {
		merged := maps.Clone(wantLabels)
		if merged == nil {
			merged = make(map[string]string)
		}
		maps.Copy(merged, wantK8sBridgeLabels(wantComponentJobSet))
		v.wantLabels = merged
	}
}

func withArgs(wantArgs []string) verifyJobSetOpt {
	return func(v *jobSetVerifier) {
		v.wantArgs = wantArgs
	}
}

func withSlurmVolume() verifyJobSetOpt {
	return func(v *jobSetVerifier) {
		v.verifyVolume = true
	}
}

func withRequests(requests corev1.ResourceList) verifyJobSetOpt {
	return func(v *jobSetVerifier) {
		v.wantRequests = requests
	}
}

func withLimits(limits corev1.ResourceList) verifyJobSetOpt {
	return func(v *jobSetVerifier) {
		v.wantLimits = limits
	}
}

func withNodeSelector(nodeSelector map[string]string) verifyJobSetOpt {
	return func(v *jobSetVerifier) {
		v.wantNodeSelector = nodeSelector
		v.hasNodeSelector = true
	}
}

func withSlurmdImage(image string) verifyJobSetOpt {
	return func(v *jobSetVerifier) {
		v.wantSlurmdImage = image
	}
}

func withPreStopHook(command []string) verifyJobSetOpt {
	return func(v *jobSetVerifier) {
		v.wantPreStopHook = command
	}
}

func withContainersCount(count int) verifyJobSetOpt {
	return func(v *jobSetVerifier) {
		v.wantContainers = &count
	}
}

func verifyJobSet(t *testing.T, obj client.Object, err error, wantJobID string, wantReplicas int32, opts ...verifyJobSetOpt) (*corev1.PodSpec, *corev1.Container) {
	t.Helper()
	if err != nil {
		t.Fatalf("Translate() returned unexpected error: %v", err)
	}
	if obj == nil {
		t.Fatalf("Translate() returned nil object, want non-nil")
	}

	js, ok := obj.(*jobsetv1alpha2.JobSet)
	if !ok {
		t.Fatalf("Translate() returned type %T, want *jobsetv1alpha2.JobSet", obj)
	}

	podSpec, slurmdContainer := verifyJobSetBasics(t, js, wantJobID, wantReplicas)
	if podSpec == nil || slurmdContainer == nil {
		return nil, nil
	}

	var v jobSetVerifier
	for _, opt := range opts {
		opt(&v)
	}

	verifyJobSetOptions(t, js, podSpec, slurmdContainer, &v)

	return podSpec, slurmdContainer
}

func verifyJobSetBasics(t *testing.T, js *jobsetv1alpha2.JobSet, wantJobID string, wantReplicas int32) (*corev1.PodSpec, *corev1.Container) {
	t.Helper()
	if !slices.Equal(js.Finalizers, []string{FinalizerJobSet}) {
		t.Errorf("Translate() returned JobSet.Finalizers = %v, want [%s]", js.Finalizers, FinalizerJobSet)
	}
	if js.Spec.Suspend == nil || !*js.Spec.Suspend {
		t.Error("Translate() returned JobSet.Suspend = false, want true")
	}
	if js.Spec.FailurePolicy == nil {
		t.Error("Translate() returned FailurePolicy = nil, want non-nil")
	} else if js.Spec.FailurePolicy.MaxRestarts != 3 {
		t.Errorf("Translate() returned FailurePolicy.MaxRestarts = %v, want 3", js.Spec.FailurePolicy.MaxRestarts)
	}

	if len(js.Spec.ReplicatedJobs) != 1 {
		t.Fatalf("Translate() returned %d ReplicatedJobs, want 1", len(js.Spec.ReplicatedJobs))
	}
	repJob := js.Spec.ReplicatedJobs[0]
	if repJob.Replicas != wantReplicas {
		t.Errorf("Translate() returned Replicas = %d, want %d", repJob.Replicas, wantReplicas)
	}

	if wantJobID != "" {
		wantPodLabels := wantK8sBridgeLabels(wantComponentWorker)
		wantPodLabels[LabelSlurmJobID] = wantJobID
		if diff := cmp.Diff(wantPodLabels, repJob.Template.Spec.Template.Labels); diff != "" {
			t.Errorf("Translate() returned unexpected Pod labels (-want +got):\n%s", diff)
		}
	}

	podSpec := &repJob.Template.Spec.Template.Spec
	var slurmdContainer *corev1.Container
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == ContainerSlurmd {
			slurmdContainer = &podSpec.Containers[i]
			break
		}
	}
	if slurmdContainer == nil {
		t.Fatalf("Container %q not found in PodSpec", ContainerSlurmd)
	}

	return podSpec, slurmdContainer
}

func verifyJobSetOptions(t *testing.T, js *jobsetv1alpha2.JobSet, podSpec *corev1.PodSpec, slurmd *corev1.Container, v *jobSetVerifier) {
	t.Helper()
	if v.wantContainers != nil && len(podSpec.Containers) != *v.wantContainers {
		t.Fatalf("Expected %d containers in PodSpec, got %d", *v.wantContainers, len(podSpec.Containers))
	}
	if v.wantLabels != nil {
		if diff := cmp.Diff(v.wantLabels, js.Labels); diff != "" {
			t.Errorf("Translate() returned unexpected JobSet labels (-want +got):\n%s", diff)
		}
	}
	if v.wantArgs != nil {
		if diff := cmp.Diff(v.wantArgs, slurmd.Args); diff != "" {
			t.Errorf("Translate() returned unexpected slurmd args (-want +got):\n%s", diff)
		}
	}
	if v.wantSlurmdImage != "" && slurmd.Image != v.wantSlurmdImage {
		t.Errorf("Expected slurmd image %q, got %q", v.wantSlurmdImage, slurmd.Image)
	}
	if v.hasNodeSelector {
		if diff := cmp.Diff(v.wantNodeSelector, podSpec.NodeSelector); diff != "" {
			t.Errorf("Translate() returned unexpected NodeSelector (-want +got):\n%s", diff)
		}
	}
	verifyJobSetResources(t, slurmd, v)
	verifyJobSetLifecycle(t, slurmd, v)
	for name, wantValue := range v.wantEnv {
		var gotValues []string
		for _, env := range slurmd.Env {
			if env.Name == name {
				gotValues = append(gotValues, env.Value)
			}
		}
		if diff := cmp.Diff([]string{wantValue}, gotValues); diff != "" {
			t.Errorf("Translate() returned unexpected slurmd env %s (-want +got):\n%s", name, diff)
		}
	}
	if v.verifyVolume {
		verifySlurmVolumeAndMount(t, *podSpec, *slurmd)
	}
}

func verifyJobSetResources(t *testing.T, slurmd *corev1.Container, v *jobSetVerifier) {
	t.Helper()
	if _, wantCPULimit := v.wantLimits[corev1.ResourceCPU]; !wantCPULimit {
		if _, hasCPULimit := slurmd.Resources.Limits[corev1.ResourceCPU]; hasCPULimit {
			t.Errorf("Translate() set unexpected CPU limit %v on slurmd container", slurmd.Resources.Limits[corev1.ResourceCPU])
		}
	}
	if _, wantMemLimit := v.wantLimits[corev1.ResourceMemory]; !wantMemLimit {
		if _, hasMemLimit := slurmd.Resources.Limits[corev1.ResourceMemory]; hasMemLimit {
			t.Errorf("Translate() set unexpected memory limit %v on slurmd container", slurmd.Resources.Limits[corev1.ResourceMemory])
		}
	}
	if v.wantRequests != nil {
		for resName, wantQty := range v.wantRequests {
			gotQty := slurmd.Resources.Requests[resName]
			if diff := cmp.Diff(wantQty, gotQty); diff != "" {
				t.Errorf("Translate() returned unexpected slurmd resource request for %s (-want +got):\n%s", resName, diff)
			}
		}
	}
	if v.wantLimits != nil {
		for resName, wantQty := range v.wantLimits {
			gotQty := slurmd.Resources.Limits[resName]
			if diff := cmp.Diff(wantQty, gotQty); diff != "" {
				t.Errorf("Translate() returned unexpected slurmd resource limit for %s (-want +got):\n%s", resName, diff)
			}
		}
	}
}

func verifyJobSetLifecycle(t *testing.T, slurmd *corev1.Container, v *jobSetVerifier) {
	t.Helper()
	if v.wantPreStopHook != nil {
		if slurmd.Lifecycle == nil || slurmd.Lifecycle.PreStop == nil {
			t.Fatalf("slurmd container has no preStop lifecycle hook configured")
		} else if slurmd.Lifecycle.PreStop.Exec == nil || len(slurmd.Lifecycle.PreStop.Exec.Command) == 0 {
			t.Fatalf("slurmd container preStop lifecycle hook has no Exec command")
		} else if diff := cmp.Diff(v.wantPreStopHook, slurmd.Lifecycle.PreStop.Exec.Command); diff != "" {
			t.Errorf("Translate() returned unexpected preStop hook command (-want +got):\n%s", diff)
		}
	}
}

//nolint:staticcheck // t.Fatalf terminates test execution
func verifySlurmVolumeAndMount(t *testing.T, podSpec corev1.PodSpec, container corev1.Container) {
	t.Helper()
	if len(podSpec.Volumes) == 0 {
		t.Fatalf("Expected at least 1 volume in PodSpec, got 0")
	}
	var slurmVol *corev1.Volume
	for i := range podSpec.Volumes {
		if podSpec.Volumes[i].Name == SlurmConfigVolume {
			slurmVol = &podSpec.Volumes[i]
			break
		}
	}
	if slurmVol == nil {
		t.Fatalf("Expected volume %s in PodSpec, not found", SlurmConfigVolume)
	}
	if slurmVol.Projected == nil || len(slurmVol.Projected.Sources) == 0 {
		t.Fatalf("Expected projected volume source with at least 1 source")
	}
	if slurmVol.Projected.DefaultMode == nil || *slurmVol.Projected.DefaultMode != int32(SlurmKeyMode) {
		t.Errorf("Expected volume defaultMode %d, got %v", SlurmKeyMode, slurmVol.Projected.DefaultMode)
	}

	var slurmMount *corev1.VolumeMount
	for i := range container.VolumeMounts {
		if container.VolumeMounts[i].Name == SlurmConfigVolume {
			slurmMount = &container.VolumeMounts[i]
			break
		}
	}
	if slurmMount == nil {
		t.Fatalf("Expected volume mount %s in slurmd container, not found", SlurmConfigVolume)
	}
	if slurmMount.MountPath != SlurmConfigDir || !slurmMount.ReadOnly {
		t.Errorf("Expected volume mount path %s readOnly=true, got path=%s readOnly=%v", SlurmConfigDir, slurmMount.MountPath, slurmMount.ReadOnly)
	}
}

func translateTestJobSet(t *testing.T, b *testWMSpecBuilder) *jobsetv1alpha2.JobSet {
	t.Helper()
	return translateTestJobSetWithJob(t, newTestJob(), b)
}

func translateTestJobSetWithJob(t *testing.T, job *testJobBuilder, b *testWMSpecBuilder) *jobsetv1alpha2.JobSet {
	t.Helper()
	obj, err := NewJobSetTranslator().Translate(job.build(), b.build(), getTestController())
	if err != nil {
		t.Fatalf("Translate() returned unexpected error: %v", err)
		return nil
	}
	js, ok := obj.(*jobsetv1alpha2.JobSet)
	if !ok || js == nil {
		t.Fatalf("Translate() returned %T (%v), want non-nil *jobsetv1alpha2.JobSet", obj, obj)
		return nil
	}
	return js
}

func workerPodLabels(t *testing.T, js *jobsetv1alpha2.JobSet) map[string]string {
	t.Helper()
	if len(js.Spec.ReplicatedJobs) != 1 {
		t.Fatalf("Translate() returned %d ReplicatedJobs, want 1", len(js.Spec.ReplicatedJobs))
	}
	return js.Spec.ReplicatedJobs[0].Template.Spec.Template.Labels
}
