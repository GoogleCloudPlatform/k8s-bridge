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
	"errors"
	"fmt"
	"maps"
	"math"
	"net"
	"regexp"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
)

const (
	LabelSlurmJobID             = "k8s-bridge.x-k8s.io/slurm-job-id"
	LabelSlurmJobName           = "k8s-bridge.x-k8s.io/slurm-job-name"
	LabelSlurmPartition         = "k8s-bridge.x-k8s.io/slurm-partition"
	LabelKueueQueue             = "kueue.x-k8s.io/queue-name"
	LabelKueuePriorityClass     = "kueue.x-k8s.io/priority-class"
	GRESGPU                     = "gpu"
	tresNode                    = "node"
	ContainerSlurmd             = "slurmd"
	DefaultSlurmdImage          = "placeholder-image"
	defaultMemoryPerCPUMB       = 2048
	SlurmConfigVolume           = "slurm-etc"
	SlurmConfigDir              = "/etc/slurm"
	SlurmKeyFile                = "slurm.key"
	SlurmKeyMode                = 0o600
	SlurmctldDefaultPort        = 6817
	argConfServer               = "--conf-server"
	argNodeName                 = "-N"
	valNodeName                 = "$(SLURM_NODE_NAME)"
	envJobSetName               = "JOBSET_NAME"
	envReplicatedJobName        = "REPLICATED_JOB_NAME"
	envJobIndex                 = "JOB_INDEX"
	envPodIndex                 = "POD_INDEX"
	envSlurmNodeName            = "SLURM_NODE_NAME"
	fieldPathJobSetName         = "metadata.labels['jobset.sigs.k8s.io/jobset-name']"
	fieldPathReplicatedJobName  = "metadata.labels['jobset.sigs.k8s.io/replicatedjob-name']"
	fieldPathJobIndex           = "metadata.labels['jobset.sigs.k8s.io/job-index']"
	fieldPathJobCompletionIndex = "metadata.labels['batch.kubernetes.io/job-completion-index']"
	ReasonJobHeldUser           = "JobHeldUser"
	ReasonJobHeldAdmin          = "JobHeldAdmin"
	ReasonPriority              = "Priority"
	AutoHoldMarker              = "k8s-bridge-auto-hold"

	// LINT.IfChange(max_restarts)
	// DefaultJobSetMaxRestarts and NoRequeueJobSetMaxRestarts set failurePolicy.maxRestarts
	// on a translated JobSet. A job that was submitted with sbatch --no-requeue must not restart.
	DefaultJobSetMaxRestarts   int32 = 3
	NoRequeueJobSetMaxRestarts int32 = 0
	// LINT.ThenChange(test/e2e/jobset_restart_policy_test.go:max_restarts)

	SlurmdProbePeriodSeconds            = 10
	SlurmdProbeTimeoutSeconds           = 5
	SlurmdStartupProbeFailureThreshold  = 30
	SlurmdLivenessProbeFailureThreshold = 6
	SlurmdProbeCommand                  = `printf 'GET /livez HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n' | socat -T 2 - TCP:127.0.0.1:6818 2>/dev/null | grep -q '^HTTP/1\.[01] 20[04]' && (scontrol listpids 2>/dev/null | awk 'NR>1 {found=1; exit} END {exit !found}' || scontrol -o show node "$SLURM_NODE_NAME" 2>/dev/null | grep -Eq 'State=[^ ]*(ALLOCATED|MIXED|COMPLETING)')`

	LabelAppName      = "app.kubernetes.io/name"
	LabelAppComponent = "app.kubernetes.io/component"
	LabelAppManagedBy = "app.kubernetes.io/managed-by"
	AppName           = "k8s-bridge"
	ComponentJobSet   = "jobset"
	ComponentWorker   = "worker"
)

const (
	// FinalizerJobSet is added to every JobSet created by k8s-bridge. It keeps the JobSet, and its
	// slurmd replica count, until k8s-bridge has deregistered the JobSet's dynamic Slurm nodes.
	FinalizerJobSet = "k8s-bridge.x-k8s.io/slurm-node-cleanup"

	jobSetNamePrefix = "slurm-job-"
)

var (
	invalidLabelCharsRegex = regexp.MustCompile(`[^a-z0-9_.-]+`)
	repeatedDashRegex      = regexp.MustCompile(`-{2,}`)
)

// jobSetNameForJobID returns the name of the JobSet created for the given Slurm job ID.
func jobSetNameForJobID(jobID string) string {
	return jobSetNamePrefix + jobID
}

// JobTranslator defines the interface for translating Slurm jobs into Kubernetes resources.
type JobTranslator interface {
	// Translate converts a Slurm job into a Kubernetes object.
	// Returns (nil, nil) if the job should be skipped.
	// Returns (nil, err) if translation fails and the job should be cancelled.
	Translate(job *slurmclienttypes.V0044JobInfo, wmSpec *k8sbridgev1alpha1.WorkloadMixingSpec, controller *slinkyv1beta1.Controller) (client.Object, error)
}

// JobSetTranslator implements JobTranslator to translate Slurm jobs into Kubernetes JobSets.
type JobSetTranslator struct{}

// NewJobSetTranslator creates a new JobSetTranslator.
func NewJobSetTranslator() *JobSetTranslator {
	return &JobSetTranslator{}
}

func findContainer(podSpec *corev1.PodSpec, name string) *corev1.Container {
	if podSpec == nil {
		return nil
	}
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == name {
			return &podSpec.Containers[i]
		}
	}
	return nil
}

// Translate implements the JobTranslator interface.
func (t *JobSetTranslator) Translate(
	job *slurmclienttypes.V0044JobInfo,
	wmSpec *k8sbridgev1alpha1.WorkloadMixingSpec,
	controller *slinkyv1beta1.Controller,
) (client.Object, error) {
	if job == nil {
		return nil, errors.New("job is nil")
	}
	if wmSpec == nil {
		return nil, errors.New("wmSpec is nil")
	}
	if controller == nil {
		return nil, errors.New("controller is nil")
	}

	jobID := ptr.Deref(job.JobId, 0)
	if jobID == 0 {
		return nil, errors.New("job ID is unset or 0")
	}

	// 0. Check for explicit user or admin hold
	if isJobExplicitlyHeld(job) {
		return nil, nil // Defer translation until explicit hold is released
	}

	// 1. Resolve Partition & Mapping
	mapping, err := resolvePartitionMapping(ptr.Deref(job.Partition, ""), wmSpec)
	if err != nil {
		return nil, err
	}
	if mapping == nil {
		return nil, nil // Skip
	}

	// 2. Resolve Replicas (requested node count)
	replicas, err := resolveReplicas(job)
	if err != nil {
		return nil, err
	}

	// 3. Resource Calculation
	resources, gresInfo, err := resolvePodResources(job, replicas, wmSpec)
	if err != nil {
		return nil, err
	}

	// 4. Resolve ConfServer
	confServer, err := resolveConfServer(controller)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve conf-server: %w", err)
	}

	// 5. Construct JobSet
	jobIDStr := strconv.Itoa(int(jobID))
	sanitizedJobName := slurmJobNameLabelValue(ptr.Deref(job.Name, ""))
	labels := buildJobSetLabels(jobIDStr, sanitizedJobName, mapping)

	var podTemplate corev1.PodTemplateSpec
	if wmSpec.PodTemplate != nil {
		podTemplate = *wmSpec.PodTemplate.DeepCopy()
		applyPodTemplateOverrides(&podTemplate, jobIDStr, sanitizedJobName, resources, gresInfo.NodeSelectors)
	} else {
		podTemplate = buildPodTemplate(jobIDStr, sanitizedJobName, resources, gresInfo.NodeSelectors)
	}

	if err := injectSlurmVolumes(&podTemplate.Spec, controller); err != nil {
		return nil, fmt.Errorf("failed to inject slurm volumes: %w", err)
	}

	if err := appendSlurmdArgs(&podTemplate.Spec, jobIDStr, confServer, gresInfo.ResolvedGres, mapping.DynamicNodeFeature); err != nil {
		return nil, fmt.Errorf("failed to append slurmd args: %w", err)
	}

	injectPreStopHook(&podTemplate.Spec)
	injectSlurmdProbes(&podTemplate.Spec, jobIDStr)

	maxRestarts := resolveMaxRestarts(job)
	log.Log.WithName("translator").V(1).Info("Translated Slurm job to JobSet",
		"jobID", jobIDStr,
		"partition", mapping.Partition,
		"localQueue", mapping.LocalQueue,
		"priorityClass", mapping.WorkloadPriorityClass,
		"dynamicNodeFeature", mapping.DynamicNodeFeature,
		"replicas", replicas,
		"maxRestarts", maxRestarts,
		"requests", resources.Requests,
		"limits", resources.Limits,
		"nodeSelectors", podTemplate.Spec.NodeSelector,
		"confServer", confServer)

	return buildJobSet(jobIDStr, replicas, maxRestarts, labels, podTemplate), nil
}

// resolveMaxRestarts returns the JobSet restart budget for a Slurm job.
// Requeue is false for a job that Slurm must not requeue, for example a job
// that was submitted with sbatch --no-requeue. A nil Requeue keeps the default budget.
func resolveMaxRestarts(job *slurmclienttypes.V0044JobInfo) int32 {
	if !ptr.Deref(job.Requeue, true) {
		return NoRequeueJobSetMaxRestarts
	}
	return DefaultJobSetMaxRestarts
}

func applyPodTemplateOverrides(
	template *corev1.PodTemplateSpec,
	jobIDStr string,
	sanitizedJobName string,
	resources corev1.ResourceRequirements,
	calculatedNodeSelectors map[string]string,
) {
	// 1. Merge Labels
	labels := maps.Clone(template.Labels)
	if labels == nil {
		labels = make(map[string]string)
	}
	delete(labels, LabelSlurmJobName)
	maps.Copy(labels, k8sBridgeLabels(ComponentWorker, jobIDStr, sanitizedJobName))
	template.Labels = labels

	// 2. Ensure slurmd container exists and update resources
	foundSlurmd := false
	for i := range template.Spec.Containers {
		if template.Spec.Containers[i].Name == ContainerSlurmd {
			mergeContainerResources(&template.Spec.Containers[i], resources, jobIDStr)
			foundSlurmd = true
			break
		}
	}

	if !foundSlurmd {
		slurmdContainer := corev1.Container{
			Name:      ContainerSlurmd,
			Image:     DefaultSlurmdImage,
			Resources: resources,
		}
		template.Spec.Containers = append(template.Spec.Containers, slurmdContainer)
	}

	// 3. Merge NodeSelectors (calculated overrides template)
	if len(calculatedNodeSelectors) > 0 {
		if template.Spec.NodeSelector == nil {
			template.Spec.NodeSelector = make(map[string]string)
		}
		maps.Copy(template.Spec.NodeSelector, calculatedNodeSelectors)
	}

	// 4. Default RestartPolicy to Never if not explicitly specified
	if template.Spec.RestartPolicy == "" {
		template.Spec.RestartPolicy = corev1.RestartPolicyNever
	}
}

// mergeContainerResources applies Slurm-calculated resources onto the slurmd container
// from podTemplate while preserving unrelated custom resources (e.g. ephemeral-storage)
// and any limits explicitly configured in podTemplate.
func mergeContainerResources(container *corev1.Container, calculated corev1.ResourceRequirements, jobIDStr string) {
	logger := log.Log.WithName("translator")

	if container.Resources.Requests == nil {
		container.Resources.Requests = make(corev1.ResourceList)
	}
	for resName, calcQty := range calculated.Requests {
		if prevQty, ok := container.Resources.Requests[resName]; ok {
			logger.V(1).Info("Overwriting user-provided slurmd resource request from podTemplate",
				"jobID", jobIDStr, "resource", resName, "templateValue", prevQty.String(), "calculatedValue", calcQty.String())
		}
		container.Resources.Requests[resName] = calcQty
	}

	for resName, calcQty := range calculated.Limits {
		if container.Resources.Limits == nil {
			container.Resources.Limits = make(corev1.ResourceList)
		}
		if prevQty, ok := container.Resources.Limits[resName]; ok {
			logger.V(1).Info("Overwriting user-provided slurmd resource limit from podTemplate",
				"jobID", jobIDStr, "resource", resName, "templateValue", prevQty.String(), "calculatedValue", calcQty.String())
		}
		container.Resources.Limits[resName] = calcQty
	}
}

// resolvePartitionMapping resolves the partition for the job and returns the corresponding mapping.
//
// In Slurm, the partition field can contain a comma-separated list of partitions (e.g. "part1,part2").
// Reference: https://slurm.schedmd.com/sbatch.html#OPT_partition
//
// Behavior:
// - If the partition field is empty or not set, it returns (nil, nil) to skip the job.
// - If it contains a single partition, it checks if it is mapped in WorkloadMixing.
//   - If yes, returns the mapping.
//   - If no, returns (nil, nil) to skip the job.
//
// - If it contains multiple partitions:
//   - It checks if any of them is mapped/managed.
//   - If at least one is managed, it returns an error (fail) because we do not support
//     translating multi-partition jobs that could run on managed partitions.
//   - If none are managed, it returns (nil, nil) to skip the job.
func resolvePartitionMapping(
	partitionStr string,
	wmSpec *k8sbridgev1alpha1.WorkloadMixingSpec,
) (*k8sbridgev1alpha1.PartitionMapping, error) {
	if partitionStr == "" {
		return nil, nil // Skip
	}

	partitions := strings.Split(partitionStr, ",")
	if len(partitions) > 1 {
		// Check if any of them is mapped
		for _, part := range partitions {
			part = strings.TrimSpace(part)
			if findPartitionMapping(part, wmSpec) != nil {
				return nil, fmt.Errorf("multiple partitions specified with managed partition: %s", partitionStr) // Fail
			}
		}
		return nil, nil // Skip (multiple partitions, none are managed)
	}

	partition := strings.TrimSpace(partitions[0])
	mapping := findPartitionMapping(partition, wmSpec)
	if mapping == nil {
		return nil, nil // Skip (single partition, not managed)
	}

	return mapping, nil
}

// isExplicitUserOrAdminHold determines whether a pending job is held due to an explicit user or administrator hold.
func isExplicitUserOrAdminHold(stateReason string) bool {
	return stateReason == ReasonJobHeldUser || stateReason == ReasonJobHeldAdmin
}

func isJobExplicitlyHeld(job *slurmclienttypes.V0044JobInfo) bool {
	if job == nil {
		return false
	}
	if !ptr.Deref(job.Hold, false) {
		return false
	}
	stateReason := ptr.Deref(job.StateReason, "")
	if !isExplicitUserOrAdminHold(stateReason) {
		return false
	}
	if stateReason == ReasonJobHeldAdmin {
		return true
	}
	if stateReason == ReasonJobHeldUser {
		return !strings.Contains(ptr.Deref(job.Comment, ""), AutoHoldMarker)
	}
	return false
}

func findPartitionMapping(partition string, wmSpec *k8sbridgev1alpha1.WorkloadMixingSpec) *k8sbridgev1alpha1.PartitionMapping {
	for _, pm := range wmSpec.PartitionsMapping {
		if pm.Partition == partition {
			return &pm
		}
	}
	return nil
}

func k8sBridgeLabels(component, jobIDStr, sanitizedJobName string) map[string]string {
	labels := map[string]string{
		LabelAppName:      AppName,
		LabelAppComponent: component,
		LabelAppManagedBy: AppName,
		LabelSlurmJobID:   jobIDStr,
	}
	if sanitizedJobName != "" {
		labels[LabelSlurmJobName] = sanitizedJobName
	}
	return labels
}

func slurmJobNameLabelValue(slurmJobName string) string {
	value := invalidLabelCharsRegex.ReplaceAllString(strings.ToLower(slurmJobName), "-")
	value = repeatedDashRegex.ReplaceAllString(value, "-")
	value = strings.Trim(value, "-_.")
	if len(value) > validation.LabelValueMaxLength {
		value = strings.Trim(value[:validation.LabelValueMaxLength], "-_.")
	}
	return value
}

func buildJobSetLabels(jobIDStr, sanitizedJobName string, mapping *k8sbridgev1alpha1.PartitionMapping) map[string]string {
	labels := k8sBridgeLabels(ComponentJobSet, jobIDStr, sanitizedJobName)
	labels[LabelSlurmPartition] = mapping.Partition
	labels[LabelKueueQueue] = mapping.LocalQueue
	if mapping.WorkloadPriorityClass != "" {
		labels[LabelKueuePriorityClass] = mapping.WorkloadPriorityClass
	}
	return labels
}

func buildPodTemplate(jobIDStr, sanitizedJobName string, resources corev1.ResourceRequirements, nodeSelectors map[string]string) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: k8sBridgeLabels(ComponentWorker, jobIDStr, sanitizedJobName),
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:      ContainerSlurmd,
					Image:     DefaultSlurmdImage,
					Resources: resources,
				},
			},
			NodeSelector: nodeSelectors,
		},
	}
}

func buildJobSet(
	jobIDStr string,
	replicas int32,
	maxRestarts int32,
	labels map[string]string,
	podTemplate corev1.PodTemplateSpec,
) *jobsetv1alpha2.JobSet {
	jobSetName := jobSetNameForJobID(jobIDStr)
	var podFailurePolicy *batchv1.PodFailurePolicy
	if findContainer(&podTemplate.Spec, ContainerSlurmd) != nil &&
		(podTemplate.Spec.RestartPolicy == "" || podTemplate.Spec.RestartPolicy == corev1.RestartPolicyNever) {
		podFailurePolicy = &batchv1.PodFailurePolicy{
			Rules: []batchv1.PodFailurePolicyRule{
				{
					Action: batchv1.PodFailurePolicyActionFailJob,
					OnExitCodes: &batchv1.PodFailurePolicyOnExitCodesRequirement{
						ContainerName: ptr.To(ContainerSlurmd),
						Operator:      batchv1.PodFailurePolicyOnExitCodesOpNotIn,
						Values:        []int32{0},
					},
				},
			},
		}
	}

	return &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:       jobSetName,
			Labels:     labels,
			Finalizers: []string{FinalizerJobSet},
		},
		Spec: jobsetv1alpha2.JobSetSpec{
			Suspend: ptr.To(true),
			FailurePolicy: &jobsetv1alpha2.FailurePolicy{
				MaxRestarts: maxRestarts,
			},
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
				{
					Name:     ContainerSlurmd,
					Replicas: replicas,
					Template: batchv1.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Parallelism:      ptr.To(int32(1)),
							Completions:      ptr.To(int32(1)),
							CompletionMode:   ptr.To(batchv1.IndexedCompletion),
							PodFailurePolicy: podFailurePolicy,
							Template:         podTemplate,
						},
					},
				},
			},
		},
	}
}

func injectPreStopHook(spec *corev1.PodSpec) {
	for i := range spec.Containers {
		if spec.Containers[i].Name == ContainerSlurmd {
			if spec.Containers[i].Lifecycle == nil {
				spec.Containers[i].Lifecycle = &corev1.Lifecycle{}
			}
			// Do not overwrite the PreStop hook if the user provided a custom one in the pod template.
			if spec.Containers[i].Lifecycle.PreStop == nil {
				spec.Containers[i].Lifecycle.PreStop = &corev1.LifecycleHandler{
					Exec: &corev1.ExecAction{
						Command: []string{
							"sh", "-c",
							"scontrol update nodename=$SLURM_NODE_NAME state=DOWN reason=\"pod_terminated\" && scontrol delete nodename=$SLURM_NODE_NAME",
						},
					},
				}
			}
			break
		}
	}
}

func injectSlurmdProbes(spec *corev1.PodSpec, jobIDStr string) {
	logger := log.Log.WithName("translator")
	for i := range spec.Containers {
		if spec.Containers[i].Name == ContainerSlurmd {
			if spec.Containers[i].StartupProbe == nil {
				spec.Containers[i].StartupProbe = defaultSlurmdStartupProbe()
			} else {
				logger.V(1).Info("Custom startupProbe already configured on slurmd container, skipping default probe injection", "jobID", jobIDStr)
			}
			if spec.Containers[i].LivenessProbe == nil {
				spec.Containers[i].LivenessProbe = defaultSlurmdLivenessProbe()
			} else {
				logger.V(1).Info("Custom livenessProbe already configured on slurmd container, skipping default probe injection", "jobID", jobIDStr)
			}
			break
		}
	}
}

func defaultSlurmdStartupProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sh", "-c", SlurmdProbeCommand},
			},
		},
		PeriodSeconds:    SlurmdProbePeriodSeconds,
		TimeoutSeconds:   SlurmdProbeTimeoutSeconds,
		FailureThreshold: SlurmdStartupProbeFailureThreshold,
	}
}

func defaultSlurmdLivenessProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sh", "-c", SlurmdProbeCommand},
			},
		},
		PeriodSeconds:    SlurmdProbePeriodSeconds,
		TimeoutSeconds:   SlurmdProbeTimeoutSeconds,
		FailureThreshold: SlurmdLivenessProbeFailureThreshold,
	}
}

// parseTresString parses a Slurm TRES (Trackable Resources) string into a map.
//
// A TRES string is a semicolon-delimited list of key=value or key:value pairs.
// This function also handles comma-separated lists by normalizing commas to semicolons.
//
// It uses LastIndexAny to split by the last '=' or ':' to correctly handle keys
// that contain colons (e.g. gres/gpu:a3_ultra=2).
//
// Example inputs:
//   - "cpu=4;mem=1024"
//   - "gres/gpu=1;cpu=2"
//   - "gpu:2,cpu:4"
//   - "gres/gpu:a3_ultra=2"
//
// Reference:
//   - Slurm TRES: https://slurm.schedmd.com/tres.html
//   - Slurm REST API: https://slurm.schedmd.com/rest_api.html
func parseTresString(tresStr string) map[string]int64 {
	res := make(map[string]int64)
	if tresStr == "" {
		return res
	}
	normalized := strings.ReplaceAll(tresStr, ",", ";")
	parts := strings.SplitSeq(normalized, ";")
	for part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idx := strings.LastIndexAny(part, "=:")
		if idx == -1 {
			continue
		}
		key := strings.TrimSpace(part[:idx])
		valStr := strings.TrimSpace(part[idx+1:])
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err == nil {
			res[key] = val
		}
	}
	return res
}

func lookupTres(tresStr string, resName string, resourceType string) (int64, bool) {
	tresMap := parseTresString(tresStr)

	// 1. Try specific type if available
	if resourceType != "" {
		// Try gres/<name>:<type>
		if val, ok := tresMap["gres/"+resName+":"+resourceType]; ok {
			return val, true
		}
		// Try <name>:<type>
		if val, ok := tresMap[resName+":"+resourceType]; ok {
			return val, true
		}
	}

	// 2. Try generic
	if val, ok := tresMap["gres/"+resName]; ok {
		return val, true
	}
	if val, ok := tresMap[resName]; ok {
		return val, true
	}

	return 0, false
}

type gresRequest struct {
	name         string
	resourceType string
	count        int64
}

// resolveReplicas returns the number of slurmd pods (dynamic Slurm nodes) to create for the job.
//
// Reference: _pack_node_cnt() in src/slurmctld/job_mgr.c
// https://github.com/SchedMD/slurm/blob/slurm-26-05-5-1/src/slurmctld/job_mgr.c#L11133-L11198
func resolveReplicas(job *slurmclienttypes.V0044JobInfo) (int32, error) {
	if nodes, ok := requestedNodeCount(job); ok {
		return nodes, nil
	}

	log.Log.WithName("translator").V(1).Info("Could not determine requested node count from TresReqStr, falling back to NodeCount",
		"jobID", strconv.Itoa(int(ptr.Deref(job.JobId, 0))), "tresReqStr", ptr.Deref(job.TresReqStr, ""))
	// Check Infinite before Set. slurmrestd sends an infinite value with set=false and number=0.
	// Reference: DUMP_FUNC(UINT32_NO_VAL) in src/plugins/data_parser/v0.0.44/parsers.c
	// https://github.com/SchedMD/slurm/blob/slurm-26-05-5-1/src/plugins/data_parser/v0.0.44/parsers.c#L3106-L3132
	if job.NodeCount != nil && ptr.Deref(job.NodeCount.Infinite, false) {
		return 0, errors.New("infinite NodeCount is not supported")
	}
	if job.NodeCount == nil || !ptr.Deref(job.NodeCount.Set, false) {
		return 0, errors.New("job NodeCount is unset")
	}
	replicas := ptr.Deref(job.NodeCount.Number, 0)
	if replicas <= 0 {
		return 0, fmt.Errorf("invalid NodeCount: %d", replicas)
	}
	return replicas, nil
}

// requestedNodeCount returns the node count from the job's tres_req_str, if present and valid.
func requestedNodeCount(job *slurmclienttypes.V0044JobInfo) (int32, bool) {
	nodes, ok := parseTresString(ptr.Deref(job.TresReqStr, ""))[tresNode]
	if !ok || nodes <= 0 || nodes > math.MaxInt32 {
		return 0, false
	}
	return int32(nodes), true
}

func resolveTasksPerNode(job *slurmclienttypes.V0044JobInfo, replicas int32) int64 {
	if replicas <= 0 {
		return 0
	}
	if job.TasksPerNode != nil && ptr.Deref(job.TasksPerNode.Set, false) {
		val := int64(ptr.Deref(job.TasksPerNode.Number, 0))
		if val > 0 {
			return val
		}
	}
	var tasks int64 = 1
	if job.Tasks != nil && ptr.Deref(job.Tasks.Set, false) {
		val := int64(ptr.Deref(job.Tasks.Number, 0))
		if val > 0 {
			tasks = val
		}
	}
	return (tasks + int64(replicas) - 1) / int64(replicas)
}

// assumedThreadsPerCore is the number of hardware threads per CPU core that the translator
// assumes for the nodes that run slurmd pods. The translator does not know the node of a pod
// when it builds the JobSet.
const assumedThreadsPerCore = 2

// resolveCPUsPerPod returns max(cpus_per_task * tasksPerNode, GPUs per node * --cpus-per-gpu),
// because slurmrestd reports cpus_per_task=1 even for jobs that use --cpus-per-gpu.
// The result is rounded up to whole cores, see roundUpToWholeCores.
func resolveCPUsPerPod(job *slurmclienttypes.V0044JobInfo, tasksPerNode int64, gpu *resolvedGRES) int64 {
	cpusPerTask := int64(1)
	if job.CpusPerTask != nil && ptr.Deref(job.CpusPerTask.Set, false) {
		if n := int64(ptr.Deref(job.CpusPerTask.Number, 0)); n > 0 {
			cpusPerTask = n
		}
	}
	cpu := cpusPerTask * tasksPerNode
	if gpu != nil {
		if cpusPerGPU, ok := lookupTres(ptr.Deref(job.CpusPerTres, ""), GRESGPU, gpu.resourceType); ok {
			cpu = max(cpu, gpu.count*cpusPerGPU)
		}
	}
	return roundUpToWholeCores(cpu)
}

// roundUpToWholeCores rounds cpus up to a multiple of assumedThreadsPerCore. By default
// (SelectTypeParameters=CR_Core_Memory), Slurm allocates every CPU of an allocated core,
// so a job that asks for 3 CPUs on a node with 2 threads per core gets 4 CPUs.
//
// Reference: CR_Core in slurm.conf(5)
// https://github.com/SchedMD/slurm/blob/slurm-26-05-5-1/doc/man/man5/slurm.conf.5#L5098-L5105
func roundUpToWholeCores(cpus int64) int64 {
	return (cpus + assumedThreadsPerCore - 1) / assumedThreadsPerCore * assumedThreadsPerCore
}

func parseGRES(tresStr string) ([]gresRequest, error) {
	var reqs []gresRequest
	if tresStr == "" {
		return reqs, nil
	}
	parts := strings.SplitSeq(tresStr, ",")
	for part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !strings.HasPrefix(part, "gres/") {
			continue
		}
		part = strings.TrimPrefix(part, "gres/")

		// Normalize: replace '=' with ':' to handle both formats
		part = strings.ReplaceAll(part, "=", ":")

		elements := strings.Split(part, ":")
		if len(elements) < 2 {
			continue
		}

		name := elements[0]
		valStr := elements[len(elements)-1]
		var resourceType string
		if len(elements) > 2 {
			resourceType = strings.Join(elements[1:len(elements)-1], ":")
		}

		count, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid GRES count in %q: %w", part, err)
		}

		reqs = append(reqs, gresRequest{
			name:         name,
			resourceType: resourceType,
			count:        count,
		})
	}
	return reqs, nil
}

func findGRESRequest(tresStr string, resourceName string) (*gresRequest, error) {
	reqs, err := parseGRES(tresStr)
	if err != nil {
		return nil, err
	}
	var matches []*gresRequest
	for i := range reqs {
		if reqs[i].name == resourceName {
			matches = append(matches, &reqs[i])
		}
	}
	if len(matches) > 1 {
		var details []string
		for _, req := range matches {
			s := req.name
			if req.resourceType != "" {
				s = fmt.Sprintf("%s:%s", req.name, req.resourceType)
			}
			details = append(details, fmt.Sprintf("gres/%s=%d", s, req.count))
		}
		return nil, fmt.Errorf("multiple heterogeneous %s requests are not supported: %s", resourceName, strings.Join(details, ", "))
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	return nil, nil
}

type resolvedGRES struct {
	name         string
	count        int64
	resourceType string
}

type GresResolutionInfo struct {
	NodeSelectors map[string]string
	ResolvedGres  []resolvedGRES
}

func resolveGRES(job *slurmclienttypes.V0044JobInfo, replicas int32, gresName string) (*resolvedGRES, error) {
	if replicas <= 0 {
		return nil, fmt.Errorf("invalid replicas count: %d", replicas)
	}
	if job.TresPerNode != nil && *job.TresPerNode != "" {
		req, err := findGRESRequest(*job.TresPerNode, gresName)
		if err != nil {
			return nil, err
		}
		if req != nil {
			return &resolvedGRES{name: gresName, count: req.count, resourceType: req.resourceType}, nil
		}
	}

	if job.TresPerTask != nil && *job.TresPerTask != "" {
		req, err := findGRESRequest(*job.TresPerTask, gresName)
		if err != nil {
			return nil, err
		}
		if req != nil {
			tasksPerNode := resolveTasksPerNode(job, replicas)
			return &resolvedGRES{name: gresName, count: req.count * tasksPerNode, resourceType: req.resourceType}, nil
		}
	}

	if job.TresPerJob != nil && *job.TresPerJob != "" {
		req, err := findGRESRequest(*job.TresPerJob, gresName)
		if err != nil {
			return nil, err
		}
		if req != nil {
			if req.count%int64(replicas) != 0 {
				return nil, fmt.Errorf("TresPerJob %s count (%d) is not cleanly divisible by NodeCount (%d)", gresName, req.count, replicas)
			}
			return &resolvedGRES{name: gresName, count: req.count / int64(replicas), resourceType: req.resourceType}, nil
		}
	}

	return nil, nil
}

func findGRESMapping(resName string, resType string, wmSpec *k8sbridgev1alpha1.WorkloadMixingSpec) *k8sbridgev1alpha1.GenericResourceMapping {
	for _, m := range wmSpec.MappedResources {
		if m.ResourceName == resName && m.ResourceType == resType {
			return &m
		}
	}
	return nil
}

func resolvePodResources(job *slurmclienttypes.V0044JobInfo, replicas int32, wmSpec *k8sbridgev1alpha1.WorkloadMixingSpec) (corev1.ResourceRequirements, *GresResolutionInfo, error) {
	if replicas <= 0 {
		return corev1.ResourceRequirements{}, nil, fmt.Errorf("invalid replicas count: %d", replicas)
	}
	tasksPerNode := resolveTasksPerNode(job, replicas)

	// Resolve GPUs
	resolvedGPU, err := resolveGRES(job, replicas, GRESGPU)
	if err != nil {
		return corev1.ResourceRequirements{}, nil, err
	}

	var gpuRequest int64
	var k8sGpuResourceName string
	var nodeSelectors map[string]string

	if resolvedGPU != nil {
		gpuRequest = resolvedGPU.count
		mapping := findGRESMapping(GRESGPU, resolvedGPU.resourceType, wmSpec)
		if mapping == nil {
			return corev1.ResourceRequirements{}, nil, fmt.Errorf("missing GRES mapping for gpu:%s", resolvedGPU.resourceType)
		}
		k8sGpuResourceName = mapping.K8sResourceName
		// Copy the map so that the JobSet does not share it with the WorkloadMixing spec.
		nodeSelectors = maps.Clone(mapping.NodeSelectors)
	}

	cpu := resolveCPUsPerPod(job, tasksPerNode, resolvedGPU)

	// Resolve Memory per pod
	var memCandidates []int64
	if job.MemoryPerNode != nil && ptr.Deref(job.MemoryPerNode.Set, false) {
		memCandidates = append(memCandidates, ptr.Deref(job.MemoryPerNode.Number, 0))
	}
	if job.MemoryPerCpu != nil && ptr.Deref(job.MemoryPerCpu.Set, false) {
		memCandidates = append(memCandidates, ptr.Deref(job.MemoryPerCpu.Number, 0)*cpu)
	}
	if job.MemoryPerTres != nil && resolvedGPU != nil {
		if gpuMem, ok := lookupTres(ptr.Deref(job.MemoryPerTres, ""), GRESGPU, resolvedGPU.resourceType); ok {
			memCandidates = append(memCandidates, gpuMem*gpuRequest)
		}
	}
	// Fallback memory
	memCandidates = append(memCandidates, defaultMemoryPerCPUMB*cpu)

	var maxMemMB int64
	for _, mem := range memCandidates {
		if mem > maxMemMB {
			maxMemMB = mem
		}
	}

	cpuQuantity, err := resource.ParseQuantity(strconv.FormatInt(cpu, 10))
	if err != nil {
		return corev1.ResourceRequirements{}, nil, fmt.Errorf("failed to parse CPU quantity: %w", err)
	}
	memQuantity, err := resource.ParseQuantity(fmt.Sprintf("%dMi", maxMemMB))
	if err != nil {
		return corev1.ResourceRequirements{}, nil, fmt.Errorf("failed to parse Memory quantity: %w", err)
	}

	reqs := corev1.ResourceList{
		corev1.ResourceCPU:    cpuQuantity,
		corev1.ResourceMemory: memQuantity,
	}
	var limits corev1.ResourceList

	if gpuRequest > 0 {
		gpuQuantity, err := resource.ParseQuantity(strconv.FormatInt(gpuRequest, 10))
		if err != nil {
			return corev1.ResourceRequirements{}, nil, fmt.Errorf("failed to parse GPU quantity: %w", err)
		}
		reqs[corev1.ResourceName(k8sGpuResourceName)] = gpuQuantity
		limits = corev1.ResourceList{
			corev1.ResourceName(k8sGpuResourceName): gpuQuantity,
		}
	}

	var gresList []resolvedGRES
	if resolvedGPU != nil {
		gresList = append(gresList, *resolvedGPU)
	}

	gresInfo := &GresResolutionInfo{
		NodeSelectors: nodeSelectors,
		ResolvedGres:  gresList,
	}

	return corev1.ResourceRequirements{
		Requests: reqs,
		Limits:   limits,
	}, gresInfo, nil
}

func resolveConfServer(controller *slinkyv1beta1.Controller) (string, error) {
	if controller.Spec.External {
		if controller.Spec.ExternalConfig.Host == "" || controller.Spec.ExternalConfig.Port == 0 {
			return "", errors.New("external controller is missing Host or Port in ExternalConfig")
		}
		return net.JoinHostPort(controller.Spec.ExternalConfig.Host, strconv.Itoa(int(controller.Spec.ExternalConfig.Port))), nil //nolint:unconvert
	}
	host := controller.ServiceFQDNShort()
	if host == "" {
		return "", errors.New("failed to determine controller service FQDN")
	}
	port := controller.Spec.Service.Port
	if port == 0 {
		port = SlurmctldDefaultPort
	}
	return net.JoinHostPort(host, strconv.Itoa(int(port))), nil //nolint:unconvert
}

func injectSlurmVolumes(podSpec *corev1.PodSpec, controller *slinkyv1beta1.Controller) error {
	if podSpec == nil {
		return errors.New("podSpec is nil")
	}

	keyRef := controller.Spec.SlurmKeyRef
	if keyRef.Name == "" || keyRef.Key == "" {
		return errors.New("controller slurmKeyRef name or key is empty")
	}

	// 1. Inject Volume into PodSpec.Volumes if not present
	hasVolume := false
	for _, v := range podSpec.Volumes {
		if v.Name == SlurmConfigVolume {
			hasVolume = true
			break
		}
	}
	if !hasVolume {
		vol := corev1.Volume{
			Name: SlurmConfigVolume,
			VolumeSource: corev1.VolumeSource{
				Projected: &corev1.ProjectedVolumeSource{
					DefaultMode: ptr.To(int32(SlurmKeyMode)),
					Sources: []corev1.VolumeProjection{
						{
							Secret: &corev1.SecretProjection{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: keyRef.Name,
								},
								Items: []corev1.KeyToPath{
									{
										Key:  keyRef.Key,
										Path: SlurmKeyFile,
									},
								},
							},
						},
					},
				},
			},
		}
		podSpec.Volumes = append(podSpec.Volumes, vol)
	}

	// 2. Inject VolumeMount into slurmd container if not present
	for i := range podSpec.Containers {
		c := &podSpec.Containers[i]
		if c.Name == ContainerSlurmd {
			for _, vm := range c.VolumeMounts {
				if vm.MountPath == SlurmConfigDir || vm.Name == SlurmConfigVolume {
					return nil
				}
			}
			c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
				Name:      SlurmConfigVolume,
				MountPath: SlurmConfigDir,
				ReadOnly:  true,
			})
			return nil
		}
	}

	return fmt.Errorf("container %q not found in PodSpec", ContainerSlurmd)
}

var dynamicNodeFeatureRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

const UniqueFeaturePrefix = "k8s-id-"

func appendSlurmdArgs(podSpec *corev1.PodSpec, jobIDStr string, confServer string, gresList []resolvedGRES, dynamicNodeFeature string) error {
	for i := range podSpec.Containers {
		c := &podSpec.Containers[i]
		if c.Name == ContainerSlurmd {
			hasConfServer := false
			for _, arg := range c.Args {
				if arg == argConfServer || strings.HasPrefix(arg, argConfServer+"=") {
					hasConfServer = true
					break
				}
			}

			var newArgs []string
			if !hasConfServer && confServer != "" {
				newArgs = append(newArgs, argConfServer, confServer)
			}

			newArgs = append(newArgs, argNodeName, valNodeName)

			deterministicNodeEnvs := []corev1.EnvVar{
				{
					Name: envJobSetName,
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: fieldPathJobSetName,
						},
					},
				},
				{
					Name: envReplicatedJobName,
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: fieldPathReplicatedJobName,
						},
					},
				},
				{
					Name: envJobIndex,
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: fieldPathJobIndex,
						},
					},
				},
				{
					Name: envPodIndex,
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: fieldPathJobCompletionIndex,
						},
					},
				},
				{
					Name:  envSlurmNodeName,
					Value: fmt.Sprintf("$(%s)-$(%s)-$(%s)-$(%s)", envJobSetName, envReplicatedJobName, envJobIndex, envPodIndex),
				},
			}
			managedEnvNames := map[string]bool{
				envJobSetName:        true,
				envReplicatedJobName: true,
				envJobIndex:          true,
				envPodIndex:          true,
				envSlurmNodeName:     true,
			}
			filteredEnvs := make([]corev1.EnvVar, 0, len(c.Env)+len(deterministicNodeEnvs))
			for _, env := range c.Env {
				if !managedEnvNames[env.Name] {
					filteredEnvs = append(filteredEnvs, env)
				}
			}
			c.Env = append(filteredEnvs, deterministicNodeEnvs...)

			var confParts []string
			var features []string
			if jobIDStr != "" {
				features = append(features, fmt.Sprintf("%s%s", UniqueFeaturePrefix, jobIDStr))
			}
			if dynamicNodeFeature != "" {
				if !dynamicNodeFeatureRegex.MatchString(dynamicNodeFeature) {
					return fmt.Errorf("invalid dynamicNodeFeature %q: must match pattern ^[a-zA-Z0-9_-]+$", dynamicNodeFeature)
				}
				features = append(features, dynamicNodeFeature)
			}
			if len(features) > 0 {
				confParts = append(confParts, fmt.Sprintf("Features=%s", strings.Join(features, ",")))
			}
			if len(gresList) > 0 {
				var gresSpecs []string
				for _, gres := range gresList {
					if gres.count > 0 {
						if gres.resourceType != "" {
							gresSpecs = append(gresSpecs, fmt.Sprintf("%s:%s:%d", gres.name, gres.resourceType, gres.count))
						} else {
							gresSpecs = append(gresSpecs, fmt.Sprintf("%s:%d", gres.name, gres.count))
						}
					}
				}
				if len(gresSpecs) > 0 {
					confParts = append(confParts, fmt.Sprintf("Gres=%s", strings.Join(gresSpecs, ",")))
				}
			}
			newArgs = append(newArgs, "-Z", "--conf", fmt.Sprintf("'%s'", strings.Join(confParts, " ")))
			c.Args = append(c.Args, newArgs...)
			return nil
		}
	}
	return fmt.Errorf("container %q not found in PodSpec", ContainerSlurmd)
}
