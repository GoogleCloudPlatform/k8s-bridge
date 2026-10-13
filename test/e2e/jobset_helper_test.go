//go:build e2e

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

package e2e

import (
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

const (
	jobsetTimeout = 3 * time.Minute

	// jobSetRecreationWindow is how long the "JobSet goes missing" spec keeps
	// watching for a re-created JobSet after the Slurm job has been cancelled.
	jobSetRecreationWindow = 15 * time.Second
)

// verifySlurmJobCancelledWithoutJobSetRecreation waits for the Slurm job to be
// CANCELLED with the "JobSet missing" comment while asserting, on every poll,
// that no JobSet named jobSetName other than originalUID exists. If the
// controller re-creates a JobSet an operator deleted, the workload is resurrected
// instead of cancelled; that is reported immediately with a clear message rather
// than as a timeout on RUNNING != CANCELLED. The watch continues for a short
// window after the cancellation to catch a late re-creation.
func verifySlurmJobCancelledWithoutJobSetRecreation(jobID, jobSetName, originalUID string) {
	GinkgoHelper()

	jobSetNotRecreated := func(g Gomega) {
		utils.ExpectJobSetNotRecreated(g, jobSetName, slurmNamespace, originalUID)
	}

	By("verifying the job is cancelled in Slurm with the expected comment and the JobSet is not re-created")
	utils.NewSlurmJobVerifier(slurmNamespace, jobID).
		WithState("CANCELLED").
		WithComment("JobSet missing").
		WithInvariant(jobSetNotRecreated).
		Verify()

	By("verifying the JobSet stays absent after the cancellation")
	Consistently(jobSetNotRecreated, jobSetRecreationWindow, time.Second).Should(Succeed())
}

// verifyJobSetCreatedWithCustomTemplate asserts that the translated JobSet exists
// and has the merged custom template specs. jobNameLabel is the expected
// (sanitized) value of the k8s-bridge.x-k8s.io/slurm-job-name label.
func verifyJobSetCreatedWithCustomTemplate(jobSetName, jobID, jobNameLabel string) {
	GinkgoHelper()

	By("verifying the JobSet is created with the custom template merged")
	verifyJobSetCreated := func(g Gomega) {
		js, err := utils.JobSet(jobSetName, slurmNamespace)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to get JobSet")

		// Verify Kueue queue label is configured
		g.Expect(js.Labels).To(HaveKeyWithValue("kueue.x-k8s.io/queue-name", "test-local-queue"))

		// Verify Slurm --partition and --job-name are carried over as labels
		g.Expect(js.Labels).To(HaveKeyWithValue("k8s-bridge.x-k8s.io/slurm-partition", defaultSlurmPartition))
		g.Expect(js.Labels).To(HaveKeyWithValue("k8s-bridge.x-k8s.io/slurm-job-name", jobNameLabel))

		// Verify ReplicatedJobs
		g.Expect(js.Spec.ReplicatedJobs).To(HaveLen(1))
		repJob := js.Spec.ReplicatedJobs[0]
		g.Expect(repJob.Name).To(Equal("slurmd"))

		// Verify PodTemplate metadata
		podTemplate := repJob.Template.Spec.Template
		g.Expect(podTemplate.Labels).To(HaveKeyWithValue("custom-label", "true"))
		g.Expect(podTemplate.Labels).To(HaveKeyWithValue("k8s-bridge.x-k8s.io/slurm-job-id", jobID))
		g.Expect(podTemplate.Labels).To(HaveKeyWithValue("k8s-bridge.x-k8s.io/slurm-job-name", jobNameLabel))

		// Verify Containers
		containers := podTemplate.Spec.Containers
		// We expect 2 containers: "sidecar" (from template) and "slurmd" (injected/merged)
		g.Expect(containers).To(HaveLen(2))

		var sidecarContainer, slurmdContainer *corev1.Container
		for i := range containers {
			switch containers[i].Name {
			case "sidecar":
				sidecarContainer = &containers[i]
			case "slurmd":
				slurmdContainer = &containers[i]
			}
		}

		g.Expect(sidecarContainer).NotTo(BeNil(), "Sidecar container not found")
		g.Expect(sidecarContainer.Image).To(Equal(utils.CurlImage))

		g.Expect(slurmdContainer).NotTo(BeNil(), "Slurmd container not found")
		g.Expect(slurmdContainer.Resources.Requests.Cpu().String()).To(Equal("2"))
		g.Expect(slurmdContainer.Resources.Requests.Memory().Cmp(resource.MustParse("4096Mi"))).To(Equal(0))

		// Verify Slurmd container args contain --conf-server, -N $(SLURM_NODE_NAME), and --conf with the
		// pod's resources (-c 2 --mem=4096 => CPUs=2 RealMemory=4096) and unique Features
		g.Expect(slurmdContainer.Args).To(HaveLen(7))
		g.Expect(slurmdContainer.Args[0]).To(Equal("--conf-server"))
		g.Expect(slurmdContainer.Args[1]).To(Equal("slurm-controller.slurm:6817"))
		g.Expect(slurmdContainer.Args[2]).To(Equal("-N"))
		g.Expect(slurmdContainer.Args[3]).To(Equal("$(SLURM_NODE_NAME)"))
		g.Expect(slurmdContainer.Args[4]).To(Equal("-Z"))
		g.Expect(slurmdContainer.Args[5]).To(Equal("--conf"))
		g.Expect(slurmdContainer.Args[6]).To(MatchRegexp(`^'CPUs=2 RealMemory=4096 Features=k8s-id-[0-9a-f-]+,dynamic_kpartition'$`))

		// Verify Slurmd container has Downward API env vars for deterministic node naming
		var slurmNodeNameEnv, jobSetNameEnv, replicatedJobNameEnv, jobIndexEnv, podIndexEnv *corev1.EnvVar
		for i := range slurmdContainer.Env {
			switch slurmdContainer.Env[i].Name {
			case "SLURM_NODE_NAME":
				slurmNodeNameEnv = &slurmdContainer.Env[i]
			case "JOBSET_NAME":
				jobSetNameEnv = &slurmdContainer.Env[i]
			case "REPLICATED_JOB_NAME":
				replicatedJobNameEnv = &slurmdContainer.Env[i]
			case "JOB_INDEX":
				jobIndexEnv = &slurmdContainer.Env[i]
			case "POD_INDEX":
				podIndexEnv = &slurmdContainer.Env[i]
			}
		}
		g.Expect(slurmNodeNameEnv).NotTo(BeNil(), "Expected SLURM_NODE_NAME env var on slurmd container")
		g.Expect(slurmNodeNameEnv.Value).To(Equal("$(JOBSET_NAME)-$(REPLICATED_JOB_NAME)-$(JOB_INDEX)-$(POD_INDEX)"))
		g.Expect(jobSetNameEnv).NotTo(BeNil(), "Expected JOBSET_NAME env var on slurmd container")
		g.Expect(jobSetNameEnv.ValueFrom).NotTo(BeNil())
		g.Expect(jobSetNameEnv.ValueFrom.FieldRef).NotTo(BeNil())
		g.Expect(jobSetNameEnv.ValueFrom.FieldRef.FieldPath).To(Equal("metadata.labels['jobset.sigs.k8s.io/jobset-name']"))
		g.Expect(replicatedJobNameEnv).NotTo(BeNil(), "Expected REPLICATED_JOB_NAME env var on slurmd container")
		g.Expect(replicatedJobNameEnv.ValueFrom).NotTo(BeNil())
		g.Expect(replicatedJobNameEnv.ValueFrom.FieldRef).NotTo(BeNil())
		g.Expect(replicatedJobNameEnv.ValueFrom.FieldRef.FieldPath).To(
			Equal("metadata.labels['jobset.sigs.k8s.io/replicatedjob-name']"),
		)
		g.Expect(jobIndexEnv).NotTo(BeNil(), "Expected JOB_INDEX env var on slurmd container")
		g.Expect(jobIndexEnv.ValueFrom).NotTo(BeNil())
		g.Expect(jobIndexEnv.ValueFrom.FieldRef).NotTo(BeNil())
		g.Expect(jobIndexEnv.ValueFrom.FieldRef.FieldPath).To(Equal("metadata.labels['jobset.sigs.k8s.io/job-index']"))
		g.Expect(podIndexEnv).NotTo(BeNil(), "Expected POD_INDEX env var on slurmd container")
		g.Expect(podIndexEnv.ValueFrom).NotTo(BeNil())
		g.Expect(podIndexEnv.ValueFrom.FieldRef).NotTo(BeNil())
		g.Expect(podIndexEnv.ValueFrom.FieldRef.FieldPath).To(
			Equal("metadata.labels['batch.kubernetes.io/job-completion-index']"),
		)

		// Verify Slurmd volume mount for /etc/slurm
		var slurmEtcMount *corev1.VolumeMount
		for i := range slurmdContainer.VolumeMounts {
			if slurmdContainer.VolumeMounts[i].Name == "slurm-etc" {
				slurmEtcMount = &slurmdContainer.VolumeMounts[i]
				break
			}
		}
		g.Expect(slurmEtcMount).NotTo(BeNil(), "Volume mount slurm-etc not found on slurmd container")
		g.Expect(slurmEtcMount.MountPath).To(Equal("/etc/slurm"))
		g.Expect(slurmEtcMount.ReadOnly).To(BeTrue())

		// Verify Volumes (slurm-etc)
		var slurmEtcVol *corev1.Volume
		for i := range podTemplate.Spec.Volumes {
			if podTemplate.Spec.Volumes[i].Name == "slurm-etc" {
				slurmEtcVol = &podTemplate.Spec.Volumes[i]
				break
			}
		}
		g.Expect(slurmEtcVol).NotTo(BeNil(), "Volume slurm-etc not found in PodSpec")
		g.Expect(slurmEtcVol.Projected).NotTo(BeNil(), "Expected projected volume source")
		g.Expect(slurmEtcVol.Projected.Sources).To(HaveLen(1))
		g.Expect(slurmEtcVol.Projected.Sources[0].Secret).NotTo(BeNil())
		g.Expect(slurmEtcVol.Projected.Sources[0].Secret.Name).To(Equal("slurm-auth-slurm"))
		g.Expect(slurmEtcVol.Projected.Sources[0].Secret.Items).To(ConsistOf(corev1.KeyToPath{
			Key:  "slurm.key",
			Path: "slurm.key",
		}))
		g.Expect(slurmEtcVol.Projected.DefaultMode).NotTo(BeNil())
		g.Expect(*slurmEtcVol.Projected.DefaultMode).To(Equal(int32(0o600)))

		// Verify PodFailurePolicy for slurmd
		g.Expect(repJob.Template.Spec.PodFailurePolicy).NotTo(BeNil())
		g.Expect(repJob.Template.Spec.PodFailurePolicy.Rules).To(HaveLen(1))
		rule := repJob.Template.Spec.PodFailurePolicy.Rules[0]
		g.Expect(rule.Action).To(Equal(batchv1.PodFailurePolicyActionFailJob))
		g.Expect(rule.OnExitCodes).NotTo(BeNil())
		g.Expect(rule.OnExitCodes.ContainerName).NotTo(BeNil())
		g.Expect(*rule.OnExitCodes.ContainerName).To(Equal("slurmd"))
		g.Expect(rule.OnExitCodes.Operator).To(Equal(batchv1.PodFailurePolicyOnExitCodesOpNotIn))
		g.Expect(rule.OnExitCodes.Values).To(Equal([]int32{0}))

		// Verify injected startupProbe and livenessProbe on slurmd container
		const expectedProbeCmd = `printf 'GET /livez HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n' | ` +
			`socat -T 2 - TCP:127.0.0.1:6818 2>/dev/null | grep -q '^HTTP/1\.[01] 20[04]' && ` +
			`(scontrol listpids 2>/dev/null | awk 'NR>1 {found=1; exit} END {exit !found}' || ` +
			`scontrol -o show node "$SLURM_NODE_NAME" 2>/dev/null | ` +
			`grep -Eq 'State=[^ ]*(ALLOCATED|MIXED|COMPLETING)')`
		g.Expect(slurmdContainer.StartupProbe).NotTo(BeNil(), "Expected startupProbe on slurmd container")
		g.Expect(slurmdContainer.StartupProbe.Exec).NotTo(BeNil())
		g.Expect(slurmdContainer.StartupProbe.Exec.Command).To(Equal([]string{"sh", "-c", expectedProbeCmd}))
		g.Expect(slurmdContainer.StartupProbe.PeriodSeconds).To(Equal(int32(10)))
		g.Expect(slurmdContainer.StartupProbe.TimeoutSeconds).To(Equal(int32(5)))
		g.Expect(slurmdContainer.StartupProbe.FailureThreshold).To(Equal(int32(30)))

		g.Expect(slurmdContainer.LivenessProbe).NotTo(BeNil(), "Expected livenessProbe on slurmd container")
		g.Expect(slurmdContainer.LivenessProbe.Exec).NotTo(BeNil())
		g.Expect(slurmdContainer.LivenessProbe.Exec.Command).To(Equal([]string{"sh", "-c", expectedProbeCmd}))
		g.Expect(slurmdContainer.LivenessProbe.PeriodSeconds).To(Equal(int32(10)))
		g.Expect(slurmdContainer.LivenessProbe.TimeoutSeconds).To(Equal(int32(5)))
		g.Expect(slurmdContainer.LivenessProbe.FailureThreshold).To(Equal(int32(6)))

		// Verify user-supplied postStart hook is preserved alongside injected preStop hook
		g.Expect(slurmdContainer.Lifecycle).NotTo(BeNil(), "Expected Lifecycle to be set on slurmd container")
		g.Expect(slurmdContainer.Lifecycle.PostStart).NotTo(
			BeNil(), "Expected postStart user provisioning hook to be preserved on slurmd container",
		)
		g.Expect(slurmdContainer.Lifecycle.PreStop).NotTo(
			BeNil(), "Expected preStop hook to be injected on slurmd container",
		)
	}
	Eventually(verifyJobSetCreated, jobsetTimeout, time.Second).Should(Succeed())

	By("verifying the Slurm job received the unique k8s-id- constraint")
	verifySlurmJobConstraint := func(g Gomega) {
		scontrolOutput, err := utils.RunOnLoginNodeAsUser(
			slurmNamespace, utils.DefaultSlurmTestUser, "scontrol show job "+jobID,
		)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to run scontrol show job")
		g.Expect(scontrolOutput).To(MatchRegexp(fmt.Sprintf(`Features=.*k8s-id-%s`, jobID)))
	}
	Eventually(verifySlurmJobConstraint, jobsetTimeout, time.Second).Should(Succeed())
}

// patchJobSetCondition artificially merges a status condition into a JobSet's status subresource.
func patchJobSetCondition(jobSetName, ns, condType, status, reason, message string) {
	GinkgoHelper()
	utils.PatchJobSetCondition(jobSetName, ns, condType, status, reason, message)
}

// setupJobSetTerminalStateTest applies the template config, submits a batch job,
// waits for JobSet creation, and returns the jobID and jobSetName.
func setupJobSetTerminalStateTest() (string, string) {
	GinkgoHelper()
	const configPath = "test/e2e/testdata/workloadmixing_template.yaml"

	By("applying the WorkloadMixing template CR")
	utils.ApplyManifestWithCleanup(configPath)

	By("submitting a batch job")
	jobID := utils.NewBatchJobBuilder(slurmNamespace).
		WithPartition(defaultSlurmPartition).
		WithWrap("sleep 300").
		Submit()
	jobSetName := fmt.Sprintf("slurm-job-%s", jobID)

	By("waiting for JobSet to be created")
	Eventually(func(g Gomega) {
		cmd := exec.Command("kubectl", "get", "jobset", jobSetName, "-n", slurmNamespace)
		_, err := utils.Run(cmd)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to find JobSet")
	}, jobsetTimeout, time.Second).Should(Succeed())

	return jobID, jobSetName
}

// verifySlurmdResourceConf asserts that the translated JobSet requests
// expectedCPUs CPUs and expectedMemMB MiB on the slurmd container and forwards
// exactly those values to slurmd through `-Z --conf CPUs=… RealMemory=…`, so the
// dynamic node registers with the pod's resources rather than with the hardware
// of the Kubernetes node it shares a kernel with.
func verifySlurmdResourceConf(jobSetName, expectedCPUs, expectedMemMB string) {
	GinkgoHelper()

	By(fmt.Sprintf("verifying the JobSet slurmd container requests %s CPUs / %s MiB and passes them to slurmd -Z --conf",
		expectedCPUs, expectedMemMB))
	Eventually(func(g Gomega) {
		js, err := utils.JobSet(jobSetName, slurmNamespace)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to get JobSet")
		g.Expect(js.Spec.ReplicatedJobs).To(HaveLen(1))

		var slurmdContainer *corev1.Container
		containers := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Spec.Containers
		for i := range containers {
			if containers[i].Name == "slurmd" {
				slurmdContainer = &containers[i]
				break
			}
		}
		g.Expect(slurmdContainer).NotTo(BeNil(), "Slurmd container not found")
		g.Expect(slurmdContainer.Resources.Requests.Cpu().String()).To(Equal(expectedCPUs))
		g.Expect(slurmdContainer.Resources.Requests.Memory().Cmp(resource.MustParse(expectedMemMB+"Mi"))).To(Equal(0))

		conf := slurmdConfArg(slurmdContainer.Args)
		g.Expect(conf).NotTo(BeEmpty(), fmt.Sprintf("slurmd container has no --conf argument: %v", slurmdContainer.Args))
		g.Expect(conf).To(MatchRegexp(`\bCPUs=%s\b`, expectedCPUs),
			"slurmd --conf does not register the pod's CPU request")
		g.Expect(conf).To(MatchRegexp(`\bRealMemory=%s\b`, expectedMemMB),
			"slurmd --conf does not register the pod's memory request")
	}, jobsetTimeout, time.Second).Should(Succeed())
}

// slurmdConfArg returns the value passed to slurmd's --conf flag, or "" if absent.
func slurmdConfArg(args []string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "--conf" {
			return args[i+1]
		}
	}
	return ""
}

// verifyTwoNodeTaskLayout submits `sbatch -N 2 --ntasks=4 --cpus-per-task=1`
// plus extraSbatchArgs and verifies that each of the two worker pods requests,
// and registers its Slurm node with, the CPUs and memory of 2 tasks, and that
// slurmctld runs 2 of the 4 tasks on each node.
//
// 1 CPU per task keeps the footprint at 2 pods x 2 CPUs, which fits on the
// Kind node of CI.
func verifyTwoNodeTaskLayout(extraSbatchArgs ...string) {
	GinkgoHelper()

	const (
		configPath = "test/e2e/testdata/workloadmixing_template.yaml"
		// 2 tasks x 1 CPU per pod and, with no --mem given, 2 x 2048 MiB.
		expectedCPUs  = "2"
		expectedMemMB = "4096"
		doneMarker    = "E2E_TOPOLOGY_DONE"
	)

	By("applying the WorkloadMixing template CR")
	utils.ApplyManifestWithCleanup(configPath)

	By(fmt.Sprintf("submitting a 2-node, 4-task batch job with extra sbatch args %q", extraSbatchArgs))
	jobID := utils.NewBatchJobBuilder(slurmNamespace).
		WithPartition(defaultSlurmPartition).
		WithNodes(2).
		WithNtasks(4).
		WithCpusPerTask(1).
		// Sleep after the marker so the batch host pod, the only place the
		// output file exists, stays alive while the output is read.
		WithWrap(fmt.Sprintf("srun hostname; echo %s; sleep 300", doneMarker)).
		WithArgs(extraSbatchArgs...).
		Submit()
	jobSetName := fmt.Sprintf("slurm-job-%s", jobID)
	nodeName0 := fmt.Sprintf("%s-slurmd-0-0", jobSetName)
	nodeName1 := fmt.Sprintf("%s-slurmd-1-0", jobSetName)

	verifySlurmdResourceConf(jobSetName, expectedCPUs, expectedMemMB)

	By("verifying both worker pods are scheduled and running")
	utils.WaitForWorkerPodsRunning(slurmNamespace, jobSetName, 2, 3*time.Minute)

	By("verifying both dynamic nodes are allocated to the running job in Slurmctld")
	utils.NewSlurmJobVerifier(slurmNamespace, jobID).
		WithState("RUNNING").
		WithNodeList(fmt.Sprintf("%s,%s", nodeName0, nodeName1)).
		WithTimeout(3 * time.Minute).
		Verify()

	By("verifying both nodes registered with the pod's CPUs and memory, not the host's")
	for _, nodeName := range []string{nodeName0, nodeName1} {
		utils.VerifySlurmNodeResources(slurmNamespace, nodeName, expectedCPUs, expectedMemMB, 2*time.Minute)
	}

	By("verifying slurmctld ran 2 of the 4 tasks on each node")
	output := utils.WaitForBatchJobOutput(slurmNamespace, jobID, doneMarker, 2*time.Minute)
	Expect(utils.CountStepTasksPerNode(output, nodeName0, nodeName1)).To(Equal(map[string]int{
		nodeName0: 2,
		nodeName1: 2,
	}), "unexpected task layout, job output:\n"+output)
}
