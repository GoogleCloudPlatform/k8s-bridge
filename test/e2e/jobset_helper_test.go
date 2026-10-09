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
	"strings"
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

func setLocalQueueStopPolicy(queueName, stopPolicy, wantActiveStatus string) {
	GinkgoHelper()

	patch := fmt.Sprintf(`{"spec":{"stopPolicy":%q}}`, stopPolicy)
	cmd := exec.Command(
		"kubectl", "patch", "localqueue", queueName,
		"-n", slurmNamespace, "--type=merge", "-p", patch,
	)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to set stopPolicy=%s on LocalQueue %s", stopPolicy, queueName)

	Eventually(func(g Gomega) {
		getCmd := exec.Command(
			"kubectl", "get", "localqueue", queueName,
			"-n", slurmNamespace, "-o", "jsonpath={.status.conditions[?(@.type=='Active')].status}",
		)
		out, err := utils.Run(getCmd)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(strings.TrimSpace(out)).To(Equal(wantActiveStatus))
	}, 30*time.Second, 500*time.Millisecond).Should(Succeed())
}

func holdLocalQueueWithCleanup(queueName string) {
	GinkgoHelper()

	DeferCleanup(func() {
		delCmd := exec.Command(
			"kubectl", "delete", "jobsets", "--all",
			"-n", slurmNamespace, "--ignore-not-found=true", "--wait=true",
		)
		_, _ = utils.Run(delCmd)
		setLocalQueueStopPolicy(queueName, "None", "True")
	})

	setLocalQueueStopPolicy(queueName, "Hold", "False")
}

func waitForJobSetUID(jobSetName string) string {
	GinkgoHelper()
	var uid string
	Eventually(func(g Gomega) {
		var err error
		uid, err = utils.JobSetUID(jobSetName, slurmNamespace)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(uid).NotTo(BeEmpty())
	}, jobsetTimeout, time.Second).Should(Succeed())
	return uid
}

func deleteJobSetForSlurmJob(jobSetName string) {
	GinkgoHelper()
	cmd := exec.Command("kubectl", "delete", "jobset", jobSetName, "-n", slurmNamespace, "--wait=true")
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to delete JobSet "+jobSetName)
}

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

		// Verify Slurmd container args contain --conf-server, -N $(SLURM_NODE_NAME), and --conf with unique Features
		g.Expect(slurmdContainer.Args).To(HaveLen(7))
		g.Expect(slurmdContainer.Args[0]).To(Equal("--conf-server"))
		g.Expect(slurmdContainer.Args[1]).To(Equal("slurm-controller.slurm:6817"))
		g.Expect(slurmdContainer.Args[2]).To(Equal("-N"))
		g.Expect(slurmdContainer.Args[3]).To(Equal("$(SLURM_NODE_NAME)"))
		g.Expect(slurmdContainer.Args[4]).To(Equal("-Z"))
		g.Expect(slurmdContainer.Args[5]).To(Equal("--conf"))
		g.Expect(slurmdContainer.Args[6]).To(MatchRegexp(`^'Features=k8s-id-[0-9a-f-]+,dynamic_kpartition'$`))

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
	_ = waitForJobSetUID(jobSetName)

	return jobID, jobSetName
}
