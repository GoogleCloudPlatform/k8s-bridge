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

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

// namespace where the project is deployed in
const namespace = "k8s-bridge-system"

// serviceAccountName created for the project
const serviceAccountName = "k8s-bridge-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "k8s-bridge-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "k8s-bridge-metrics-binding"

var _ = Describe("Manager", func() {
	BeforeEach(func() {
		utils.ResetSlurmAndK8sState(slurmNamespace)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			utils.DumpFailureDiagnostics(controllerPodName, namespace, slurmNamespace)
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Slurm Control Plane", func() {
		It("should verify Slurm control plane is ready", func() {
			By("checking that slurm-controller statefulset is ready")
			cmd := exec.Command("kubectl", "get", "statefulset", "slurm-controller", "-n", "slurm",
				"-o", "jsonpath={.status.readyReplicas}")
			output, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())
			Expect(output).To(Equal("1"), "slurm-controller not ready")
		})

		It("should place job in HELD state when submitted to single workloadmixing partition", func() {
			By("submitting a job targeting workloadmixing-partition from the login pod as " + utils.DefaultSlurmTestUser)
			jobID := utils.NewBatchJobBuilder(slurmNamespace).
				WithPartition(defaultSlurmPartition).
				WithWrap("sleep 10").
				Submit()

			By("verifying the job state is HELD (Reason=JobHeldUser)")
			utils.VerifySlurmJobReason(slurmNamespace, jobID, "JobHeldUser")
		})

		It("should reject job submission targeting multiple partitions when one is workloadmixing", func() {
			By("submitting a job targeting workloadmixing-partition,workloadmixing-debug-partition")
			utils.NewBatchJobBuilder(slurmNamespace).
				WithPartition("workloadmixing-partition,workloadmixing-debug-partition").
				WithWrap("sleep 10").
				SubmitExpectError("Batch job submission failed")
		})
	})

	Context("Slurm Job Translation and Lifecycle", func() {
		It("should translate Slurm job to JobSet with custom template", func() {
			const configPath = "test/e2e/testdata/workloadmixing_template.yaml"

			By("applying the WorkloadMixing template CR")
			utils.ApplyManifestWithCleanup(configPath)

			By("submitting a batch job to Slurm with an explicit --job-name")
			jobID := utils.NewBatchJobBuilder(slurmNamespace).
				WithPartition(defaultSlurmPartition).
				WithCpusPerTask(2).
				WithMem(4096).
				WithWrap("sleep 300").
				WithArgs("--job-name=E2E_Custom-Template.Job!").
				Submit()
			jobSetName := fmt.Sprintf("slurm-job-%s", jobID)

			// The job name is lower-cased, invalid label characters are replaced
			// with "-" and leading/trailing separators are trimmed.
			verifyJobSetCreatedWithCustomTemplate(jobSetName, jobID, "e2e_custom-template.job")
		})

		It("should cancel Slurm job if translation fails due to unmapped GRES", func() {
			const configPath = "test/e2e/testdata/workloadmixing_template.yaml"

			By("applying the WorkloadMixing template CR")
			utils.ApplyManifestWithCleanup(configPath)

			By("submitting a batch job to Slurm requesting unmapped GRES (GPU)")
			jobID := utils.NewBatchJobBuilder(slurmNamespace).
				WithPartition(defaultSlurmPartition).
				WithGres("gpu:l4:1").
				WithWrap("sleep 300").
				Submit()
			jobSetName := fmt.Sprintf("slurm-job-%s", jobID)

			By("verifying the job is cancelled in Slurm with the expected comment")
			utils.VerifySlurmJobCancelled(slurmNamespace, jobID,
				"Translation failed: missing GRES mapping for gpu:l4")

			By("verifying no JobSet was created")
			utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
		})

		It("k8s-bridge core should cancel Slurm job if it requests multiple partitions", func() {
			const configPath = "test/e2e/testdata/workloadmixing_multi_unmanaged.yaml"

			By("applying the WorkloadMixing CR mapping partitions not managed by job_submit.lua")
			utils.ApplyManifestWithCleanup(configPath)

			By("submitting a batch job requesting multiple partitions that bypass job_submit.lua rejection")
			jobID := utils.NewBatchJobBuilder(slurmNamespace).
				WithHold().
				WithPartition("workloadmixing-other-partition,workloadmixing-other-partition-2").
				WithWrap("sleep 300").
				Submit()
			jobSetName := fmt.Sprintf("slurm-job-%s", jobID)

			By("verifying the job is cancelled in Slurm with the expected comment")
			utils.VerifySlurmJobCancelled(slurmNamespace, jobID,
				"Multi-partition jobs with multiple mapped partitions are not supported")

			By("verifying no JobSet was created")
			utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
		})

		It("should cancel Slurm job when JobSet completes", func() {
			jobID, jobSetName := setupJobSetTerminalStateTest()

			By("artificially completing the JobSet")
			patchJobSetCondition(jobSetName, "slurm", "Completed", "True", "E2ETest", "Completed for E2E")

			By("verifying the job is cancelled in Slurm with the expected comment")
			utils.VerifySlurmJobCancelled(slurmNamespace, jobID, "JobSet completed")

			By("verifying the JobSet is deleted")
			utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
		})

		It("should cancel Slurm job when JobSet fails", func() {
			jobID, jobSetName := setupJobSetTerminalStateTest()

			By("artificially failing the JobSet")
			patchJobSetCondition(jobSetName, "slurm", "Failed", "True", "E2ETest", "Failed for E2E")

			By("verifying the job is cancelled in Slurm with the expected comment")
			utils.VerifySlurmJobCancelled(slurmNamespace, jobID, "JobSet failed")

			By("verifying the JobSet is deleted")
			utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
		})

		It("should cancel Slurm job when JobSet goes missing", func() {
			jobID, jobSetName := setupJobSetTerminalStateTest()

			By("recording the UID of the JobSet created by the controller")
			originalUID, err := utils.JobSetUID(jobSetName, slurmNamespace)
			Expect(err).NotTo(HaveOccurred(), "Failed to read JobSet UID")
			Expect(originalUID).NotTo(BeEmpty(), "JobSet "+jobSetName+" has no UID")

			By("artificially deleting the JobSet")
			cmd := exec.Command("kubectl", "delete", "jobset", jobSetName, "-n", slurmNamespace, "--wait=true")
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to delete JobSet")

			verifySlurmJobCancelledWithoutJobSetRecreation(jobID, jobSetName, originalUID)
		})
	})

	Context("Slurm Worker Connectivity and Job Execution", func() {
		It("should launch dynamic worker pod, register node with slurmctld, "+
			"execute batch task, and tear down cleanly", func() {
			const configPath = "test/e2e/testdata/workloadmixing_template.yaml"

			By("applying the WorkloadMixing template CR")
			utils.ApplyManifestWithCleanup(configPath)

			By("submitting an end-to-end batch job to Slurm as unprivileged user")
			jobID := utils.NewBatchJobBuilder(slurmNamespace).
				WithPartition(defaultSlurmPartition).
				WithCpusPerTask(1).
				WithWrap("hostname; uptime; echo 'E2E_TASK_SUCCESS'").
				Submit()
			jobSetName := fmt.Sprintf("slurm-job-%s", jobID)
			nodeName := fmt.Sprintf("%s-slurmd-0-0", jobSetName)

			By("verifying worker pod is scheduled and running")
			workerPodName := utils.WaitForWorkerPodsRunning(slurmNamespace, jobSetName, 1, 3*time.Minute)[0]

			By("verifying dynamic node registration in Slurmctld")
			utils.WaitForSlurmNodeState(slurmNamespace, nodeName, "IDLE", 3*time.Minute)

			By("verifying batch job execution and transition to COMPLETED in Slurm")
			utils.WaitForSlurmJobCompletion(slurmNamespace, jobID, 3*time.Minute)

			By("verifying slurmd startup and liveness probes on worker pod")
			utils.VerifySlurmdProbesSuccess(slurmNamespace, workerPodName)

			By("verifying teardown reconciler cleans up JobSet and dynamic node")
			utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
			utils.VerifySlurmNodeNotFound(slurmNamespace, nodeName, 2*time.Minute)
		})

		It("should register the dynamic node with the CPUs the single-node job requested "+
			"rather than the host's hardware", func() {
			const configPath = "test/e2e/testdata/workloadmixing_template.yaml"

			// -N 1 --ntasks=2 -c 2 => the pod requests 2 * 2 CPUs and, with no
			// --mem given, 4 * 2048 MiB of memory. While the partition has no
			// nodes slurmctld estimates the pending job needs 2 nodes (as if no
			// node fit both tasks), so this also covers capping that estimate at
			// the submitted -N 1: 2 pods with 2 CPUs each could never run the job.
			const (
				expectedCPUs  = "4"
				expectedMemMB = "8192"
			)

			By("applying the WorkloadMixing template CR")
			utils.ApplyManifestWithCleanup(configPath)

			By("submitting a single-node batch job requesting " + expectedCPUs + " CPUs")
			jobID := utils.NewBatchJobBuilder(slurmNamespace).
				WithPartition(defaultSlurmPartition).
				WithNodes(1).
				WithNtasks(2).
				WithCpusPerTask(2).
				WithWrap("sleep 300").
				Submit()
			jobSetName := fmt.Sprintf("slurm-job-%s", jobID)
			nodeName := fmt.Sprintf("%s-slurmd-0-0", jobSetName)

			verifySlurmdResourceConf(jobSetName, expectedCPUs, expectedMemMB)

			By("verifying worker pod is scheduled and running")
			utils.WaitForWorkerPodsRunning(slurmNamespace, jobSetName, 1, 3*time.Minute)

			By("verifying the job is running on the dynamic node in Slurmctld")
			utils.NewSlurmJobVerifier(slurmNamespace, jobID).
				WithState("RUNNING").
				WithNodeList(nodeName).
				WithTimeout(3 * time.Minute).
				Verify()

			By("verifying slurmd registered the node with the pod's CPUs and memory, not the host's")
			utils.VerifySlurmNodeResources(slurmNamespace, nodeName, expectedCPUs, expectedMemMB, 2*time.Minute)
		})

		It("should execute a multi-node batch job across two dynamic worker pods "+
			"without liveness probe failure", func() {
			const configPath = "test/e2e/testdata/workloadmixing_template.yaml"

			By("applying the WorkloadMixing template CR")
			utils.ApplyManifestWithCleanup(configPath)

			By("submitting a 2-node batch job to Slurm as unprivileged user")
			jobID := utils.NewBatchJobBuilder(slurmNamespace).
				WithPartition(defaultSlurmPartition).
				WithNodes(2).
				WithCpusPerTask(1).
				WithWrap("sleep 25; srun --nodes=2 hostname; echo 'E2E_MULTINODE_SUCCESS'").
				Submit()
			jobSetName := fmt.Sprintf("slurm-job-%s", jobID)
			nodeName0 := fmt.Sprintf("%s-slurmd-0-0", jobSetName)
			nodeName1 := fmt.Sprintf("%s-slurmd-1-0", jobSetName)

			By("verifying both worker pods are scheduled and running")
			workerPodNames := utils.WaitForWorkerPodsRunning(slurmNamespace, jobSetName, 2, 3*time.Minute)

			By("verifying both dynamic nodes are allocated to the running job in Slurmctld")
			utils.NewSlurmJobVerifier(slurmNamespace, jobID).
				WithState("RUNNING").
				WithNodeList(fmt.Sprintf("%s,%s", nodeName0, nodeName1)).
				WithTimeout(3 * time.Minute).
				Verify()

			By("verifying slurmd probes succeed on both worker pods while multi-node job is active")
			for _, podName := range workerPodNames {
				utils.VerifySlurmdProbesSuccess(slurmNamespace, podName)
			}

			By("verifying multi-node batch job execution and transition to COMPLETED in Slurm")
			utils.WaitForSlurmJobCompletion(slurmNamespace, jobID, 3*time.Minute)

			By("verifying slurmd probes did not restart or kill either worker pod")
			for _, podName := range workerPodNames {
				utils.VerifySlurmdProbesSuccess(slurmNamespace, podName)
			}

			By("verifying teardown reconciler cleans up JobSet and both dynamic nodes")
			utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
			utils.VerifySlurmNodeNotFound(slurmNamespace, nodeName0, 2*time.Minute)
			utils.VerifySlurmNodeNotFound(slurmNamespace, nodeName1, 2*time.Minute)
		})

		It("should lay out the tasks of a multi-node job against the CPUs the pods requested "+
			"rather than the host's hardware", func() {
			// Without --ntasks-per-node slurmctld spreads the tasks over what
			// the nodes registered: with 2 CPUs each it has to place 2 tasks per
			// node, whereas nodes registered with the host's CPUs let it pack
			// the first node and leave a single task for the second.
			verifyTwoNodeTaskLayout()
		})

		It("should lay out the tasks of a multi-node job as --ntasks-per-node requests", func() {
			// --ntasks-per-node=2 pins the layout itself; what is verified here
			// is that k8s-bridge sizes and registers each pod for exactly the
			// tasks slurmctld will place on it.
			verifyTwoNodeTaskLayout("--ntasks-per-node=2")
		})
	})
})
