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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/GoogleCloudPlatform/k8s-bridge/internal/controller/slurmjob"
	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

var _ = Describe("Interactive Slurm Allocations", func() {
	BeforeEach(func() {
		utils.ResetSlurmAndK8sState(slurmNamespace)
	})

	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			utils.DumpFailureDiagnostics(controllerPodName, namespace, slurmNamespace)
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	It("should serve an salloc session from a dynamic worker pod and tear it down when the shell exits", func() {
		const configPath = "test/e2e/testdata/workloadmixing_template.yaml"

		By("applying the WorkloadMixing template CR")
		utils.ApplyManifestWithCleanup(configPath)

		session := utils.StartSallocSession(slurmNamespace, utils.DefaultSlurmTestUser,
			[]string{"--partition=" + defaultSlurmPartition, "--cpus-per-task=1"},
			`echo "E2E_SALLOC_HOSTNAME=$(srun hostname)"`)
		jobID := session.JobID()
		jobSetName := session.JobSetName()
		nodeName := fmt.Sprintf("%s-slurmd-0-0", jobSetName)

		By("verifying worker pod is scheduled and running")
		workerPodName := utils.WaitForWorkerPodsRunning(slurmNamespace, jobSetName, 1, 3*time.Minute)[0]

		By("verifying the allocation is running on the dynamic node in Slurmctld")
		utils.NewSlurmJobVerifier(slurmNamespace, jobID).
			WithState("RUNNING").
			WithNodeList(nodeName).
			WithTimeout(3 * time.Minute).
			Verify()

		By("verifying salloc was granted the allocation and srun ran on the worker from the login node")
		Eventually(session.Output).Should(SatisfyAll(
			ContainSubstring("salloc: Granted job allocation "+jobID),
			ContainSubstring("E2E_SALLOC_HOSTNAME="+nodeName),
		))

		By("verifying slurmd probes left the idle interactive worker alone")
		utils.VerifySlurmdProbesStayHealthy(slurmNamespace, jobID,
			slurmjob.SlurmdProbePeriodSeconds*time.Second, workerPodName)

		session.Exit()

		By("verifying the job transitions to COMPLETED in Slurm once the shell exits")
		utils.WaitForSlurmJobCompletion(slurmNamespace, jobID, 2*time.Minute)
		Eventually(session.Output).Should(ContainSubstring("salloc: Relinquishing job allocation " + jobID))

		By("verifying teardown reconciler cleans up JobSet and dynamic node")
		utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
		utils.VerifySlurmNodeNotFound(slurmNamespace, nodeName, 2*time.Minute)
	})

	It("should serve an srun --pty shell on a dynamic worker pod and tear it down when the shell exits", func() {
		const configPath = "test/e2e/testdata/workloadmixing_template.yaml"

		By("applying the WorkloadMixing template CR")
		utils.ApplyManifestWithCleanup(configPath)

		session := utils.StartSrunPtySession(slurmNamespace, utils.DefaultSlurmTestUser,
			[]string{"--partition=" + defaultSlurmPartition, "--cpus-per-task=1"},
			`echo "E2E_SRUN_HOSTNAME=$(hostname)"; [ -t 0 ] && echo E2E_PTY_IS_TTY`)
		jobID := session.JobID()
		jobSetName := session.JobSetName()
		nodeName := fmt.Sprintf("%s-slurmd-0-0", jobSetName)

		By("verifying worker pod is scheduled and running")
		workerPodName := utils.WaitForWorkerPodsRunning(slurmNamespace, jobSetName, 1, 3*time.Minute)[0]

		By("verifying the allocation is running on the dynamic node in Slurmctld")
		utils.NewSlurmJobVerifier(slurmNamespace, jobID).
			WithState("RUNNING").
			WithNodeList(nodeName).
			WithTimeout(3 * time.Minute).
			Verify()

		By("verifying the shell runs on the worker pod attached to a pseudo-terminal")
		Eventually(session.Output).Should(SatisfyAll(
			ContainSubstring("srun: job "+jobID+" has been allocated resources"),
			ContainSubstring("E2E_SRUN_HOSTNAME="+nodeName),
			ContainSubstring("E2E_PTY_IS_TTY"),
			Not(ContainSubstring("disregarding --pty")),
		))

		By("verifying slurmd probes left the idle interactive worker alone")
		utils.VerifySlurmdProbesStayHealthy(slurmNamespace, jobID,
			slurmjob.SlurmdProbePeriodSeconds*time.Second, workerPodName)

		session.Exit()

		By("verifying the job transitions to COMPLETED in Slurm once the shell exits")
		utils.WaitForSlurmJobCompletion(slurmNamespace, jobID, 2*time.Minute)

		By("verifying teardown reconciler cleans up JobSet and dynamic node")
		utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
		utils.VerifySlurmNodeNotFound(slurmNamespace, nodeName, 2*time.Minute)
	})
})
