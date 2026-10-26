//go:build e2e_gpu

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

package gpu

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

var _ = Describe("GPU Manager", Ordered, func() {
	var controllerPodName string

	BeforeAll(func() {
		utils.SetupSlinkySlurm()
		DeferCleanup(func() {
			utils.TeardownSlinkySlurm()
		})

		utils.CreateNamespaceWithCleanup(namespace)

		controllerPodName = utils.DeployControllerManager(managerImage, namespace)
	})

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

	Context("Slurm GPU Job Translation and Lifecycle", func() {
		// Every GRES specification runs as its own spec so that a failure of one
		// specification does not hide the results of the remaining ones.
		DescribeTable("should translate Slurm GPU job to JobSet with GPU limits and tolerations",
			func(args ...string) {
				const configPath = "test/e2e/testdata/workloadmixing_gpu.yaml"

				By("applying the WorkloadMixing GPU CR")
				utils.ApplyManifestWithCleanup(configPath)

				By("submitting a batch job requesting GPU to Slurm")
				jobID := utils.NewBatchJobBuilder(slurmNamespace).
					WithPartition(defaultSlurmPartition).
					WithCpusPerTask(2).
					WithMem(4096).
					WithArgs(args...).
					WithWrap("sleep 1").
					Submit()
				jobSetName := fmt.Sprintf("slurm-job-%s", jobID)

				utils.VerifyJobSetCreatedWithGPU(jobSetName, slurmNamespace, jobID, "1")

				By("waiting for the Slurm job to finish")
				utils.VerifySlurmJobCompleted(slurmNamespace, jobID)

				By("verifying the JobSet is deleted")
				utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
			},
			Entry("--gres=gpu:1", "--gres=gpu:1"),
			Entry("--gres=gpu:nvidia_l4:1", "--gres=gpu:nvidia_l4:1"),
			Entry("--gpus=1", "--gpus=1"),
			Entry("--gpus=nvidia_l4:1", "--gpus=nvidia_l4:1"),
			Entry("--gpus-per-node=1", "--gpus-per-node=1"),
			Entry("--gpus-per-node=nvidia_l4:1", "--gpus-per-node=nvidia_l4:1"),
			Entry("--gpus-per-task=1", "--ntasks=1", "--gpus-per-task=1"),
			Entry("--gpus-per-task=nvidia_l4:1", "--ntasks=1", "--gpus-per-task=nvidia_l4:1"),
		)

		// slurmrestd reports cpus_per_task=1 for a job without -c, so the pod must take its
		// CPUs from --cpus-per-gpu. Each entry asks for 1 GPU, and Slurm allocates whole cores.
		DescribeTable("should request the CPUs that Slurm allocates for --cpus-per-gpu",
			func(args ...string) {
				const (
					configPath = "test/e2e/testdata/workloadmixing_gpu.yaml"
					cpusPerGPU = "2"
				)

				By("applying the WorkloadMixing GPU CR")
				utils.ApplyManifestWithCleanup(configPath)

				By("submitting a batch job with --cpus-per-gpu and without --cpus-per-task")
				jobID := utils.NewBatchJobBuilder(slurmNamespace).
					WithPartition(defaultSlurmPartition).
					WithMem(4096).
					WithArgs(args...).
					WithArgs("--cpus-per-gpu=" + cpusPerGPU).
					WithWrap("sleep 1").
					Submit()
				jobSetName := fmt.Sprintf("slurm-job-%s", jobID)

				utils.VerifyJobSetCreatedWithGPU(jobSetName, slurmNamespace, jobID, "1")

				By("verifying the slurmd pod requests the CPUs for --cpus-per-gpu")
				// Memory: --mem=4096 and the 2048 MiB per CPU fallback both give 4096Mi.
				utils.VerifyJobSetSizing(jobSetName, slurmNamespace, 1, cpusPerGPU, "4096Mi")

				By("verifying that Slurm allocated the same number of CPUs to the job")
				utils.NewSlurmJobVerifier(slurmNamespace, jobID).
					WithState("COMPLETED").
					WithNumCPUs(cpusPerGPU).
					Verify()

				By("verifying the JobSet is deleted")
				utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
			},
			Entry("--gpus-per-node=nvidia_l4:1", "--gpus-per-node=nvidia_l4:1"),
			Entry("--gpus-per-task=nvidia_l4:1", "--ntasks=1", "--gpus-per-task=nvidia_l4:1"),
			Entry("--gpus=nvidia_l4:1", "--gpus=nvidia_l4:1"),
		)

		It("should cancel Slurm job if translation fails due to unmapped GRES", func() {
			const configPath = "test/e2e/testdata/workloadmixing_template.yaml"

			By("applying the WorkloadMixing template CR without GPU mappings")
			utils.ApplyManifestWithCleanup(configPath)

			By("submitting a batch job to Slurm requesting unmapped GRES (gpu:l4:1)")
			jobID := utils.NewBatchJobBuilder(slurmNamespace).
				WithPartition(defaultSlurmPartition).
				WithGres("gpu:l4:1").
				WithWrap("sleep 1").
				Submit()
			jobSetName := fmt.Sprintf("slurm-job-%s", jobID)

			By("verifying the job is cancelled in Slurm with the expected comment")
			utils.VerifySlurmJobCancelled(slurmNamespace, jobID,
				"Translation failed: missing GRES mapping for gpu:l4")

			By("verifying no JobSet was created")
			utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
		})

		It("should cancel Slurm GPU job when JobSet completes", func() {
			const configPath = "test/e2e/testdata/workloadmixing_gpu.yaml"

			By("applying the WorkloadMixing GPU CR")
			utils.ApplyManifestWithCleanup(configPath)

			By("submitting a long-running batch job requesting GPU to Slurm")
			jobID := utils.NewBatchJobBuilder(slurmNamespace).
				WithPartition(defaultSlurmPartition).
				WithCpusPerTask(2).
				WithMem(4096).
				WithGres("gpu:nvidia_l4:1").
				WithWrap("sleep 300").
				Submit()
			jobSetName := fmt.Sprintf("slurm-job-%s", jobID)

			utils.VerifyJobSetCreatedWithGPU(jobSetName, slurmNamespace, jobID, "1")

			By("artificially completing the JobSet")
			utils.PatchJobSetCondition(jobSetName, slurmNamespace, "Completed", "True", "E2ETest", "Completed for E2E")

			By("verifying the job is cancelled in Slurm with the expected comment")
			utils.NewSlurmJobVerifier(slurmNamespace, jobID).
				WithState("CANCELLED").
				WithComment("JobSet completed").
				Verify()

			By("verifying the JobSet is deleted")
			utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
		})
	})
})
