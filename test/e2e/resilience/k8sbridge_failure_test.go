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

package resilience

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/GoogleCloudPlatform/k8s-bridge/internal/controller/slurmjob"
	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

var _ = Describe("k8s-bridge controller manager failure", Serial, Label("disruptive", "k8s-bridge-failure"), func() {
	BeforeEach(func() {
		utils.ResetSlurmAndK8sState(slurmNamespace)

		By("applying the WorkloadMixing template CR")
		utils.ApplyManifestWithCleanup(workloadMixingManifest)
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			dumpDiagnostics()
		}
	})

	It("keeps a running job while the controller manager is down and manages it again after the restart", func() {
		job := startGatedJob()
		before := takeSnapshot(job)
		oldPodName := currentControllerPodName()

		By("stopping the controller manager and keeping it stopped")
		utils.StopControllerManager(managerImage, namespace)
		expectWorkloadUndisturbed(job, before, outageWindow, checkSlurmJob)

		By("starting the controller manager again")
		controllerPodName = utils.StartControllerManager(managerImage, namespace)
		Expect(controllerPodName).NotTo(Equal(oldPodName), "The controller manager still runs in the old pod")

		// This window is longer than the orphan grace period, so it also covers the
		// orphan audit of the new poller.
		By("verifying that the restarted controller manager keeps the healthy job")
		expectWorkloadUndisturbed(job, before, outageWindow, checkSlurmJob)
		expectSlurmJobUnchanged(job, before)

		By("verifying that the restarted controller manager deletes the job resources when the job ends")
		releaseAndVerifyCleanup(job)
	})

	It("deletes the resources of a job that ended while the controller manager was down", func() {
		job := startGatedJob()
		before := takeSnapshot(job)

		By("stopping the controller manager and keeping it stopped")
		utils.StopControllerManager(managerImage, namespace)

		By("ending the job while the controller manager is down")
		job.release()
		utils.WaitForSlurmJobCompletion(slurmNamespace, job.ID, 2*time.Minute)

		By("verifying that no other component deletes the JobSet while the controller manager is down")
		Consistently(func(g Gomega) {
			uid, err := utils.JobSetUID(job.JobSetName, slurmNamespace)
			g.Expect(err).NotTo(HaveOccurred(), "Failed to get the UID of JobSet "+job.JobSetName)
			g.Expect(uid).To(Equal(before.JobSetUID), "JobSet %s changed while the controller manager was down",
				job.JobSetName)
		}, baselineWindow, pollInterval).Should(Succeed())

		By("starting the controller manager again")
		controllerPodName = utils.StartControllerManager(managerImage, namespace)

		By("verifying that the restarted controller manager tears down the JobSet and the dynamic node")
		utils.VerifyJobSetNotFound(job.JobSetName, slurmNamespace)
		expectJobCompletedEvent(job)
		utils.VerifySlurmNodeNotFound(slurmNamespace, job.NodeName, 2*time.Minute)

		By("verifying that the Slurm job stays COMPLETED and k8s-bridge did not cancel it")
		state, err := utils.QuerySlurmJobState(slurmNamespace, job.ID)
		Expect(err).NotTo(HaveOccurred(), "Failed to get the state of Slurm job "+job.ID)
		Expect(state.JobState).To(Equal("COMPLETED"))
		Expect(state.Comment).To(Equal(before.SlurmJob.Comment), "k8s-bridge changed the comment of the job")
	})

	It("translates a job that was submitted while the controller manager was down", func() {
		By("stopping the controller manager and keeping it stopped")
		utils.StopControllerManager(managerImage, namespace)

		jobID := submitGatedJob()
		jobSetName := "slurm-job-" + jobID

		By("verifying that the job stays held without a JobSet while the controller manager is down")
		Consistently(func(g Gomega) {
			state, err := utils.QuerySlurmJobState(slurmNamespace, jobID)
			g.Expect(err).NotTo(HaveOccurred(), "Failed to get the state of Slurm job "+jobID)
			g.Expect(state.JobState).To(Equal("PENDING"), "Slurm job %s is not PENDING", jobID)
			g.Expect(state.Reason).To(Equal("JobHeldUser"), "Slurm job %s is not held", jobID)
			g.Expect(state.Comment).To(ContainSubstring(slurmjob.AutoHoldMarker),
				"Slurm job %s has no k8s-bridge hold marker, so the poller would skip it", jobID)
			uid, err := utils.JobSetUID(jobSetName, slurmNamespace)
			g.Expect(err).NotTo(HaveOccurred(), "Failed to get the UID of JobSet "+jobSetName)
			g.Expect(uid).To(BeEmpty(), "JobSet %s exists while the controller manager is down", jobSetName)
		}, baselineWindow, pollInterval).Should(Succeed())

		By("starting the controller manager again")
		controllerPodName = utils.StartControllerManager(managerImage, namespace)

		By("verifying that the restarted controller manager translates and starts the job")
		job := waitForGatedJob(jobID)
		releaseAndVerifyCleanup(job)
	})
})
