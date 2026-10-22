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

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

var _ = Describe("slurmctld failure with persistence", Serial, Label("disruptive", "slurmctld-failure"), func() {
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

	It("keeps a running job when the slurmctld pod is lost and restores it from the persistent state", func() {
		job := startGatedJob()
		before := takeSnapshot(job)

		// The baseline check gives slurmctld time to write the job state, and it shows
		// that the checks pass before the failure.
		expectWorkloadUndisturbed(job, before, baselineWindow, checkSlurmJob)

		pvcUID := utils.VerifySlurmctldPersistence(slurmNamespace)
		managerPod := currentControllerPodName()
		managerRestarts := controllerRestartCount(managerPod)

		By("stopping slurmctld and keeping it stopped")
		outageStart := time.Now()
		outage := utils.StartSlurmctldOutage(slurmNamespace)
		utils.ExpectSlurmctldDown(slurmNamespace)
		// The window is longer than the orphan grace period plus two poll cycles. While
		// slurmctld is down, a wrong orphan decision cannot delete the JobSet, because the
		// teardown stops at the failed delete of the dynamic node. The log check shows it.
		expectWorkloadUndisturbed(job, before, outageWindow, skipSlurmJobCheck)
		expectPollerSkippedOrphanAudit(managerPod, outageStart)

		By("starting slurmctld again on the same state volume")
		outage.End()
		Expect(utils.VerifySlurmctldPersistence(slurmNamespace)).To(Equal(pvcUID),
			"slurmctld uses another state PVC after the restart")

		By("verifying that the controller manager did not restart during the outage")
		Expect(currentControllerPodName()).To(Equal(managerPod), "The controller-manager pod changed")
		Expect(controllerRestartCount(managerPod)).To(Equal(managerRestarts), "The controller manager restarted")

		By("verifying that Slurm restored the job and its dynamic node")
		expectSlurmJobUnchanged(job, before)
		utils.WaitForSlurmNodeAllocated(slurmNamespace, job.NodeName, 2*time.Minute)
		expectWorkloadUndisturbed(job, before, outageWindow, checkSlurmJob)
		utils.VerifyNoSlurmdLivenessFailure(slurmNamespace, job.PodName)

		By("verifying that the job completes and k8s-bridge deletes its resources")
		releaseAndVerifyCleanup(job)
	})
})
