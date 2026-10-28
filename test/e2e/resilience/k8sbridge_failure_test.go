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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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
		expectWorkloadUndisturbed(job, before, outageWindow)

		By("starting the controller manager again")
		controllerPodName = utils.StartControllerManager(managerImage, namespace)
		Expect(controllerPodName).NotTo(Equal(oldPodName), "The controller manager still runs in the old pod")

		// This window is longer than the orphan grace period, so it also covers the
		// orphan audit of the new poller.
		By("verifying that the restarted controller manager keeps the healthy job")
		expectWorkloadUndisturbed(job, before, outageWindow)
		expectSlurmJobUnchanged(job, before)

		By("verifying that the restarted controller manager deletes the job resources when the job ends")
		releaseAndVerifyCleanup(job)
	})
})
