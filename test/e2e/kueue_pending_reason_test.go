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

const (
	pendingReasonWorkloadMixingPath = "test/e2e/testdata/workloadmixing_template.yaml"
	pendingReasonClusterQueue       = "cluster-queue"
	// defaultClusterQueueCPUQuota matches the cpu nominalQuota in testdata/kueue_resources.yaml.
	defaultClusterQueueCPUQuota = "1000"
	pendingInKubernetesPrefix   = "Pending in Kubernetes:"
	pendingReasonUserComment    = "e2e-pending-reason"
	// pendingReasonLocalQueue is the LocalQueue of the WorkloadMixing template.
	pendingReasonLocalQueue = "test-local-queue"
)

// setClusterQueueCPUQuota sets the cpu nominalQuota of the default flavor of the ClusterQueue.
// The test op makes the patch fail instead of changing another resource if cpu is not the first one.
func setClusterQueueCPUQuota(clusterQueue, quota string) {
	GinkgoHelper()
	const resource = "/spec/resourceGroups/0/flavors/0/resources/0"
	patch := fmt.Sprintf(`[{"op":"test","path":"%s/name","value":"cpu"},{"op":"replace","path":"%s/nominalQuota","value":%q}]`,
		resource, resource, quota)
	cmd := exec.Command("kubectl", "patch", "clusterqueue", clusterQueue, "--type=json", "-p", patch)
	_, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to set the cpu quota of ClusterQueue %s to %s", clusterQueue, quota)
}

var _ = Describe("Kubernetes pending reasons", func() {
	BeforeEach(func() {
		utils.ResetSlurmAndK8sState(slurmNamespace)
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			utils.DumpFailureDiagnostics(controllerPodName, namespace, slurmNamespace)
		}
	})

	It("should show the Kueue pending reasons in the Slurm job comments until the JobSets are admitted", func() {
		By("applying the WorkloadMixing template CR")
		utils.ApplyManifestWithCleanup(pendingReasonWorkloadMixingPath)

		By("removing the cpu quota of the ClusterQueue so that Kueue cannot admit the JobSet")
		DeferCleanup(setClusterQueueCPUQuota, pendingReasonClusterQueue, defaultClusterQueueCPUQuota)
		setClusterQueueCPUQuota(pendingReasonClusterQueue, "0")

		jobID := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition(defaultSlurmPartition).
			WithNodes(1).
			WithCpusPerTask(1).
			WithArgs("--comment=" + pendingReasonUserComment).
			WithWrap("sleep 300").
			Submit()
		jobSetName := fmt.Sprintf("slurm-job-%s", jobID)
		// Kueue reports the missing quota only on the Workloads at the head of the queue; the
		// others are shown as queued in their LocalQueue.
		secondJobID := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition(defaultSlurmPartition).
			WithNodes(1).
			WithCpusPerTask(1).
			WithWrap("sleep 300").
			Submit()
		secondJobSetName := fmt.Sprintf("slurm-job-%s", secondJobID)
		kueueReason := Or(ContainSubstring("quota"), ContainSubstring("queued in "+pendingReasonLocalQueue))

		By("verifying the Slurm job comments show a Kueue pending reason")
		Eventually(func(g Gomega) {
			js, err := utils.QuerySlurmJobState(slurmNamespace, jobID)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(js.JobState).To(Equal("PENDING"))
			g.Expect(js.Comment).To(ContainSubstring(pendingInKubernetesPrefix + " Kueue:"))
			g.Expect(js.Comment).To(kueueReason)
			g.Expect(js.Comment).To(HavePrefix(pendingReasonUserComment + ", "))
			g.Expect(js.Comment).NotTo(ContainSubstring("JobSet creation failed"))

			second, err := utils.QuerySlurmJobState(slurmNamespace, secondJobID)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(second.Comment).To(HavePrefix(pendingInKubernetesPrefix + " Kueue:"))
			g.Expect(second.Comment).To(kueueReason)
		}, jobsetTimeout, time.Second).Should(Succeed())

		By("restoring the cpu quota of the ClusterQueue")
		setClusterQueueCPUQuota(pendingReasonClusterQueue, defaultClusterQueueCPUQuota)

		By("verifying the Pending status is replaced by the Unheld status")
		Eventually(func(g Gomega) {
			js, err := utils.QuerySlurmJobState(slurmNamespace, jobID)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(js.Comment).NotTo(ContainSubstring(pendingInKubernetesPrefix))
			g.Expect(js.Comment).NotTo(ContainSubstring("k8s-bridge-auto-hold"))
			g.Expect(js.Comment).To(Equal(fmt.Sprintf(
				"%s, Unheld by k8s-bridge: Kubernetes resource %s created successfully", pendingReasonUserComment, jobSetName)))

			second, err := utils.QuerySlurmJobState(slurmNamespace, secondJobID)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(second.Comment).To(Equal(fmt.Sprintf(
				"Unheld by k8s-bridge: Kubernetes resource %s created successfully", secondJobSetName)))
		}, jobsetTimeout, time.Second).Should(Succeed())

		By("verifying the first job runs with the Unheld status")
		Eventually(func(g Gomega) {
			js, err := utils.QuerySlurmJobState(slurmNamespace, jobID)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(js.JobState).To(Equal("RUNNING"))
			g.Expect(js.Comment).To(Equal(fmt.Sprintf(
				"%s, Unheld by k8s-bridge: Kubernetes resource %s created successfully", pendingReasonUserComment, jobSetName)))
		}, jobsetTimeout, time.Second).Should(Succeed())
	})
})
