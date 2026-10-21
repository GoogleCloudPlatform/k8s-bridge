//go:build e2e

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

package e2e

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/utils/ptr"

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

var _ = Describe("Partition Priority Mapping", func() {

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

	verifyJobSetPriority := func(g Gomega, jobID, expectedPriority string) {
		js, err := utils.JobSet(fmt.Sprintf("slurm-job-%s", jobID), slurmNamespace)
		g.Expect(err).NotTo(HaveOccurred())
		labels := js.GetLabels()
		if expectedPriority == "" {
			g.Expect(labels).NotTo(HaveKey("kueue.x-k8s.io/priority-class"))
		} else {
			g.Expect(labels).To(HaveKeyWithValue("kueue.x-k8s.io/priority-class", expectedPriority))
		}
	}

	waitForPollerCycle := func() {
		GinkgoHelper()
		By("submitting a control job and waiting for its JobSet to prove a poller cycle ran")
		controlJobID := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition("workloadmixing-partition").
			WithWrap("sleep 300").
			Submit()
		Eventually(func(g Gomega) {
			_, err := utils.JobSet(fmt.Sprintf("slurm-job-%s", controlJobID), slurmNamespace)
			g.Expect(err).NotTo(HaveOccurred())
		}).Should(Succeed(), "Control job was never translated; poller did not complete a cycle")
	}

	It("maps distinct partitions to respective Kueue priorities", func() {
		utils.ApplyManifestWithCleanup("test/e2e/testdata/partition_mapping_distinct.yaml")

		jobID1 := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition("workloadmixing-partition").
			WithWrap("sleep 300").
			Submit()
		jobID2 := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition("workloadmixing-debug-partition").
			WithWrap("sleep 300").
			Submit()

		Eventually(func(g Gomega) {
			js1, err := utils.JobSet(fmt.Sprintf("slurm-job-%s", jobID1), slurmNamespace)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(js1.GetLabels()).To(HaveKeyWithValue("kueue.x-k8s.io/queue-name", "test-local-queue"))
		}).Should(Succeed())

		Eventually(func(g Gomega) { verifyJobSetPriority(g, jobID1, "prod-high") }).Should(Succeed())
		Eventually(func(g Gomega) { verifyJobSetPriority(g, jobID2, "batch-low") }).Should(Succeed())
	})

	It("injects priority based on effective partition resolved via job_submit.lua (default partition fallback)", func() {
		utils.ApplyManifestWithCleanup("test/e2e/testdata/partition_mapping_fallback.yaml")

		By("setting workloadmixing-partition as the default partition in Slurm")
		_, err := utils.RunOnLoginNodeAsUser(
			slurmNamespace, "root", "scontrol update PartitionName=workloadmixing-partition Default=YES",
		)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_, _ = utils.RunOnLoginNodeAsUser(
				slurmNamespace, "root", "scontrol update PartitionName=workloadmixing-partition Default=NO",
			)
		})

		By("submitting a job without an explicit partition")
		jobID := utils.NewBatchJobBuilder(slurmNamespace).WithWrap("sleep 300").Submit()

		Eventually(func(g Gomega) { verifyJobSetPriority(g, jobID, "prod-high") }).Should(Succeed())
	})

	It("omits Kueue priority class when left blank in WorkloadMixing config", func() {
		utils.ApplyManifestWithCleanup("test/e2e/testdata/partition_mapping_blank.yaml")

		jobID := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition("workloadmixing-partition").
			WithWrap("sleep 300").
			Submit()

		Eventually(func(g Gomega) { verifyJobSetPriority(g, jobID, "") }).Should(Succeed())
	})

	It("preserves priority class label during Kueue suspension (preemption) cycle", func() {
		utils.ApplyManifestWithCleanup("test/e2e/testdata/partition_mapping_suspension.yaml")

		jobID := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition("workloadmixing-partition").
			WithWrap("sleep 300").
			Submit()

		var originalUID string
		Eventually(func(g Gomega) {
			js, err := utils.JobSet(fmt.Sprintf("slurm-job-%s", jobID), slurmNamespace)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(js.GetLabels()).To(HaveKeyWithValue("kueue.x-k8s.io/priority-class", "ops-prio"))
			originalUID = string(js.GetUID())
		}).Should(Succeed())

		By("simulating localized Kueue preemption by deactivating the native Kueue Workload")
		jobSetName := fmt.Sprintf("slurm-job-%s", jobID)

		var workloadName string
		Eventually(func(g Gomega) {
			name, err := workloadOwnedBy(originalUID)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(name).NotTo(BeEmpty(), "No Kueue Workload owned by JobSet %s yet", jobSetName)
			workloadName = name
		}).Should(Succeed())

		// We patch the Kueue Workload directly to active: false.
		// Objective: Instruct Kueue to preempt/suspend the job securely.
		// Reason: Manually patching `suspend: true` directly onto the JobSet causes friction with Kueue's
		// Mutating Webhooks. By setting the underlying Kueue Workload to inactive, we let Kueue natively
		// handle the eviction and automatically apply `suspend: true` to the JobSet.
		suspendCmd := fmt.Sprintf(
			`kubectl patch workload %s -n %s --type=merge -p '{"spec":{"active":false}}'`,
			workloadName, slurmNamespace,
		)
		cmd := exec.Command("sh", "-c", suspendCmd)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("verifying the JobSet gets suspended by Kueue and the underlying Slurm job remains PENDING")
		Eventually(func(g Gomega) {
			js, err := utils.JobSet(jobSetName, slurmNamespace)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ptr.Deref(js.Spec.Suspend, false)).To(BeTrue())
		}).Should(Succeed())

		slurmJob, err := utils.QuerySlurmJobState(slurmNamespace, jobID)
		Expect(err).NotTo(HaveOccurred())
		Expect(slurmJob.JobState).To(Equal("PENDING"))

		By("simulating Kueue readmission by reactivating the native Kueue Workload")
		// We patch the Kueue Workload directly to active: true.
		// Objective: Instruct Kueue to re-enqueue and readmit the job.
		// Reason: Manually unsuspending the JobSet directly is blocked by Kueue, which forces the JobSet back
		// to `suspend: true` if the workload isn't natively admitted. Making the Workload active again
		// naturally invites Kueue to readmit it and securely flip the JobSet back to `suspend: false`.
		readmitCmd := fmt.Sprintf(
			`kubectl patch workload %s -n %s --type=merge -p '{"spec":{"active":true}}'`,
			workloadName, slurmNamespace,
		)
		cmd = exec.Command("sh", "-c", readmitCmd)
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred())

		By("verifying JobSet UID and priority class label remained unchanged and JobSet is readmitted")
		Eventually(func(g Gomega) {
			js, err := utils.JobSet(jobSetName, slurmNamespace)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(string(js.GetUID())).To(Equal(originalUID))
			g.Expect(js.GetLabels()).To(HaveKeyWithValue("kueue.x-k8s.io/priority-class", "ops-prio"))
			g.Expect(ptr.Deref(js.Spec.Suspend, true)).To(BeFalse())
		}).Should(Succeed())

		By("verifying the underlying Slurm job is resumed (PENDING) by the bridge")
		utils.NewSlurmJobVerifier(slurmNamespace, jobID).WithState("PENDING").Verify()
	})

	It("ignores jobs on partitions unmanaged by WorkloadMixing", func() {
		utils.ApplyManifestWithCleanup("test/e2e/testdata/partition_mapping_unmanaged.yaml")

		By("verifying unmanaged partition does not create a JobSet")
		jobIDUnmanaged := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition("workloadmixing-other-partition").
			WithWrap("sleep 300").
			Submit()
		waitForPollerCycle()
		_, err := utils.JobSet(fmt.Sprintf("slurm-job-%s", jobIDUnmanaged), slurmNamespace)
		Expect(err).To(HaveOccurred(), "JobSet should not be created for unmanaged partition")
		Expect(err.Error()).To(MatchRegexp(`(?i)not\s*found`))
	})

	It("defers JobSets when corresponding Slurm job is explicitly held (priority=0)", func() {
		utils.ApplyManifestWithCleanup("test/e2e/testdata/partition_mapping_held.yaml")

		By(fmt.Sprintf(
			"submitting a held job as %s (via the login pod) to correctly bypass root exemption in job_submit.lua",
			utils.DefaultSlurmTestUser,
		))
		jobID := utils.NewBatchJobBuilder(slurmNamespace).
			WithHold().
			WithPartition("workloadmixing-partition").
			WithWrap("sleep 300").
			Submit()
		waitForPollerCycle()

		By("verifying the JobSet is deferred (not created) while the Slurm job is held")
		_, err := utils.JobSet(fmt.Sprintf("slurm-job-%s", jobID), slurmNamespace)
		Expect(err).To(HaveOccurred(), "JobSet should not exist while job is explicitly held")
		Expect(err.Error()).To(MatchRegexp(`(?i)not\s*found`))

		By("releasing the hold on the Slurm job natively as " + utils.DefaultSlurmTestUser)
		utils.ReleaseSlurmJob(slurmNamespace, jobID)

		By("verifying the JobSet is subsequently created once the explicit hold is removed")
		Eventually(func(g Gomega) {
			_, err := utils.JobSet(fmt.Sprintf("slurm-job-%s", jobID), slurmNamespace)
			g.Expect(err).NotTo(HaveOccurred())
		}).Should(Succeed())

	})

	It(
		"relies exclusively on static Kueue priority mapping regardless of Slurm numerical PriorityTier/PriorityJobFactor",
		func() {
			utils.ApplyManifestWithCleanup("test/e2e/testdata/partition_mapping_numerical.yaml")

			By("updating the numerical PriorityTier and PriorityJobFactor of the partition")
			_, err := utils.RunOnLoginNodeAsUser(
				slurmNamespace,
				"root",
				"scontrol update PartitionName=workloadmixing-partition PriorityTier=1000 PriorityJobFactor=65533",
			)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				_, _ = utils.RunOnLoginNodeAsUser(
					slurmNamespace,
					"root",
					"scontrol update PartitionName=workloadmixing-partition PriorityTier=1 PriorityJobFactor=1",
				)
			})

			jobID := utils.NewBatchJobBuilder(slurmNamespace).
				WithPartition("workloadmixing-partition").
				WithWrap("sleep 300").
				Submit()

			Eventually(func(g Gomega) { verifyJobSetPriority(g, jobID, "prod-high") }).Should(Succeed())
		},
	)
})

func workloadOwnedBy(ownerUID string) (string, error) {
	out, err := utils.Run(exec.Command("kubectl", "get", "workload", "-n", slurmNamespace, "-o", "json"))
	if err != nil {
		return "", err
	}
	var list struct {
		Items []struct {
			Metadata struct {
				Name            string `json:"name"`
				OwnerReferences []struct {
					UID string `json:"uid"`
				} `json:"ownerReferences"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		return "", fmt.Errorf("failed to parse Workload list: %w", err)
	}
	for _, wl := range list.Items {
		for _, ref := range wl.Metadata.OwnerReferences {
			if ref.UID == ownerUID {
				return wl.Metadata.Name, nil
			}
		}
	}
	return "", nil
}
