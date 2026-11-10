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
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

const (
	kueueTASResourcesPath = "test/e2e/testdata/kueue_tas_resources.yaml"
	tasWorkloadMixingPath = "test/e2e/testdata/workloadmixing_tas.yaml"

	annotationRequiredTopology  = "kueue.x-k8s.io/podset-required-topology"
	annotationPreferredTopology = "kueue.x-k8s.io/podset-preferred-topology"
	hostnameTopologyLevel       = "kubernetes.io/hostname"

	tasTimeout = 3 * time.Minute
)

// rankLineRegex matches "RANK <step> <SLURM_PROCID> <SLURMD_NODENAME>".
var rankLineRegex = regexp.MustCompile(`(?m)^RANK (ONE|MULTI) (\d+) (\S+)$`)

var _ = Describe("Topology-Aware Scheduling", Ordered, func() {
	BeforeAll(func() {
		By("deploying the Kueue TAS resources (Topology, ResourceFlavor, ClusterQueue, LocalQueue)")
		utils.ApplyManifestWithCleanup(kueueTASResourcesPath)
	})

	BeforeEach(func() {
		// ResetSlurmAndK8sState deletes every WorkloadMixing, so the TAS one is applied after it.
		utils.ResetSlurmAndK8sState(slurmNamespace)
		By("applying the TAS WorkloadMixing CR")
		utils.ApplyManifestWithCleanup(tasWorkloadMixingPath)
	})

	AfterEach(func() {
		if CurrentSpecReport().Failed() {
			utils.DumpFailureDiagnostics(controllerPodName, namespace, slurmNamespace)
		}
	})

	It("should pass --extra annotations to the slurmd Pods on top of the podTemplate defaults", func() {
		jobID := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition(defaultSlurmPartition).
			WithNodes(1).
			WithArgs("--extra=example.com/overridden=from-extra,example.com/extra-only=x").
			WithWrap("sleep 300").
			Submit()
		jobSetName := fmt.Sprintf("slurm-job-%s", jobID)
		want := map[string]string{
			"example.com/default-only": "from-template",
			"example.com/overridden":   "from-extra",
			"example.com/extra-only":   "x",
		}

		By("verifying the JobSet Pod template has the merged annotations")
		Eventually(func(g Gomega) {
			js, err := utils.JobSet(jobSetName, slurmNamespace)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(js.Spec.ReplicatedJobs).NotTo(BeEmpty())
			annotations := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Annotations
			for k, v := range want {
				g.Expect(annotations).To(HaveKeyWithValue(k, v))
			}
		}, tasTimeout, time.Second).Should(Succeed())

		By("verifying the running slurmd Pod has the merged annotations")
		utils.WaitForWorkerPodsRunning(slurmNamespace, jobSetName, 1, tasTimeout)
		for _, pod := range jobSetPods(jobSetName) {
			for k, v := range want {
				Expect(pod.Annotations).To(HaveKeyWithValue(k, v), "Pod %s", pod.Name)
			}
		}
	})

	It("should pass a TAS request from --extra to the Kueue Workload", func() {
		jobID := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition(defaultSlurmPartition).
			WithNodes(2).
			WithArgs(fmt.Sprintf("--extra=%s=%s", annotationRequiredTopology, hostnameTopologyLevel)).
			WithWrap("sleep 300").
			Submit()
		jobSetName := fmt.Sprintf("slurm-job-%s", jobID)

		By("verifying the Kueue Workload has the required topology request")
		Eventually(func(g Gomega) {
			wl, err := utils.WorkloadForJobSet(jobSetName, slurmNamespace)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(wl.Spec.PodSets).NotTo(BeEmpty())
			for _, ps := range wl.Spec.PodSets {
				g.Expect(ps.TopologyRequest).NotTo(BeNil(), "Workload %s has a PodSet without a topology request", wl.Name)
				g.Expect(ps.TopologyRequest.Required).To(HaveValue(Equal(hostnameTopologyLevel)))
			}
		}, tasTimeout, time.Second).Should(Succeed())

		// Sanity check only: two Pods may share a node even without TAS.
		By("verifying both slurmd Pods run on the same node")
		utils.WaitForWorkerPodsRunning(slurmNamespace, jobSetName, 2, tasTimeout)
		pods := jobSetPods(jobSetName)
		Expect(pods).To(HaveLen(2))
		Expect(pods[0].Spec.NodeName).To(Equal(pods[1].Spec.NodeName),
			"Pods %s and %s run on different nodes", pods[0].Name, pods[1].Name)
	})

	It("should cancel the Slurm job when Kueue rejects the TAS annotations", func() {
		By("submitting a job that sets both the required and the preferred topology")
		jobID := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition(defaultSlurmPartition).
			WithNodes(1).
			WithArgs(fmt.Sprintf("--extra=%s=%s,%s=%s",
				annotationRequiredTopology, hostnameTopologyLevel,
				annotationPreferredTopology, hostnameTopologyLevel)).
			WithWrap("sleep 300").
			Submit()

		utils.VerifySlurmJobCancelled(slurmNamespace, jobID, "JobSet admission rejected")
		js, err := utils.QuerySlurmJobState(slurmNamespace, jobID)
		Expect(err).NotTo(HaveOccurred())
		Expect(js.Comment).To(ContainSubstring("topology annotation"), "Kueue TAS validation did not reject the JobSet")
		utils.VerifyJobSetNotFound(fmt.Sprintf("slurm-job-%s", jobID), slurmNamespace)
	})

	It("should assign SLURM_PROCID in the order of the JobSet job index", func() {
		const nodes = 3
		const tasksPerNode = 2
		// Pod 0 registers last (see workloadmixing_tas.yaml), so only alpha_step_rank gives it rank 0.
		// alpha_step_rank only applies to block distribution, so the 1-task-per-node step needs -m block.
		session := utils.StartSallocSession(slurmNamespace, utils.DefaultSlurmTestUser,
			[]string{
				"-p", defaultSlurmPartition,
				fmt.Sprintf("--nodes=%d", nodes),
				fmt.Sprintf("--ntasks-per-node=%d", tasksPerNode),
			},
			// srun inherits SLURM_NTASKS from salloc, so the ONE step sets --ntasks explicitly.
			fmt.Sprintf(`srun -m block --ntasks=%d bash -c 'echo "RANK ONE $SLURM_PROCID $SLURMD_NODENAME"'; `, nodes)+
				`srun bash -c 'echo "RANK MULTI $SLURM_PROCID $SLURMD_NODENAME"'`)
		jobSetName := session.JobSetName()

		By("waiting for both steps to print their ranks")
		var ranks [][]string
		Eventually(func(g Gomega) {
			ranks = rankLineRegex.FindAllStringSubmatch(session.Output(), -1)
			g.Expect(ranks).To(HaveLen(nodes+nodes*tasksPerNode), "srun has not printed all ranks yet")
		}, tasTimeout, time.Second).Should(Succeed())

		By("verifying each rank runs on the slurmd Pod with the matching JobSet job index")
		for _, m := range ranks {
			step, procID, nodeName := m[1], m[2], m[3]
			rank, err := strconv.Atoi(procID)
			Expect(err).NotTo(HaveOccurred())
			jobIndex := rank
			if step == "MULTI" {
				jobIndex = rank / tasksPerNode
			}
			Expect(nodeName).To(Equal(fmt.Sprintf("%s-slurmd-%d-0", jobSetName, jobIndex)),
				"%s step: SLURM_PROCID %d runs on node %s", step, rank, nodeName)
		}
	})
})

// jobSetPods returns the Pods created for the named JobSet.
func jobSetPods(jobSetName string) []corev1.Pod {
	GinkgoHelper()
	out, err := utils.Run(exec.Command("kubectl", "get", "pods", "-n", slurmNamespace,
		"-l", "jobset.sigs.k8s.io/jobset-name="+jobSetName, "-o", "json"))
	Expect(err).NotTo(HaveOccurred(), "Failed to list Pods of JobSet "+jobSetName)
	var list corev1.PodList
	Expect(json.Unmarshal([]byte(out), &list)).To(Succeed())
	return list.Items
}
