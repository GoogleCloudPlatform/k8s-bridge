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
	"regexp"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

const (
	kueueTASResourcesPath = "test/e2e/testdata/kueue_tas_resources.yaml"
	tasWorkloadMixingPath = "test/e2e/testdata/workloadmixing_tas.yaml"

	annotationRequiredTopology  = "kueue.x-k8s.io/podset-required-topology"
	annotationPreferredTopology = "kueue.x-k8s.io/podset-preferred-topology"
	hostnameTopologyLevel       = "kubernetes.io/hostname"
)

// rankLineRegex matches "RANK <step> <SLURM_PROCID> <SLURMD_NODENAME>".
var rankLineRegex = regexp.MustCompile(`(?m)^RANK (ONE|MULTI|CYCLIC) (\d+) (\S+)$`)

var _ = Describe("Topology-Aware Scheduling", Ordered, ContinueOnFailure, func() {
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
		}, jobsetTimeout, time.Second).Should(Succeed())

		By("verifying the running slurmd Pod has the merged annotations")
		utils.WaitForWorkerPodsRunning(slurmNamespace, jobSetName, 1, jobsetTimeout)
		for _, pod := range utils.JobSetPods(slurmNamespace, jobSetName) {
			for k, v := range want {
				Expect(pod.Annotations).To(HaveKeyWithValue(k, v), "Pod %s", pod.Name)
			}
		}
	})

	It("should pass a TAS request from --extra to the Kueue Workload", func() {
		jobID := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition(defaultSlurmPartition).
			WithNodes(1).
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
		}, jobsetTimeout, time.Second).Should(Succeed())
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
		// Tasks per node of each checked step. alpha_step_rank only applies to block distribution.
		stepTasksPerNode := map[string]int{"ONE": 1, "MULTI": tasksPerNode}
		session := utils.StartSallocSession(slurmNamespace, utils.DefaultSlurmTestUser,
			[]string{
				"-p", defaultSlurmPartition,
				fmt.Sprintf("--nodes=%d", nodes),
				fmt.Sprintf("--ntasks-per-node=%d", tasksPerNode),
			},
			// ONE and CYCLIC set their task layout explicitly, because srun inherits it from salloc.
			fmt.Sprintf(`srun -m block --ntasks=%d --ntasks-per-node=1 bash -c 'echo "RANK ONE $SLURM_PROCID $SLURMD_NODENAME"'; `, nodes)+
				`srun bash -c 'echo "RANK MULTI $SLURM_PROCID $SLURMD_NODENAME"'; `+
				fmt.Sprintf(`srun -m cyclic --ntasks=%d --ntasks-per-node=1 bash -c 'echo "RANK CYCLIC $SLURM_PROCID $SLURMD_NODENAME"'`, nodes))
		jobSetName := session.JobSetName()
		nodeName := func(jobIndex int) string { return fmt.Sprintf("%s-slurmd-%d-0", jobSetName, jobIndex) }

		By("waiting for all steps to print their ranks")
		var checked []string
		var cyclicRank0Node string
		Eventually(func(g Gomega) {
			checked, cyclicRank0Node = nil, ""
			lines := rankLineRegex.FindAllStringSubmatch(session.Output(), -1)
			g.Expect(lines).To(HaveLen(nodes+nodes*tasksPerNode+nodes), "srun has not printed all ranks yet")
			for _, m := range lines {
				if m[1] == "CYCLIC" {
					if m[2] == "0" {
						cyclicRank0Node = m[3]
					}
					continue
				}
				checked = append(checked, strings.Join(m[1:], " "))
			}
		}, jobsetTimeout, time.Second).Should(Succeed())

		// Cyclic ignores alpha_step_rank and follows node registration order.
		By("verifying pod 0 did not register first, so the check below can fail")
		Expect(cyclicRank0Node).NotTo(BeEmpty())
		Expect(cyclicRank0Node).NotTo(Equal(nodeName(0)), "pod 0 registered first, so the rank check cannot detect a missing alpha_step_rank")

		By("verifying each rank runs on the slurmd Pod with the matching JobSet job index")
		var want []string
		for step, perNode := range stepTasksPerNode {
			for rank := 0; rank < nodes*perNode; rank++ {
				want = append(want, fmt.Sprintf("%s %d %s", step, rank, nodeName(rank/perNode)))
			}
		}
		Expect(checked).To(ConsistOf(want))
	})
})
