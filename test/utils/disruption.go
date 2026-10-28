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

package utils

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// This file contains helpers that stop components of the test cluster for a
// controlled time. Only suites on a dedicated Kind cluster may use them,
// because they delete pods and cordon nodes.

const (
	// controllerManagerSelector selects the pods of the controller-manager Deployment.
	controllerManagerSelector = "control-plane=controller-manager"
	// ControllerManagerContainer is the container of the controller-manager pod.
	ControllerManagerContainer = "manager"

	// slurmctldStateSaveVolume is the volume claim template of the slurmctld
	// StatefulSet when persistence is enabled. The StatefulSet controller names
	// the claim "<template>-<pod>".
	slurmctldStateSaveVolume = "statesave"
	slurmctldStateSavePVC    = slurmctldStateSaveVolume + "-" + DefaultSlurmControllerPod

	slurmRestAPIDeployment = "slurm-restapi"

	// abruptGracePeriod gives a container 1 second to stop after SIGTERM. Then the kubelet
	// kills the container. kubectl delete waits until the kubelet confirms that the
	// container stopped, so the old process cannot do more work after the delete returns.
	abruptGracePeriod = "--grace-period=1"
)

// VerifyKindContext stops the suite when kubectl does not use the Kind cluster that
// KIND_CLUSTER names. Disruptive suites must call it before they change the cluster.
func VerifyKindContext() {
	ginkgo.GinkgoHelper()

	cluster := defaultKindCluster
	if v, ok := os.LookupEnv("KIND_CLUSTER"); ok {
		cluster = v
	}
	ginkgo.By("verifying that kubectl uses the Kind cluster " + cluster)
	out, err := Run(exec.Command("kubectl", "config", "current-context"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to read the current kubectl context")
	gomega.Expect(strings.TrimSpace(out)).To(gomega.Equal("kind-"+cluster),
		"kubectl does not use the Kind cluster %q. Set KUBECONFIG to the kubeconfig file of that cluster", cluster)
}

// UncordonAllNodes uncordons each node of the cluster. A suite that cordons nodes
// calls it at start, in case an earlier run stopped before its cleanup.
func UncordonAllNodes() {
	ginkgo.GinkgoHelper()

	ginkgo.By("uncordoning all nodes")
	out, err := Run(exec.Command("kubectl", "get", "nodes", "-o", "name"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to list nodes")
	for _, node := range GetNonEmptyLines(out) {
		_, err := Run(exec.Command("kubectl", "uncordon", node))
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to uncordon "+node)
	}
}

// ContainerRestartCount returns the restart count of a container of a pod. It returns an
// error if the pod has no status for the container, for example for a wrong container name.
func ContainerRestartCount(namespace, podName, container string) (string, error) {
	out, err := Run(exec.Command("kubectl", "get", "pod", podName, "-n", namespace, "-o",
		fmt.Sprintf(`jsonpath={.status.containerStatuses[?(@.name=="%s")].restartCount}`, container)))
	if err != nil {
		return "", err
	}
	count := strings.TrimSpace(out)
	if count == "" {
		return "", fmt.Errorf("pod %s/%s has no status for container %s", namespace, podName, container)
	}
	return count, nil
}

// StopControllerManager stops the controller manager and keeps it stopped. It scales the
// Deployment to 0 replicas, so that no new pod starts. Then it deletes the pod with a
// 1-second grace period and waits until the container stopped. The manager gets SIGTERM,
// and the kubelet kills it if it still runs after 1 second. This is not a crash test: the
// manager can start its shutdown. A DeferCleanup starts the manager again, so that the
// next spec starts with a running manager.
func StopControllerManager(managerImage, namespace string) {
	ginkgo.GinkgoHelper()

	ginkgo.By("scaling the controller-manager Deployment to 0 replicas")
	_, err := Run(exec.Command("kubectl", "scale", "deployment", controllerManagerDeployment,
		"-n", namespace, "--replicas=0"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to scale the controller-manager Deployment to 0")
	ginkgo.DeferCleanup(func() {
		StartControllerManager(managerImage, namespace)
	})

	ginkgo.By("stopping the controller-manager pod with a 1-second grace period")
	_, err = Run(exec.Command("kubectl", "delete", "pod", "-n", namespace, "-l", controllerManagerSelector,
		abruptGracePeriod, "--wait=true", "--timeout=2m", "--ignore-not-found"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to delete the controller-manager pod")

	ginkgo.By("verifying that no controller-manager pod exists")
	gomega.Eventually(func(g gomega.Gomega) {
		out, err := Run(exec.Command("kubectl", "get", "pods", "-n", namespace,
			"-l", controllerManagerSelector, "-o", "name"))
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(GetNonEmptyLines(out)).To(gomega.BeEmpty(), "A controller-manager pod still exists")
	}, time.Minute, time.Second).Should(gomega.Succeed())
}

// StartControllerManager scales the controller-manager Deployment to 1 replica, waits
// until the new pod holds the leader lease, and returns the name of the pod.
func StartControllerManager(managerImage, namespace string) string {
	ginkgo.GinkgoHelper()

	ginkgo.By("scaling the controller-manager Deployment to 1 replica")
	_, err := Run(exec.Command("kubectl", "scale", "deployment", controllerManagerDeployment,
		"-n", namespace, "--replicas=1"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to scale the controller-manager Deployment to 1")
	return WaitForControllerManagerReady(managerImage, namespace)
}

// VerifySlurmctldPersistence asserts that the slurmctld pod keeps its state on a bound
// PersistentVolumeClaim, and returns the UID of the claim.
func VerifySlurmctldPersistence(slurmNamespace string) string {
	ginkgo.GinkgoHelper()

	ginkgo.By("verifying that slurmctld keeps its state on PVC " + slurmctldStateSavePVC)
	var uid string
	gomega.Eventually(func(g gomega.Gomega) {
		out, err := Run(exec.Command("kubectl", "get", "pvc", slurmctldStateSavePVC, "-n", slurmNamespace,
			"-o", `jsonpath={.metadata.uid}{" "}{.status.phase}`))
		g.Expect(err).NotTo(gomega.HaveOccurred(),
			"slurmctld has no state PVC. Enable persistence in the Slurm Controller of the suite")
		fields := strings.Fields(out)
		g.Expect(fields).To(gomega.HaveLen(2), "Unexpected PVC status %q", out)
		g.Expect(fields[1]).To(gomega.Equal("Bound"), "PVC %s is not Bound", slurmctldStateSavePVC)

		claim, err := Run(exec.Command("kubectl", "get", "pod", DefaultSlurmControllerPod, "-n", slurmNamespace,
			"-o", fmt.Sprintf(`jsonpath={.spec.volumes[?(@.name=="%s")].persistentVolumeClaim.claimName}`,
				slurmctldStateSaveVolume)))
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(strings.TrimSpace(claim)).To(gomega.Equal(slurmctldStateSavePVC),
			"The slurmctld pod does not use PVC %s", slurmctldStateSavePVC)
		uid = fields[0]
	}, 2*time.Minute, time.Second).Should(gomega.Succeed())
	return uid
}

// SlurmctldOutage is a time in which no slurmctld runs. StartSlurmctldOutage starts it,
// and End ends it.
type SlurmctldOutage struct {
	// NodeName is the node of the slurmctld pod. With persistence enabled, the state
	// volume is available only on this node.
	NodeName string
	// OldPodUID is the UID of the slurmctld pod before the outage.
	OldPodUID string

	slurmNamespace string
	ended          bool
}

// StartSlurmctldOutage stops slurmctld and keeps it stopped until End.
//
// It cordons the node of the slurmctld pod, deletes the pod with a 1-second grace period,
// and waits until the replacement pod is Pending. The StatefulSet creates the replacement at
// once, but the state volume is available only on the cordoned node. So the new pod cannot
// start until End uncordons the node. The old and the new slurmctld thus never use the
// volume at the same time. A DeferCleanup calls End if the spec does not.
func StartSlurmctldOutage(slurmNamespace string) *SlurmctldOutage {
	ginkgo.GinkgoHelper()

	o := &SlurmctldOutage{slurmNamespace: slurmNamespace}
	out, err := Run(exec.Command("kubectl", "get", "pod", DefaultSlurmControllerPod, "-n", slurmNamespace,
		"-o", `jsonpath={.spec.nodeName}{" "}{.metadata.uid}`))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to read the slurmctld pod")
	fields := strings.Fields(out)
	gomega.Expect(fields).To(gomega.HaveLen(2), "Unexpected slurmctld pod fields %q", out)
	o.NodeName, o.OldPodUID = fields[0], fields[1]

	ginkgo.By("cordoning node " + o.NodeName + ", which has the slurmctld state volume")
	_, err = Run(exec.Command("kubectl", "cordon", o.NodeName))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to cordon node "+o.NodeName)
	ginkgo.DeferCleanup(func() {
		o.End()
	})

	ginkgo.By("stopping the slurmctld pod with a 1-second grace period")
	_, err = Run(exec.Command("kubectl", "delete", "pod", DefaultSlurmControllerPod, "-n", slurmNamespace,
		abruptGracePeriod, "--wait=true", "--timeout=2m"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to delete the slurmctld pod")

	ginkgo.By("verifying that the new slurmctld pod cannot start")
	gomega.Eventually(func(g gomega.Gomega) {
		out, err := Run(exec.Command("kubectl", "get", "pod", DefaultSlurmControllerPod, "-n", slurmNamespace,
			"-o", `jsonpath={.metadata.uid}{" "}{.status.phase}{" "}`+
				`{.status.conditions[?(@.type=="PodScheduled")].reason}`))
		g.Expect(err).NotTo(gomega.HaveOccurred(), "The StatefulSet did not create a new slurmctld pod")
		fields := strings.Fields(out)
		g.Expect(fields).To(gomega.HaveLen(3), "Unexpected slurmctld pod status %q", out)
		g.Expect(fields[0]).NotTo(gomega.Equal(o.OldPodUID), "The old slurmctld pod still exists")
		g.Expect(fields[1]).To(gomega.Equal("Pending"), "The new slurmctld pod is not Pending")
		g.Expect(fields[2]).To(gomega.Equal("Unschedulable"), "The new slurmctld pod is not unschedulable")
	}, 2*time.Minute, time.Second).Should(gomega.Succeed())

	return o
}

// End uncordons the node, waits until slurmctld and slurmrestd answer again, and verifies
// that the new slurmctld pod runs on the node of the state volume. After the uncordon
// succeeds, later calls do nothing. If the uncordon fails, the DeferCleanup of
// StartSlurmctldOutage tries again.
func (o *SlurmctldOutage) End() {
	ginkgo.GinkgoHelper()

	if o.ended {
		return
	}

	ginkgo.By("uncordoning node " + o.NodeName)
	gomega.Eventually(func() error {
		_, err := Run(exec.Command("kubectl", "uncordon", o.NodeName))
		return err
	}, time.Minute, 2*time.Second).Should(gomega.Succeed(), "Failed to uncordon node "+o.NodeName)
	o.ended = true

	WaitForSlurmctldReady(o.slurmNamespace)

	out, err := Run(exec.Command("kubectl", "get", "pod", DefaultSlurmControllerPod, "-n", o.slurmNamespace,
		"-o", "jsonpath={.spec.nodeName}"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to read the slurmctld pod")
	gomega.Expect(strings.TrimSpace(out)).To(gomega.Equal(o.NodeName),
		"The new slurmctld pod runs on another node than its state volume")
}

// WaitForSlurmctldReady waits until the slurmctld pod is Ready, the slurmrestd Deployment
// is Available, and scontrol ping on the login node reports slurmctld UP.
func WaitForSlurmctldReady(slurmNamespace string) {
	ginkgo.GinkgoHelper()

	ginkgo.By("waiting for the slurmctld pod to be Ready")
	gomega.Eventually(func() error {
		_, err := Run(exec.Command("kubectl", "wait", "pod/"+DefaultSlurmControllerPod, "-n", slurmNamespace,
			"--for", "condition=Ready", "--timeout", "30s"))
		return err
	}, 5*time.Minute, time.Second).Should(gomega.Succeed(), "The slurmctld pod did not become Ready")

	ginkgo.By("waiting for the slurmrestd Deployment to be Available")
	_, err := Run(exec.Command("kubectl", "wait", "deployment/"+slurmRestAPIDeployment, "-n", slurmNamespace,
		"--for", "condition=Available", "--timeout", "5m"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "The slurmrestd Deployment did not become Available")

	ginkgo.By("waiting for scontrol ping to report slurmctld UP")
	gomega.Eventually(func(g gomega.Gomega) {
		out, err := RunOnLoginNodeAsUser(slurmNamespace, DefaultSlurmTestUser, "scontrol ping")
		g.Expect(err).NotTo(gomega.HaveOccurred(), "scontrol ping failed")
		g.Expect(out).To(gomega.ContainSubstring("is UP"), "slurmctld is not UP: %s", out)
	}, 2*time.Minute, 2*time.Second).Should(gomega.Succeed())
}

// ExpectSlurmctldDown asserts that scontrol ping on the login node reports slurmctld DOWN.
func ExpectSlurmctldDown(slurmNamespace string) {
	ginkgo.GinkgoHelper()

	ginkgo.By("verifying that scontrol ping reports slurmctld DOWN")
	gomega.Eventually(func(g gomega.Gomega) {
		// scontrol ping can exit with an error while slurmctld is down, so the check
		// uses only the output.
		out, _ := RunOnLoginNodeAsUser(slurmNamespace, DefaultSlurmTestUser, "scontrol ping")
		g.Expect(out).To(gomega.ContainSubstring("DOWN"), "scontrol ping does not report slurmctld DOWN: %s", out)
		g.Expect(out).NotTo(gomega.ContainSubstring("is UP"), "slurmctld is still UP: %s", out)
	}, time.Minute, 2*time.Second).Should(gomega.Succeed())
}

// WaitForSlurmNodeAllocated waits until slurmctld reports the node ALLOCATED or MIXED,
// without the NOT_RESPONDING or DOWN flag. After a slurmctld restart, this shows that
// slurmctld restored the allocation of the node. slurmctld loads the node state from its
// state file, so this check alone does not prove that slurmd answered. The job must end
// through slurmd to prove that.
func WaitForSlurmNodeAllocated(slurmNamespace, nodeName string, timeout time.Duration) {
	ginkgo.GinkgoHelper()

	ginkgo.By(fmt.Sprintf("waiting for Slurm node %s to be allocated", nodeName))
	gomega.Eventually(func(g gomega.Gomega) {
		ns, err := QuerySlurmNodeState(slurmNamespace, nodeName)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to query Slurm node state for "+nodeName)
		state := strings.ToUpper(ns.State)
		g.Expect(state).To(gomega.Or(gomega.HavePrefix("ALLOCATED"), gomega.HavePrefix("MIXED")),
			"Slurm node %s is not allocated: %s", nodeName, ns.State)
		// scontrol shows NOT_RESPONDING as "*" after the state or as "+NOT_RESPONDING".
		g.Expect(state).NotTo(gomega.ContainSubstring("*"), "Slurm node %s does not respond: %s", nodeName, ns.State)
		g.Expect(state).NotTo(gomega.ContainSubstring("NOT_RESPONDING"),
			"Slurm node %s does not respond: %s", nodeName, ns.State)
		g.Expect(state).NotTo(gomega.ContainSubstring("DOWN"), "Slurm node %s is down: %s", nodeName, ns.State)
	}, timeout, time.Second).Should(gomega.Succeed())
}
