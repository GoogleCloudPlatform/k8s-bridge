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
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// This file contains helpers that stop components of the test cluster for a
// controlled time. Only suites on a dedicated Kind cluster may use them,
// because they delete pods.

const (
	// controllerManagerSelector selects the pods of the controller-manager Deployment.
	controllerManagerSelector = "control-plane=controller-manager"

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
