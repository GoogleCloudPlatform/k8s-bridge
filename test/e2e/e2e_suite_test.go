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
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

const (
	slurmNamespace        = utils.DefaultSlurmNamespace
	defaultSlurmPartition = utils.DefaultSlurmPartition
)

var (
	// managerImage is the manager image to be built and loaded for testing.
	managerImage      = utils.ManagerImage("example.com/k8s-bridge:v0.0.1")
	controllerPodName string
)

// TestE2E runs the e2e test suite to validate the solution in an isolated environment.
// The default setup requires Kind and CertManager.
//
// To enable kubectl kuberc (use custom kubectl configurations), set: KUBECTL_KUBERC=true
// By default, kuberc is disabled to ensure consistent test behavior across different environments.
// To skip CertManager installation, set: CERT_MANAGER_INSTALL_SKIP=true
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting k8s-bridge e2e test suite\n")
	suiteConfig, reporterConfig := GinkgoConfiguration()
	suiteConfig.Timeout = 20 * time.Minute
	RunSpecs(t, "e2e suite", suiteConfig, reporterConfig)
}

var _ = BeforeSuite(func() {
	By("building the manager image")
	cmd := exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage))
	_, err := utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to build the manager image")

	// TODO(user): If you want to change the e2e test vendor from Kind,
	// ensure the image is built and available, then remove the following block.
	By("loading the manager image on Kind")
	err = utils.LoadImageToKindClusterWithName(managerImage)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to load the manager image into Kind")

	utils.PreloadThirdPartyImages()

	utils.ConfigureKubectlKubeRC()
	utils.EnsureCertManager()
	utils.EnsureJobSet()
	utils.EnsureKueue()

	By("installing CRDs")
	cmd = exec.Command("make", "install")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to install CRDs")

	utils.SetupSlinkySlurm()
	// A small static node in its own partition, like the Slinky NodeSet partitions next to the
	// k8s-bridge partitions in real clusters. It must register before any spec submits a job.
	utils.SetupStaticSlurmNode()

	By("deploying Kueue resources (ResourceFlavor, ClusterQueue, LocalQueue)")
	cmd = exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/kueue_resources.yaml")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to deploy Kueue resources")

	utils.CreateNamespaceWithCleanup(namespace)

	By("labeling the namespace to enforce the restricted security policy")
	cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
		"pod-security.kubernetes.io/enforce=restricted")
	_, err = utils.Run(cmd)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

	controllerPodName = utils.DeployControllerManager(managerImage, namespace)
})

var _ = AfterSuite(func() {
	utils.TeardownStaticSlurmNode()
	utils.TeardownSlinkySlurm()

	By("deleting Kueue resources")
	cmd := exec.Command("kubectl", "delete", "-f", "test/e2e/testdata/kueue_resources.yaml", "--ignore-not-found")
	if _, err := utils.Run(cmd); err != nil {
		utils.WarnError(err)
	}

	By("uninstalling CRDs")
	cmd = exec.Command("make", "uninstall", "ignore-not-found=true")
	if _, err := utils.Run(cmd); err != nil {
		utils.WarnError(err)
	}

	utils.EnsureKueueTeardown()
	utils.EnsureJobSetTeardown()
	utils.EnsureCertManagerTeardown()
})
