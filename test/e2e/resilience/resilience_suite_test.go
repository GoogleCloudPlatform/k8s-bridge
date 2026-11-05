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
	"fmt"
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

const (
	// namespace is the namespace of the k8s-bridge controller manager.
	namespace             = "k8s-bridge-system"
	slurmNamespace        = utils.DefaultSlurmNamespace
	defaultSlurmPartition = utils.DefaultSlurmPartition

	// slurmClusterManifest deploys a Slurm Controller with persistence enabled.
	slurmClusterManifest   = "test/e2e/testdata/slurm_cluster_persistent.yaml"
	kueueResourcesManifest = "test/e2e/testdata/kueue_resources.yaml"
	workloadMixingManifest = "test/e2e/testdata/workloadmixing_template.yaml"
	// workloadMixingName is the name of the WorkloadMixing in workloadMixingManifest.
	workloadMixingName = "workloadmixing-template"

	// managerOverlayDir differs from the overlay directory of the default suite, so
	// that both suites can run at the same time from one checkout.
	managerOverlayDir = "bin/e2e-manager-overlay-resilience"

	// orphanGracePeriod is the --orphan-grace-period of the manager in this suite. It is
	// shorter than the default of 1 minute, so that the outage windows stay short.
	orphanGracePeriod = 15 * time.Second
	// outageWindow is longer than the orphan grace period plus two poll cycles, so the
	// orphan audit runs at least once after the grace period. If k8s-bridge wrongly
	// decides that a running job is gone while Slurm answers, it deletes the JobSet
	// within this window.
	outageWindow = orphanGracePeriod + 2*utils.E2EPollInterval + 10*time.Second
	// baselineWindow is the time of the invariant check before a failure. slurmctld
	// writes its state files a few seconds after each change.
	baselineWindow = 10 * time.Second
)

var (
	// managerImage is the manager image to be built and loaded for testing.
	managerImage      = utils.ManagerImage("example.com/k8s-bridge:v0.0.1")
	controllerPodName string
	// clusterVerified is true after BeforeSuite verified that kubectl uses the Kind
	// cluster of the suite. Until then, AfterSuite changes nothing.
	clusterVerified bool
)

// TestE2EResilience runs the e2e specs that stop a component while a job runs. Run them
// only on a dedicated Kind cluster. make test-e2e-resilience creates such a cluster with
// its own kubeconfig file.
func TestE2EResilience(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting k8s-bridge resilience e2e test suite\n")
	suiteConfig, reporterConfig := GinkgoConfiguration()
	suiteConfig.Timeout = 40 * time.Minute
	RunSpecs(t, "e2e resilience suite", suiteConfig, reporterConfig)
}

var _ = BeforeSuite(func() {
	utils.ConfigureKubectlKubeRC()
	// The specs delete pods and cordon nodes, so stop before any change if kubectl
	// uses another cluster.
	utils.VerifyKindContext()
	clusterVerified = true
	utils.UncordonAllNodes()

	By("building the manager image")
	_, err := utils.Run(exec.Command("make", "docker-build", fmt.Sprintf("IMG=%s", managerImage)))
	Expect(err).NotTo(HaveOccurred(), "Failed to build the manager image")

	By("loading the manager image on Kind")
	Expect(utils.LoadImageToKindClusterWithName(managerImage)).To(Succeed(),
		"Failed to load the manager image into Kind")

	utils.PreloadThirdPartyImages()

	utils.EnsureCertManager()
	utils.EnsureJobSet()
	utils.EnsureKueue()

	utils.InstallCRDs()

	utils.SetupSlinkySlurmWithCluster(slurmClusterManifest)
	utils.VerifySlurmctldPersistence(slurmNamespace)

	By("deploying Kueue resources (ResourceFlavor, ClusterQueue, LocalQueue)")
	_, err = utils.Run(exec.Command("kubectl", "apply", "-f", kueueResourcesManifest))
	Expect(err).NotTo(HaveOccurred(), "Failed to deploy Kueue resources")

	utils.CreateNamespaceWithCleanup(namespace)

	By("labeling the namespace to enforce the restricted security policy")
	_, err = utils.Run(exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
		"pod-security.kubernetes.io/enforce=restricted"))
	Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

	controllerPodName = utils.DeployControllerManagerWithOptions(managerImage, namespace, utils.ManagerOptions{
		OverlayDir: managerOverlayDir,
		ExtraArgs:  []string{"--orphan-grace-period=" + orphanGracePeriod.String()},
	})
})

var _ = AfterSuite(func() {
	if !clusterVerified {
		return
	}

	utils.UncordonAllNodes()
	utils.TeardownSlinkySlurmWithCluster(slurmClusterManifest)

	By("deleting Kueue resources")
	cmd := exec.Command("kubectl", "delete", "-f", kueueResourcesManifest, "--ignore-not-found")
	if _, err := utils.Run(cmd); err != nil {
		utils.WarnError(err)
	}

	utils.EnsureKueueTeardown()
	utils.EnsureJobSetTeardown()
	utils.EnsureCertManagerTeardown()
})
