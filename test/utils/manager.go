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
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const E2EPollInterval = 2 * time.Second

const managerOverlayDir = "bin/e2e-manager-overlay"

// controllerManagerDeployment is the name of the controller-manager Deployment.
const controllerManagerDeployment = "k8s-bridge-controller-manager"

// ManagerOptions changes how DeployControllerManagerWithOptions deploys the controller manager.
type ManagerOptions struct {
	// OverlayDir is the directory of the generated Kustomize overlay, relative to
	// the project directory. Suites that run at the same time from one checkout
	// must use different directories. The default is bin/e2e-manager-overlay.
	OverlayDir string
	// ExtraArgs are added to the manager command after --poll-interval.
	ExtraArgs []string
}

// DeployControllerManager deploys the controller manager in-cluster via Kustomize manifests,
// testing the full containerized lifecycle, RBAC, and deployment configuration.
// It registers cleanup with `make undeploy`, waits for the controller pod to be running, and returns the pod name.
func DeployControllerManager(managerImage, namespace string) string {
	ginkgo.GinkgoHelper()
	return DeployControllerManagerWithOptions(managerImage, namespace, ManagerOptions{})
}

// DeployControllerManagerWithOptions does the same as DeployControllerManager, with the
// overlay directory and the extra manager arguments from opts.
func DeployControllerManagerWithOptions(managerImage, namespace string, opts ManagerOptions) string {
	ginkgo.GinkgoHelper()

	overlayDir := opts.OverlayDir
	if overlayDir == "" {
		overlayDir = managerOverlayDir
	}

	ginkgo.By("removing any pre-existing controller-manager deployment")
	cleanupCmd := exec.Command(
		"kubectl", "delete", "deployment", controllerManagerDeployment,
		"-n", namespace, "--ignore-not-found", "--wait=true", "--timeout=2m",
	)
	_, err := Run(cleanupCmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to remove pre-existing controller-manager deployment")

	ginkgo.By("rendering the E2E controller-manager overlay in " + overlayDir)
	_, err = Run(exec.Command("make", "kustomize"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to install kustomize")
	projectDir, err := GetProjectDir()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	overlay := filepath.Join(projectDir, overlayDir)
	gomega.Expect(os.MkdirAll(overlay, 0o755)).To(gomega.Succeed())
	configDefault, err := filepath.Rel(overlay, filepath.Join(projectDir, "config", "default"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(os.WriteFile(filepath.Join(overlay, "kustomization.yaml"),
		[]byte(managerOverlay(managerImage, configDefault, opts.ExtraArgs)), 0o644)).To(gomega.Succeed())

	ginkgo.By("deploying the controller-manager")
	cmd := exec.Command("sh", "-c", fmt.Sprintf(
		"bin/kustomize build %s | kubectl apply --server-side --force-conflicts -f -", overlayDir))
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to deploy the controller-manager")
	ginkgo.DeferCleanup(func() {
		ginkgo.By("undeploying the controller-manager")
		cmd := exec.Command("make", "undeploy", "ignore-not-found=true")
		_, _ = Run(cmd)
	})

	return WaitForControllerManagerReady(managerImage, namespace)
}

// InstallCRDs applies the CRDs from config/crd and registers their deletion. Unlike
// make install, it does not regenerate the CRD files. Suites that run at the same time
// from one checkout thus do not write the same files.
func InstallCRDs() {
	ginkgo.GinkgoHelper()

	ginkgo.By("installing CRDs")
	_, err := Run(exec.Command("make", "kustomize"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to install kustomize")
	_, err = Run(exec.Command("sh", "-c",
		"bin/kustomize build config/crd | kubectl apply --server-side --force-conflicts -f -"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to install CRDs")
	ginkgo.DeferCleanup(func() {
		ginkgo.By("uninstalling CRDs")
		cmd := exec.Command("sh", "-c", "bin/kustomize build config/crd | kubectl delete --ignore-not-found -f -")
		if _, err := Run(cmd); err != nil {
			WarnError(err)
		}
	})
}

// managerOverlay renders a Kustomize overlay of configDefault, the path of
// config/default relative to the overlay directory.
func managerOverlay(managerImage, configDefault string, extraArgs []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, `# Generated by test/utils/manager.go; do not edit.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - %[1]s
patches:
  - target:
      kind: Deployment
      name: %[2]s
    patch: |-
      - op: replace
        path: /spec/template/spec/containers/0/image
        value: %[3]s
      - op: add
        path: /spec/template/spec/containers/0/command/-
        value: --poll-interval=%[4]s
`, configDefault, controllerManagerDeployment, managerImage, E2EPollInterval)
	for _, arg := range extraArgs {
		fmt.Fprintf(&b, `      - op: add
        path: /spec/template/spec/containers/0/command/-
        value: %s
`, strconv.Quote(arg))
	}
	return b.String()
}

func WaitForControllerManagerReady(managerImage, namespace string) string {
	ginkgo.GinkgoHelper()

	ginkgo.By("waiting for controller-manager rollout to complete")
	cmd := exec.Command("kubectl", "rollout", "status", "deployment/"+controllerManagerDeployment,
		"-n", namespace, "--timeout=2m")
	_, err := Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed waiting for controller-manager rollout")

	var controllerPodName string
	ginkgo.By("getting the name of the controller-manager pod")
	verifyControllerUp := func(g gomega.Gomega) {
		var err error
		controllerPodName, err = ControllerPodName(namespace)
		g.Expect(err).NotTo(gomega.HaveOccurred())

		ginkgo.By("validating the pod's status")
		cmd := exec.Command("kubectl", "get",
			"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
			"-n", namespace,
		)
		output, err := Run(cmd)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(output).To(gomega.Equal("Running"), "Incorrect controller-manager pod status")

		ginkgo.By("validating controller pod has acquired leader election")
		cmd = exec.Command("kubectl", "get", "lease", "ead791bb.x-k8s.io",
			"-n", namespace, "-o", "jsonpath={.spec.holderIdentity}")
		output, err = Run(cmd)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(output).To(gomega.HavePrefix(controllerPodName), "Controller pod has not acquired leader lease yet")
	}
	gomega.Eventually(verifyControllerUp, 3*time.Minute, time.Second).Should(gomega.Succeed())

	return controllerPodName
}

func ControllerPodName(namespace string) (string, error) {
	cmd := exec.Command("kubectl", "get",
		"pods", "-l", controllerManagerSelector,
		"-o", "go-template={{ range .items }}"+
			"{{ if not .metadata.deletionTimestamp }}"+
			"{{ .metadata.name }}"+
			"{{ \"\\n\" }}{{ end }}{{ end }}",
		"-n", namespace,
	)
	output, err := Run(cmd)
	if err != nil {
		return "", err
	}
	lines := GetNonEmptyLines(output)
	if len(lines) == 0 {
		return "", fmt.Errorf("no controller-manager pod found in namespace %s", namespace)
	}
	return lines[0], nil
}

// DumpFailureDiagnostics collects logs, events, and pod descriptions when an E2E spec fails.
func DumpFailureDiagnostics(controllerPodName, namespace, slurmNamespace string) {
	if live, err := ControllerPodName(namespace); err == nil {
		controllerPodName = live
	}
	if controllerPodName != "" {
		ginkgo.By("Fetching controller manager pod logs")
		cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
		if logs, err := Run(cmd); err == nil {
			_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Controller logs:\n %s", logs)
		} else {
			_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to get Controller logs: %s", err)
		}
	}

	ginkgo.By("Fetching Kubernetes events")
	cmd := exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
	if eventsOutput, err := Run(cmd); err == nil {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
	} else {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to get Kubernetes events: %s", err)
	}

	ginkgo.By("Fetching slurm-restapi logs")
	cmd = exec.Command("kubectl", "logs", "-n", slurmNamespace,
		"deployment/slurm-restapi", "--all-containers", "--tail=200")
	if slurmRESTAPILogs, err := Run(cmd); err == nil {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Slurm RestApi logs:\n %s", slurmRESTAPILogs)
	} else {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to get Slurm RestApi logs: %s", err)
	}

	ginkgo.By("Fetching slurmctld logs")
	cmd = exec.Command("kubectl", "logs", "-n", slurmNamespace,
		DefaultSlurmControllerPod, "-c", DefaultSlurmctldContainer, "--tail=200")
	if slurmctldLogs, err := Run(cmd); err == nil {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Slurmctld logs:\n %s", slurmctldLogs)
	} else {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Failed to get Slurmctld logs: %s", err)
	}
}
