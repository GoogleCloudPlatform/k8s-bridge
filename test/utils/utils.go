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
	"regexp"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	certmanagerVersion = "v1.21.2"
	jobsetVersion      = "v0.12.0"
	kueueVersion       = "v0.19.4"

	thirdPartyManifestDir = "test/e2e/testdata/third_party"

	defaultKindBinary  = "kind"
	defaultKindCluster = "kind"
)

// LINT.IfChange(curl_image)
const CurlImage = "curlimages/curl:8.16.0"

// LINT.ThenChange(
//   test/e2e/testdata/workloadmixing_template.yaml:curl_image,
//   .github/workflows/test-e2e.yml:curl_image
// )

var (
	certmanagerManifest = fmt.Sprintf("%s/cert-manager-%s.yaml", thirdPartyManifestDir, certmanagerVersion)
	jobsetManifest      = fmt.Sprintf("%s/jobset-%s.yaml", thirdPartyManifestDir, jobsetVersion)
	kueueManifest       = fmt.Sprintf("%s/kueue-%s.yaml", thirdPartyManifestDir, kueueVersion)
)

func WarnError(err error) {
	_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "warning: %v\n", err)
}

// Run executes the provided command within this context
func Run(cmd *exec.Cmd) (string, error) {
	dir, _ := GetProjectDir()
	cmd.Dir = dir

	if err := os.Chdir(cmd.Dir); err != nil {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "chdir dir: %q\n", err)
	}

	cmd.Env = append(os.Environ(), "GO111MODULE=on")
	command := strings.Join(cmd.Args, " ")
	_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "running: %q\n", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%q failed with error %q: %w", command, string(output), err)
	}

	return string(output), nil
}

// UninstallCertManager uninstalls the cert manager
func UninstallCertManager() {
	url := certmanagerManifest
	cmd := exec.Command("kubectl", "delete", "-f", url, "--ignore-not-found")
	if _, err := Run(cmd); err != nil {
		WarnError(err)
	}

	// Delete leftover leases in kube-system (not cleaned by default)
	kubeSystemLeases := []string{
		"cert-manager-cainjector-leader-election",
		"cert-manager-controller",
	}
	for _, lease := range kubeSystemLeases {
		cmd = exec.Command("kubectl", "delete", "lease", lease,
			"-n", "kube-system", "--ignore-not-found", "--force", "--grace-period=0")
		if _, err := Run(cmd); err != nil {
			WarnError(err)
		}
	}
}

// InstallCertManager installs the cert manager bundle.
func InstallCertManager() error {
	url := certmanagerManifest
	cmd := exec.Command("kubectl", "apply", "-f", url)
	if _, err := Run(cmd); err != nil {
		return err
	}
	// Wait for cert-manager-webhook to be ready, which can take time if cert-manager
	// was re-installed after uninstalling on a cluster.
	cmd = exec.Command("kubectl", "wait", "deployment.apps/cert-manager-webhook",
		"--for", "condition=Available",
		"--namespace", "cert-manager",
		"--timeout", "5m",
	)

	_, err := Run(cmd)
	return err
}

// IsCertManagerCRDsInstalled checks if any Cert Manager CRDs are installed
// by verifying the existence of key CRDs related to Cert Manager.
func IsCertManagerCRDsInstalled() bool {
	// List of common Cert Manager CRDs
	certManagerCRDs := []string{
		"certificates.cert-manager.io",
		"issuers.cert-manager.io",
		"clusterissuers.cert-manager.io",
		"certificaterequests.cert-manager.io",
		"orders.acme.cert-manager.io",
		"challenges.acme.cert-manager.io",
	}

	// Execute the kubectl command to get all CRDs
	cmd := exec.Command("kubectl", "get", "crds")
	output, err := Run(cmd)
	if err != nil {
		return false
	}

	// Check if any of the Cert Manager CRDs are present
	crdList := GetNonEmptyLines(output)
	for _, crd := range certManagerCRDs {
		for _, line := range crdList {
			if strings.Contains(line, crd) {
				return true
			}
		}
	}

	return false
}

func kindCommand(args ...string) *exec.Cmd {
	cluster := defaultKindCluster
	if v, ok := os.LookupEnv("KIND_CLUSTER"); ok {
		cluster = v
	}
	kindBinary := defaultKindBinary
	if v, ok := os.LookupEnv("KIND"); ok {
		kindBinary = v
	} else if projectDir, err := GetProjectDir(); err == nil {
		localKind := projectDir + "/bin/kind"
		if _, err := os.Stat(localKind); err == nil {
			kindBinary = localKind
		}
	}
	return exec.Command(kindBinary, append(args, "--name", cluster)...)
}

// LoadImageToKindClusterWithName loads a local docker image to the kind cluster
func LoadImageToKindClusterWithName(name string) error {
	_, err := Run(kindCommand("load", "docker-image", name))
	return err
}

var manifestImageRegex = regexp.MustCompile(`(?m)^\s*(?:-\s*)?image:[ \t]*["']?([^"'\s]+)["']?\s*$`)

func PreloadThirdPartyImages() {
	ginkgo.By("preloading third-party controller images into Kind")
	projectDir, err := GetProjectDir()
	if err != nil {
		WarnError(err)
		return
	}
	seen := map[string]bool{CurlImage: true}
	images := []string{CurlImage}
	for _, manifest := range []string{certmanagerManifest, jobsetManifest, kueueManifest} {
		data, err := os.ReadFile(filepath.Join(projectDir, manifest))
		if err != nil {
			WarnError(err)
			continue
		}
		for _, m := range manifestImageRegex.FindAllStringSubmatch(string(data), -1) {
			if img := m[1]; !seen[img] && !strings.Contains(img, "@") {
				seen[img] = true
				images = append(images, img)
			}
		}
	}

	var loadable []string
	for _, img := range images {
		if _, err := Run(exec.Command("docker", "image", "inspect", "--format", "{{.Id}}", img)); err != nil {
			if _, err := Run(exec.Command("docker", "pull", "-q", img)); err != nil {
				WarnError(err)
				continue
			}
		}
		loadable = append(loadable, img)
	}
	if len(loadable) == 0 {
		return
	}
	if _, err := Run(kindCommand(append([]string{"load", "docker-image"}, loadable...)...)); err != nil {
		WarnError(err)
	}
}

// GetNonEmptyLines converts given command output string into individual objects
// according to line breakers, and ignores the empty elements in it.
func GetNonEmptyLines(output string) []string {
	var res []string
	elements := strings.SplitSeq(output, "\n")
	for element := range elements {
		if element != "" {
			res = append(res, element)
		}
	}

	return res
}

// GetProjectDir will return the directory where the project is
func GetProjectDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return wd, fmt.Errorf("failed to get current working directory: %w", err)
	}
	if before, _, ok := strings.Cut(wd, "/test/e2e"); ok {
		return before, nil
	}
	return wd, nil
}

// InstallJobSet installs the JobSet operator.
func InstallJobSet() error {
	url := jobsetManifest
	cmd := exec.Command("kubectl", "apply", "--server-side", "--force-conflicts", "-f", url)
	if _, err := Run(cmd); err != nil {
		return err
	}
	cmd = exec.Command("kubectl", "wait", "deployment.apps/jobset-controller-manager",
		"--for", "condition=Available",
		"--namespace", "jobset-system",
		"--timeout", "5m",
	)
	_, err := Run(cmd)
	return err
}

// UninstallJobSet uninstalls the JobSet operator.
func UninstallJobSet() {
	url := jobsetManifest
	cmd := exec.Command("kubectl", "delete", "-f", url, "--ignore-not-found")
	if _, err := Run(cmd); err != nil {
		WarnError(err)
	}
}

// IsJobSetCRDsInstalled checks if JobSet CRDs are installed.
func IsJobSetCRDsInstalled() bool {
	cmd := exec.Command("kubectl", "get", "crds", "jobsets.jobset.x-k8s.io", "--ignore-not-found")
	output, err := Run(cmd)
	if err != nil {
		return false
	}
	return len(strings.TrimSpace(output)) > 0
}

// InstallKueue installs the Kueue operator with DRA and ProvisioningRequest feature gates disabled.
func InstallKueue() error {
	projectDir, err := GetProjectDir()
	if err != nil {
		return err
	}

	cmd := exec.Command("kubectl", "create", "ns", "kueue-system", "--dry-run=client", "-o", "yaml")
	out, err := Run(cmd)
	if err != nil {
		return err
	}
	cmd = exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(out)
	if _, err := Run(cmd); err != nil {
		return err
	}

	customConfigPath := projectDir + "/test/e2e/testdata/kueue_custom_config.yaml"
	cmd = exec.Command("kubectl", "apply", "-f", customConfigPath)
	if _, err := Run(cmd); err != nil {
		return err
	}

	url := kueueManifest
	cmd = exec.Command("kubectl", "apply", "--server-side", "--force-conflicts", "-f", url)
	if _, err := Run(cmd); err != nil {
		return err
	}

	cmd = exec.Command("kubectl", "apply", "--server-side", "--force-conflicts", "-f", customConfigPath)
	if _, err := Run(cmd); err != nil {
		return err
	}

	cmd = exec.Command("kubectl", "rollout", "restart", "deployment/kueue-controller-manager",
		"--namespace", "kueue-system",
	)
	if _, err := Run(cmd); err != nil {
		return err
	}

	cmd = exec.Command("kubectl", "rollout", "status", "deployment/kueue-controller-manager",
		"--namespace", "kueue-system",
		"--timeout", "5m",
	)
	_, err = Run(cmd)
	return err
}

// UninstallKueue uninstalls the Kueue operator.
func UninstallKueue() {
	url := kueueManifest
	cmd := exec.Command("kubectl", "delete", "-f", url, "--ignore-not-found")
	if _, err := Run(cmd); err != nil {
		WarnError(err)
	}
}

// IsKueueCRDsInstalled checks if Kueue CRDs are installed.
func IsKueueCRDsInstalled() bool {
	cmd := exec.Command("kubectl", "get", "crds", "clusterqueues.kueue.x-k8s.io", "--ignore-not-found")
	output, err := Run(cmd)
	if err != nil {
		return false
	}
	return len(strings.TrimSpace(output)) > 0
}

var (
	shouldCleanupCertManager = false
	shouldCleanupJobSet      = false
	shouldCleanupKueue       = false
)

// EnsureCertManager installs CertManager if needed for webhook tests.
// Skips installation if CERT_MANAGER_INSTALL_SKIP=true or if already present.
func EnsureCertManager() {
	ginkgo.GinkgoHelper()

	if os.Getenv("CERT_MANAGER_INSTALL_SKIP") == envValTrue {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Skipping CertManager installation (CERT_MANAGER_INSTALL_SKIP=true)\n")
		return
	}

	ginkgo.By("checking if CertManager is already installed")
	if IsCertManagerCRDsInstalled() {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "CertManager is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupCertManager = true

	ginkgo.By("installing CertManager")
	gomega.Expect(InstallCertManager()).To(gomega.Succeed(), "Failed to install CertManager")
}

// EnsureCertManagerTeardown uninstalls CertManager if it was installed by EnsureCertManager.
func EnsureCertManagerTeardown() {
	ginkgo.GinkgoHelper()

	if !shouldCleanupCertManager {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Skipping CertManager cleanup (not installed by this suite)\n")
		return
	}

	ginkgo.By("uninstalling CertManager")
	UninstallCertManager()
	shouldCleanupCertManager = false
}

// EnsureJobSet installs JobSet if not already installed and not skipped.
func EnsureJobSet() {
	ginkgo.GinkgoHelper()

	if os.Getenv("JOBSET_INSTALL_SKIP") == envValTrue {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Skipping JobSet installation (JOBSET_INSTALL_SKIP=true)\n")
		return
	}

	ginkgo.By("checking if JobSet is already installed")
	if IsJobSetCRDsInstalled() {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "JobSet is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupJobSet = true

	ginkgo.By("installing JobSet")
	gomega.Expect(InstallJobSet()).To(gomega.Succeed(), "Failed to install JobSet")
}

// EnsureJobSetTeardown uninstalls JobSet if it was installed by EnsureJobSet.
func EnsureJobSetTeardown() {
	ginkgo.GinkgoHelper()

	if !shouldCleanupJobSet {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Skipping JobSet cleanup (not installed by this suite)\n")
		return
	}

	ginkgo.By("uninstalling JobSet")
	UninstallJobSet()
	shouldCleanupJobSet = false
}

// EnsureKueue installs Kueue if not already installed and not skipped.
func EnsureKueue() {
	ginkgo.GinkgoHelper()

	if os.Getenv("KUEUE_INSTALL_SKIP") == envValTrue {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Skipping Kueue installation (KUEUE_INSTALL_SKIP=true)\n")
		return
	}

	ginkgo.By("checking if Kueue is already installed")
	if IsKueueCRDsInstalled() {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Kueue is already installed. Skipping installation.\n")
		return
	}

	// Mark for cleanup before installation to handle interruptions and partial installs.
	shouldCleanupKueue = true

	ginkgo.By("installing Kueue")
	gomega.Expect(InstallKueue()).To(gomega.Succeed(), "Failed to install Kueue")
}

// EnsureKueueTeardown uninstalls Kueue if it was installed by EnsureKueue.
func EnsureKueueTeardown() {
	ginkgo.GinkgoHelper()

	if !shouldCleanupKueue {
		_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "Skipping Kueue cleanup (not installed by this suite)\n")
		return
	}

	ginkgo.By("uninstalling Kueue")
	UninstallKueue()
	shouldCleanupKueue = false
}
