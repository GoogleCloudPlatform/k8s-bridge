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
	"os"
	"os/exec"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

// verifyMetricsEndpoint creates RBAC, generates a service account token, deploys the curl pod,
// and asserts that the metrics output contains the specified substrings on the secure HTTPS endpoint.
func verifyMetricsEndpoint(expectedMetrics ...string) {
	verifyMetricsEndpointState(expectedMetrics, nil)
}

// verifyMetricsEndpointState asserts that the secure HTTPS metrics endpoint contains all expectedPresent
// substrings and none of the expectedAbsent substrings.
func verifyMetricsEndpointState(expectedPresent, expectedAbsent []string) {
	verifyMetricsEndpointWithOptions(true, 8443, expectedPresent, expectedAbsent)
}

// verifyInsecureMetricsEndpoint deploys the curl pod to access an unauthenticated
// metrics endpoint on the specified port, and asserts that the metrics output
// contains the specified substrings.
func verifyInsecureMetricsEndpoint(port int, expectedMetrics ...string) {
	verifyMetricsEndpointWithOptions(false, port, expectedMetrics, nil)
}

func verifyMetricsEndpointWithOptions(secure bool, port int, expectedPresent, expectedAbsent []string) {
	GinkgoHelper()

	var token string
	if secure {
		By("creating a ClusterRoleBinding for the service account to allow access to metrics")
		err := createMetricsRBAC()
		Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")
		DeferCleanup(func() {
			By("cleaning up the ClusterRoleBinding (deferred)")
			cmd := exec.Command("kubectl", "delete", "clusterrolebinding", metricsRoleBindingName, "--ignore-not-found")
			_, _ = utils.Run(cmd)
		})

		By("getting the service account token")
		var errToken error
		token, errToken = serviceAccountToken()
		Expect(errToken).NotTo(HaveOccurred())
		Expect(token).NotTo(BeEmpty())
	}

	DeferCleanup(func() {
		By("cleaning up the curl pod for metrics (deferred)")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace, "--ignore-not-found")
		_, _ = utils.Run(cmd)
	})

	By("scraping and verifying expected metrics on the metrics endpoint")
	Eventually(func(g Gomega) {
		err := createCurlMetricsPodWithOptions(secure, port, token)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

		g.Eventually(func(inner Gomega) {
			cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
				"-o", "jsonpath={.status.phase}",
				"-n", namespace)
			output, runErr := utils.Run(cmd)
			inner.Expect(runErr).NotTo(HaveOccurred())
			inner.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
		}, 2*time.Minute, time.Second).Should(Succeed())

		metricsOutput, err := getMetricsOutput()
		g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
		g.Expect(metricsOutput).NotTo(BeEmpty())

		for _, expected := range expectedPresent {
			g.Expect(metricsOutput).To(ContainSubstring(expected))
		}
		for _, absent := range expectedAbsent {
			g.Expect(metricsOutput).NotTo(ContainSubstring(absent))
		}
	}, 2*time.Minute, 5*time.Second).Should(Succeed())
}

// serviceAccountToken returns a token for the specified service account in the given namespace.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	By("creating temporary file to store the token request")
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}
	defer func() {
		_ = os.Remove(tokenRequestFile)
	}()

	var out string
	verifyTokenCreation := func(g Gomega) {
		By("executing kubectl command to create the token")
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		By("parsing the JSON output to extract the token")
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

func createMetricsRBAC() error {
	cmd := exec.Command("kubectl", "delete", "clusterrolebinding", metricsRoleBindingName, "--ignore-not-found")
	_, _ = utils.Run(cmd)

	cmd = exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
		"--clusterrole=k8s-bridge-metrics-reader",
		fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
	)
	_, err := utils.Run(cmd)
	return err
}

func createCurlMetricsPodWithOptions(secure bool, port int, token string) error {
	// Ensure any existing curl-metrics pod is cleaned up before creation
	cleanupCmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace, "--ignore-not-found")
	_, _ = utils.Run(cleanupCmd)

	var curlCmd string
	if secure {
		curlCmd = fmt.Sprintf(
			"for i in $(seq 1 30); do curl -v -k -H 'Authorization: Bearer %s' "+
				"https://%s.%s.svc.cluster.local:%d/metrics && exit 0 || sleep 2; done; exit 1",
			token, metricsServiceName, namespace, port,
		)
	} else {
		curlCmd = fmt.Sprintf(
			"for i in $(seq 1 30); do curl -v "+
				"http://%s.%s.svc.cluster.local:%d/metrics && exit 0 || sleep 2; done; exit 1",
			metricsServiceName, namespace, port,
		)
	}

	cmd := exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
		"--namespace", namespace,
		"--image="+utils.CurlImage,
		"--overrides",
		fmt.Sprintf(`{
			"spec": {
				"containers": [{
					"name": "curl",
					"image": %q,
					"command": ["/bin/sh", "-c"],
					"args": [%q],
					"securityContext": {
						"allowPrivilegeEscalation": false,
						"capabilities": {
							"drop": ["ALL"]
						},
						"readOnlyRootFilesystem": true,
						"runAsNonRoot": true,
						"runAsUser": 1000,
						"seccompProfile": {
							"type": "RuntimeDefault"
						}
					}
				}],
				"serviceAccountName": "%s"
			}
		}`, utils.CurlImage, curlCmd, serviceAccountName),
	)
	_, err := utils.Run(cmd)
	return err
}

// tokenRequest represents the Kubernetes TokenRequest API response.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
