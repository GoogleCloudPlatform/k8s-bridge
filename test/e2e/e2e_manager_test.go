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
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

var _ = Describe("Manager Standard Checks", func() {
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

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerRunning := func(g Gomega) {
				By("validating the pod's status")
				cmd := exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerRunning).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("validating that the metrics service is available")
			cmd := exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("ensuring the controller pod is ready")
			verifyControllerPodReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Controller pod not ready")
			}
			Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

			// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

			verifyMetricsEndpoint(
				"< HTTP/1.1 200 OK",
				"k8s_bridge_active_workload_mixings",
				"k8s_bridge_events_queue_depth",
			)
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		It("should reconcile WorkloadMixing custom resource and report metrics", func() {
			const (
				samplePath  = "config/samples/k8s-bridge_v1alpha1_workloadmixing.yaml"
				invalidName = "workloadmixing-invalid"
				missingCtrl = "non-existent-controller"
			)

			By("applying the WorkloadMixing sample CR")
			utils.ApplyManifestWithCleanup(samplePath)

			By("verifying the WorkloadMixing status condition becomes Available=True with Reason=Reconciled")
			Eventually(func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "workloadmixing", "workloadmixing-sample", "-n", slurmNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Available')].status}:"+
						"{.status.conditions[?(@.type=='Available')].reason}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True:Reconciled"))
			}, 2*time.Minute, time.Second).Should(Succeed())

			By("applying an invalid WorkloadMixing CR referencing a missing Controller")
			invalidYAML := `apiVersion: k8s-bridge.x-k8s.io/v1alpha1
kind: WorkloadMixing
metadata:
  name: ` + invalidName + `
  namespace: ` + slurmNamespace + `
spec:
  controllerRef:
    name: ` + missingCtrl + `
  partitionsMapping:
    - partition: invalid-partition
      localQueue: test-local-queue
`
			applyCmd := exec.Command("sh", "-c", "cat <<'EOF' | kubectl apply -f -\n"+invalidYAML+"EOF\n")
			_, err := utils.Run(applyCmd)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				delCmd := exec.Command("kubectl", "delete", "workloadmixing", invalidName,
					"-n", slurmNamespace, "--ignore-not-found")
				_, _ = utils.Run(delCmd)
			})

			By("verifying invalid WorkloadMixing reports Available=False with aggregated rule messages")
			Eventually(func(g Gomega) {
				statusCmd := exec.Command("kubectl", "get", "workloadmixing", invalidName, "-n", slurmNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Available')].status}:"+
						"{.status.conditions[?(@.type=='Available')].reason}")
				statusOut, runErr := utils.Run(statusCmd)
				g.Expect(runErr).NotTo(HaveOccurred())
				g.Expect(statusOut).To(Equal("False:ControllerNotFound"))

				msgCmd := exec.Command("kubectl", "get", "workloadmixing", invalidName, "-n", slurmNamespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Available')].message}")
				msgOut, runErr := utils.Run(msgCmd)
				g.Expect(runErr).NotTo(HaveOccurred())
				g.Expect(msgOut).To(ContainSubstring(`Referenced Controller "` + missingCtrl + `" not found`))
				g.Expect(msgOut).To(ContainSubstring(`No RestApi found for Controller "` + missingCtrl + `"`))
			}, 2*time.Minute, time.Second).Should(Succeed())

			By("verifying built-in and custom WorkloadMixing metrics (including availability 1 and 0)")
			verifyMetricsEndpoint(
				`controller_runtime_reconcile_total{controller="workloadmixing"`,
				"k8s_bridge_active_workload_mixings 2",
				`k8s_bridge_workload_mixing_available{namespace="slurm",workload_mixing="workloadmixing-sample"} 1`,
				`k8s_bridge_workload_mixing_available{namespace="slurm",workload_mixing="workloadmixing-invalid"} 0`,
				"k8s_bridge_tracked_partitions",
				"k8s_bridge_events_queue_depth",
			)

			By("deleting the invalid WorkloadMixing CR and verifying its availability metric is cleaned up")
			delCmd := exec.Command("kubectl", "delete", "workloadmixing", invalidName, "-n", slurmNamespace, "--wait=true")
			_, err = utils.Run(delCmd)
			Expect(err).NotTo(HaveOccurred())

			verifyMetricsEndpointState(
				[]string{
					"k8s_bridge_active_workload_mixings 1",
					`k8s_bridge_workload_mixing_available{namespace="slurm",workload_mixing="workloadmixing-sample"} 1`,
				},
				[]string{
					`workload_mixing="workloadmixing-invalid"`,
				},
			)
		})

		It("should serve metrics over insecure HTTP when configured", func() {
			By("patching the controller-manager deployment for insecure HTTP metrics on port 8080")
			patchCmd := exec.Command("kubectl", "patch", "deployment", "k8s-bridge-controller-manager",
				"-n", namespace,
				"--type=json",
				"-p", `[
					{
						"op": "replace",
						"path": "/spec/template/spec/containers/0/args",
						"value": [
							"--leader-elect",
							"--health-probe-bind-address=:8081",
							"--metrics-bind-address=:8080",
							"--metrics-secure=false"
						]
					}
				]`,
			)
			_, err := utils.Run(patchCmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to patch manager for insecure metrics")

			DeferCleanup(func() {
				By("restoring secure metrics configuration on controller-manager")
				restoreCmd := exec.Command("kubectl", "patch", "deployment", "k8s-bridge-controller-manager",
					"-n", namespace,
					"--type=json",
					"-p", `[
						{
							"op": "replace",
							"path": "/spec/template/spec/containers/0/args",
							"value": [
								"--leader-elect",
								"--health-probe-bind-address=:8081",
								"--metrics-bind-address=:8443"
							]
						}
					]`,
				)
				_, err := utils.Run(restoreCmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to restore secure metrics configuration on controller-manager")
				controllerPodName = utils.WaitForControllerManagerReady(managerImage, namespace)
			})

			By("updating the metrics service port to 8080 for insecure access")
			svcPatchCmd := exec.Command("kubectl", "patch", "service", metricsServiceName,
				"-n", namespace,
				"--type=json",
				"-p", `[
					{"op": "replace", "path": "/spec/ports/0/port", "value": 8080},
					{"op": "replace", "path": "/spec/ports/0/targetPort", "value": 8080}
				]`,
			)
			_, err = utils.Run(svcPatchCmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to patch metrics service port")

			DeferCleanup(func() {
				By("restoring metrics service port to 8443")
				restoreSvcCmd := exec.Command("kubectl", "patch", "service", metricsServiceName,
					"-n", namespace,
					"--type=json",
					"-p", `[
						{"op": "replace", "path": "/spec/ports/0/port", "value": 8443},
						{"op": "replace", "path": "/spec/ports/0/targetPort", "value": 8443}
					]`,
				)
				_, err := utils.Run(restoreSvcCmd)
				Expect(err).NotTo(HaveOccurred(), "Failed to restore metrics service port")
			})

			By("waiting for the updated controller pod to roll out and be ready")
			controllerPodName = utils.WaitForControllerManagerReady(managerImage, namespace)

			By("verifying insecure metrics endpoint via unauthenticated HTTP on port 8080")
			verifyInsecureMetricsEndpoint(
				8080,
				"< HTTP/1.1 200 OK",
				"k8s_bridge_active_workload_mixings",
				"k8s_bridge_events_queue_depth",
			)
		})
	})
})
