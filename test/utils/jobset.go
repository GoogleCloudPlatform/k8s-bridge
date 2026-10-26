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
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

const (
	jobsetTimeout = 3 * time.Minute
)

// JobSet fetches a JobSet by name and namespace.
func JobSet(name, ns string) (*jobsetv1alpha2.JobSet, error) {
	cmd := exec.Command("kubectl", "get", "jobset", name, "-n", ns, "-o", "json")
	jsonOutput, err := Run(cmd)
	if err != nil {
		return nil, err
	}

	var js jobsetv1alpha2.JobSet
	if err := json.Unmarshal([]byte(jsonOutput), &js); err != nil {
		return nil, err
	}
	return &js, nil
}

// VerifyJobSetNotFound asserts that a JobSet does not exist (or has been deleted).
//
// NOTE: Negative assertions verifying that a JobSet is not created must be preceded
// by a sentinel condition (such as verifying the Slurm job is CANCELLED) to prevent race
// conditions where the assertion executes before the controller has processed the job.
func VerifyJobSetNotFound(name, ns string) {
	ginkgo.GinkgoHelper()

	ginkgo.By("verifying the JobSet does not exist: " + name)
	gomega.Eventually(func() error {
		cmd := exec.Command("kubectl", "get", "jobset", name, "-n", ns)
		output, err := Run(cmd)
		if err != nil {
			if strings.Contains(output, "NotFound") || strings.Contains(output, "not found") {
				return nil
			}
			return fmt.Errorf("unexpected error checking for JobSet: %w", err)
		}
		return fmt.Errorf("JobSet %s still exists in namespace %s", name, ns)
	}, jobsetTimeout, time.Second).Should(gomega.Succeed(), "JobSet still exists or failed to check status: "+name)
}

// JobSetUID returns the UID of the named JobSet, or "" if it does not exist.
func JobSetUID(name, ns string) (string, error) {
	out, err := Run(exec.Command(
		"kubectl", "get", "jobset", name, "-n", ns, "--ignore-not-found", "-o", "jsonpath={.metadata.uid}",
	))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// JobSetRestarts returns status.restarts of the named JobSet.
// It returns 0 if the field is unset or the JobSet does not exist.
func JobSetRestarts(name, ns string) (int32, error) {
	out, err := Run(exec.Command(
		"kubectl", "get", "jobset", name, "-n", ns, "--ignore-not-found", "-o", "jsonpath={.status.restarts}",
	))
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(out)
	if s == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to parse status.restarts %q of JobSet %s: %w", s, name, err)
	}
	return int32(n), nil
}

// ExpectJobSetNotRecreated asserts that no JobSet with the given name exists
// other than the one identified by originalUID. It is meant to be called from
// inside an Eventually/Consistently body: a JobSet carrying a different UID
// means the controller re-created a workload that was deleted, which is a
// terminal verdict, so the surrounding polling is aborted immediately with
// gomega.StopTrying instead of waiting for its timeout.
func ExpectJobSetNotRecreated(g gomega.Gomega, name, ns, originalUID string) {
	uid, err := JobSetUID(name, ns)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to look up JobSet "+name)
	if uid != "" && uid != originalUID {
		gomega.StopTrying(fmt.Sprintf(
			"JobSet %s/%s was re-created by the controller after it had been deleted (uid %s, original uid %s)",
			ns, name, uid, originalUID,
		)).Now()
	}
}

// VerifyJobSetCreatedWithGPU asserts that a JobSet is created with GPU limits and tolerations.
func VerifyJobSetCreatedWithGPU(name, ns, jobID, expectedGPUCount string) {
	ginkgo.GinkgoHelper()

	ginkgo.By("verifying the JobSet is created with GPU limits and tolerations")
	verifyJobSetCreated := func(g gomega.Gomega) {
		js, err := JobSet(name, ns)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to get JobSet")

		// Verify Kueue queue label
		g.Expect(js.Labels).To(gomega.HaveKeyWithValue("kueue.x-k8s.io/queue-name", "test-local-queue"))

		// Verify ReplicatedJobs
		g.Expect(js.Spec.ReplicatedJobs).To(gomega.HaveLen(1))
		repJob := js.Spec.ReplicatedJobs[0]
		g.Expect(repJob.Name).To(gomega.Equal("slurmd"))

		// Verify PodTemplate metadata
		podTemplate := repJob.Template.Spec.Template
		g.Expect(podTemplate.Labels).To(gomega.HaveKeyWithValue("k8s-bridge.x-k8s.io/slurm-job-id", jobID))

		// Verify Tolerations for GPU
		hasGPUToleration := false
		for _, tol := range podTemplate.Spec.Tolerations {
			if tol.Key == "nvidia.com/gpu" {
				hasGPUToleration = true
				break
			}
		}
		g.Expect(hasGPUToleration).To(gomega.BeTrue(), "Missing nvidia.com/gpu toleration in JobSet pod template")

		// Verify slurmd container has GPU limits/requests
		slurmdContainer := findSlurmdContainer(&podTemplate.Spec)
		g.Expect(slurmdContainer).NotTo(gomega.BeNil(), "slurmd container not found")
		gpuQuantity, hasLimit := slurmdContainer.Resources.Limits["nvidia.com/gpu"]
		g.Expect(hasLimit).To(gomega.BeTrue(), "Missing nvidia.com/gpu limit in slurmd container")
		g.Expect(gpuQuantity.String()).To(gomega.Equal(expectedGPUCount))

		hasGresArg := false
		for _, arg := range slurmdContainer.Args {
			if strings.Contains(arg, "Gres=") {
				hasGresArg = true
				break
			}
		}
		g.Expect(hasGresArg).To(
			gomega.BeTrue(),
			fmt.Sprintf("Missing Gres= in slurmd container args: %v", slurmdContainer.Args),
		)
	}
	gomega.Eventually(verifyJobSetCreated, jobsetTimeout, time.Second).Should(gomega.Succeed())
}

// VerifyJobSetSizing asserts that the JobSet creates wantReplicas slurmd pods and
// that each slurmd container requests wantCPU CPUs and wantMemory memory.
func VerifyJobSetSizing(name, ns string, wantReplicas int32, wantCPU, wantMemory string) {
	ginkgo.GinkgoHelper()

	gomega.Eventually(func(g gomega.Gomega) {
		js, err := JobSet(name, ns)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to get JobSet")
		g.Expect(js.Spec.ReplicatedJobs).To(gomega.HaveLen(1))
		repJob := js.Spec.ReplicatedJobs[0]
		g.Expect(repJob.Replicas).To(gomega.Equal(wantReplicas), "JobSet replicas do not match the requested node count")

		slurmdContainer := findSlurmdContainer(&repJob.Template.Spec.Template.Spec)
		g.Expect(slurmdContainer).NotTo(gomega.BeNil(), "slurmd container not found")
		gotCPU := slurmdContainer.Resources.Requests.Cpu()
		g.Expect(gotCPU.Cmp(resource.MustParse(wantCPU))).To(gomega.Equal(0),
			fmt.Sprintf("slurmd CPU request is %s, want %s", gotCPU.String(), wantCPU))
		gotMemory := slurmdContainer.Resources.Requests.Memory()
		g.Expect(gotMemory.Cmp(resource.MustParse(wantMemory))).To(gomega.Equal(0),
			fmt.Sprintf("slurmd memory request is %s, want %s", gotMemory.String(), wantMemory))
	}, jobsetTimeout, time.Second).Should(gomega.Succeed())
}

// findSlurmdContainer returns the slurmd container of podSpec, or nil if there is none.
func findSlurmdContainer(podSpec *corev1.PodSpec) *corev1.Container {
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == "slurmd" {
			return &podSpec.Containers[i]
		}
	}
	return nil
}

// PatchJobSetCondition artificially merges a status condition into a JobSet's status subresource.
func PatchJobSetCondition(jobSetName, ns, condType, status, reason, message string) {
	ginkgo.GinkgoHelper()

	patch := fmt.Sprintf(
		`{"status": {"conditions": [{"type": "%s", "status": "%s", "reason": "%s", `+
			`"message": "%s", "lastTransitionTime": "%s"}]}}`,
		condType, status, reason, message, time.Now().Format(time.RFC3339),
	)
	cmd := exec.Command("kubectl", "patch", "jobset", jobSetName, "-n", ns,
		"--type=merge", "--subresource", "status", "-p", patch)
	_, err := Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to patch JobSet condition")
}

// WaitForWorkerPodsRunning polls until expectedCount worker pods created by the JobSet reach Running phase.
func WaitForWorkerPodsRunning(ns, jobSetName string, expectedCount int, timeout time.Duration) []string {
	ginkgo.GinkgoHelper()

	var podNames []string
	ginkgo.By(fmt.Sprintf("waiting for %d worker pod(s) of JobSet %s to be Running", expectedCount, jobSetName))
	gomega.Eventually(func(g gomega.Gomega) {
		cmd := exec.Command("kubectl", "get", "pods", "-n", ns,
			"-l", fmt.Sprintf("jobset.sigs.k8s.io/jobset-name=%s", jobSetName),
			"-o", "jsonpath={range .items[*]}{.metadata.name}{\" \"}{.status.phase}{\"\\n\"}{end}")
		output, err := Run(cmd)
		g.Expect(err).NotTo(gomega.HaveOccurred())

		lines := GetNonEmptyLines(output)
		g.Expect(lines).NotTo(gomega.BeEmpty(), "No worker pods found for JobSet "+jobSetName)

		var running []string
		for _, line := range lines {
			parts := strings.Fields(line)
			if len(parts) >= 2 && parts[1] == "Running" {
				running = append(running, parts[0])
			}
		}
		g.Expect(running).To(gomega.HaveLen(expectedCount),
			fmt.Sprintf("Expected %d worker pod(s) in Running phase, got %d (%v)", expectedCount, len(running), lines))
		podNames = running
	}, timeout, time.Second).Should(gomega.Succeed(), "Timed out waiting for worker pods to enter Running phase")

	return podNames
}
