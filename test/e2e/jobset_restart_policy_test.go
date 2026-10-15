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
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

const (
	// failingSlurmdConfigPath maps workloadmixing-partition to a pod template whose slurmd
	// container exits with code 1 instead of starting slurmd. The podFailurePolicy of the
	// translated JobSet fails the child Job on that exit code, which drives the JobSet
	// failure policy without a dynamic Slurm node ever registering.
	failingSlurmdConfigPath = "test/e2e/testdata/workloadmixing_failing_slurmd.yaml"

	// defaultJobSetMaxRestarts mirrors slurmjob.DefaultJobSetMaxRestarts: the restart
	// budget of a JobSet translated from a Slurm job that may be requeued.
	defaultJobSetMaxRestarts int32 = 3

	// restartPolicyTimeout bounds the wait for a JobSet to run through all of its restart
	// cycles and for k8s-bridge to cancel the Slurm job afterwards.
	restartPolicyTimeout = 5 * time.Minute
)

var _ = Describe("JobSet restart policy", func() {
	BeforeEach(func() {
		utils.ResetSlurmAndK8sState(slurmNamespace)
	})

	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			utils.DumpFailureDiagnostics(controllerPodName, namespace, slurmNamespace)
		}
	})

	It("should not restart the JobSet after a failure when the job is submitted with --no-requeue", func() {
		By("applying the WorkloadMixing CR whose slurmd container always fails")
		utils.ApplyManifestWithCleanup(failingSlurmdConfigPath)

		By("submitting a batch job with --no-requeue")
		jobID := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition(defaultSlurmPartition).
			WithWrap("sleep 300").
			WithArgs("--no-requeue").
			Submit()
		jobSetName := fmt.Sprintf("slurm-job-%s", jobID)

		By("verifying Slurm recorded the job as not requeueable")
		verifySlurmJobRequeue(jobID, "0")

		By("verifying the translated JobSet has failurePolicy.maxRestarts=0")
		_ = waitForJobSetUID(jobSetName)
		verifyJobSetMaxRestarts(jobSetName, 0)

		By("verifying the Slurm job is cancelled after the first failure while the JobSet never restarts")
		restarts := newJobSetRestartTracker(jobSetName, 0)
		utils.NewSlurmJobVerifier(slurmNamespace, jobID).
			WithState("CANCELLED").
			WithComment("JobSet failed").
			WithTimeout(restartPolicyTimeout).
			WithInvariant(restarts.expectWithinBudget).
			Verify()
		Expect(restarts.maxObserved).To(Equal(int32(0)),
			"JobSet %s was restarted although the Slurm job was submitted with --no-requeue", jobSetName)

		By("verifying the JobSet is deleted")
		utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
	})

	It("should restart the JobSet up to 3 times after failures when the job may be requeued", func() {
		By("applying the WorkloadMixing CR whose slurmd container always fails")
		utils.ApplyManifestWithCleanup(failingSlurmdConfigPath)

		By("submitting a batch job without --no-requeue")
		jobID := utils.NewBatchJobBuilder(slurmNamespace).
			WithPartition(defaultSlurmPartition).
			WithWrap("sleep 300").
			Submit()
		jobSetName := fmt.Sprintf("slurm-job-%s", jobID)

		By("verifying Slurm recorded the job as requeueable")
		verifySlurmJobRequeue(jobID, "1")

		By(fmt.Sprintf("verifying the translated JobSet has failurePolicy.maxRestarts=%d", defaultJobSetMaxRestarts))
		_ = waitForJobSetUID(jobSetName)
		verifyJobSetMaxRestarts(jobSetName, defaultJobSetMaxRestarts)

		By(fmt.Sprintf("waiting for the JobSet to use up its %d restarts", defaultJobSetMaxRestarts))
		restarts := newJobSetRestartTracker(jobSetName, defaultJobSetMaxRestarts)
		Eventually(func(g Gomega) {
			restarts.expectWithinBudget(g)
			g.Expect(restarts.maxObserved).To(Equal(defaultJobSetMaxRestarts),
				"JobSet %s has not reached %d restarts yet", jobSetName, defaultJobSetMaxRestarts)
		}, restartPolicyTimeout, time.Second).Should(Succeed())

		By("verifying the Slurm job is cancelled once the restart budget is used up")
		utils.NewSlurmJobVerifier(slurmNamespace, jobID).
			WithState("CANCELLED").
			WithComment("JobSet failed").
			WithTimeout(restartPolicyTimeout).
			WithInvariant(restarts.expectWithinBudget).
			Verify()
		Expect(restarts.maxObserved).To(Equal(defaultJobSetMaxRestarts),
			"JobSet %s restart count mismatch", jobSetName)

		By("verifying the JobSet is deleted")
		utils.VerifyJobSetNotFound(jobSetName, slurmNamespace)
	})
})

// verifySlurmJobRequeue waits until scontrol reports Requeue=wantRequeue for the job.
func verifySlurmJobRequeue(jobID, wantRequeue string) {
	GinkgoHelper()
	Eventually(func(g Gomega) {
		js, err := utils.QuerySlurmJobState(slurmNamespace, jobID)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to get Slurm job state")
		g.Expect(js.Requeue).To(Equal(wantRequeue), "Slurm job Requeue flag mismatch")
	}, 30*time.Second, time.Second).Should(Succeed())
}

// verifyJobSetMaxRestarts asserts that the JobSet carries the given failurePolicy.maxRestarts.
func verifyJobSetMaxRestarts(jobSetName string, wantMaxRestarts int32) {
	GinkgoHelper()
	js, err := utils.JobSet(jobSetName, slurmNamespace)
	Expect(err).NotTo(HaveOccurred(), "Failed to get JobSet")
	Expect(js.Spec.FailurePolicy).NotTo(BeNil(), "JobSet %s has no failurePolicy", jobSetName)
	Expect(js.Spec.FailurePolicy.MaxRestarts).To(Equal(wantMaxRestarts), "JobSet %s maxRestarts mismatch", jobSetName)
}

// jobSetRestartTracker follows status.restarts of a JobSet across the polls of an
// Eventually. k8s-bridge deletes the JobSet as soon as it fails, so the final restart
// count cannot be read afterwards; the tracker records the highest value seen while the
// JobSet exists instead. Each restart attempt lives for several seconds (the failing
// slurmd container sleeps before it exits), so 1 s polls see every attempt.
type jobSetRestartTracker struct {
	jobSetName  string
	maxRestarts int32
	maxObserved int32
}

func newJobSetRestartTracker(jobSetName string, maxRestarts int32) *jobSetRestartTracker {
	return &jobSetRestartTracker{jobSetName: jobSetName, maxRestarts: maxRestarts}
}

// expectWithinBudget reads status.restarts, records the highest value seen, and aborts
// the surrounding poll with StopTrying as soon as the restart budget is exceeded, so that
// an unexpected restart is reported immediately instead of as a timeout. A JobSet that
// k8s-bridge already deleted is tolerated.
func (t *jobSetRestartTracker) expectWithinBudget(g Gomega) {
	out, err := utils.Run(exec.Command(
		"kubectl", "get", "jobset", t.jobSetName, "-n", slurmNamespace,
		"--ignore-not-found", "-o", "jsonpath={.status.restarts}",
	))
	g.Expect(err).NotTo(HaveOccurred(), "Failed to read status.restarts of JobSet "+t.jobSetName)
	// The field is omitted while it is 0 and once the JobSet is gone.
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return
	}
	restarts, err := strconv.ParseInt(trimmed, 10, 32)
	g.Expect(err).NotTo(HaveOccurred(), "Failed to parse status.restarts %q of JobSet %s", trimmed, t.jobSetName)
	t.maxObserved = max(t.maxObserved, int32(restarts))
	if t.maxObserved > t.maxRestarts {
		StopTrying(fmt.Sprintf("JobSet %s was restarted %d times, at most %d restarts are allowed",
			t.jobSetName, t.maxObserved, t.maxRestarts)).Now()
	}
}
