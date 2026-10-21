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
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	DefaultSlurmNamespace     = "slurm"
	DefaultSlurmControllerPod = "slurm-controller-0"
	DefaultSlurmctldContainer = "slurmctld"
	DefaultSlurmPartition     = "workloadmixing-partition"

	// StaticSlurmNodeName, StaticSlurmPartition and StaticSlurmNodeCPUs describe the node of the
	// static NodeSet (slurm-worker-static) in test/e2e/testdata/slurm_static_nodeset.yaml.
	StaticSlurmNodeName        = "static-0"
	StaticSlurmPartition       = "static"
	StaticSlurmNodeCPUs        = 1
	staticSlurmNodeSetManifest = "test/e2e/testdata/slurm_static_nodeset.yaml"

	// LINT.IfChange(slurm_version_pair)
	// DefaultSlinkyOperatorVersion and DefaultSlurmImageTag define the pinned,
	// immutable Slinky operator + Slurm 26.05 daemon version pair used across E2E tests.
	DefaultSlinkyOperatorVersion = "1.2.2"
	DefaultSlurmImageTag         = "26.05-ubuntu24.04"
	// LINT.ThenChange(
	//   test/e2e/testdata/slurm_operator.yaml,
	//   test/e2e/testdata/slurm_cluster.yaml:slurm_version_pair,
	//   test/e2e/testdata/slurm_login.yaml:slurm_version_pair,
	//   test/e2e/testdata/slurm_static_nodeset.yaml:slurm_version_pair,
	//   test/e2e/testdata/workloadmixing_template.yaml:slurm_version_pair,
	//   test/e2e/testdata/workloadmixing_gpu.yaml:slurm_version_pair
	// )
)

var (
	batchJobRegex = regexp.MustCompile(`Submitted batch job (\d+)`)
	// Field patterns are anchored with \b so they cannot match as a substring of
	// a longer field name. scontrol emits several such pairs -- NodeList= also
	// occurs inside ReqNodeList= and ExcNodeList=, and ExitCode= inside
	// DerivedExitCode= -- and because the match is leftmost, whichever name
	// appears first in the output would otherwise win.
	jobStateRegex   = regexp.MustCompile(`\bJobState=(\S+)`)
	reasonRegex     = regexp.MustCompile(`\bReason=(\S+)`)
	commentRegex    = regexp.MustCompile(`\bComment=(.*?)(?:\s+\w+=|$)`)
	exitCodeRegex   = regexp.MustCompile(`\bExitCode=(\S+)`)
	derivedECRegex  = regexp.MustCompile(`\bDerivedEC=(\S+)`)
	nodeListRegex   = regexp.MustCompile(`\bNodeList=(\S+)`)
	allocNodesRegex = regexp.MustCompile(`\bAllocNodes=(\S+)`)
	startTimeRegex  = regexp.MustCompile(`\bStartTime=(\S+)`)
	restartsRegex   = regexp.MustCompile(`\bRestarts=(\S+)`)
	// requeueRegex matches Slurm's "Requeue=0|1" field: 0 when the job was submitted
	// with --no-requeue (or JobRequeue=0 is configured), 1 otherwise.
	requeueRegex = regexp.MustCompile(`\bRequeue=(\S+)`)
	// userIDRegex splits Slurm's "name(uid)" rendering, e.g. UserId=foo(2000),
	// into the account name and its numeric UID.
	userIDRegex         = regexp.MustCompile(`\bUserId=([^(\s]+)\((\d+)\)`)
	nodeStateRegex      = regexp.MustCompile(`\bState=(\S+)`)
	availFeaturesRegex  = regexp.MustCompile(`\bAvailableFeatures=(\S+)`)
	activeFeaturesRegex = regexp.MustCompile(`\bActiveFeatures=(\S+)`)
	nodeReasonRegex     = regexp.MustCompile(`\bReason=(.*?)(?:\s+\[|$)`)
)

// SetupSlinkySlurm deploys the Slinky Slurm operator, JobSubmit and CliFilter Lua configmaps, and Slurm cluster.
func SetupSlinkySlurm() {
	ginkgo.GinkgoHelper()

	ginkgo.By("creating slinky namespace")
	cmd := exec.Command("kubectl", "create", "ns", "slinky", "--dry-run=client", "-o", "yaml")
	out, err := Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	cmd = exec.Command("kubectl", "apply", "--server-side", "--force-conflicts", "-f", "-")
	cmd.Stdin = strings.NewReader(out)
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("deploying Slinky slurm-operator")
	cmd = exec.Command(
		"kubectl", "apply", "--server-side", "--force-conflicts",
		"-f", "test/e2e/testdata/slurm_operator.yaml",
	)
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to deploy Slinky operator")

	ginkgo.By("waiting for Slinky operator manager to be Available")
	cmd = exec.Command("kubectl", "wait", "deployment/slurm-operator",
		"--for", "condition=Available",
		"--namespace", "slinky",
		"--timeout", "5m",
	)
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "slinky operator deployment failed to become available")

	ginkgo.By("waiting for Slinky operator webhook to be Available")
	cmd = exec.Command("kubectl", "wait", "deployment/slurm-operator-webhook",
		"--for", "condition=Available",
		"--namespace", "slinky",
		"--timeout", "5m",
	)
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "slinky operator webhook deployment failed to become available")

	ginkgo.By("waiting for Slinky operator webhook pods to be Ready")
	cmd = exec.Command("kubectl", "wait", "pod",
		"-l", "app.kubernetes.io/name=slurm-operator-webhook",
		"--for", "condition=Ready",
		"--namespace", "slinky",
		"--timeout", "1m",
	)
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "slurm-operator-webhook pods failed to become ready")

	ginkgo.By("creating slurm namespace")
	cmd = exec.Command("kubectl", "create", "ns", DefaultSlurmNamespace, "--dry-run=client", "-o", "yaml")
	out, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	cmd = exec.Command("kubectl", "apply", "--server-side", "--force-conflicts", "-f", "-")
	cmd.Stdin = strings.NewReader(out)
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("ensuring Kueue resources (LocalQueue) are deployed in slurm namespace")
	cmd = exec.Command("kubectl", "apply", "-f", "test/e2e/testdata/kueue_resources.yaml")
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to deploy Kueue resources")

	ginkgo.By("creating slurm-job-submit-lua ConfigMap from hack/job_submit.lua")
	cmd = exec.Command("kubectl", "create", "configmap", "slurm-job-submit-lua",
		"--from-file=job_submit.lua=hack/job_submit.lua",
		"--namespace", DefaultSlurmNamespace,
		"--dry-run=client", "-o", "yaml",
	)
	out, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	cmd = exec.Command("kubectl", "apply", "--server-side", "--force-conflicts", "-f", "-")
	cmd.Stdin = strings.NewReader(out)
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("creating slurm-cli-filter-lua ConfigMap from hack/cli_filter.lua")
	cmd = exec.Command("kubectl", "create", "configmap", "slurm-cli-filter-lua",
		"--from-file=cli_filter.lua=hack/cli_filter.lua",
		"--namespace", DefaultSlurmNamespace,
		"--dry-run=client", "-o", "yaml",
	)
	out, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	cmd = exec.Command("kubectl", "apply", "--server-side", "--force-conflicts", "-f", "-")
	cmd.Stdin = strings.NewReader(out)
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("deleting any pre-existing immutable Slurm auth secrets")
	cmd = exec.Command(
		"kubectl", "delete", "secret", "slurm-auth-jwt", "slurm-auth-slurm",
		"-n", DefaultSlurmNamespace, "--ignore-not-found",
	)
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	ginkgo.By("deploying Slurm Controller and RestApi CRs")
	cmd = exec.Command(
		"kubectl", "apply", "--server-side", "--force-conflicts",
		"-f", "test/e2e/testdata/slurm_cluster.yaml",
	)
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to deploy Slurm resources")

	ginkgo.By("waiting for Slurm slurmctld StatefulSet to be created")
	gomega.Eventually(func() error {
		cmd := exec.Command("kubectl", "get", "statefulset/slurm-controller", "--namespace", DefaultSlurmNamespace)
		_, err := Run(cmd)
		return err
	}, "2m", "1s").Should(gomega.Succeed(), "slurm-controller statefulset was not created")

	ginkgo.By("waiting for Slurm slurmctld StatefulSet to be Ready")
	cmd = exec.Command("kubectl", "wait", "statefulset/slurm-controller",
		"--for", "jsonpath={.status.readyReplicas}=1",
		"--namespace", DefaultSlurmNamespace,
		"--timeout", "5m",
	)
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "slurm-controller statefulset failed to become ready")

	ginkgo.By("waiting for Slurm slurmrestd Deployment to be created")
	gomega.Eventually(func() error {
		cmd := exec.Command("kubectl", "get", "deployment/slurm-restapi", "--namespace", DefaultSlurmNamespace)
		_, err := Run(cmd)
		return err
	}, "2m", "1s").Should(gomega.Succeed(), "slurm-restapi deployment was not created")

	ginkgo.By("waiting for Slurm slurmrestd Deployment to be Available")
	cmd = exec.Command("kubectl", "wait", "deployment/slurm-restapi",
		"--for", "condition=Available",
		"--namespace", DefaultSlurmNamespace,
		"--timeout", "5m",
	)
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "slurm-restapi deployment failed to become available")

	setupSlurmLoginNode()
}

func SetupStaticSlurmNode() {
	ginkgo.GinkgoHelper()

	ginkgo.By("deploying the static Slurm NodeSet")
	_, err := Run(exec.Command("kubectl", "apply", "--server-side", "--force-conflicts", "-f", staticSlurmNodeSetManifest))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to deploy the static Slurm NodeSet")

	WaitForSlurmNodeState(DefaultSlurmNamespace, StaticSlurmNodeName, "IDLE", 5*time.Minute)

	ginkgo.By(fmt.Sprintf("verifying Slurm node %s has %d CPU(s) in partition %s",
		StaticSlurmNodeName, StaticSlurmNodeCPUs, StaticSlurmPartition))
	output, err := RunOnLoginNodeAsUser(DefaultSlurmNamespace, DefaultSlurmTestUser,
		"scontrol show node "+StaticSlurmNodeName)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to query Slurm node "+StaticSlurmNodeName)
	gomega.Expect(output).To(gomega.MatchRegexp(fmt.Sprintf(`\bCPUTot=%d\b`, StaticSlurmNodeCPUs)))
	gomega.Expect(output).To(gomega.MatchRegexp(`\bPartitions=` + StaticSlurmPartition + `\b`))
}

// TeardownStaticSlurmNode deletes the static NodeSet deployed by SetupStaticSlurmNode.
func TeardownStaticSlurmNode() {
	ginkgo.GinkgoHelper()

	ginkgo.By("deleting the static Slurm NodeSet")
	cmd := exec.Command("kubectl", "delete", "-f", staticSlurmNodeSetManifest, "--ignore-not-found", "--wait=true")
	if _, err := Run(cmd); err != nil {
		WarnError(err)
	}
}

// TeardownSlinkySlurm deletes Slurm resources and namespaces.
func TeardownSlinkySlurm() {
	ginkgo.GinkgoHelper()

	teardownSlurmLoginNode()

	ginkgo.By("deleting Slurm resources")
	cmd := exec.Command("kubectl", "delete", "-f", "test/e2e/testdata/slurm_cluster.yaml", "--ignore-not-found")
	if _, err := Run(cmd); err != nil {
		WarnError(err)
	}

	ginkgo.By("deleting slurm-job-submit-lua ConfigMap")
	cmd = exec.Command("kubectl", "delete", "configmap", "slurm-job-submit-lua",
		"-n", DefaultSlurmNamespace, "--ignore-not-found")
	if _, err := Run(cmd); err != nil {
		WarnError(err)
	}

	ginkgo.By("deleting slurm-cli-filter-lua ConfigMap")
	cmd = exec.Command("kubectl", "delete", "configmap", "slurm-cli-filter-lua",
		"-n", DefaultSlurmNamespace, "--ignore-not-found")
	if _, err := Run(cmd); err != nil {
		WarnError(err)
	}

	ginkgo.By("deleting slurm namespace")
	cmd = exec.Command("kubectl", "delete", "ns", DefaultSlurmNamespace, "--ignore-not-found", "--wait=false")
	if _, err := Run(cmd); err != nil {
		WarnError(err)
	}
	waitCmd := exec.Command("kubectl", "wait", "--for=delete", "ns/"+DefaultSlurmNamespace, "--timeout=2m")
	_, _ = Run(waitCmd)

	ginkgo.By("deleting Slinky slurm-operator")
	cmd = exec.Command("kubectl", "delete", "-f", "test/e2e/testdata/slurm_operator.yaml", "--ignore-not-found")
	if _, err := Run(cmd); err != nil {
		WarnError(err)
	}

	ginkgo.By("deleting slinky namespace")
	cmd = exec.Command("kubectl", "delete", "ns", "slinky", "--ignore-not-found", "--wait=false")
	if _, err := Run(cmd); err != nil {
		WarnError(err)
	}
	waitCmd = exec.Command("kubectl", "wait", "--for=delete", "ns/slinky", "--timeout=2m")
	_, _ = Run(waitCmd)
}

// jobSetPodSelector matches every Job and Pod created by a JobSet.
const jobSetPodSelector = "jobset.sigs.k8s.io/jobset-name"

const resetDeleteTimeout = "--timeout=2m"

const resetPodGracePeriod = "--grace-period=5"

const slurmResetScript = `set -eu
dyn_nodes() { scontrol -o show nodes 2>/dev/null | sed -n 's/^NodeName=\(slurm-job-[^ ]*\).*/\1/p'; }
squeue -h -o %A | xargs -r scancel
for n in $(dyn_nodes); do scontrol update nodename="$n" state=DOWN reason=e2e-reset || true; done
timeout 60 sh -c 'while [ -n "$(squeue -h -o %A)" ]; do sleep 0.2; done' ||
  { echo "jobs still queued:" >&2; squeue >&2; exit 1; }
for n in $(dyn_nodes); do scontrol delete nodename="$n" || true; done
[ -z "$(dyn_nodes)" ] || { echo "dynamic nodes still registered: $(dyn_nodes)" >&2; exit 1; }
`

const slurmCancelScript = `squeue -h -o %A | xargs -r scancel`

func ResetSlurmAndK8sState(slurmNamespace string) {
	ginkgo.GinkgoHelper()

	restartsBefore := slurmctldRestartCount(slurmNamespace)

	ginkgo.By("deleting all WorkloadMixing resources in " + slurmNamespace)
	_, err := Run(exec.Command(
		"kubectl", "delete", "workloadmixing", "--all", "-n", slurmNamespace,
		"--ignore-not-found", "--wait=true", resetDeleteTimeout,
	))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to delete WorkloadMixing resources")

	ginkgo.By("cancelling all Slurm jobs")
	_, err = Run(exec.Command(
		"kubectl", "exec", "-n", slurmNamespace,
		DefaultSlurmControllerPod, "-c", DefaultSlurmctldContainer,
		"--", "sh", "-c", slurmCancelScript,
	))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to cancel Slurm jobs")

	ginkgo.By("deleting all JobSet resources in " + slurmNamespace)
	_, err = Run(exec.Command(
		"kubectl", "delete", "jobset", "--all", "-n", slurmNamespace,
		"--ignore-not-found", "--cascade=foreground", "--wait=true", resetDeleteTimeout,
	))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to delete JobSet resources")

	waitForJobSetPodsToTerminate(slurmNamespace)

	ginkgo.By("cancelling Slurm jobs and removing dynamic nodes")
	_, err = Run(exec.Command(
		"kubectl", "exec", "-n", slurmNamespace,
		DefaultSlurmControllerPod, "-c", DefaultSlurmctldContainer,
		"--", "sh", "-c", slurmResetScript,
	))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to reset Slurm state")

	ginkgo.By("verifying no JobSet or Kueue Workload is left in " + slurmNamespace)
	gomega.Eventually(func(g gomega.Gomega) {
		out, err := Run(exec.Command("kubectl", "get", "jobset,workload", "-n", slurmNamespace, "-o", "name"))
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(GetNonEmptyLines(out)).To(gomega.BeEmpty(), "Leftover JobSets or Workloads after reset")
	}, 2*time.Minute, time.Second).Should(gomega.Succeed())

	gomega.Expect(slurmctldRestartCount(slurmNamespace)).To(gomega.Equal(restartsBefore),
		"slurmctld container was restarted by the kubelet during reset")
}

func slurmctldRestartCount(slurmNamespace string) string {
	ginkgo.GinkgoHelper()
	out, err := Run(exec.Command(
		"kubectl", "get", "pod", DefaultSlurmControllerPod, "-n", slurmNamespace, "-o",
		fmt.Sprintf(`jsonpath={.status.containerStatuses[?(@.name=="%s")].restartCount}`, DefaultSlurmctldContainer),
	))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to read slurmctld restart count")
	return strings.TrimSpace(out)
}

// waitForJobSetPodsToTerminate removes every Job and Pod left behind by JobSets
// in slurmNamespace and returns only once the kubelet has confirmed that all of
// their containers are dead. It must run BEFORE slurmResetScript deletes the
// dynamic nodes: any slurmd still alive at that point can re-register its node
// afterwards and leak it into the next spec.
func waitForJobSetPodsToTerminate(slurmNamespace string) {
	ginkgo.GinkgoHelper()

	// JobSets that the controller deleted itself (background propagation) may
	// still have Jobs waiting for the garbage collector. Mark them for
	// foreground deletion so the Job controller stops replacing pods, but do not
	// wait here: the garbage collector would terminate the pods with their full
	// default grace period, whereas the explicit pod deletion below shortens it.
	ginkgo.By("deleting leftover JobSet Jobs in " + slurmNamespace)
	_, err := Run(exec.Command(
		"kubectl", "delete", "jobs", "-n", slurmNamespace, "-l", jobSetPodSelector,
		"--ignore-not-found", "--cascade=foreground", "--wait=false",
	))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to delete JobSet Jobs")

	ginkgo.By("deleting leftover JobSet pods in " + slurmNamespace + " and waiting for their containers to terminate")
	_, err = Run(exec.Command(
		"kubectl", "delete", "pods", "-n", slurmNamespace, "-l", jobSetPodSelector,
		"--ignore-not-found", resetPodGracePeriod, "--wait=true", resetDeleteTimeout,
	))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to delete JobSet pods")

	// A Pod object only disappears after the kubelet has reaped all of its
	// containers, so "no JobSet pods" means "no slurmd containers"; a Job in
	// foreground deletion only disappears after all of its pods have.
	ginkgo.By("verifying no JobSet Job or pod is left in " + slurmNamespace)
	gomega.Eventually(func(g gomega.Gomega) {
		out, err := Run(exec.Command(
			"kubectl", "get", "jobs,pods", "-n", slurmNamespace, "-l", jobSetPodSelector, "-o", "name",
		))
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(GetNonEmptyLines(out)).To(gomega.BeEmpty(),
			"JobSet Jobs or pods still exist after reset; a live slurmd could register a stale dynamic node")
	}, time.Minute, time.Second).Should(gomega.Succeed())
}

// SubmitBatchJob submits a job to Slurm as the given user from the login node,
// parses and returns the assigned Job ID, and registers scancel in
// DeferCleanup.
func SubmitBatchJob(slurmNamespace, username string, args ...string) string {
	ginkgo.GinkgoHelper()

	output, err := RunOnLoginNodeAsUser(slurmNamespace, username, "sbatch "+ShellQuote(args))
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to submit batch job as "+username)
	gomega.Expect(output).To(gomega.ContainSubstring("Submitted batch job"))

	matches := batchJobRegex.FindStringSubmatch(output)
	gomega.Expect(matches).To(gomega.HaveLen(2), "Failed to parse Job ID from output: "+output)
	jobID := matches[1]
	gomega.Expect(jobID).NotTo(gomega.BeEmpty())

	ginkgo.DeferCleanup(func() {
		// The submitting user owns the job, so it may cancel it itself.
		// Ignoring failures here because the job might have already been cleaned up.
		_, _ = RunOnLoginNodeAsUser(slurmNamespace, username, "scancel "+jobID)
	})

	// Confirm Slurm recorded the job against the user we submitted as. For the
	// default unprivileged user this is the property the whole login-node path
	// exists to provide (job_submit.lua exempts root), and without the check a
	// regression to root submission would quietly weaken every spec that
	// depends on it rather than failing outright.
	NewSlurmJobVerifier(slurmNamespace, jobID).
		WithUser(username).
		WithTimeout(30 * time.Second).
		Verify()

	return jobID
}

// Slurm job comment markers. job_submit.lua adds k8sBridgeAutoHoldMarker when it holds a job.
// The k8s-bridge controller replaces the marker with k8sBridgeUnheldComment when it releases the hold.
const (
	k8sBridgeAutoHoldMarker = "k8s-bridge-auto-hold"
	k8sBridgeUnheldComment  = "Unheld by k8s-bridge"
)

// ReleaseSlurmJob releases an explicit hold on a Slurm job as DefaultSlurmTestUser.
// TODO: Restore checking for exit code == 0 on `scontrol release` once SchedMD fixes
// the exit code 1 bug in update_job / job_limits_check when the dynamic partition
// has 0 registered nodes (currently ESLURM_TOO_MANY_REQUESTED_CPUS is an unimplemented
// FIXME in job_limits_check that falls through to ESLURM_REQUESTED_PART_CONFIG_UNAVAILABLE,
// even when EnforcePartLimits=NO and job_submit.lua has already transitioned the job
// to k8s-bridge-auto-hold).
func ReleaseSlurmJob(slurmNamespace, jobID string) {
	ginkgo.GinkgoHelper()
	_, _ = RunOnLoginNodeAsUser(slurmNamespace, DefaultSlurmTestUser, "scontrol release "+jobID)
	gomega.Eventually(func(g gomega.Gomega) {
		js, err := QuerySlurmJobState(slurmNamespace, jobID)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to get Slurm job state")
		g.Expect(js.Comment).To(gomega.Or(
			gomega.ContainSubstring(k8sBridgeAutoHoldMarker),
			gomega.ContainSubstring(k8sBridgeUnheldComment),
		), "Slurm job %s was not handed over to k8s-bridge after release", jobID)
	}, 30*time.Second, time.Second).Should(gomega.Succeed())
}

// SubmitBatchJobExpectError submits an sbatch command as the given user and
// expects an error (e.g. rejected by cli_filter.lua or job_submit.lua).
func SubmitBatchJobExpectError(slurmNamespace, username string, args ...string) string {
	ginkgo.GinkgoHelper()

	output, err := RunOnLoginNodeAsUser(slurmNamespace, username, "sbatch "+ShellQuote(args))
	gomega.Expect(err).To(gomega.HaveOccurred(), "Expected sbatch submission to fail")
	return output
}

// jobSubmitTimeout bounds interactive Slurm submissions run through kubectl exec:
// salloc and srun block until the allocation is granted or refused, so without a
// deadline a stuck submission would hang the whole suite instead of failing.
const jobSubmitTimeout = 2 * time.Minute

// SubmitAllocJobExpectError submits an salloc command as the given user and expects an error.
func SubmitAllocJobExpectError(slurmNamespace, username string, args ...string) string {
	ginkgo.GinkgoHelper()
	ctx, cancel := context.WithTimeout(context.Background(), jobSubmitTimeout)
	defer cancel()

	output, err := RunOnLoginNodeAsUserWithContext(ctx, slurmNamespace, username, "salloc "+ShellQuote(args))
	gomega.Expect(ctx.Err()).NotTo(gomega.HaveOccurred(), "salloc submission timed out")
	gomega.Expect(err).To(gomega.HaveOccurred(), "Expected salloc submission to fail")
	return output
}

// SubmitRunJobExpectError submits an srun command as the given user and expects an error.
func SubmitRunJobExpectError(slurmNamespace, username string, args ...string) string {
	ginkgo.GinkgoHelper()
	ctx, cancel := context.WithTimeout(context.Background(), jobSubmitTimeout)
	defer cancel()

	output, err := RunOnLoginNodeAsUserWithContext(ctx, slurmNamespace, username, "srun "+ShellQuote(args))
	gomega.Expect(ctx.Err()).NotTo(gomega.HaveOccurred(), "srun submission timed out")
	gomega.Expect(err).To(gomega.HaveOccurred(), "Expected srun submission to fail")
	return output
}

// BatchJobBuilder constructs arguments for an sbatch command and executes submission.
type BatchJobBuilder struct {
	slurmNamespace string
	user           string
	partition      string
	wrap           string
	nodes          int
	cpusPerTask    int
	mem            int
	gres           string
	hold           bool
	extraArgs      []string
}

// NewBatchJobBuilder returns a builder that submits as DefaultSlurmTestUser.
// Use WithUser to submit as somebody else, for example to exercise behaviour
// that differs for privileged accounts.
func NewBatchJobBuilder(slurmNamespace string) *BatchJobBuilder {
	return &BatchJobBuilder{
		slurmNamespace: slurmNamespace,
		user:           DefaultSlurmTestUser,
	}
}

// WithUser submits the job as the given user on the login node. The user must
// exist in the account database mounted onto the login pod.
func (b *BatchJobBuilder) WithUser(username string) *BatchJobBuilder {
	b.user = username
	return b
}

func (b *BatchJobBuilder) WithPartition(partition string) *BatchJobBuilder {
	b.partition = partition
	return b
}

func (b *BatchJobBuilder) WithWrap(wrap string) *BatchJobBuilder {
	b.wrap = wrap
	return b
}

func (b *BatchJobBuilder) WithNodes(nodes int) *BatchJobBuilder {
	b.nodes = nodes
	return b
}

func (b *BatchJobBuilder) WithCpusPerTask(cpus int) *BatchJobBuilder {
	b.cpusPerTask = cpus
	return b
}

func (b *BatchJobBuilder) WithMem(mem int) *BatchJobBuilder {
	b.mem = mem
	return b
}

func (b *BatchJobBuilder) WithGres(gres string) *BatchJobBuilder {
	b.gres = gres
	return b
}

func (b *BatchJobBuilder) WithHold() *BatchJobBuilder {
	b.hold = true
	return b
}

func (b *BatchJobBuilder) WithArgs(args ...string) *BatchJobBuilder {
	b.extraArgs = append(b.extraArgs, args...)
	return b
}

func (b *BatchJobBuilder) BuildArgs() []string {
	var args []string
	if b.hold {
		args = append(args, "--hold")
	}
	if b.partition != "" {
		args = append(args, fmt.Sprintf("--partition=%s", b.partition))
	}
	if b.nodes > 0 {
		args = append(args, fmt.Sprintf("--nodes=%d", b.nodes))
	}
	if b.cpusPerTask > 0 {
		args = append(args, fmt.Sprintf("--cpus-per-task=%d", b.cpusPerTask))
	}
	if b.mem > 0 {
		args = append(args, fmt.Sprintf("--mem=%d", b.mem))
	}
	if b.gres != "" {
		args = append(args, fmt.Sprintf("--gres=%s", b.gres))
	}
	if b.wrap != "" {
		args = append(args, fmt.Sprintf("--wrap=%s", b.wrap))
	}
	args = append(args, b.extraArgs...)
	return args
}

func (b *BatchJobBuilder) Submit() string {
	ginkgo.GinkgoHelper()
	return SubmitBatchJob(b.slurmNamespace, b.user, b.BuildArgs()...)
}

func (b *BatchJobBuilder) SubmitExpectError(expectedSubstrings ...string) string {
	ginkgo.GinkgoHelper()
	output := SubmitBatchJobExpectError(b.slurmNamespace, b.user, b.BuildArgs()...)
	for _, expected := range expectedSubstrings {
		gomega.Expect(output).To(gomega.ContainSubstring(expected))
	}
	return output
}

type SlurmJobState struct {
	JobState   string
	Reason     string
	Comment    string
	ExitCode   string
	DerivedEC  string
	NodeList   string
	AllocNodes string
	// StartTime and Restarts change when Slurm starts the job again, for
	// example after a requeue.
	StartTime string
	Restarts  string
	// Requeue is Slurm's "Requeue=0|1" flag: "0" when the job may not be requeued
	// (sbatch --no-requeue or JobRequeue=0), "1" otherwise.
	Requeue string
	// UserName and UserID are the job's owner, parsed from Slurm's
	// "UserId=name(uid)" rendering.
	UserName string
	UserID   int
}

func extractRegexField(re *regexp.Regexp, s string) string {
	m := re.FindStringSubmatch(s)
	if len(m) == 2 {
		return m[1]
	}
	return ""
}

func extractRegexComment(re *regexp.Regexp, s string) string {
	m := re.FindStringSubmatch(s)
	if len(m) == 2 {
		return strings.TrimSpace(m[1])
	}
	return ""
}

// extractJobOwner parses "UserId=name(uid)" and returns an error if the field
// is missing or the UID cannot be parsed.
func extractJobOwner(s string) (string, int, error) {
	m := userIDRegex.FindStringSubmatch(s)
	if len(m) != 3 {
		return "", -1, fmt.Errorf("failed to parse UserId from scontrol output: %q", s)
	}
	uid, err := strconv.Atoi(m[2])
	if err != nil {
		return m[1], -1, fmt.Errorf("failed to parse UID %q from UserId field: %w", m[2], err)
	}
	return m[1], uid, nil
}

// QuerySlurmJobState reads a job's state with scontrol.
//
// scontrol show job is a user-level query, so it runs as DefaultSlurmTestUser
// on the login node rather than as root inside slurmctld.
func QuerySlurmJobState(slurmNamespace, jobID string) (SlurmJobState, error) {
	output, err := RunOnLoginNodeAsUser(slurmNamespace, DefaultSlurmTestUser, "scontrol show job "+jobID)
	if err != nil {
		return SlurmJobState{}, err
	}

	userName, userID, err := extractJobOwner(output)
	if err != nil {
		return SlurmJobState{}, err
	}

	return SlurmJobState{
		JobState:   extractRegexField(jobStateRegex, output),
		Reason:     extractRegexField(reasonRegex, output),
		Comment:    extractRegexComment(commentRegex, output),
		ExitCode:   extractRegexField(exitCodeRegex, output),
		DerivedEC:  extractRegexField(derivedECRegex, output),
		NodeList:   extractRegexField(nodeListRegex, output),
		AllocNodes: extractRegexField(allocNodesRegex, output),
		StartTime:  extractRegexField(startTimeRegex, output),
		Restarts:   extractRegexField(restartsRegex, output),
		Requeue:    extractRegexField(requeueRegex, output),
		UserName:   userName,
		UserID:     userID,
	}, nil
}

const defaultSlurmJobTimeout = 3 * time.Minute

type SlurmJobVerifier struct {
	slurmNamespace   string
	jobID            string
	expectedState    string
	expectedReason   string
	expectedComment  string
	expectedUserName string
	expectedNodeList string
	timeout          time.Duration
	invariants       []func(g gomega.Gomega)
}

func NewSlurmJobVerifier(slurmNamespace, jobID string) *SlurmJobVerifier {
	return &SlurmJobVerifier{
		slurmNamespace: slurmNamespace,
		jobID:          jobID,
		timeout:        defaultSlurmJobTimeout,
	}
}

func (v *SlurmJobVerifier) WithState(state string) *SlurmJobVerifier {
	v.expectedState = state
	return v
}

func (v *SlurmJobVerifier) WithReason(reason string) *SlurmJobVerifier {
	v.expectedReason = reason
	return v
}

func (v *SlurmJobVerifier) WithComment(comment string) *SlurmJobVerifier {
	v.expectedComment = comment
	return v
}

// WithUser expects the job to be owned by the given user, i.e. the name in
// Slurm's UserId=name(uid). The account's UID is not asserted here: the mapping
// from name to UID is a property of the account database, which SetupSlinkySlurm
// verifies once on the login node rather than on every job.
func (v *SlurmJobVerifier) WithUser(username string) *SlurmJobVerifier {
	v.expectedUserName = username
	return v
}

func (v *SlurmJobVerifier) WithNodeList(nodeList string) *SlurmJobVerifier {
	v.expectedNodeList = nodeList
	return v
}

func (v *SlurmJobVerifier) WithTimeout(timeout time.Duration) *SlurmJobVerifier {
	v.timeout = timeout
	return v
}

// WithInvariant registers a check that runs before every poll of Verify, for
// conditions that must hold during the whole wait rather than only at its end
// (e.g. "the JobSet is never re-created"). The check may call
// gomega.StopTrying to abort the wait as soon as the invariant is broken.
func (v *SlurmJobVerifier) WithInvariant(check func(g gomega.Gomega)) *SlurmJobVerifier {
	v.invariants = append(v.invariants, check)
	return v
}

func (v *SlurmJobVerifier) Verify() {
	ginkgo.GinkgoHelper()
	timeout := v.timeout
	if timeout == 0 {
		timeout = defaultSlurmJobTimeout
	}
	gomega.Eventually(func(g gomega.Gomega) {
		for _, check := range v.invariants {
			check(g)
		}
		js, err := QuerySlurmJobState(v.slurmNamespace, v.jobID)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to get Slurm job state")
		if v.expectedState != "" {
			g.Expect(js.JobState).To(gomega.Equal(v.expectedState), "Slurm job state mismatch")
		}
		if v.expectedReason != "" {
			g.Expect(js.Reason).To(gomega.Equal(v.expectedReason), "Slurm job reason mismatch")
		}
		if v.expectedComment != "" {
			g.Expect(js.Comment).To(gomega.ContainSubstring(v.expectedComment), "Slurm job comment mismatch")
		}
		if v.expectedUserName != "" {
			g.Expect(js.UserName).To(gomega.Equal(v.expectedUserName), "Slurm job owner mismatch")
		}
		if v.expectedNodeList != "" {
			g.Expect(js.NodeList).To(gomega.Equal(v.expectedNodeList), "Slurm job NodeList mismatch")
		}
	}, timeout, time.Second).Should(gomega.Succeed())
}

func VerifySlurmJobCancelled(slurmNamespace, jobID, expectedCommentSubstr string) {
	ginkgo.GinkgoHelper()
	NewSlurmJobVerifier(slurmNamespace, jobID).
		WithState("CANCELLED").
		WithComment(expectedCommentSubstr).
		Verify()
}

func VerifySlurmJobCompleted(slurmNamespace, jobID string) {
	ginkgo.GinkgoHelper()
	NewSlurmJobVerifier(slurmNamespace, jobID).
		WithState("COMPLETED").
		Verify()
}

func VerifySlurmJobReason(slurmNamespace, jobID, expectedReason string) {
	ginkgo.GinkgoHelper()
	NewSlurmJobVerifier(slurmNamespace, jobID).
		WithReason(expectedReason).
		Verify()
}

// SlurmNodeState captures parsed attributes of a Slurm node from scontrol show node.
type SlurmNodeState struct {
	NodeName          string
	State             string
	AvailableFeatures string
	ActiveFeatures    string
	Reason            string
}

// QuerySlurmNodeState reads a node's state with scontrol.
func QuerySlurmNodeState(slurmNamespace, nodeName string) (SlurmNodeState, error) {
	output, err := RunOnLoginNodeAsUser(slurmNamespace, DefaultSlurmTestUser, "scontrol show node "+nodeName)
	if err != nil {
		return SlurmNodeState{}, err
	}

	return SlurmNodeState{
		NodeName:          nodeName,
		State:             extractRegexField(nodeStateRegex, output),
		AvailableFeatures: extractRegexField(availFeaturesRegex, output),
		ActiveFeatures:    extractRegexField(activeFeaturesRegex, output),
		Reason:            extractRegexComment(nodeReasonRegex, output),
	}, nil
}

// WaitForSlurmNodeState polls until the node enters expectedState and asserts it is not unexpectedly down.
func WaitForSlurmNodeState(slurmNamespace, nodeName, expectedState string, timeout time.Duration) {
	ginkgo.GinkgoHelper()
	ginkgo.By(fmt.Sprintf("waiting for Slurm node %s to enter state %s", nodeName, expectedState))

	gomega.Eventually(func(g gomega.Gomega) {
		ns, err := QuerySlurmNodeState(slurmNamespace, nodeName)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to query Slurm node state for "+nodeName)
		g.Expect(strings.ToUpper(ns.State)).To(gomega.ContainSubstring(strings.ToUpper(expectedState)),
			fmt.Sprintf("Node %s state mismatch: got %s, expected %s", nodeName, ns.State, expectedState))
		g.Expect(ns.Reason).NotTo(gomega.ContainSubstring("Node unexpected socket down"),
			fmt.Sprintf("Node %s is down unexpectedly: %s", nodeName, ns.Reason))
	}, timeout, time.Second).Should(gomega.Succeed())
}

// VerifySlurmNodeNotFound asserts that a dynamic node does not exist in Slurm.
func VerifySlurmNodeNotFound(slurmNamespace, nodeName string, timeout time.Duration) {
	ginkgo.GinkgoHelper()
	ginkgo.By(fmt.Sprintf("verifying Slurm dynamic node %s is removed", nodeName))

	gomega.Eventually(func() error {
		output, err := RunOnLoginNodeAsUser(slurmNamespace, DefaultSlurmTestUser, "scontrol show node "+nodeName)
		if err != nil || strings.Contains(output, "not found") || strings.Contains(output, "No nodes in the cluster") {
			return nil
		}
		return fmt.Errorf("node %s still exists in Slurm:\n%s", nodeName, output)
	}, timeout, time.Second).Should(gomega.Succeed(), "Node "+nodeName+" was not removed from Slurm")
}

// WaitForSlurmJobCompletion asserts that a Slurm job reaches COMPLETED state with exit code 0:0.
func WaitForSlurmJobCompletion(slurmNamespace, jobID string, timeout time.Duration) {
	ginkgo.GinkgoHelper()
	ginkgo.By(fmt.Sprintf("waiting for Slurm job %s to reach COMPLETED state", jobID))

	gomega.Eventually(func(g gomega.Gomega) {
		js, err := QuerySlurmJobState(slurmNamespace, jobID)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to query Slurm job state for job "+jobID)
		g.Expect(js.JobState).To(gomega.Equal("COMPLETED"), "Slurm job state mismatch")
		if js.ExitCode != "" {
			g.Expect(js.ExitCode).To(gomega.Equal("0:0"), "Slurm job exit code non-zero")
		}
	}, timeout, time.Second).Should(gomega.Succeed())
}

// GetSlurmJobOutput reads the batch output file from the working directory on the login node.
func GetSlurmJobOutput(slurmNamespace, jobID string) (string, error) {
	cmd := fmt.Sprintf(
		"cat /tmp/slurm-%s.out 2>/dev/null || cat /var/spool/slurm/slurm-%s.out 2>/dev/null || true",
		jobID, jobID,
	)
	return RunOnLoginNodeAsUser(slurmNamespace, DefaultSlurmTestUser, cmd)
}

// VerifySlurmdNotKilledByProbes asserts that the slurmd container of podName did
// not restart and that no failed probe killed it. Call it after the Slurm job
// ends. If the pod is already deleted, it checks only the pod events.
func VerifySlurmdNotKilledByProbes(slurmNamespace, podName string) {
	ginkgo.GinkgoHelper()
	ginkgo.By(fmt.Sprintf("verifying slurmd probes did not restart or kill the container on pod %s", podName))
	evidence := readSlurmdProbeEvidence(gomega.Default, slurmNamespace, podName)
	if evidence.restartCount != "" {
		expectNoSlurmdRestart(gomega.Default, evidence)
	}
	expectSlurmdNotKilledByProbes(gomega.Default, evidence)
}

// VerifySlurmdProbesStayHealthy asserts that the slurmd probes on podNames keep
// passing for two probe periods. Call it while Slurm job jobID is RUNNING on the
// pods. The job must stay RUNNING until the check ends.
func VerifySlurmdProbesStayHealthy(slurmNamespace, jobID string, probePeriod time.Duration, podNames ...string) {
	ginkgo.GinkgoHelper()

	for _, podName := range podNames {
		ginkgo.By(fmt.Sprintf("waiting for the slurmd startupProbe to pass on pod %s", podName))
		gomega.Eventually(func(g gomega.Gomega) {
			cmd := exec.Command("kubectl", "get", "pod", podName, "-n", slurmNamespace,
				"-o", "jsonpath={.status.containerStatuses[?(@.name=='slurmd')].started}")
			started, err := Run(cmd)
			g.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to get slurmd container status of pod "+podName)
			g.Expect(strings.TrimSpace(started)).To(gomega.Equal("true"),
				fmt.Sprintf("slurmd startupProbe has not passed on pod %s yet", podName))
		}, time.Minute, time.Second).Should(gomega.Succeed())
	}

	// observationWindow leaves room for a livenessProbe to start
	// (within one period) and finish (within its 5s timeout) inside the window.
	observationWindow := 2 * probePeriod
	ginkgo.By(fmt.Sprintf("verifying slurmd probes keep passing on pods %s for %s while Slurm job %s runs",
		strings.Join(podNames, ", "), observationWindow, jobID))
	gomega.Consistently(func(g gomega.Gomega) {
		evidence := make([]slurmdProbeEvidence, 0, len(podNames))
		for _, podName := range podNames {
			evidence = append(evidence, readSlurmdProbeEvidence(g, slurmNamespace, podName))
		}

		js, err := QuerySlurmJobState(slurmNamespace, jobID)
		g.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to query Slurm job state for job "+jobID)
		g.Expect(js.JobState).To(gomega.Equal("RUNNING"), fmt.Sprintf(
			"Slurm job %s stopped running before the probe observation window ended. Make the job run longer.", jobID))
		for _, e := range evidence {
			expectNoSlurmdRestart(g, e)
			expectSlurmdNotKilledByProbes(g, e)
			expectNoSlurmdLivenessFailure(g, e)
		}
	}, observationWindow, 2*time.Second).Should(gomega.Succeed())
}

// slurmdProbeEvidence holds what the kubelet reported about the probes of the
// slurmd container of one pod. restartCount is empty if the pod does not exist,
// for example because it was deleted after the job ended.
type slurmdProbeEvidence struct {
	podName         string
	restartCount    string
	killingEvents   string
	unhealthyEvents string
}

// readSlurmdProbeEvidence reads the restart count of the slurmd container of
// podName and the messages of the Killing and Unhealthy events of the pod.
// It asserts through g that each kubectl call succeeds.
func readSlurmdProbeEvidence(g gomega.Gomega, slurmNamespace, podName string) slurmdProbeEvidence {
	ginkgo.GinkgoHelper()
	evidence := slurmdProbeEvidence{podName: podName}
	cmd := exec.Command("kubectl", "get", "pod", podName, "-n", slurmNamespace, "--ignore-not-found",
		"-o", "jsonpath={.status.containerStatuses[?(@.name=='slurmd')].restartCount}")
	out, err := Run(cmd)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to get slurmd restart count of pod "+podName)
	evidence.restartCount = strings.TrimSpace(out)
	evidence.killingEvents = podEventMessages(g, slurmNamespace, podName, "Killing")
	evidence.unhealthyEvents = podEventMessages(g, slurmNamespace, podName, "Unhealthy")
	return evidence
}

// podEventMessages returns the messages of the events with the given reason
// for podName. It asserts through g that kubectl succeeds.
func podEventMessages(g gomega.Gomega, slurmNamespace, podName, reason string) string {
	ginkgo.GinkgoHelper()
	cmd := exec.Command("kubectl", "get", "events", "-n", slurmNamespace,
		"--field-selector", fmt.Sprintf("involvedObject.name=%s,reason=%s", podName, reason),
		"-o", "jsonpath={.items[*].message}")
	out, err := Run(cmd)
	g.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to get %s events of pod %s", reason, podName))
	return strings.TrimSpace(out)
}

// expectNoSlurmdRestart asserts through g that the slurmd container has a
// restart count of 0.
func expectNoSlurmdRestart(g gomega.Gomega, evidence slurmdProbeEvidence) {
	ginkgo.GinkgoHelper()
	g.Expect(evidence.restartCount).To(gomega.Equal("0"),
		fmt.Sprintf("Expected slurmd container on pod %s to have 0 restarts", evidence.podName))
}

// expectSlurmdNotKilledByProbes asserts through g that the kubelet did not kill
// the slurmd container because a probe failed.
func expectSlurmdNotKilledByProbes(g gomega.Gomega, evidence slurmdProbeEvidence) {
	ginkgo.GinkgoHelper()
	g.Expect(evidence.killingEvents).NotTo(gomega.ContainSubstring("failed liveness probe"),
		fmt.Sprintf("Pod %s slurmd container failed liveness probe: %s", evidence.podName, evidence.killingEvents))
	g.Expect(evidence.killingEvents).NotTo(gomega.ContainSubstring("failed startup probe"),
		fmt.Sprintf("Pod %s slurmd container failed startup probe: %s", evidence.podName, evidence.killingEvents))
}

// expectNoSlurmdLivenessFailure asserts through g that the slurmd container did
// not report a failed livenessProbe.
func expectNoSlurmdLivenessFailure(g gomega.Gomega, evidence slurmdProbeEvidence) {
	ginkgo.GinkgoHelper()
	g.Expect(evidence.unhealthyEvents).NotTo(gomega.ContainSubstring("Liveness probe failed"),
		fmt.Sprintf("Pod %s slurmd container reported unhealthy liveness probe: %s",
			evidence.podName, evidence.unhealthyEvents))
}
