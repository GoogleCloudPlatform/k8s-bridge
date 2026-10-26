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
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

const (
	// unheldCommentPrefix starts the Slurm job comment that k8s-bridge sets when it
	// releases the hold of a translated job.
	unheldCommentPrefix = "Unheld by k8s-bridge"

	// releaseFilePrefix is the path prefix of the file that ends a gated job.
	releaseFilePrefix = "/tmp/e2e-release-"
	// gateScript is the batch script of a gated job. It runs until the test creates the
	// release file of the job. It fails after 15 minutes, so that a broken run cannot
	// leave a job that runs forever.
	gateScript = `for i in $(seq 900); do [ -e ` + releaseFilePrefix +
		`$SLURM_JOB_ID ] && exit 0; sleep 1; done; exit 1`

	slurmdContainer = "slurmd"
	pollInterval    = 2 * time.Second
)

// gatedJob is a 1-node Slurm batch job that runs until the test releases it. Its batch
// step always has a running process, so the slurmd probe passes with a local check
// also while slurmctld is down.
type gatedJob struct {
	ID         string
	JobSetName string
	NodeName   string
	PodName    string
}

// submitGatedJob submits a gated job and returns its Slurm job ID. It does not wait for
// the job to start.
func submitGatedJob() string {
	GinkgoHelper()

	By("submitting a gated batch job")
	return utils.NewBatchJobBuilder(slurmNamespace).
		WithPartition(defaultSlurmPartition).
		WithNodes(1).
		WithWrap(gateScript).
		Submit()
}

// waitForGatedJob waits until the gated job runs on its dynamic node and the startup
// probe of its slurmd container passed.
func waitForGatedJob(jobID string) gatedJob {
	GinkgoHelper()

	job := gatedJob{ID: jobID, JobSetName: "slurm-job-" + jobID}
	job.NodeName = job.JobSetName + "-slurmd-0-0"
	job.PodName = utils.WaitForWorkerPodsRunning(slurmNamespace, job.JobSetName, 1, 3*time.Minute)[0]

	By("verifying that the gated job runs on its dynamic node " + job.NodeName)
	utils.NewSlurmJobVerifier(slurmNamespace, jobID).
		WithState("RUNNING").
		WithNodeList(job.NodeName).
		WithComment(unheldCommentPrefix).
		Verify()

	By("waiting for the slurmd startup probe to pass on pod " + job.PodName)
	Eventually(func(g Gomega) {
		status := slurmdStatus(getPod(g, job.PodName))
		g.Expect(status).NotTo(BeNil(), "Pod %s has no slurmd container status", job.PodName)
		g.Expect(ptr.Deref(status.Started, false)).To(BeTrue(), "The slurmd startup probe has not passed yet")
	}, 2*time.Minute, time.Second).Should(Succeed())

	return job
}

// startGatedJob submits a gated job and waits until it runs.
func startGatedJob() gatedJob {
	GinkgoHelper()
	return waitForGatedJob(submitGatedJob())
}

// release creates the release file of the job, so that the job ends with exit code 0.
func (j gatedJob) release() {
	GinkgoHelper()

	By("releasing gated job " + j.ID)
	_, err := utils.Run(exec.Command("kubectl", "exec", "-n", slurmNamespace, j.PodName, "-c", slurmdContainer,
		"--", "touch", releaseFilePrefix+j.ID))
	Expect(err).NotTo(HaveOccurred(), "Failed to release gated job "+j.ID)
}

// workloadSnapshot is the state of a gated job that a controller failure must not change.
type workloadSnapshot struct {
	JobSetUID      string
	PodUID         string
	SlurmdRestarts int32
	SlurmJob       utils.SlurmJobState
}

// takeSnapshot records the state of a running gated job.
func takeSnapshot(job gatedJob) workloadSnapshot {
	GinkgoHelper()

	By("recording the state of gated job " + job.ID)
	var s workloadSnapshot
	var err error
	s.JobSetUID, err = utils.JobSetUID(job.JobSetName, slurmNamespace)
	Expect(err).NotTo(HaveOccurred(), "Failed to get the UID of JobSet "+job.JobSetName)
	Expect(s.JobSetUID).NotTo(BeEmpty(), "JobSet %s does not exist", job.JobSetName)

	pod := getPod(Default, job.PodName)
	s.PodUID = string(pod.UID)
	status := slurmdStatus(pod)
	Expect(status).NotTo(BeNil(), "Pod %s has no slurmd container status", job.PodName)
	s.SlurmdRestarts = status.RestartCount

	s.SlurmJob, err = utils.QuerySlurmJobState(slurmNamespace, job.ID)
	Expect(err).NotTo(HaveOccurred(), "Failed to get the state of Slurm job "+job.ID)
	Expect(s.SlurmJob.JobState).To(Equal("RUNNING"))
	Expect(s.SlurmJob.StartTime).NotTo(BeEmpty(), "Slurm job %s has no StartTime", job.ID)
	return s
}

// expectWorkloadUndisturbed checks during window that the JobSet of the job, its worker
// pod, and its Kueue Workload keep the state of the snapshot. If checkSlurm is true, it
// also checks that the Slurm job stays RUNNING. Set checkSlurm to false while slurmctld
// is down.
func expectWorkloadUndisturbed(job gatedJob, s workloadSnapshot, window time.Duration, checkSlurm bool) {
	GinkgoHelper()

	By(fmt.Sprintf("verifying for %s that the JobSet, the worker pod, and the Workload of job %s do not change",
		window, job.ID))
	Consistently(func(g Gomega) {
		js, err := utils.JobSet(job.JobSetName, slurmNamespace)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to get JobSet "+job.JobSetName)
		g.Expect(string(js.UID)).To(Equal(s.JobSetUID), "JobSet %s was created again", job.JobSetName)
		g.Expect(js.DeletionTimestamp).To(BeNil(), "JobSet %s is being deleted", job.JobSetName)
		g.Expect(ptr.Deref(js.Spec.Suspend, false)).To(BeFalse(), "JobSet %s is suspended", job.JobSetName)
		g.Expect(meta.IsStatusConditionTrue(js.Status.Conditions, string(jobsetv1alpha2.JobSetCompleted))).
			To(BeFalse(), "JobSet %s is Completed", job.JobSetName)
		g.Expect(meta.IsStatusConditionTrue(js.Status.Conditions, string(jobsetv1alpha2.JobSetFailed))).
			To(BeFalse(), "JobSet %s is Failed", job.JobSetName)

		pod := getPod(g, job.PodName)
		g.Expect(string(pod.UID)).To(Equal(s.PodUID), "Worker pod %s was replaced", job.PodName)
		g.Expect(pod.DeletionTimestamp).To(BeNil(), "Worker pod %s is being deleted", job.PodName)
		g.Expect(pod.Status.Phase).To(Equal(corev1.PodRunning), "Worker pod %s is not Running", job.PodName)
		status := slurmdStatus(pod)
		g.Expect(status).NotTo(BeNil(), "Pod %s has no slurmd container status", job.PodName)
		g.Expect(status.RestartCount).To(Equal(s.SlurmdRestarts), "The slurmd container of %s restarted", job.PodName)

		expectWorkloadAdmitted(g, s.JobSetUID)

		if checkSlurm {
			state, err := utils.QuerySlurmJobState(slurmNamespace, job.ID)
			g.Expect(err).NotTo(HaveOccurred(), "Failed to get the state of Slurm job "+job.ID)
			g.Expect(state.JobState).To(Equal("RUNNING"), "Slurm job %s is not RUNNING", job.ID)
		}
	}, window, pollInterval).Should(Succeed())
}

// expectSlurmJobUnchanged checks that the Slurm job still runs as before the failure: on
// the same nodes, with the same start time and comment, and without a restart.
func expectSlurmJobUnchanged(job gatedJob, s workloadSnapshot) {
	GinkgoHelper()

	By("verifying that Slurm did not restart, requeue, or change job " + job.ID)
	Eventually(func(g Gomega) {
		state, err := utils.QuerySlurmJobState(slurmNamespace, job.ID)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to get the state of Slurm job "+job.ID)
		g.Expect(state.JobState).To(Equal("RUNNING"), "Slurm job %s is not RUNNING", job.ID)
		g.Expect(state.NodeList).To(Equal(s.SlurmJob.NodeList), "Slurm job %s moved to other nodes", job.ID)
		g.Expect(state.StartTime).To(Equal(s.SlurmJob.StartTime), "Slurm job %s started again", job.ID)
		g.Expect(state.Restarts).To(Equal(s.SlurmJob.Restarts), "Slurm job %s was requeued", job.ID)
		g.Expect(state.Comment).To(Equal(s.SlurmJob.Comment), "The comment of Slurm job %s changed", job.ID)
	}, time.Minute, pollInterval).Should(Succeed())
}

// releaseAndVerifyCleanup releases the job, waits until it completes, and checks that
// k8s-bridge deletes its JobSet and its dynamic node.
func releaseAndVerifyCleanup(job gatedJob) {
	GinkgoHelper()

	job.release()
	utils.WaitForSlurmJobCompletion(slurmNamespace, job.ID, 2*time.Minute)

	By("verifying that k8s-bridge deletes the JobSet and the dynamic node of job " + job.ID)
	utils.VerifyJobSetNotFound(job.JobSetName, slurmNamespace)
	utils.VerifySlurmNodeNotFound(slurmNamespace, job.NodeName, 2*time.Minute)
}

// getPod reads a pod from the Slurm namespace. It reports errors through g.
func getPod(g Gomega, podName string) *corev1.Pod {
	GinkgoHelper()

	out, err := utils.Run(exec.Command("kubectl", "get", "pod", podName, "-n", slurmNamespace, "-o", "json"))
	g.Expect(err).NotTo(HaveOccurred(), "Failed to get pod "+podName)
	pod := &corev1.Pod{}
	g.Expect(json.Unmarshal([]byte(out), pod)).To(Succeed(), "Failed to parse pod "+podName)
	return pod
}

// slurmdStatus returns the status of the slurmd container, or nil if the pod has none.
func slurmdStatus(pod *corev1.Pod) *corev1.ContainerStatus {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == slurmdContainer {
			return &pod.Status.ContainerStatuses[i]
		}
	}
	return nil
}

// kueueWorkloadList holds the fields of Kueue Workloads that the specs read.
type kueueWorkloadList struct {
	Items []struct {
		Metadata metav1.ObjectMeta `json:"metadata"`
		Status   struct {
			Conditions []metav1.Condition `json:"conditions"`
		} `json:"status"`
	} `json:"items"`
}

// expectWorkloadAdmitted checks through g that the Kueue Workload of the JobSet with UID
// jobSetUID is admitted and not evicted.
func expectWorkloadAdmitted(g Gomega, jobSetUID string) {
	GinkgoHelper()

	out, err := utils.Run(exec.Command("kubectl", "get", "workloads.kueue.x-k8s.io", "-n", slurmNamespace,
		"-o", "json"))
	g.Expect(err).NotTo(HaveOccurred(), "Failed to list Kueue Workloads")
	var list kueueWorkloadList
	g.Expect(json.Unmarshal([]byte(out), &list)).To(Succeed(), "Failed to parse Kueue Workloads")

	found := false
	for _, wl := range list.Items {
		if !slices.ContainsFunc(wl.Metadata.OwnerReferences, func(owner metav1.OwnerReference) bool {
			return string(owner.UID) == jobSetUID
		}) {
			continue
		}
		found = true
		g.Expect(meta.IsStatusConditionTrue(wl.Status.Conditions, "Admitted")).To(BeTrue(),
			"Kueue Workload %s is not admitted", wl.Metadata.Name)
		g.Expect(meta.IsStatusConditionTrue(wl.Status.Conditions, "Evicted")).To(BeFalse(),
			"Kueue Workload %s is evicted", wl.Metadata.Name)
	}
	g.Expect(found).To(BeTrue(), "No Kueue Workload belongs to the JobSet with UID %s", jobSetUID)
}

// currentControllerPodName returns the name of the running controller-manager pod.
func currentControllerPodName() string {
	GinkgoHelper()

	name, err := utils.ControllerPodName(namespace)
	Expect(err).NotTo(HaveOccurred(), "Failed to get the controller-manager pod")
	return name
}

// controllerRestartCount returns the restart count of the manager container of a pod.
func controllerRestartCount(podName string) string {
	GinkgoHelper()

	count, err := utils.ContainerRestartCount(namespace, podName, utils.ControllerManagerContainer)
	Expect(err).NotTo(HaveOccurred(), "Failed to get the restart count of pod "+podName)
	return count
}

// dumpDiagnostics writes the default diagnostics, and the state of the nodes, the Slurm
// pods, and the Slurm jobs, to the Ginkgo log.
func dumpDiagnostics() {
	utils.DumpFailureDiagnostics(controllerPodName, namespace, slurmNamespace)

	for _, command := range []string{
		"kubectl get nodes -o wide",
		"kubectl get pods,pvc -n " + slurmNamespace + " -o wide",
		"kubectl describe pod " + utils.DefaultSlurmControllerPod + " -n " + slurmNamespace,
		"kubectl get jobsets.jobset.x-k8s.io,workloads.kueue.x-k8s.io -n " + slurmNamespace + " -o wide",
	} {
		args := strings.Fields(command)
		out, err := utils.Run(exec.Command(args[0], args[1:]...))
		if err != nil {
			utils.WarnError(err)
			continue
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "%s:\n%s\n", command, out)
	}

	for _, command := range []string{"scontrol ping", "squeue -a", "scontrol show nodes", "scontrol show jobs"} {
		out, err := utils.RunOnLoginNodeAsUser(slurmNamespace, utils.DefaultSlurmTestUser, command)
		if err != nil {
			utils.WarnError(err)
			continue
		}
		_, _ = fmt.Fprintf(GinkgoWriter, "%s:\n%s\n", command, out)
	}
}
