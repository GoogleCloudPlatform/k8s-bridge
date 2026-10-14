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
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

type interactiveSessionKind string

const (
	sessionSalloc           interactiveSessionKind = "salloc"
	sessionSrunPty          interactiveSessionKind = "srun --pty"
	interactiveJobIDTimeout                        = 30 * time.Second
)

// salloc skips "Pending job allocation" when the allocation is granted immediately, so match "Granted" too.
var interactiveJobIDRegex = regexp.MustCompile(`(?m)^(?:salloc: (?:Pending|Granted) job allocation|srun: job) (\d+)`)

// InteractiveSession is an salloc or srun --pty session held open on the login node as an unprivileged user.
type InteractiveSession struct {
	slurmNamespace string
	user           string
	kind           interactiveSessionKind
	tag            string
	outputFile     string
	exitSentinel   string
	jobID          string
}

func newInteractiveSession(slurmNamespace, user string, kind interactiveSessionKind) *InteractiveSession {
	prefix := "e2e-salloc"
	if kind == sessionSrunPty {
		prefix = "e2e-srun-pty"
	}
	tag := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	return &InteractiveSession{
		slurmNamespace: slurmNamespace,
		user:           user,
		kind:           kind,
		tag:            tag,
		outputFile:     "/tmp/" + tag + ".out",
		exitSentinel:   "/tmp/" + tag + ".exit",
	}
}

// StartSallocSession opens `salloc <sallocArgs>` on the login node as user and runs commands inside it.
func StartSallocSession(slurmNamespace, user string, sallocArgs []string, commands string) *InteractiveSession {
	ginkgo.GinkgoHelper()
	s := newInteractiveSession(slurmNamespace, user, sessionSalloc)

	argv := append([]string{"salloc"}, sallocArgs...)
	argv = append(argv, "bash", "-c", s.sessionScript(commands))
	s.launch(ShellQuote(argv))
	return s
}

// StartSrunPtySession opens `srun --pty <srunArgs>` on the login node as user and runs commands inside it.
func StartSrunPtySession(slurmNamespace, user string, srunArgs []string, commands string) *InteractiveSession {
	ginkgo.GinkgoHelper()
	s := newInteractiveSession(slurmNamespace, user, sessionSrunPty)

	argv := append([]string{"srun", "--pty"}, srunArgs...)
	argv = append(argv, "bash", "-c", s.sessionScript(commands))
	// srun disregards --pty unless stdin is a tty, so run the client under a pseudo-terminal from script(1).
	s.launch(ShellQuote([]string{"script", "-qefc", ShellQuote(argv), "/dev/null"}))
	return s
}

func (s *InteractiveSession) sessionScript(commands string) string {
	return fmt.Sprintf("%s; until [ -e %s ]; do sleep 1; done", commands, s.exitSentinel)
}

func (s *InteractiveSession) launch(clientCmd string) {
	ginkgo.GinkgoHelper()
	ginkgo.By(fmt.Sprintf("starting a detached %s session on the login node as %s", s.kind, s.user))
	launcher := fmt.Sprintf("rm -f %s %s; nohup setsid %s >%s 2>&1 </dev/null &",
		s.outputFile, s.exitSentinel, clientCmd, s.outputFile)
	_, err := RunOnLoginNodeAsUser(s.slurmNamespace, s.user, launcher)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), fmt.Sprintf("Failed to start %s session on the login node", s.kind))

	ginkgo.DeferCleanup(s.cleanup)
}

func (s *InteractiveSession) cleanup() {
	_, _ = fmt.Fprintf(ginkgo.GinkgoWriter, "%s session %s output:\n%s\n", s.kind, s.tag, s.Output())

	_, _ = RunOnLoginNodeAsUser(s.slurmNamespace, s.user, "touch "+s.exitSentinel)
	if s.jobID != "" {
		_, _ = RunOnLoginNodeAsUser(s.slurmNamespace, s.user, "scancel "+s.jobID)
	}
	// Bracketing the first character keeps pkill from matching the shell running it.
	pkillPattern := fmt.Sprintf("[%s]%s", s.tag[:1], s.tag[1:])
	_, _ = RunOnLoginNodeAsUser(s.slurmNamespace, s.user, "pkill -f "+pkillPattern)
	_, _ = RunOnLoginNodeAsUser(s.slurmNamespace, s.user, fmt.Sprintf("rm -f %s %s", s.outputFile, s.exitSentinel))
}

// Output returns the session output so far, stripped of pseudo-terminal carriage returns.
func (s *InteractiveSession) Output() string {
	// `|| true` keeps a not yet created output file from failing the read, so any error left is from kubectl exec.
	out, err := RunOnLoginNodeAsUser(s.slurmNamespace, s.user, "cat "+s.outputFile+" 2>/dev/null || true")
	if err != nil {
		WarnError(fmt.Errorf("failed to read %s session output from the login node: %w", s.kind, err))
		return ""
	}
	return strings.ReplaceAll(out, "\r", "")
}

// JobID returns the Slurm job ID, waiting until the client has reported it.
func (s *InteractiveSession) JobID() string {
	ginkgo.GinkgoHelper()
	if s.jobID != "" {
		return s.jobID
	}

	ginkgo.By(fmt.Sprintf("waiting for the %s session to report its job allocation", s.kind))
	gomega.Eventually(func(g gomega.Gomega) {
		out := s.Output()
		matches := interactiveJobIDRegex.FindStringSubmatch(out)
		g.Expect(matches).To(gomega.HaveLen(2),
			fmt.Sprintf("%s has not reported its job allocation yet, output so far:\n%s", s.kind, out))
		s.jobID = matches[1]
	}, interactiveJobIDTimeout, time.Second).Should(gomega.Succeed())

	NewSlurmJobVerifier(s.slurmNamespace, s.jobID).
		WithUser(s.user).
		WithTimeout(30 * time.Second).
		Verify()
	return s.jobID
}

// JobSetName returns the name of the JobSet created for the session's job.
func (s *InteractiveSession) JobSetName() string {
	ginkgo.GinkgoHelper()
	return fmt.Sprintf("slurm-job-%s", s.JobID())
}

// Exit makes the session's shell return, as if the user typed exit, by creating the sentinel file.
func (s *InteractiveSession) Exit() {
	ginkgo.GinkgoHelper()
	ginkgo.By(fmt.Sprintf("exiting the %s session shell", s.kind))

	switch s.kind {
	case sessionSalloc:
		_, err := RunOnLoginNodeAsUser(s.slurmNamespace, s.user, "touch "+s.exitSentinel)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to signal the salloc session shell to exit")
	case sessionSrunPty:
		podName := s.workerPodName()
		cmd := exec.Command("kubectl", "exec", "-n", s.slurmNamespace, podName, "-c", "slurmd",
			"--", "touch", s.exitSentinel)
		_, err := Run(cmd)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(),
			"Failed to signal the srun --pty session shell to exit in pod "+podName)
	}
}

func (s *InteractiveSession) workerPodName() string {
	ginkgo.GinkgoHelper()
	jobSetName := s.JobSetName()
	cmd := exec.Command("kubectl", "get", "pods", "-n", s.slurmNamespace,
		"-l", fmt.Sprintf("%s=%s", jobSetPodSelector, jobSetName),
		"--field-selector", "status.phase=Running",
		"-o", "jsonpath={.items[*].metadata.name}")
	out, err := Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to list worker pods of JobSet "+jobSetName)
	pods := strings.Fields(out)
	gomega.Expect(pods).To(gomega.HaveLen(1), "Expected exactly one running worker pod for JobSet "+jobSetName)
	return pods[0]
}
