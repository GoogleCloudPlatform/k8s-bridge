// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

const (
	// DefaultSlurmLoginDeployment is the Slinky LoginSet-managed Deployment that
	// E2E tests use to submit jobs, so submissions travel the same path a real
	// cluster user would take instead of executing inside slurmctld as root.
	DefaultSlurmLoginDeployment = "deploy/slurm-login"
	// DefaultSlurmLoginContainer is the container within the login pod.
	DefaultSlurmLoginContainer = "login"
	// LINT.IfChange(slurm_test_user)
	// DefaultSlurmTestUser is the unprivileged user that submits E2E jobs. It is
	// provisioned via a lifecycle postStart hook on the login and compute pods.
	DefaultSlurmTestUser = "foo"
	// DefaultSlurmTestUserID is the UID/GID of DefaultSlurmTestUser.
	DefaultSlurmTestUserID = 2000
	// LINT.ThenChange(
	//   test/e2e/testdata/slurm_login.yaml:slurm_test_user,
	//   test/e2e/testdata/workloadmixing_template.yaml:slurm_test_user,
	//   test/e2e/testdata/workloadmixing_tas.yaml:slurm_test_user,
	//   test/e2e/testdata/workloadmixing_gpu.yaml:slurm_test_user
	// )

	slurmLoginManifest = "test/e2e/testdata/slurm_login.yaml"
)

// setupSlurmLoginNode provisions the Slinky LoginSet used by E2E tests to
// submit jobs as an unprivileged user.
func setupSlurmLoginNode() {
	ginkgo.GinkgoHelper()

	ginkgo.By("deploying the Slinky LoginSet")
	cmd := exec.Command("kubectl", "apply", "--server-side", "--force-conflicts", "-f", slurmLoginManifest)
	_, err := Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to deploy LoginSet")

	ginkgo.By("waiting for Slurm login Deployment to be created")
	gomega.Eventually(func() error {
		checkCmd := exec.Command("kubectl", "get", "deployment/slurm-login", "--namespace", DefaultSlurmNamespace)
		_, checkErr := Run(checkCmd)
		return checkErr
	}, "2m", "5s").Should(gomega.Succeed(), "slurm-login deployment was not created")

	ginkgo.By("waiting for Slurm login Deployment to be Available")
	cmd = exec.Command("kubectl", "wait", "deployment/slurm-login",
		"--for", "condition=Available",
		"--namespace", DefaultSlurmNamespace,
		"--timeout", "5m",
	)
	_, err = Run(cmd)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "slurm-login deployment failed to become available")

	// Fail here rather than later inside an opaque sbatch error if the
	// postStart user provisioning hook did not take effect.
	verifyLoginNodeUser(DefaultSlurmNamespace, DefaultSlurmTestUser, DefaultSlurmTestUserID)
}

// teardownSlurmLoginNode deletes the Slinky LoginSet resource.
func teardownSlurmLoginNode() {
	ginkgo.GinkgoHelper()

	ginkgo.By("deleting the Slinky LoginSet")
	cmd := exec.Command("kubectl", "delete", "-f", slurmLoginManifest, "--ignore-not-found")
	if _, err := Run(cmd); err != nil {
		WarnError(err)
	}
}

// verifyLoginNodeUser asserts that the given user resolves on the login node
// with the expected UID, confirming the postStart hook took effect.
func verifyLoginNodeUser(slurmNamespace, username string, uid int) {
	ginkgo.GinkgoHelper()
	ginkgo.By(fmt.Sprintf("verifying user %q resolves on the login node", username))

	gomega.Eventually(func(g gomega.Gomega) {
		output, err := RunOnLoginNodeAsUser(slurmNamespace, username, "id")
		g.Expect(err).NotTo(gomega.HaveOccurred(), "User "+username+" did not resolve on the login node")
		g.Expect(strings.TrimSpace(output)).To(
			gomega.ContainSubstring(fmt.Sprintf("uid=%d(%s)", uid, username)),
			"Unexpected uid for user "+username)
	}, 30*time.Second, time.Second).Should(gomega.Succeed())
}

// ShellQuote renders argv as a single POSIX shell word list.
//
// Commands dispatched through `runuser -l <user> -c` collapse into one shell
// string, so arguments that contain spaces (notably --wrap='sleep 300') must be
// quoted explicitly to survive the extra round of word splitting.
func ShellQuote(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, "'"+strings.ReplaceAll(arg, "'", `'\''`)+"'")
	}
	return strings.Join(quoted, " ")
}

// RunOnLoginNodeAsUser executes a shell command on the Slurm login node as the
// given unprivileged user.
//
// Tests exercise Slurm the way a real cluster user does: from the login node,
// as a non-root account, rather than as root inside slurmctld where permission
// and authentication problems would go unnoticed.
//
// The -l flag starts a login shell, which lands in the user's home directory
// (/tmp) and exports a consistent HOME/USER, giving Slurm a writable working
// directory for batch output files.
func RunOnLoginNodeAsUser(slurmNamespace, username, command string) (string, error) {
	return RunOnLoginNodeAsUserWithContext(context.Background(), slurmNamespace, username, command)
}

// RunOnLoginNodeAsUserWithContext executes a shell command on the Slurm login node
// as the given user, bounded by the provided context.
func RunOnLoginNodeAsUserWithContext(ctx context.Context, slurmNamespace, username, command string) (string, error) {
	cmd := exec.CommandContext(ctx,
		"kubectl", "exec", "-n", slurmNamespace,
		DefaultSlurmLoginDeployment, "-c", DefaultSlurmLoginContainer,
		"--", "runuser", "-l", username, "-c", command,
	)
	return Run(cmd)
}
