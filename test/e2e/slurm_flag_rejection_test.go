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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

const (
	cmdSrun        = "srun"
	cmdSalloc      = "salloc"
	flagContiguous = "--contiguous"
)

type flagRejectionTestCase struct {
	Flag    string
	Args    []string
	Command string
}

// flagRejectionTestCases enumerates all Slurm CLI options unsupported by k8s-bridge.
var flagRejectionTestCases = []flagRejectionTestCase{
	// Topology options
	{Flag: "--consolidate-segments", Args: []string{"--consolidate-segments"}},
	{Flag: flagContiguous, Args: []string{flagContiguous}},
	{Flag: "--segment", Args: []string{"--segment=2"}},
	{Flag: "--spread-job", Args: []string{"--spread-job"}},
	{Flag: "--spread-segments", Args: []string{"--spread-segments"}},
	{Flag: "--switches", Args: []string{"--switches=2"}},

	// Existing job attachment
	{Flag: "--jobid", Args: []string{"--jobid=123"}, Command: cmdSrun},

	// Job arrays, scheduling constraints, and dependencies
	{Flag: "--array (-a)", Args: []string{"--array=1-2"}},
	{Flag: "--begin (-b)", Args: []string{"--begin=now+1hour"}},
	{Flag: "--constraint (-C)", Args: []string{"--constraint=gpu"}},
	{Flag: "--dependency (-d)", Args: []string{"--dependency=afterok:1"}},
	{Flag: "--het-group", Args: []string{"--het-group=0"}, Command: cmdSrun},
	{Flag: "--no-kill (-k)", Args: []string{"--no-kill"}},
	{Flag: "--requeue", Args: []string{"--requeue"}},

	// Scheduling priority, node selection, and low-level execution options
	{Flag: "--core-spec (-S)", Args: []string{"--core-spec=2"}},
	{Flag: "--immediate (-I)", Args: []string{"--immediate"}, Command: cmdSalloc},
	{Flag: "--nice", Args: []string{"--nice=100"}},
	{Flag: "--priority", Args: []string{"--priority=1000"}},
	{Flag: "--thread-spec", Args: []string{"--thread-spec=2"}},
	{Flag: "--time-min", Args: []string{"--time-min=10"}},
	{Flag: "--use-min-nodes", Args: []string{"--use-min-nodes"}},
	{Flag: "--prefer", Args: []string{"--prefer=fast"}},
	{Flag: "--qos (-q)", Args: []string{"--qos=special"}},
	{Flag: "--network", Args: []string{"--network=fabric=custom"}},
	{Flag: "--cpu-freq", Args: []string{"--cpu-freq=medium"}},
	{Flag: "--delay-boot", Args: []string{"--delay-boot=300"}},
	{Flag: "--exclude (-x)", Args: []string{"--exclude=node01"}},
	{Flag: "--gpu-freq", Args: []string{"--gpu-freq=medium"}},
	{Flag: "--mcs-label", Args: []string{"--mcs-label=test"}},
	{Flag: "--mem-bind", Args: []string{"--mem-bind=local"}},
	{Flag: "--mem-update", Args: []string{"--mem-update=10@10"}},
	{Flag: "--no-allocate (-Z)", Args: []string{"--no-allocate"}, Command: cmdSrun},
	{Flag: "--nodefile (-F)", Args: []string{"--nodefile=/etc/hosts"}},
	{Flag: "--nodelist (-w)", Args: []string{"--nodelist=node01"}},
	{Flag: "--oversubscribe (-s)", Args: []string{"--oversubscribe"}},
	{Flag: "--reboot", Args: []string{"--reboot"}},
	{Flag: "--resv-ports", Args: []string{"--resv-ports"}},
	{Flag: "--x11", Args: []string{"--x11"}, Command: cmdSrun},
	{Flag: "--exclusive", Args: []string{"--exclusive"}},

	// Fine-grained hardware topology and binding options
	{Flag: "--cores-per-socket", Args: []string{"--cores-per-socket=2"}},
	{Flag: "--cpu-bind", Args: []string{"--cpu-bind=cores"}, Command: cmdSrun},
	{Flag: "--extra-node-info (-B)", Args: []string{"-B", "1:2:1"}},
	{
		Flag: "--gpus-per-socket",
		Args: []string{"--gres=gpu:1", "--gpus-per-socket=1"},
	},
	{Flag: "--gres-flags", Args: []string{"--gres-flags=enforce-binding"}},
	{Flag: "--mincpus", Args: []string{"--mincpus=2"}},
	{Flag: "--ntasks-per-core", Args: []string{"--ntasks-per-core=1"}},
	{Flag: "--ntasks-per-gpu", Args: []string{"--gres=gpu:1", "--ntasks-per-gpu=1"}},
	{Flag: "--ntasks-per-socket", Args: []string{"--ntasks-per-socket=1"}},
	{Flag: "--sockets-per-node", Args: []string{"--sockets-per-node=1"}},
	{Flag: "--threads-per-core", Args: []string{"--threads-per-core=1"}},
	{Flag: "--tres-bind", Args: []string{"--tres-bind=gres/gpu:closest"}},
}

// expectedRejection is the message prefix cli_filter.lua reports to the submitting user.
const expectedRejection = "Validation error: job uses an option that is not supported by k8s-bridge"

var _ = Describe("Rejection of job options unsupported by k8s-bridge", Ordered, ContinueOnFailure, func() {
	BeforeAll(func() {
		utils.ResetSlurmAndK8sState(slurmNamespace)
		const configPath = "test/e2e/testdata/workloadmixing_template.yaml"
		utils.ApplyManifestWithCleanup(configPath)
	})

	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			utils.DumpFailureDiagnostics(controllerPodName, namespace, slurmNamespace)
		}
	})

	// submitAndCollect runs one submission and records a readable diagnosis instead
	// of aborting the spec, so every flag is exercised on each run and a failure
	// report lists all of the offenders at once.
	submitAndCollect := func(failures *[]string, description string, expectedFlag string, submit func() string) {
		By(fmt.Sprintf("submitting %s", description), func() {
			var output string
			messages := InterceptGomegaFailures(func() {
				output = submit()
			})
			if len(messages) > 0 {
				*failures = append(*failures,
					fmt.Sprintf("%s: submission did not fail: %s", description, strings.Join(messages, "; ")))
				return
			}
			expectedMsg := fmt.Sprintf("%s (%s)", expectedRejection, expectedFlag)
			if !strings.Contains(output, expectedMsg) {
				*failures = append(*failures,
					fmt.Sprintf("%s: expected %q, got: %s",
						description, expectedMsg, strings.TrimSpace(output)))
			}
		})
	}

	It("should reject unsupported job options on job submission", func() {
		var failures []string
		for _, testCase := range flagRejectionTestCases {
			tc := testCase
			expectedFlag := strings.Fields(tc.Flag)[0]
			submitAndCollect(&failures, tc.Flag, expectedFlag, func() string {
				if tc.Command == cmdSalloc {
					cmdArgs := append([]string{"-p", defaultSlurmPartition, "--no-shell"}, tc.Args...)
					return utils.SubmitAllocJobExpectError(slurmNamespace, utils.DefaultSlurmTestUser, cmdArgs...)
				}
				if tc.Command == cmdSrun {
					cmdArgs := append(append([]string{"-p", defaultSlurmPartition}, tc.Args...), "/bin/true")
					return utils.SubmitRunJobExpectError(slurmNamespace, utils.DefaultSlurmTestUser, cmdArgs...)
				}
				cmdArgs := append(append([]string{"-p", defaultSlurmPartition}, tc.Args...), "--wrap=sleep 10")
				return utils.SubmitBatchJobExpectError(slurmNamespace, utils.DefaultSlurmTestUser, cmdArgs...)
			})
		}
		Expect(failures).To(BeEmpty(), "unsupported job options were not rejected as expected:\n%s",
			strings.Join(failures, "\n"))
	})

	representativeFlags := []flagRejectionTestCase{
		{Flag: flagContiguous, Args: []string{flagContiguous}},
		{Flag: "--switches", Args: []string{"--switches=2"}},
		{Flag: "--nice", Args: []string{"--nice=100"}},
		{Flag: "--exclude (-x)", Args: []string{"--exclude=node01"}},
	}

	It("should reject unsupported job options submitted with salloc and srun", func() {
		var failures []string
		for _, testCase := range representativeFlags {
			tc := testCase
			expectedFlag := strings.Fields(tc.Flag)[0]
			submitAndCollect(&failures, tc.Flag+" (salloc)", expectedFlag, func() string {
				cmdArgs := append([]string{"-p", defaultSlurmPartition, "--no-shell"}, tc.Args...)
				return utils.SubmitAllocJobExpectError(slurmNamespace, utils.DefaultSlurmTestUser, cmdArgs...)
			})
			submitAndCollect(&failures, tc.Flag+" (srun)", expectedFlag, func() string {
				cmdArgs := append(append([]string{"-p", defaultSlurmPartition}, tc.Args...), "/bin/true")
				return utils.SubmitRunJobExpectError(slurmNamespace, utils.DefaultSlurmTestUser, cmdArgs...)
			})
		}
		Expect(failures).To(BeEmpty(), "unsupported job options were not rejected as expected:\n%s",
			strings.Join(failures, "\n"))
	})

	It("should allow supported job options on workload-mixing partitions", func() {
		// Using --test-only verifies cli_filter passes without creating an actual job
		cmdArgs := []string{
			"--test-only", "-p", defaultSlurmPartition,
			"--job-name=supported-options",
			"--deadline=now+1hour",
			"--tmp=1024",
			"--time=10",
			"--comment=test",
			"--no-requeue",
			"--wrap=sleep 10",
		}
		output, _ := utils.RunOnLoginNodeAsUser(
			slurmNamespace, utils.DefaultSlurmTestUser, "sbatch "+utils.ShellQuote(cmdArgs),
		)
		Expect(output).NotTo(ContainSubstring(expectedRejection),
			"Supported job options should not be rejected by cli_filter.lua, got: %s", output)
	})
})
