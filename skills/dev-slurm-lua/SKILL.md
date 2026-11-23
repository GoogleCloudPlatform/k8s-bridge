---
name: dev-slurm-lua
description: >-
  Implements, modifies, and tests the Slurm Lua plugins (hack/job_submit.lua and
  hack/cli_filter.lua) that validate CLI options, place jobs on bridged
  partitions into auto-hold, and protect k8s-bridge job constraints. Use when
  adding allowed Slurm CLI flags, changing job submission or modification
  interception rules in Lua, or testing plugin behavior with non-root users.
  Don't use for Go controller or JobSet translator changes (use
  dev-controller-and-translator) or cluster partition mapping configuration (use
  infra-configure-crs).
---

# Slurm Lua Plugins Workflow (`dev-slurm-lua`)

## 1. Preconditions & Inputs

Before modifying or testing the Slurm Lua scripts, inspect:
- `hack/cli_filter.lua`: Client-side Lua plugin on login pods (`CliFilterParameters=workload_mixing_partitions=<csv>` in `slurm.conf`).
- `hack/job_submit.lua`: Server-side Lua plugin on `slurmctld` (`WORKLOAD_MIXING_PARTITIONS` env var).
- `test/e2e/slurm_flag_rejection_test.go`, `test/e2e/partition_priority_mapping_test.go`, and `test/e2e/e2e_test.go`: Paired E2E specs validating CLI flag rejection and submission holds.

---

## 2. Guardrails

- **Test `job_submit.lua` modification rules as a non-root user:** Run tests via `runuser -u <nonroot-user> -- ...` (`kubectl exec` defaults to `root`, and `slurm_job_modify` in `hack/job_submit.lua` exempts `root`/`slurm`).
- **Preserve cross-component markers and rejection message formats:**
  - Keep `AutoHoldMarker` in `internal/controller/slurmjob/translator.go` and `"k8s-bridge-auto-hold"` in `hack/job_submit.lua` in sync.
  - The paired E2E specs in Section 1 assert on `REJECTION_MESSAGE` in `hack/cli_filter.lua`.
- **Use long option names without leading dashes in `ALLOWED_OPTIONS` (`hack/cli_filter.lua`):** `slurm.json_cli_options` normalizes short flags automatically.

---

## 3. Step-by-Step Procedure

### Step 1: Modify `hack/cli_filter.lua` or `hack/job_submit.lua`
1. **CLI flag allowlist (`hack/cli_filter.lua`):** Update `ALLOWED_OPTIONS` or `pre_submit_validate` while preserving existing hetjob, `argv`, and `spank:` handling.
2. **Submission & modification hooks (`hack/job_submit.lua`):** Update `slurm_job_submit` or `slurm_job_modify` while preserving existing `AutoHoldMarker` hold/unhold and non-admin immutability invariants.
3. **Sync paired E2E tests:** Update the paired E2E specs in Section 1 whenever `ALLOWED_OPTIONS` or Lua rejection rules change.

### Step 2: Update Lua Scripts in an Active Cluster
Select the option matching your deployment. In both options, wait ~60s for kubelet `ConfigMap` propagation into `slurm-controller-0` (or verify `sha256sum /etc/slurm/cli_filter.lua` in `slurmctld` matches `hack/cli_filter.lua`) before running `scontrol reconfigure`:
- **Option A — Helm-Deployed Slinky Slurm Cluster (`slurm` release in `slurm` namespace):**

```bash
helm upgrade slurm oci://ghcr.io/slinkyproject/charts/slurm \
  --namespace slurm --reuse-values \
  --set-file 'configFiles.job_submit\.lua=hack/job_submit.lua' \
  --set-file 'configFiles.cli_filter\.lua=hack/cli_filter.lua' \
  --wait
kubectl -n slurm exec slurm-controller-0 -c slurmctld -- scontrol reconfigure
```

- **Option B — Kind E2E Cluster (`test/e2e/testdata/slurm_cluster.yaml`):**

```bash
kubectl -n slurm create configmap slurm-job-submit-lua --from-file=job_submit.lua=hack/job_submit.lua --dry-run=client -o yaml | kubectl apply -f -
kubectl -n slurm create configmap slurm-cli-filter-lua --from-file=cli_filter.lua=hack/cli_filter.lua --dry-run=client -o yaml | kubectl apply -f -
kubectl -n slurm exec slurm-controller-0 -c slurmctld -- scontrol reconfigure
```

---

## 4. Verification

1. **Non-root verification on a running cluster:** Run [scripts/test-nonroot-submission.sh](scripts/test-nonroot-submission.sh) (defaults to `deploy/slurm-login-slinky` and `k8s-partition`, or pass `LOGIN_DEPLOY=deploy/slurm-login PARTITION=workloadmixing-partition TEST_USER=foo` on a Kind E2E cluster) to verify CLI option filtering, multi-partition rejection, and user-hold release takeover.
2. **Automated E2E verification:** Run the flag rejection and partition mapping specs via [skills/test-e2e-cluster/SKILL.md](../test-e2e-cluster/SKILL.md).

---

## 5. Handoffs & Related Skills

- **To run or update the Kind E2E flag rejection tests:** Read [skills/test-e2e-cluster/SKILL.md](../test-e2e-cluster/SKILL.md).
- **To modify how `k8s-bridge` translates held jobs or handles holds in Go:** Read [skills/dev-controller-and-translator/SKILL.md](../dev-controller-and-translator/SKILL.md).
- **To configure bridged partition names across `slurm.conf` and `WorkloadMixing`:** Read [skills/infra-configure-crs/SKILL.md](../infra-configure-crs/SKILL.md).
