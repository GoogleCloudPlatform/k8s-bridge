---
name: ops-cleanup-state
description: >-
  Drains and cleans up wedged Slurm jobs, orphaned Kubernetes JobSets,
  Kueue Workloads, dynamic slurmd worker pods, and stale dynamic Slurm node
  registrations (DOWN or orphaned slurm-job-* nodes). Use when resetting cluster
  state between runs, removing stuck dynamic Slurm nodes that cause
  BadConstraints, or cancelling wedged workloads. Don't use for read-only
  root-cause diagnosis of stuck jobs (use ops-diagnose-workload) or tearing down
  the Slurm release, CRs, operator stack, and cloud cluster (use
  infra-configure-crs, infra-deploy-stack, and infra-provision-cluster).
---

# State Cleanup and Stale Node Deregistration (`ops-cleanup-state`)

## 1. Preconditions & Inputs

Before executing any cleanup commands, verify or ask the user for:
1. **Scope of Cleanup:**
   - **Targeted cleanup (`JOB_ID`):** Clean up a single wedged Slurm job ID (`slurm-job-<JOB_ID>` and its dynamic nodes `slurm-job-<JOB_ID>-slurmd-*`).
   - **Full namespace reset (`ALL`):** Cancel all Slurm jobs, delete all managed `JobSet` objects and dynamic `slurmd` pods, and deregister all `slurm-job-*` dynamic nodes in the namespace.
2. **Target Namespace (`NAMESPACE`):** Default `slurm`.
3. **Login / Controller Pods:** `slurm-controller-0` (`-c slurmctld`) and `deploy/slurm-login-slinky` (`-c login`, or `deploy/slurm-login` in Kind E2E).

---

## 2. Guardrails

- **Confirmation gate:** Present the target cluster context (`kubectl config current-context`), namespace, target job IDs or node names, and commands to the user, and obtain user confirmation before running any deletion or cancellation command.
- **Do not delete dynamic `slurmd` pods with `--force --grace-period=0`:** Use `--grace-period=5 --wait=true` so the `PreStop` hook runs (`--force` skips `PreStop`, leaving stale dynamic nodes that cause `BadConstraints`).
- **Follow the cleanup sequence (Jobs -> `JobSet` -> Pods -> Stale Slurm Nodes):** Terminate the `JobSet` and `slurmd` pods **before** deleting `slurm-job-*` nodes in `slurmctld` so running pods do not re-register them.
- **Allow self-healing before manual cleanup:** When `k8s-bridge` is running, orphaned `JobSets` auto-delete after `--orphan-grace-period` (`cmd/main.go`); clean up manually only if the state persists past that window or `k8s-bridge` is not running.
- **Mark dynamic Slurm nodes `DOWN` before running `scontrol delete`:** `slurmctld` rejects deleting nodes in `ALLOCATED`, `MIXED`, or `COMPLETING` state (`scontrol update nodename=<node> state=DOWN reason=cleanup` first).

---

## 3. Step-by-Step Procedure

> **Scope Selection:** In each step below, run **only Option A** (for a single `<JOB_ID>`) **or Option B** (for a full namespace reset).

### Step 1: Cancel Target Slurm Job(s)
After user confirmation, cancel the target Slurm job (or all jobs for a full reset):
- **Option A (single job ID):**
  ```bash
  kubectl -n slurm exec slurm-controller-0 -c slurmctld -- scancel <JOB_ID>
  ```
- **Option B (all jobs in Slurm queue; add `-p <bridged-partition>` to `squeue` on mixed static+dynamic clusters):**
  ```bash
  kubectl -n slurm exec slurm-controller-0 -c slurmctld -- sh -c 'squeue -a -h -o "%i" | xargs -r scancel'
  ```

### Step 2: Delete `JobSet` Object(s) with Foreground Cascading Deletion
Delete the `JobSet` using `--cascade=foreground --wait=true`:
- **Option A (single job ID):**
  ```bash
  kubectl -n slurm delete jobset slurm-job-<JOB_ID> \
    --cascade=foreground --wait=true --timeout=60s --ignore-not-found
  ```
- **Option B (all `k8s-bridge` managed `JobSet` objects in namespace):**
  ```bash
  kubectl -n slurm delete jobset -l app.kubernetes.io/managed-by=k8s-bridge \
    --cascade=foreground --wait=true --timeout=60s
  ```

If a `JobSet` is stuck in `Terminating` (or leaked) while `k8s-bridge` is running, ensure its `k8s-bridge.x-k8s.io/slurm-job-id` label and `WorkloadMixing` `ownerReferences` (`uid`) are intact so `k8s-bridge` removes `k8s-bridge.x-k8s.io/slurm-node-cleanup` (if already removed, check child `Job`/`Pod` objects for custom finalizers blocking `foregroundDeletion`). If `k8s-bridge` was uninstalled, remove only `k8s-bridge.x-k8s.io/slurm-node-cleanup` via the `jq` pipeline in [README.md](../../README.md) (`## Cleanup`, keeping `jobset.x-k8s.io/jobset-controller`) and run Step 4.

### Step 3: Terminate Remaining Child `Jobs` and Dynamic `slurmd` Pods
If child `Jobs` or worker pods remain after `JobSet` deletion, delete them gracefully:
- **Option A (single job ID):**
  ```bash
  kubectl -n slurm delete job -l jobset.sigs.k8s.io/jobset-name=slurm-job-<JOB_ID> \
    --cascade=foreground --wait=false --ignore-not-found
  kubectl -n slurm delete pod -l k8s-bridge.x-k8s.io/slurm-job-id=<JOB_ID> \
    --grace-period=5 --wait=true --ignore-not-found
  ```
- **Option B (all `k8s-bridge` worker jobs and pods):**
  ```bash
  kubectl -n slurm delete job -l jobset.sigs.k8s.io/replicatedjob-name=slurmd \
    --cascade=foreground --wait=false --ignore-not-found
  kubectl -n slurm delete pod -l app.kubernetes.io/managed-by=k8s-bridge,app.kubernetes.io/component=worker \
    --grace-period=5 --wait=true --ignore-not-found
  ```

### Step 4: Deregister Stale Dynamic Nodes in `slurmctld`
Once all target `slurmd` pods have terminated, run [scripts/cleanup-stale-nodes.sh](scripts/cleanup-stale-nodes.sh) to mark remaining `slurm-job-<JOB_ID>-slurmd-*` (or all `slurm-job-*`) nodes `DOWN` and delete them from `slurmctld`:
- **Option A (single job ID):**
  ```bash
  bash skills/ops-cleanup-state/scripts/cleanup-stale-nodes.sh slurm <JOB_ID>
  ```
- **Option B (all `slurm-job-*` dynamic nodes):**
  ```bash
  bash skills/ops-cleanup-state/scripts/cleanup-stale-nodes.sh slurm
  ```

---

## 4. Verification

1. Verify no orphaned `JobSet`, Kueue `Workload`, or dynamic `slurmd` worker pods remain for the cleaned-up scope:

```bash
kubectl -n slurm get jobset,pods -l app.kubernetes.io/managed-by=k8s-bridge
kubectl -n slurm get workloads
```

2. Verify `slurmctld` has no remaining `slurm-job-*` dynamic nodes or stuck jobs:

```bash
kubectl -n slurm exec slurm-controller-0 -c slurmctld -- squeue
kubectl -n slurm exec slurm-controller-0 -c slurmctld -- scontrol -o show nodes
```

---

## 5. Handoffs & Related Skills

- **To diagnose why a workload became wedged before cleaning it up:** Read [skills/ops-diagnose-workload/SKILL.md](../ops-diagnose-workload/SKILL.md).
- **To tear down `WorkloadMixing`/Kueue CRs, the Slinky `slurm` release, the base operators, or the cloud cluster:** Read [skills/infra-configure-crs/SKILL.md](../infra-configure-crs/SKILL.md), [skills/infra-deploy-stack/SKILL.md](../infra-deploy-stack/SKILL.md), and [skills/infra-provision-cluster/SKILL.md](../infra-provision-cluster/SKILL.md).
