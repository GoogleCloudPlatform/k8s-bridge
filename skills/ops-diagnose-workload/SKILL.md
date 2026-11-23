---
name: ops-diagnose-workload
description: >-
  Diagnoses rejected, held (JobHeldUser / JobHeldAdmin), suspended (Workload not
  admitted), pending (BadConstraints / Pod Pending), preempted, or cancelled
  Slurm workloads and captures diagnostic state snapshots across Slurm,
  k8s-bridge, Kueue, JobSet, and dynamic slurmd pods. Use when troubleshooting
  why a submitted Slurm job is stuck or failed, or when collecting a cluster
  diagnostic dump. Don't use for destructive state cleanup, pod deletion, or
  draining orphaned nodes (use ops-cleanup-state).
---

# Read-Only Workload Diagnosis & Snapshot Collection (`ops-diagnose-workload`)

## 1. Preconditions & Inputs

Before starting diagnosis, verify or ask the user for:
1. **Slurm `JOB_ID`:** The numeric Slurm job ID (if accepted by `slurmctld`), or the rejected `srun`/`sbatch`/`salloc` command line.
2. **Namespaces & Login Deployment:** Slurm/`WorkloadMixing` namespace (`NAMESPACE`, default `slurm`), operator namespace (`BRIDGE_NAMESPACE`, default `k8s-bridge-system`), and login deployment (`deploy/slurm-login-slinky`, or `deploy/slurm-login` in Kind E2E clusters).
3. Use [references/symptom-decision-table.md](references/symptom-decision-table.md) as a reference of common known failure patterns (non-exhaustive; always verify against live logs, events, and current source code).

---

## 2. Guardrails

- **Read-only mode:** Do **not** run `scancel`, `scontrol delete`, `kubectl delete`, `kubectl patch`, or `helm upgrade/uninstall` during this workflow. If remediation requires cancelling jobs, deleting `JobSet` objects, or removing stale dynamic nodes, report the root cause first and hand off to `ops-cleanup-state` or `infra-configure-crs`.
- **Do not patch `JobSet.spec.suspend`:** Kueue's webhook resets `JobSet.spec.suspend` unless `Workload.spec.active` is modified.
- **Never provision or resize node pools for `Pending` pods without user confirmation:** If admitted `slurmd` pods stay `Pending` with `Insufficient cpu/memory/gpu`, reshape the job to fit single-node `allocatable` capacity or report to the user.

---

## 3. Step-by-Step Procedure

### Step 1: Collect a One-Shot Diagnostic Snapshot
Run the non-destructive snapshot script [scripts/collect-diagnostic-snapshot.sh](scripts/collect-diagnostic-snapshot.sh):
- **Option A (when a Slurm `JOB_ID` exists):**
  ```bash
  bash skills/ops-diagnose-workload/scripts/collect-diagnostic-snapshot.sh <JOB_ID> slurm k8s-bridge-system
  ```
- **Option B (when diagnosing a pre-submission CLI flag rejection with no `JOB_ID`):**
  ```bash
  bash skills/ops-diagnose-workload/scripts/collect-diagnostic-snapshot.sh "" slurm k8s-bridge-system
  ```

### Step 2: Trace the 4 Handoff Stages & Check Known Symptoms
Inspect the snapshot output across the 4 handoff stages (query `scontrol show job <JOB_ID>` before `MinJobAge` in `slurm.conf` expires; default Slinky deploys omit `slurmdbd`/`sacct`). Check [references/symptom-decision-table.md](references/symptom-decision-table.md) for common known symptoms, and inspect live logs or source code directly if the behavior does not match a listed pattern:
1. **Pre-submission rejection (no `JOB_ID`):** Compare client stderr and `slurmctld` logs against `hack/cli_filter.lua`, `hack/job_submit.lua`, and `slurm.conf`/partition policy.
2. **Slurm job state (`scontrol show job <JOB_ID>`):** Check `JobState`, `Reason`, `Priority`, `Features`, `Comment`, and `JobTranslationFailed` events.
3. **Kueue & `JobSet` admission:** Check `JobSet/slurm-job-<JOB_ID>` (`SUSPENDED`) and `Workload`, `LocalQueue`, and `ClusterQueue` conditions.
4. **Dynamic `slurmd` pods & node registration:** When `SUSPENDED=false`, inspect the `JobSet`, child `Job`, `slurmd` pods/logs, and `scontrol show nodes` / `partitions`.

### Step 3: Inspect `k8s-bridge` Prometheus Metrics (Optional)
Query `/metrics` via `port-forward` (`kube-apiserver` proxy strips `Authorization` headers and fails with `401`; on non-GKE clusters, replace `gcloud auth print-access-token` with `kubectl create token <sa-with-metrics-reader>`):

```bash
(
  timeout 15 kubectl -n k8s-bridge-system port-forward deploy/k8s-bridge-controller-manager 18443:8443 >/dev/null 2>&1 &
  PF=$!; trap "kill ${PF} 2>/dev/null || true" EXIT
  sleep 2
  curl -sfk -H "Authorization: Bearer $(gcloud auth print-access-token)" https://127.0.0.1:18443/metrics | grep "^k8s_bridge_"
)
```

---

## 4. Verification

Confirm that your diagnosis output provides:
1. The observed state across Slurm (`JobState`, `Reason`, `Comment`), Kubernetes (`JobSet` `SUSPENDED`/`READY`, Pod phase), and Kueue (`Workload` `Admitted` condition).
2. The root cause identified from live logs/events/code (referencing [references/symptom-decision-table.md](references/symptom-decision-table.md) when it matches a known pattern).
3. The recommended next skill or command for the user to approve.

---

## 5. Handoffs & Related Skills

- **To clean up wedged Slurm jobs, orphaned `JobSet` objects, or stale dynamic Slurm nodes:** Read [skills/ops-cleanup-state/SKILL.md](../ops-cleanup-state/SKILL.md).
- **To fix partition parity, `dynamicNodeFeature`, GPU `mappedResources`, or user UIDs:** Read [skills/infra-configure-crs/SKILL.md](../infra-configure-crs/SKILL.md).
- **To fix a controller or translator bug identified in logs:** Read [skills/dev-controller-and-translator/SKILL.md](../dev-controller-and-translator/SKILL.md).
