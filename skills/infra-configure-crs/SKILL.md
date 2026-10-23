---
name: infra-configure-crs
description: >-
  Configures, applies, and removes all workload-mixing Custom Resources (CRs)
  and Slurm partitions for CPU, GPU, priority, and GKE CCC workloads: Kueue CRs
  (ResourceFlavor, ClusterQueue, LocalQueue, WorkloadPriorityClass), GKE
  ComputeClass CRs, Slinky slurm Helm release (slurm-values.yaml and Lua
  plugins), WorkloadMixing CRs, non-root user verification, and job submission
  checks. Use when deploying, updating, or removing Slurm partitions, Kueue
  queues, priority classes, or typed/untyped GPU mappedResources. Don't use for
  creating cloud clusters or node pools (use infra-provision-cluster),
  installing and tearing down the base operators (use infra-deploy-stack), or
  diagnosing stuck workloads and cleaning up wedged state (use
  ops-diagnose-workload or ops-cleanup-state).
---

# CR Configuration, Slurm Partitions, and Workload Setup (`infra-configure-crs`)

## 1. Preconditions & Inputs

1. **Base operators installed:** `cert-manager`, `slurm-operator`, `JobSet`, `Kueue`, and `k8s-bridge` must be running in the cluster. If not, read [skills/infra-deploy-stack/SKILL.md](../infra-deploy-stack/SKILL.md) first. If cloud node pools or GPU labels are needed, read [skills/infra-provision-cluster/SKILL.md](../infra-provision-cluster/SKILL.md) first.
2. **Select the target scenario guide and manifests** (read the linked `docs/` guide for scenario commands and manifests instead of writing YAML from memory):

| Scenario | Repository Guide | Kueue / ComputeClass Manifests | `WorkloadMixing` Manifest |
|---|---|---|---|
| **Base CPU Quickstart** | [docs/quickstart.md](../../docs/quickstart.md) | `docs/examples/quickstart/kueue-resources.yaml` | `docs/examples/quickstart/workloadmixing.yaml` |
| **Existing Kueue / Slurm Infra** | [docs/existing-infra.md](../../docs/existing-infra.md) | Existing Kueue queues ([docs/existing-infra.md](../../docs/existing-infra.md)) | Inline manifest in [docs/existing-infra.md](../../docs/existing-infra.md) |
| **GKE Custom Compute Classes (CCC)** | [docs/gke/ccc.md](../../docs/gke/ccc.md) | `docs/examples/ccc/computeclass.yaml` + `docs/examples/ccc/kueue-resources.yaml` | `docs/examples/quickstart/workloadmixing.yaml` |
| **Priority & Preemption (`prod` / `batch`)** | [docs/priorities-and-preemption.md](../../docs/priorities-and-preemption.md) | `docs/examples/priority-preemption/kueue-resources.yaml` | `docs/examples/priority-preemption/workloadmixing-priority.yaml` |
| **Pre-Provisioned GPU Workloads** | [docs/gpu-workloads.md](../../docs/gpu-workloads.md) | `docs/examples/gpu/kueue-resources.yaml` | `docs/examples/gpu/workloadmixing-gpu.yaml` |
| **GKE DWS Flex-Start GPU Workloads** | [docs/gke/gpu-dws-workloads.md](../../docs/gke/gpu-dws-workloads.md) | `docs/examples/gpu-dws/kueue-resources.yaml` | `docs/examples/gpu-dws/workloadmixing-gpu.yaml` |

---

## 2. Guardrails

- **Confirmation gate:** Show the target cluster context (`kubectl config current-context`), namespace (`slurm`), and planned `helm` or `kubectl` commands. Get user confirmation before installing, upgrading, or deleting the `slurm` Helm release or cluster CRs.
- **Enforce 3-place partition parity:** Every bridged partition name must match across all three places:
  1. `controller.slurmctld.env` -> `WORKLOAD_MIXING_PARTITIONS="part1,part2"` (`kubectl -n slurm exec slurm-controller-0 -c slurmctld -- printenv WORKLOAD_MIXING_PARTITIONS`).
  2. `controller.extraConf` (`slurm.conf`) -> `CliFilterParameters=workload_mixing_partitions=part1,part2` (`kubectl -n slurm exec deploy/slurm-login-slinky -c login -- grep CliFilterParameters /run/slurm/conf/slurm.conf` and `scontrol show config | grep CliFilterParameters`).
  3. `WorkloadMixing.spec.partitionsMapping[].partition` (`kubectl -n slurm get workloadmixing -o jsonpath='{..partitionsMapping[*].partition}'`).
- **Enforce `DynamicNodeFeature` parity and `ignore_constraint_validation`:**
  - `WorkloadMixing.spec.partitionsMapping[].dynamicNodeFeature` must match `Feature=` on the partition's `NodeSet` in `slurm.conf` (`NodeSet=k8s_nodes Feature=k8s_dynamic`).
  - `controller.extraConf` must include `SchedulerParameters=ignore_constraint_validation`.
- **Mount both Lua scripts on every `helm install`/`upgrade slurm`:** Pass `--set-file 'configFiles.job_submit\.lua=hack/job_submit.lua'` and `--set-file 'configFiles.cli_filter\.lua=hack/cli_filter.lua'`.
- **Specify a valid `slurmd` image in `WorkloadMixing.spec.podTemplate`:** Omitting `podTemplate` defaults the image to `DefaultSlurmdImage` in `internal/controller/slurmjob/translator.go` (`ImagePullBackOff`).
- **GPU `mappedResources` (`resourceName: "gpu"` only):** `docs/examples/gpu*/workloadmixing-gpu.yaml` only defines a typed GPU entry (`--gres=gpu:<type>:N`); to also accept untyped `--gpus=N` / `--gres=gpu:N` jobs, add a second entry with `resourceType` omitted ([docs/gpu-workloads.md](../../docs/gpu-workloads.md)).

---

## 3. Step-by-Step Procedure

### Step 1: Create the `slurm` Namespace and Apply Kueue & `ComputeClass` CRs
Follow the Kueue (and `ComputeClass` for GKE CCC) section in your selected scenario guide from Section 1 (`kubectl create namespace slurm` and `kubectl apply -f <SCENARIO_KUEUE_RESOURCES_YAML>`).
- **For GKE DWS Flex-start GPUs ([docs/gke/gpu-dws-workloads.md](../../docs/gke/gpu-dws-workloads.md)):** Do not create a Kueue `ProvisioningRequestConfig`; `docs/examples/gpu-dws/kueue-resources.yaml` triggers GKE DWS via `ResourceFlavor` labels and tolerations.

### Step 2: Install or Upgrade the Slinky `slurm` Cluster with Partition & Lua Plugin Config
Follow the Slurm installation section in your selected scenario guide from Section 1 (including both `--set-file` Lua flags from Section 2; when combining scenarios, merge `controller.extraConf` and `configFiles` from each `docs/examples/*/slurm-values.yaml`).
- **Drain before `helm upgrade slurm` or Slinky `NodeSet` changes:** Both restart `slurm-controller-0` (wiping in-memory state when `persistence.enabled: false`); cancel active jobs and wait for `JobSet` objects to finalize first (`ops-cleanup-state`).
- **After upgrading `slurm.conf` or Lua scripts:** Restart `slurm-controller-0` (`kubectl -n slurm delete pod slurm-controller-0`) if restart-only `slurm.conf` parameters (`GresTypes`, `SelectType`, `MaxNodeCount`) changed. Delete the login pod (`kubectl -n slurm delete pod -l app.kubernetes.io/name=login` — Slinky's `LoginSet` controller reverts `rollout restart`) to refresh `/run/slurm/conf/` and re-create any `useradd` test users.
- **Immutable `clusterName` across example files:** If `helm upgrade slurm` across `docs/examples/*/slurm-values.yaml` fails on immutable `ClusterName`, pass `--set clusterName=<live-value>` (`null` if `kubectl -n slurm get controller -o jsonpath='{..clusterName}'` is empty).

### Step 3: Apply the `WorkloadMixing` CR
Apply the `WorkloadMixing` manifest for your scenario from Section 1 (delete any previous `WorkloadMixing` CR in namespace `slurm` first if switching scenarios on the same cluster).
- **`podTemplate` merge & mutation rules (`internal/controller/slurmjob/translator.go`):** Only `slurmd` `cpu`/`memory` `requests` (with the `defaultMemoryPerCPUMB` per-CPU floor) and `gpu` `requests`/`limits` are overwritten; static `podTemplate` or `LimitRange` `limits` must cover that floor, and extra/sidecar requests must be in `ClusterQueue` `coveredResources`. Custom `/etc/slurm` `volumeMounts` or `--conf-server` `args` skip automatic secret/controller injection. `WorkloadMixing` edits apply only to new `JobSet`s.

*(Stop here and proceed to Section 4 Verification when configuring partitions or CRs. Run Step 4 only when asked to remove the CRs and Slurm release.)*

### Step 4: CR & Slurm Release Teardown (Run ONLY When Requested to Remove CRs)
Get user confirmation before deleting CRs or uninstalling releases. Delete all `WorkloadMixing` CRs first and wait until `kubectl get jobsets -A -l app.kubernetes.io/managed-by=k8s-bridge` returns no resources (so `JobSet` finalizers complete; see `ops-cleanup-state` if stuck) before uninstalling the `slurm` Helm release, Kueue/`ComputeClass` CRs, or base operators (`infra-deploy-stack`).

---

## 4. Verification

1. Verify `WorkloadMixing` is `Available=True` (`kubectl -n slurm wait --for=condition=Available workloadmixing --all --timeout=60s`) and bridged partitions appear in `sinfo` (`kubectl -n slurm exec deploy/slurm-login-slinky -c login -- sinfo`).
2. Ensure a non-root user (`someuser`) exists on the login pod ([docs/quickstart.md](../../docs/quickstart.md)) and run the scenario verification job, omitting `-it` in non-interactive shells (prepend `runuser -u someuser --` if not already present, plus `--chdir=/tmp` for `sbatch` in [docs/priorities-and-preemption.md](../../docs/priorities-and-preemption.md)):
   - **Batch (`sbatch`) caveats:** Check `scontrol show job <JOB_ID>` before `MinJobAge` expires (`--chdir=/tmp` writes output to the ephemeral worker pod; brief `Reason=PartitionConfig` during dynamic node registration is normal).
   - **GPU `srun` vs `sbatch` `LD_LIBRARY_PATH`:** For `srun`, pass `--export=ALL,LD_LIBRARY_PATH=/usr/local/nvidia/lib64` ([docs/gpu-workloads.md](../../docs/gpu-workloads.md)); for `sbatch`, set `LD_LIBRARY_PATH` inside `--wrap` instead of `--export=ALL,...` to avoid `user_env_retrieval_failed_requeued_held`.

---

## 5. Handoffs & Related Skills

- **To create or update GKE clusters and CCC or DWS Flex-start node pools:** Read [skills/infra-provision-cluster/SKILL.md](../infra-provision-cluster/SKILL.md).
- **To install, upgrade, or tear down the base operators (`cert-manager`, `slurm-operator`, `JobSet`, `Kueue`, `k8s-bridge`):** Read [skills/infra-deploy-stack/SKILL.md](../infra-deploy-stack/SKILL.md).
- **If a submitted job is rejected, stays held, or fails with `Translation failed:` or `BadConstraints`:** Read [skills/ops-diagnose-workload/SKILL.md](../ops-diagnose-workload/SKILL.md).
