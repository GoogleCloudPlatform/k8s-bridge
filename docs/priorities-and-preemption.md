<!--
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
-->

# Configuring priorities and preemption with k8s-bridge

This guide extends the [Quickstart guide](quickstart.md) to configure priority-based scheduling and preemption between Slurm partitions (`prod` and `batch`) and Kueue on Kubernetes worker nodes.

> **Note:** Run all commands in this guide from the repository root directory.

---

## Prerequisites

Complete the base setup from the [Quickstart guide](quickstart.md):
- [Step 1: Install prerequisites](quickstart.md#1-install-prerequisites) (`cert-manager`, Slinky Slurm operator, `JobSet`, and `Kueue`)
- [Step 4: Install k8s-bridge](quickstart.md#4-install-k8s-bridge)

---

## 1. Configure Kueue priority classes and preemption

[`docs/examples/priority-preemption/kueue-resources.yaml`](examples/priority-preemption/kueue-resources.yaml) defines:
- `WorkloadPriorityClass` resources for each priority tier (`prod-high` with value `100`, and `batch-low` with value `10`).
- `ClusterQueue` (`cluster-queue`) with `spec.preemption.withinClusterQueue: LowerPriority`, allowing high-priority workloads to preempt low-priority workloads when the 4 CPU and 8Gi memory quota is saturated.
- `LocalQueue` (`slurm-queue`) in the `slurm` namespace.

Create the `slurm` namespace and apply the Kueue priority resources:

```bash
kubectl create namespace slurm --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f docs/examples/priority-preemption/kueue-resources.yaml
```

---

## 2. Install or upgrade the Slurm cluster with priority partitions

[`docs/examples/priority-preemption/slurm-values.yaml`](examples/priority-preemption/slurm-values.yaml) configures:
- Two dynamic workload-mixing partitions: `prod` (high priority) and `batch` (low priority).
- `JobRequeue=1`: Automatically requeues preempted jobs when dynamic worker nodes are evicted and terminated by Kueue.
- `SlurmdTimeout=60`: Accelerates node loss detection (down from 300s default) to minimize failover delay.
- `CliFilterParameters=workload_mixing_partitions=prod,batch`: Validates flags on both partitions via `cli_filter.lua`.

> **Important:** Client-side `--requeue` is disallowed by `cli_filter.lua`. Setting `JobRequeue=1` cluster-side ensures automatic preemption recovery without requiring client flags.

Install (or upgrade an existing Quickstart deployment of) the Slinky `slurm` chart using [`docs/examples/priority-preemption/slurm-values.yaml`](examples/priority-preemption/slurm-values.yaml), `hack/job_submit.lua`, and `hack/cli_filter.lua`:

```bash
helm upgrade --install slurm oci://ghcr.io/slinkyproject/charts/slurm \
  --version 1.2.2 \
  --namespace slurm --create-namespace \
  -f docs/examples/priority-preemption/slurm-values.yaml \
  --set-file 'configFiles.job_submit\.lua=hack/job_submit.lua' \
  --set-file 'configFiles.cli_filter\.lua=hack/cli_filter.lua' \
  --wait
```

Verify the Slurm pods are ready:

```bash
kubectl -n slurm wait --for=condition=Ready pod --all --timeout=5m
kubectl -n slurm get pods
```

> **Existing cluster:** On an existing Slurm controller, add `PartitionName=prod Nodes=k8s_nodes`, `PartitionName=batch Nodes=k8s_nodes`, `JobRequeue=1`, and `SlurmdTimeout=60` to `/etc/slurm/slurm.conf` and run `scontrol reconfigure`.

---

## 3. Apply priority WorkloadMixing

[`docs/examples/priority-preemption/workloadmixing-priority.yaml`](examples/priority-preemption/workloadmixing-priority.yaml) maps each Slurm partition to the `slurm-queue` LocalQueue and assigns its corresponding `workloadPriorityClass`:
- `prod` &rarr; `prod-high`
- `batch` &rarr; `batch-low`

Apply [`docs/examples/priority-preemption/workloadmixing-priority.yaml`](examples/priority-preemption/workloadmixing-priority.yaml) and verify its status:

```bash
kubectl apply -f docs/examples/priority-preemption/workloadmixing-priority.yaml
kubectl -n slurm wait --for=condition=Available workloadmixing/priority --timeout=60s
kubectl -n slurm get workloadmixing priority \
  -o jsonpath='{range .status.conditions[*]}{.type}={.status} ({.reason}){"\n"}{end}'
```

Expected output:

```text
Available=True (Reconciled)
```

---

## 4. Run priority and preemption tests

Test priority scheduling and automatic preemption using the Slurm login pod:

```bash
export LOGIN="deploy/slurm-login-slinky"
```

### Step 4.1: Saturate quota with low-priority batch job

Submit a low-priority batch job across 2 dynamic nodes that consumes the entire 4 CPU and 8Gi memory quota of `cluster-queue`:

```bash
kubectl -n slurm exec -it $LOGIN -c login -- \
  sbatch --partition=batch \
         --nodes=2 \
         --ntasks-per-node=1 \
         --cpus-per-task=2 \
         --mem=4G \
         --time=00:10:00 \
         --wrap="echo 'Batch job started'; sleep 300; echo 'Batch job finished'"
```

Verify that the batch job is running:

```bash
kubectl -n slurm exec -it $LOGIN -c login -- squeue

kubectl get jobset -n slurm
kubectl get workload -n slurm
```

Expected status:
- Slurm job state: `R` (Running).
- JobSet: `SUSPENDED=false` with 2 worker pods.
- Kueue Workload: `Admitted=True`.

### Step 4.2: Submit high-priority production job to trigger preemption

While the batch job is running, submit a production job requesting 1 dynamic node (2 CPUs and 4Gi memory):

```bash
kubectl -n slurm exec -it $LOGIN -c login -- \
  sbatch --partition=prod \
         --nodes=1 \
         --ntasks-per-node=1 \
         --cpus-per-task=2 \
         --mem=4G \
         --time=00:05:00 \
         --wrap="echo 'Production job started'; sleep 60; echo 'Production job finished'"
```

### Step 4.3: Verify preemption and suspension

Check the workloads and JobSets:

```bash
kubectl get workloads -n slurm
kubectl get jobset -n slurm
```

Expected status:
1. Low-priority workload receives preemption conditions:
   ```yaml
   conditions:
   - status: "True"
     type: Preempted
     reason: PreemptedByWorkload
   - status: "True"
     type: Evicted
     reason: PreemptedByWorkload
   - status: "False"
     type: Admitted
   ```
2. Low-priority JobSet is suspended (`SUSPENDED=true`) and its worker pods terminate.
3. High-priority Workload is admitted (`Admitted=True`), its pod starts, and registers a dynamic node in Slurm.
4. In Slurm, the batch job transitions to the requeued pending state: `PD (Restarts=1)`.

### Step 4.4: Verify high-priority completion and batch resumption

Wait approximately 1 minute for the production job to finish:

```bash
kubectl -n slurm exec -it $LOGIN -c login -- squeue
```

Expected status:
1. Production job completes (`COMPLETED`).
2. Production JobSet and dynamic node are cleaned up.
3. Kueue unsuspends the batch JobSet (`SUSPENDED=false`).
4. New dynamic nodes register in Slurm, and the batch job resumes to completion.

---

## 5. Cleanup

Remove the priority and preemption resources, and follow the [Quickstart cleanup steps](quickstart.md#7-cleanup) to tear down the remaining components:

```bash
kubectl delete -f docs/examples/priority-preemption/workloadmixing-priority.yaml
# Wait until this returns no resources before deleting Kueue resources:
kubectl get jobsets -A -l app.kubernetes.io/managed-by=k8s-bridge
kubectl delete -f docs/examples/priority-preemption/kueue-resources.yaml
```
