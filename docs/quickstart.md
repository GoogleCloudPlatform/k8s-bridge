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

# Quickstart: Running workloads with k8s-bridge

During the process outlined in this guide you deploy Kueue, a Slinky Slurm cluster, and `k8s-bridge` on a Kubernetes cluster. During setup you configure a Kueue queue, a Slurm partition, and a `WorkloadMixing` resource that links them together. In everyday use you submit standard `srun` and `sbatch` jobs from the Slurm login pod while Kubernetes and Kueue provision the worker pods on demand.

> **Note:** If you are integrating with an already running Slurm cluster, see the [Existing Infrastructure Guide](existing-infra.md).

---

## Prerequisites & Versions

Tools required:

- `kubectl`
- `helm` 3.8+
- Worker nodes with at least `1000m` allocatable CPU and `2Gi` allocatable memory free per dynamic worker pod

| Component | Chart | Verified version |
|---|---|---|
| cert-manager | `oci://quay.io/jetstack/charts/cert-manager` | v1.21.2 |
| Slinky Slurm operator CRDs | `oci://ghcr.io/slinkyproject/charts/slurm-operator-crds` | 1.2.2 |
| Slinky Slurm operator | `oci://ghcr.io/slinkyproject/charts/slurm-operator` | 1.2.2 |
| JobSet | `oci://registry.k8s.io/jobset/charts/jobset` | 0.12.0 |
| Kueue | `oci://registry.k8s.io/kueue/charts/kueue` | 0.19.4 |
| Slurm cluster | `oci://ghcr.io/slinkyproject/charts/slurm` | 1.2.2 (Slurm 26.05) |
| k8s-bridge | `oci://ghcr.io/googlecloudplatform/charts/k8s-bridge` | 0.1.0 |

Run all commands below from the root of the `k8s-bridge` repository.

---

## 1. Install prerequisites

### 1.1. Install cert-manager

The Slurm operator webhook requires `cert-manager`:

```bash
helm install cert-manager oci://quay.io/jetstack/charts/cert-manager \
  --version v1.21.2 \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true --wait
```

> **Existing setup:** Skip this step if `cert-manager` is already installed on the cluster.

### 1.2. Install Slinky Slurm operator

Install the Slinky CRDs chart first, then the operator:

```bash
helm install slurm-operator-crds oci://ghcr.io/slinkyproject/charts/slurm-operator-crds \
  --version 1.2.2 \
  --namespace slinky --create-namespace --wait

helm install slurm-operator oci://ghcr.io/slinkyproject/charts/slurm-operator \
  --version 1.2.2 \
  --namespace slinky --create-namespace --wait
```

### 1.3. Install JobSet

`k8s-bridge` translates held Slurm jobs into Kubernetes `JobSet` objects. Install `JobSet` **before** Kueue so Kueue's `jobset.x-k8s.io/jobset` integration detects the CRD at startup:

```bash
helm install jobset oci://registry.k8s.io/jobset/charts/jobset \
  --version 0.12.0 \
  --namespace jobset-system --create-namespace --wait
```

### 1.4. Install Kueue

```bash
helm install kueue oci://registry.k8s.io/kueue/charts/kueue \
  --version 0.19.4 \
  --namespace kueue-system --create-namespace --wait
```

---

## 2. Configure Kueue quota

[`docs/examples/quickstart/kueue-resources.yaml`](examples/quickstart/kueue-resources.yaml) defines a minimal `ResourceFlavor`, `ClusterQueue`, and a `LocalQueue` (`slurm-queue`) in the `slurm` namespace. Create the `slurm` namespace and apply the queues:

```bash
kubectl create namespace slurm
kubectl apply -f docs/examples/quickstart/kueue-resources.yaml
```

---

## 3. Install the Slurm cluster

[`docs/examples/quickstart/slurm-values.yaml`](examples/quickstart/slurm-values.yaml) configures a minimal Slurm controller, `slurmrestd`, and login pod with a dynamic workload-mixing partition (`k8s-partition`), static compute nodes disabled (`nodesets.slinky.enabled: false`), and two Lua plugins (`job_submit.lua` holds jobs in `slurmctld` for `k8s-bridge`; `cli_filter.lua` validates supported `sbatch`/`srun` flags on the login pod).

Install the `slurm` chart with [`docs/examples/quickstart/slurm-values.yaml`](examples/quickstart/slurm-values.yaml), `hack/job_submit.lua`, and `hack/cli_filter.lua`:

```bash
helm install slurm oci://ghcr.io/slinkyproject/charts/slurm \
  --version 1.2.2 \
  --namespace slurm --create-namespace \
  -f docs/examples/quickstart/slurm-values.yaml \
  --set-file 'configFiles.job_submit\.lua=hack/job_submit.lua' \
  --set-file 'configFiles.cli_filter\.lua=hack/cli_filter.lua' \
  --wait
```

Verify the Slurm pods are ready:

```bash
kubectl -n slurm wait --for=condition=Ready pod --all --timeout=5m
kubectl -n slurm get pods
```

---

## 4. Install k8s-bridge

Install the `k8s-bridge` controller from the published OCI Helm chart:

```bash
helm install k8s-bridge oci://ghcr.io/googlecloudplatform/charts/k8s-bridge \
  --version 0.1.0 \
  --namespace k8s-bridge-system --create-namespace \
  --wait
```

---

## 5. Apply WorkloadMixing

[`docs/examples/quickstart/workloadmixing.yaml`](examples/quickstart/workloadmixing.yaml) links the Slurm `Controller`, the `k8s-partition` partition, the `slurm-queue` Kueue `LocalQueue`, and the `slurmd` worker pod template. Apply it and verify it reconciles:

```bash
kubectl apply -f docs/examples/quickstart/workloadmixing.yaml
kubectl -n slurm wait --for=condition=Available workloadmixing/default --timeout=60s
kubectl -n slurm get workloadmixing default \
  -o jsonpath='{range .status.conditions[*]}{.type}={.status} ({.reason}){"\n"}{end}'
```

Expected output:

```text
Available=True (Reconciled)
```

---

## 6. Run a test job with `srun`

Because `kubectl exec` enters the login container as `root` — and `hack/job_submit.lua` treats `root` and `slurm` as privileged administrative accounts that bypass normal user job-modification and immutability rules — create a non-privileged user (e.g. `someuser`) on the login pod first:

```bash
export LOGIN="deploy/slurm-login-slinky"
kubectl -n slurm exec $LOGIN -c login -- useradd -m -s /bin/bash someuser
```

Run a job as `someuser` on 1 dynamic node:

```bash
kubectl -n slurm exec -it $LOGIN -c login -- \
  runuser -u someuser -- \
  srun --partition=k8s-partition \
       --nodes=1 \
       --ntasks-per-node=1 \
       --cpus-per-task=1 \
       --mem=2G \
       --chdir=/tmp \
       hostname
```

Because `k8s-partition` starts with `0` static nodes, `srun` initially prints `Requested partition configuration not available now` and waits ~15–45 seconds while `k8s-bridge` creates the `JobSet`, Kueue admits it, and the dynamic `slurmd` pod starts and registers:

```text
srun: Requested partition configuration not available now
srun: job 1 queued and waiting for resources
srun: job 1 has been allocated resources
slurm-job-1-slurmd-0-0
```

Run across 2 dynamic nodes in parallel:

```bash
kubectl -n slurm exec -it $LOGIN -c login -- \
  runuser -u someuser -- \
  srun --partition=k8s-partition \
       --nodes=2 \
       --ntasks-per-node=1 \
       --cpus-per-task=1 \
       --mem=2G \
       --chdir=/tmp \
       hostname
```

Expected output:

```text
srun: Requested partition configuration not available now
srun: job 2 queued and waiting for resources
srun: job 2 has been allocated resources
slurm-job-2-slurmd-0-0
slurm-job-2-slurmd-1-0
```

---

## 7. Cleanup

Delete the `WorkloadMixing` first and wait until `k8s-bridge` has removed its finalizer from all JobSets (the `kubectl get` command must return no resources) before uninstalling `k8s-bridge`:

```bash
kubectl delete -f docs/examples/quickstart/workloadmixing.yaml
kubectl get jobsets -A -l app.kubernetes.io/managed-by=k8s-bridge
```

Then remove the remaining components:

```bash
helm uninstall k8s-bridge -n k8s-bridge-system
helm uninstall slurm -n slurm
kubectl delete -f docs/examples/quickstart/kueue-resources.yaml
helm uninstall kueue -n kueue-system
helm uninstall jobset -n jobset-system
helm uninstall slurm-operator slurm-operator-crds -n slinky
helm uninstall cert-manager -n cert-manager
kubectl delete namespace slurm k8s-bridge-system slinky kueue-system jobset-system cert-manager --ignore-not-found
```
