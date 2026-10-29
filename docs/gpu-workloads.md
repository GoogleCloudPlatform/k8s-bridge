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

# Running GPU workloads with k8s-bridge

This guide extends the [Quickstart guide](quickstart.md) to configure a GPU-enabled Slurm partition (`k8s-gpu`) and run `nvidia-smi` jobs via `srun` on Kubernetes GPU worker nodes.

> **Note:** Run all commands in this guide from the repository root directory.

---

## Prerequisites

1. Complete the base setup from the [Quickstart guide](quickstart.md):
   - [Step 1: Install prerequisites](quickstart.md#1-install-prerequisites) (`cert-manager`, Slinky Slurm operator, `JobSet`, and `Kueue`)
   - [Step 4: Install k8s-bridge](quickstart.md#4-install-k8s-bridge)
2. Ensure your Kubernetes cluster has 2 GPU worker nodes with the NVIDIA device plugin installed, labeled with `nvidia.com/gpu.product=NVIDIA-L4`, and each reporting `1` allocatable `nvidia.com/gpu`:

```bash
kubectl label nodes -l <gpu-node-selector> nvidia.com/gpu.product=NVIDIA-L4 --overwrite
kubectl get nodes -L nvidia.com/gpu.product
kubectl get nodes -o jsonpath="{range .items[*]}{.metadata.name}{'\t'}{.status.allocatable['nvidia\.com/gpu']}{'\n'}{end}"
```

---

## 1. Configure Kueue quota for GPUs

[`docs/examples/gpu/kueue-resources.yaml`](examples/gpu/kueue-resources.yaml) defines a `ResourceFlavor` (`gpu-flavor` matching `nvidia.com/gpu.product: NVIDIA-L4`), a `ClusterQueue` (`gpu-cluster-queue`), and a `LocalQueue` (`gpu-queue`) in the `slurm` namespace with quota for 2 GPU nodes (each with 1 GPU).

Ensure the `slurm` namespace exists and apply the GPU Kueue resources:

```bash
kubectl create namespace slurm --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f docs/examples/gpu/kueue-resources.yaml
```

---

## 2. Install or upgrade the Slurm cluster with GPU support

[`docs/examples/gpu/slurm-values.yaml`](examples/gpu/slurm-values.yaml) extends the Slurm cluster configuration with `gres.conf` (`AutoDetect=nvidia`), `GresTypes=gpu`, and the `k8s-gpu` workload-mixing partition.

Install (or upgrade an existing Quickstart deployment of) the Slinky `slurm` chart using [`docs/examples/gpu/slurm-values.yaml`](examples/gpu/slurm-values.yaml), `hack/job_submit.lua`, and `hack/cli_filter.lua`:

```bash
helm upgrade --install slurm oci://ghcr.io/slinkyproject/charts/slurm \
  --version 1.2.2 \
  --namespace slurm --create-namespace \
  -f docs/examples/gpu/slurm-values.yaml \
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

## 3. Apply GPU WorkloadMixing

[`docs/examples/gpu/workloadmixing-gpu.yaml`](examples/gpu/workloadmixing-gpu.yaml) links the `k8s-gpu` Slurm partition to the `gpu-queue` Kueue `LocalQueue`, configures the `slurmd` pod template with GPU tolerations and `LD_LIBRARY_PATH`, and maps Slurm `gpu:nvidia_l4` GRES requests (`resourceType: nvidia_l4`) to Kubernetes `nvidia.com/gpu` resources and `nvidia.com/gpu.product: NVIDIA-L4` node selectors.

> **Note:** `mappedResources` matches exact `(resourceName, resourceType)` pairs. To also allow untyped GPU requests (such as `--gres=gpu:1` or `--gpus=1`), add a second `mappedResources` entry with `resourceName: gpu` and `resourceType` omitted.

Apply [`docs/examples/gpu/workloadmixing-gpu.yaml`](examples/gpu/workloadmixing-gpu.yaml) and verify its status:

```bash
kubectl apply -f docs/examples/gpu/workloadmixing-gpu.yaml
kubectl -n slurm wait --for=condition=Available workloadmixing/gpu --timeout=60s
kubectl -n slurm get workloadmixing gpu \
  -o jsonpath='{range .status.conditions[*]}{.type}={.status} ({.reason}){"\n"}{end}'
```

---

## 4. Run `nvidia-smi` with `srun`

Run on 1 GPU node:

```bash
export LOGIN="deploy/slurm-login-slinky"

kubectl -n slurm exec -it $LOGIN -c login -- \
  srun --partition=k8s-gpu \
       --nodes=1 \
       --ntasks-per-node=1 \
       --gres=gpu:nvidia_l4:1 \
       --cpus-per-task=2 \
       --mem=8G \
       --chdir=/tmp \
       --export=ALL,LD_LIBRARY_PATH=/usr/local/nvidia/lib64 \
       /usr/local/nvidia/bin/nvidia-smi
```

Run across 2 GPU nodes in parallel:

```bash
kubectl -n slurm exec -it $LOGIN -c login -- \
  srun --partition=k8s-gpu \
       --nodes=2 \
       --ntasks-per-node=1 \
       --gres=gpu:nvidia_l4:1 \
       --cpus-per-task=2 \
       --mem=8G \
       --chdir=/tmp \
       --export=ALL,LD_LIBRARY_PATH=/usr/local/nvidia/lib64 \
       bash -c 'echo "--- $(hostname) ---"; /usr/local/nvidia/bin/nvidia-smi'
```

---

## 5. Cleanup

Remove the GPU resources and follow the [Quickstart cleanup steps](quickstart.md#7-cleanup) to tear down the remaining components:

```bash
kubectl delete -f docs/examples/gpu/workloadmixing-gpu.yaml
# Wait until this returns no resources before deleting Kueue resources:
kubectl get jobsets -A -l app.kubernetes.io/managed-by=k8s-bridge
kubectl delete -f docs/examples/gpu/kueue-resources.yaml
```
