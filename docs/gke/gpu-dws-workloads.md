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

# Autoscale Slurm GPU workloads with Dynamic Workload Scheduler and k8s-bridge

This guide extends the [Quickstart guide](../quickstart.md) and the [GPU workloads guide](../gpu-workloads.md) to set up a Google Kubernetes Engine (GKE) cluster with a Dynamic Workload Scheduler (DWS) Flex-start GPU node pool and run `nvidia-smi` via `srun` on dynamically provisioned GPU nodes.

> **Note:** Run all commands in this guide from the repository root directory.

---

## Prerequisites

- [`gcloud` CLI](https://cloud.google.com/sdk/docs/install)
- The tools and versions listed in the [Quickstart Prerequisites & Versions](../quickstart.md#prerequisites--versions) (`kubectl` and `helm` 3.8+)

---

## 1. Create a GKE cluster with a DWS Flex GPU node pool

1. Create a Standard GKE cluster:

   ```bash
   gcloud container clusters create <CLUSTER_NAME> \
       --project=<PROJECT_ID> \
       --location=<LOCATION> \
       --cluster-version=<VERSION> \
       --machine-type=e2-standard-4 \
       --num-nodes=2
   ```

   Replace the following:

   - `<CLUSTER_NAME>`: the name of your GKE cluster.
   - `<PROJECT_ID>`: your Google Cloud project ID.
   - `<LOCATION>`: the Compute Engine region or zone for the cluster.
   - `<VERSION>`: the GKE cluster version (`1.35.2-gke.1842000` or later).

2. Create a Flex-start GPU node pool with NVIDIA L4 GPUs scaled to `0` initial nodes:

   ```bash
   gcloud container node-pools create <GPU_NODE_POOL_NAME> \
       --cluster=<CLUSTER_NAME> \
       --project=<PROJECT_ID> \
       --location=<LOCATION> \
       --machine-type=g2-standard-8 \
       --accelerator=type=nvidia-l4,count=1,gpu-driver-version=default \
       --flex-start \
       --enable-autoscaling \
       --num-nodes=0 \
       --min-nodes=0 \
       --max-nodes=2 \
       --location-policy=ANY \
       --reservation-affinity=none
   ```

   Replace `<GPU_NODE_POOL_NAME>` with the name for your DWS Flex GPU node pool (for example, `dws-flex-gpu-pool`).

3. Configure `kubectl` to communicate with your cluster:

   ```bash
   gcloud container clusters get-credentials <CLUSTER_NAME> \
       --project=<PROJECT_ID> \
       --location=<LOCATION>
   ```

   > **Note:** Because the Flex-start GPU node pool is created with `--num-nodes=0 --min-nodes=0`, `kubectl get nodes -L cloud.google.com/gke-accelerator` will show `0` GPU nodes until a GPU job is submitted and provisioned on demand.

---

## 2. Install prerequisites and k8s-bridge

Complete the following steps from the [Quickstart guide](../quickstart.md) on your GKE cluster:

1. [Step 1: Install prerequisites](../quickstart.md#1-install-prerequisites) (`cert-manager`, Slinky Slurm operator `1.2.2`, `JobSet` `0.12.0`, and `Kueue` `0.19.4`).
2. [Step 4: Install k8s-bridge](../quickstart.md#4-install-k8s-bridge).

---

## 3. Configure Kueue quota for DWS Flex-start GPUs

[`docs/examples/gpu-dws/kueue-resources.yaml`](../examples/gpu-dws/kueue-resources.yaml) configures:
- `ResourceFlavor` (`gpu-flavor`) with `cloud.google.com/gke-accelerator: nvidia-l4` and `cloud.google.com/gke-flex-start: "true"` node labels, plus `nvidia.com/gpu` and `cloud.google.com/gke-queued: "true"` tolerations required by DWS Flex-start.
- `ClusterQueue` (`gpu-cluster-queue`) with `16` CPU, `64Gi` memory, and `2` `nvidia.com/gpu` nominal quota matching two `g2-standard-8` nodes.
- `LocalQueue` (`gpu-queue`) in the `slurm` namespace.

Create the `slurm` namespace and apply the Kueue resources:

```bash
kubectl create namespace slurm --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f docs/examples/gpu-dws/kueue-resources.yaml
```

---

## 4. Install or upgrade the Slurm cluster

[`docs/examples/gpu-dws/slurm-values.yaml`](../examples/gpu-dws/slurm-values.yaml) configures the Slinky Slurm cluster with `gres.conf` (`AutoDetect=nvidia`), `GresTypes=gpu`, and the `k8s-gpu` workload-mixing partition using the upstream `ghcr.io/slinkyproject` images.

Install (or upgrade) the `slurm` chart using [`docs/examples/gpu-dws/slurm-values.yaml`](../examples/gpu-dws/slurm-values.yaml), `hack/job_submit.lua`, and `hack/cli_filter.lua`:

```bash
helm upgrade --install slurm oci://ghcr.io/slinkyproject/charts/slurm \
  --version 1.2.2 \
  --namespace slurm --create-namespace \
  -f docs/examples/gpu-dws/slurm-values.yaml \
  --set-file 'configFiles.job_submit\.lua=hack/job_submit.lua' \
  --set-file 'configFiles.cli_filter\.lua=hack/cli_filter.lua' \
  --wait
```

Verify that the Slurm pods are ready:

```bash
kubectl -n slurm wait --for=condition=Ready pod --all --timeout=5m
kubectl -n slurm get pods
```

---

## 5. Apply GPU WorkloadMixing

[`docs/examples/gpu-dws/workloadmixing-gpu.yaml`](../examples/gpu-dws/workloadmixing-gpu.yaml) links the `k8s-gpu` partition to `gpu-queue`, sets `cloud.google.com/gke-accelerator: nvidia-l4` and `cloud.google.com/gke-flex-start: "true"` under `mappedResources.nodeSelectors` so GKE Cluster Autoscaler scales up the Flex-start GPU node pool from `0` nodes, and configures the `slurmd` pod template with `cloud.google.com/gke-queued: "true"` and `nvidia.com/gpu` tolerations.

Apply [`docs/examples/gpu-dws/workloadmixing-gpu.yaml`](../examples/gpu-dws/workloadmixing-gpu.yaml) and verify its status:

```bash
kubectl apply -f docs/examples/gpu-dws/workloadmixing-gpu.yaml
kubectl -n slurm wait --for=condition=Available workloadmixing/gpu --timeout=60s
kubectl -n slurm get workloadmixing gpu \
  -o jsonpath='{range .status.conditions[*]}{.type}={.status} ({.reason}){"\n"}{end}'
```

---

## 6. Run `nvidia-smi` with `srun`

When you submit a job below, Kueue admits the `JobSet` and GKE Cluster Autoscaler + DWS Flex-start dynamically provision the GPU worker node(s) (which typically takes a few minutes while the node pool scales up from `0`).

1. Run `nvidia-smi` on 1 GPU node:

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

2. Run `nvidia-smi` across 2 GPU nodes in parallel:

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

## 7. Clean up

Remove the DWS GPU resources, follow the [Quickstart cleanup steps](../quickstart.md#7-cleanup), and optionally delete the GKE cluster:

```bash
kubectl delete -f docs/examples/gpu-dws/workloadmixing-gpu.yaml
kubectl delete -f docs/examples/gpu-dws/kueue-resources.yaml

gcloud container clusters delete <CLUSTER_NAME> \
    --project=<PROJECT_ID> \
    --location=<LOCATION>
```

