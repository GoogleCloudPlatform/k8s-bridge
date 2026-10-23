---
name: infra-provision-cluster
description: >-
  Creates, resizes, updates, or deletes cloud Kubernetes clusters and CPU or
  GPU node pools on GKE (standard CPU node pools, Node Auto-Provisioning for
  Custom Compute Classes, DWS Flex-start GPU node pools, or labeling
  pre-provisioned GPU nodes). Use when creating, resizing, updating, or deleting
  cloud clusters and node pools for workload mixing. Don't use for installing
  Helm charts and operators inside the cluster (use infra-deploy-stack) or
  configuring and applying Kueue, ComputeClass, Slurm partition, and
  WorkloadMixing CRs (use infra-configure-crs).
---

# Cloud Cluster & Node Pool Provisioning (`infra-provision-cluster`)

## 1. Preconditions & Inputs

Before running `gcloud` or `kubectl label` commands, check or ask the user for:
1. **`PROJECT_ID`**, **`LOCATION`** (zone or region), and **`CLUSTER_NAME`**.
2. **Provisioning Mode:**
   - **Option A — Standard CPU Cluster / Node Pool or GKE Custom Compute Classes (CCC):** Standard GKE cluster/node pool ([docs/quickstart.md](../../docs/quickstart.md)) or GKE CCC with Node Auto-Provisioning ([docs/gke/ccc.md](../../docs/gke/ccc.md)).
   - **Option B — GKE Cluster with DWS Flex-Start GPU Node Pool:** [docs/gke/gpu-dws-workloads.md](../../docs/gke/gpu-dws-workloads.md).
   - **Option C — Pre-Provisioned GPU Nodes:** `## Prerequisites` in [docs/gpu-workloads.md](../../docs/gpu-workloads.md).

---

## 2. Guardrails

- **Confirmation gate before billable cloud actions:** Show the `gcloud container clusters` or `gcloud container node-pools` command (including `PROJECT_ID`, `LOCATION`, machine types, and node limits) and get user approval before running it.
- **Keep cluster creation separate from operator and CR installation:** Only provision cloud clusters/node pools and node labels here; use `infra-deploy-stack` for operators and `infra-configure-crs` for CRs.
- **Verify GKE version requirements in the target guide:** Check the cluster creation section in [docs/gke/ccc.md](../../docs/gke/ccc.md) or [docs/gke/gpu-dws-workloads.md](../../docs/gke/gpu-dws-workloads.md).
- **Install GPU drivers for CUDA workloads:** `test/e2e/infra/gke-gpu-cluster-setup.sh` omits GPU drivers; for CUDA / `nvidia-smi` workloads, include `gpu-driver-version=default` in `--accelerator` ([docs/gke/gpu-dws-workloads.md](../../docs/gke/gpu-dws-workloads.md)).

---

## 3. Step-by-Step Procedure

> **Branch Selection:** Select **Option A** (Step 1), **Option B** (Step 2), or **Option C** (Step 3). Run **Step 4** only when asked to resize or delete a cloud cluster or node pool.

### Step 1: Option A — Create or Update a Standard CPU Cluster / Node Pool or GKE CCC Cluster
After user confirmation, create or resize the standard GKE cluster/node pool, or run the `gcloud` commands in the cluster creation section of [docs/gke/ccc.md](../../docs/gke/ccc.md).

### Step 2: Option B — Create a GKE Cluster and DWS Flex-Start GPU Node Pool
After user confirmation, run the `gcloud` cluster and node pool creation commands in [docs/gke/gpu-dws-workloads.md](../../docs/gke/gpu-dws-workloads.md).

### Step 3: Option C — Verify and Label Pre-Provisioned GPU Nodes
Follow `## Prerequisites` in [docs/gpu-workloads.md](../../docs/gpu-workloads.md) to verify `nvidia.com/gpu` allocatable capacity and ensure the GPU model label matched by `WorkloadMixing.spec.mappedResources[].nodeSelectors` is present.

### Step 4: Cloud Cluster or Node Pool Teardown (Run ONLY When Requested to Delete Cloud Infra)
First remove in-cluster CRs (`infra-configure-crs`) and operators (`infra-deploy-stack`). After user confirmation, follow the cleanup section of [docs/gke/ccc.md](../../docs/gke/ccc.md) or [docs/gke/gpu-dws-workloads.md](../../docs/gke/gpu-dws-workloads.md) (or run `gcloud container node-pools delete <GPU_NODE_POOL_NAME> --cluster=<CLUSTER_NAME> --location=<LOCATION> --project=<PROJECT_ID>` to delete only a node pool).

---

## 4. Verification

1. Confirm `kubectl config current-context` targets the cluster and the cluster/node pools are `RUNNING` (CCC and DWS Flex-start `--num-nodes=0 --min-nodes=0` scale from 0 on workload admission; pre-provisioned GPU nodes immediately expose `nvidia.com/gpu` capacity and the `nodeSelectors` label).

---

## 5. Handoffs & Related Skills

- **Next step — Install the base operators (`cert-manager`, `slurm-operator`, `JobSet`, `Kueue`, `k8s-bridge`):** Read [skills/infra-deploy-stack/SKILL.md](../infra-deploy-stack/SKILL.md).
- **After base operators are installed — Configure and apply `ComputeClass`, Kueue, Slinky `slurm` partition, and `WorkloadMixing` CRs:** Read [skills/infra-configure-crs/SKILL.md](../infra-configure-crs/SKILL.md).
- **To run the automated GKE GPU E2E test suite:** Read [skills/test-e2e-cluster/SKILL.md](../test-e2e-cluster/SKILL.md).
