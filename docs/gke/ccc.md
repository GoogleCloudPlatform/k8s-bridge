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

# Autoscale Slurm with GKE Custom Compute Classes and k8s-bridge

This guide extends the [Quickstart guide](../quickstart.md) to configure a Google Kubernetes Engine (GKE) [Custom Compute Class (CCC)](https://cloud.google.com/kubernetes-engine/docs/concepts/custom-compute-classes).

Custom Compute Classes define hardware provisioning priorities in GKE (e.g., preferring Spot VMs, then falling back to On-Demand VMs). With Kueue and `k8s-bridge`:

1. Kueue admits translated Slurm workloads and injects the `cloud.google.com/compute-class: cost-optimized` node selector into worker pods.
2. GKE Cluster Autoscaler provisions the highest-priority available hardware into a new node pool.

> **Architectural Note:** Slurm and the WorkloadMixing operator require no configuration changes for Custom Compute Classes. Kueue's NodeLabel injection and GKE scheduling handle the integration.

> **Note:** Run all commands in this guide from the repository root directory.

---

## Prerequisites

- [`gcloud` CLI](https://cloud.google.com/sdk/docs/install)
- The tools and versions listed in the [Quickstart Prerequisites & Versions](../quickstart.md#prerequisites--versions) (`kubectl` and `helm` 3.8+)

---

## 1. Create a GKE cluster

To use Custom Compute Classes, enable Node Auto-Provisioning (NAP) on your GKE cluster.

> **Note:** NAP scales from 0 nodes. Your cluster will have no custom compute nodes initially. GKE creates the node pool when a matching job schedules in step 7.

1. Create a cluster with Node Auto-Provisioning:

   ```bash
   gcloud container clusters create <CLUSTER_NAME> \
       --project=<PROJECT_ID> \
       --location=<LOCATION> \
       --cluster-version=<VERSION> \
       --machine-type=e2-standard-4 \
       --num-nodes=2 \
       --enable-autoprovisioning \
       --min-cpu=1 \
       --max-cpu=64 \
       --min-memory=1 \
       --max-memory=256
   ```

   Replace the following:

   - `<CLUSTER_NAME>`: the name of your GKE cluster.
   - `<PROJECT_ID>`: your Google Cloud project ID.
   - `<LOCATION>`: the Compute Engine region or zone for the cluster.
   - `<VERSION>`: the GKE cluster version (`1.31` or later).

   > **Existing cluster:** Enable Node Auto-Provisioning:
   > ```bash
   > gcloud container clusters update <CLUSTER_NAME> \
   >     --project=<PROJECT_ID> \
   >     --location=<LOCATION> \
   >     --enable-autoprovisioning \
   >     --min-cpu=1 \
   >     --max-cpu=64 \
   >     --min-memory=1 \
   >     --max-memory=256
   > ```

2. Configure `kubectl`:

   ```bash
   gcloud container clusters get-credentials <CLUSTER_NAME> \
       --project=<PROJECT_ID> \
       --location=<LOCATION>
   ```

---

## 2. Install prerequisites and k8s-bridge

Complete the following steps from the [Quickstart guide](../quickstart.md) on your GKE cluster:

1. [Step 1: Install prerequisites](../quickstart.md#1-install-prerequisites) (`cert-manager`, Slinky Slurm operator `1.2.2`, `JobSet` `0.12.0`, and `Kueue` `0.19.4`).
2. [Step 4: Install k8s-bridge](../quickstart.md#4-install-k8s-bridge).

---

## 3. Create the Custom Compute Class

[`docs/examples/ccc/computeclass.yaml`](../examples/ccc/computeclass.yaml) defines a `cost-optimized` `ComputeClass`. It provisions `e2` Spot instances, falling back to `e2` On-Demand instances:

```bash
kubectl apply -f docs/examples/ccc/computeclass.yaml
kubectl get computeclass cost-optimized
```

---

## 4. Configure Kueue quota

[`docs/examples/ccc/kueue-resources.yaml`](../examples/ccc/kueue-resources.yaml) configures Kueue:
- `ResourceFlavor` (`default-flavor`): Sets `nodeLabels` to schedule pods on cost-optimized nodes.
- `ClusterQueue` (`cluster-queue`): Configures quota (`64` CPU, `256Gi` memory) using `default-flavor`.
- `LocalQueue` (`slurm-queue`): Routes `slurm` namespace jobs to `cluster-queue`.

Create the namespace and apply resources:

```bash
kubectl create namespace slurm --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f docs/examples/ccc/kueue-resources.yaml
```

---

## 5. Install or upgrade the Slurm cluster

Deploy the standard partition (`k8s-partition`).

Install or upgrade the `slurm` chart:

```bash
helm upgrade --install slurm oci://ghcr.io/slinkyproject/charts/slurm \
  --version 1.2.2 \
  --namespace slurm --create-namespace \
  -f docs/examples/quickstart/slurm-values.yaml \
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

## 6. Apply WorkloadMixing

`k8s-bridge` routes the `k8s-partition` to `slurm-queue`. Apply the base configuration:

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

## 7. Run a test job with `srun`

Submitting a job to `k8s-partition` triggers GKE Cluster Autoscaler to provision a matching node pool (this takes a few minutes).

1. Create a non-privileged user:

   ```bash
   export LOGIN="deploy/slurm-login-slinky"
   kubectl -n slurm exec $LOGIN -c login -- useradd -m -s /bin/bash someuser
   ```

2. Run a job on 1 custom compute node:

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

3. Verify GKE provisioned the node:

   ```bash
   kubectl get nodes -l cloud.google.com/compute-class=cost-optimized \
     -L cloud.google.com/compute-class,cloud.google.com/gke-provisioning
   ```

   Check the `GKE-PROVISIONING` column. It displays `spot` if capacity was available, or `standard` if GKE fell back to On-Demand capacity.

4. Run a job across 2 custom compute nodes:

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

---

## 8. Cleanup

Delete the tutorial resources:

```bash
kubectl delete -f docs/examples/quickstart/workloadmixing.yaml

# Wait until this returns no resources before deleting Kueue/ComputeClass CRs or uninstalling k8s-bridge
kubectl get jobsets -A -l app.kubernetes.io/managed-by=k8s-bridge

kubectl delete -f docs/examples/ccc/kueue-resources.yaml
kubectl delete -f docs/examples/ccc/computeclass.yaml
helm uninstall k8s-bridge -n k8s-bridge-system
helm uninstall slurm -n slurm
```

Complete the rest of the teardown tasks as listed in Quickstart [Step 7](../quickstart.md#7-cleanup).

Finally, if you created a new GKE cluster for this guide, delete it:

```bash
gcloud container clusters delete <CLUSTER_NAME> \
    --project=<PROJECT_ID> \
    --location=<LOCATION>
```

> **Previously existing cluster:** disable Node Auto-Provisioning if you enabled it in the first step:
> ```bash
> gcloud container clusters update <CLUSTER_NAME> \
>     --project=<PROJECT_ID> \
>     --location=<LOCATION> \
>     --no-enable-autoprovisioning
> ```
