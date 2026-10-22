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

# Add Workload Mixing to Existing Infrastructure

Add dynamic workload mixing to an existing Slinky Slurm cluster and Kueue deployment using `k8s-bridge`. Add dynamic partitions alongside static nodes without disruption.

## Prerequisites

- `cert-manager`
- `JobSet` (0.12.0+)
- `Kueue` (0.19.4+)
- An existing Slinky Slurm installation.

Ensure you clone the `k8s-bridge` repository and navigate to its root directory.

---

## 1. Install missing prerequisites

Review the required tools in the [Prerequisites section of the Quickstart Guide](quickstart.md#1-install-prerequisites). If your cluster lacks any components (such as `cert-manager` or `JobSet`), follow the Quickstart instructions to install them.

> **Note:** The `jobset.x-k8s.io/jobset` integration is enabled by default in Kueue. However, if `JobSet` was installed *after* Kueue, Kueue will not detect the CRD automatically. You must restart the Kueue controller manager (`kubectl rollout restart deployment/kueue-controller-manager -n kueue-system`) to activate the integration.

---

## 2. Configure the existing Kueue

Link Slurm to an existing Kueue `ClusterQueue` by creating a `LocalQueue`.

Use `kubectl get clusterqueue` to find an existing cluster queue name.

```yaml
apiVersion: kueue.x-k8s.io/v1beta2
kind: LocalQueue
metadata:
  name: dynamic-slurm-queue
  namespace: slurm
spec:
  clusterQueue: <YOUR_EXISTING_CLUSTER_QUEUE>
```

Save this as `localqueue.yaml` and run `kubectl apply -f localqueue.yaml`.

---

## 3. Update the existing Slurm cluster

Update your Slurm `values.yaml`. Ensure that `restapi` (`slurmrestd`) is enabled, and keep your static partitions and nodes.

Add the following configurations to intercept dynamic workloads:

```yaml
controller:
  slurmctld:
    env:
      - name: WORKLOAD_MIXING_PARTITIONS
        value: "dynamic-partition"
  extraConf: |-
    SchedulerParameters=ignore_constraint_validation
    JobSubmitPlugins=job_submit/lua
    CliFilterPlugins=cli_filter/lua
    CliFilterParameters=workload_mixing_partitions=dynamic-partition
    NodeSet=dynamic_nodes Feature=dynamic_feature
    PartitionName=dynamic-partition Nodes=dynamic_nodes
```

Run a helm upgrade, passing in the Lua plugins via `--set-file` to avoid syntax errors:

```bash
helm upgrade slurm oci://ghcr.io/slinkyproject/charts/slurm \
  -n slurm \
  -f your-updated-slurm-values.yaml \
  --set-file 'configFiles.job_submit\.lua=hack/job_submit.lua' \
  --set-file 'configFiles.cli_filter\.lua=hack/cli_filter.lua'
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

Create a `WorkloadMixing` resource linking your Slurm `Controller`, your dynamic partition, and your Kueue `LocalQueue`. Ensure `controllerRef.name` matches the name of your existing Slurm cluster `Controller`, and that `dynamicNodeFeature` matches the feature configured in `extraConf`. Save the following configuration as `workloadmixing.yaml`:

```yaml
apiVersion: k8s-bridge.x-k8s.io/v1alpha1
kind: WorkloadMixing
metadata:
  name: slurm-workload-mixing
  namespace: slurm
spec:
  controllerRef:
    # This must match your existing Slurm Controller resource name
    name: slurm
  partitionsMapping:
    - partition: dynamic-partition
      localQueue: dynamic-slurm-queue
      dynamicNodeFeature: dynamic_feature
  podTemplate:
    spec:
      restartPolicy: Never
      containers:
        - name: slurmd
          # Ensure this tag specifically matches your Slurm controller version
          image: "ghcr.io/slinkyproject/slurmd:26.05-ubuntu24.04"
          securityContext:
            privileged: true
```

Apply the resource:

```bash
kubectl apply -f workloadmixing.yaml
kubectl -n slurm wait --for=condition=Available workloadmixing/slurm-workload-mixing --timeout=60s
```

---

## 6. Cleanup safely

To remove workload mixing without disrupting Slurm or Kueue:

1. Delete the `WorkloadMixing` resource:
   ```bash
   kubectl delete -f workloadmixing.yaml
   ```
   Wait until `k8s-bridge` has removed its finalizer from all JobSets, i.e. the following command returns no resources:
   ```bash
   kubectl get jobsets -A -l app.kubernetes.io/managed-by=k8s-bridge
   ```
2. Uninstall `k8s-bridge`:
   ```bash
   helm uninstall k8s-bridge -n k8s-bridge-system
   kubectl delete namespace k8s-bridge-system
   ```
3. Remove your new Kueue queues:
   ```bash
   kubectl delete localqueue dynamic-slurm-queue -n slurm
   ```
4. Revert your `slurm-values.yaml` (remove `extraConf` dynamic configurations and `WORKLOAD_MIXING_PARTITIONS` environment variable). Run `helm upgrade` on your Slurm installation again, dropping the `--set-file` commands for the Lua plugins.
