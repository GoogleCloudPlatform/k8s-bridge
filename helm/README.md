# k8s-bridge Helm Chart

A Helm chart for installing the `k8s-bridge` custom controller. This controller enables Workload Mixing (resource sharing) between Slurm and Kubernetes/GKE by detecting Slurm jobs in a `HOLD` state, creating corresponding `JobSets` managed by Kueue, and registering them as dynamic Slurm nodes.

## Prerequisites

- Kubernetes 1.25+
- Helm 3.0.0+
- [cert-manager](https://cert-manager.io/) installed on the cluster (for webhook certificates).
- [Slurm Operator](https://github.com/SlinkyProject/slurm-operator) installed on the cluster.
- [Kueue](https://kueue.sigs.k8s.io/) installed on the cluster (for JobSet management).

## Installing the Chart

To install the published OCI chart with the release name `k8s-bridge`:

```console
helm install k8s-bridge oci://ghcr.io/googlecloudplatform/charts/k8s-bridge \
  --version 0.1.0 \
  --namespace k8s-bridge-system --create-namespace
```

Or to install from the local chart directory:

```console
helm install k8s-bridge . --namespace k8s-bridge-system --create-namespace
```

## Uninstalling the Chart

JobSets created by `k8s-bridge` carry the `k8s-bridge.x-k8s.io/slurm-node-cleanup` finalizer. Delete your WorkloadMixing resources first and wait until this returns no resources:

```console
kubectl get jobsets -A -l app.kubernetes.io/managed-by=k8s-bridge
```

To uninstall/delete the `k8s-bridge` deployment:

```console
helm uninstall k8s-bridge --namespace k8s-bridge-system
```

If a JobSet is stuck in `Terminating` after uninstalling, remove only this finalizer:

```console
kubectl get jobset <name> -n <namespace> -o json \
  | jq '.metadata.finalizers -= ["k8s-bridge.x-k8s.io/slurm-node-cleanup"]' \
  | kubectl replace -f -
```

## Configuration

See `values.yaml` for full configuration details.
