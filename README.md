# k8s-bridge

A custom Kubernetes controller enabling Workload Mixing (resource sharing) between Slurm and Kubernetes/GKE.

## Description
The `k8s-bridge` detects Slurm jobs in a `HOLD` state on specific partitions, creates corresponding Kubernetes `JobSets` managed by Kueue, registers them as dynamic Slurm nodes, and unholds the Slurm job to run natively on those Kubernetes-provisioned nodes.

## Prerequisites
- Kubernetes 1.25+
- Helm 3.0.0+
- [cert-manager](https://cert-manager.io/) installed on the cluster.
- [Slurm Operator](https://github.com/SlinkyProject/slurm-operator) installed on the cluster.
- [Kueue](https://kueue.sigs.k8s.io/) installed on the cluster.

## Installation

This project is distributed and installed via Helm.

**1. Install the k8s-bridge Controller:**

Install the published OCI Helm chart from GitHub Container Registry:

```sh
helm upgrade --install k8s-bridge oci://ghcr.io/googlecloudplatform/charts/k8s-bridge \
  --version 0.1.0 \
  --namespace k8s-bridge-system --create-namespace
```

Alternatively, you can install from the local `./helm` chart in the repository:

```sh
make helm-install
```

**2. Configure Workload Mixing:**

Once the controller is running, you must create a `WorkloadMixing` Custom Resource to tell the bridge how to map Slurm partitions to Kueue queues.

You can apply the provided sample configuration:

```sh
kubectl apply -f config/samples/k8s-bridge_v1alpha1_workloadmixing.yaml
```

See the `config/samples/` directory for full examples of configuring the `WorkloadMixing` resource alongside Kueue and a compatible Slurm cluster.

## Guides & Documentation

- [Quickstart Guide](docs/quickstart.md): Step-by-step instructions to deploy k8s-bridge and a sample workload mixing cluster.
- [Priorities and Preemption](docs/priorities-and-preemption.md): Configure partition-based priorities and Kueue preemption for Slurm workloads.
- [GPU Workloads](docs/gpu-workloads.md): Configure GPU-enabled Slurm partitions and run GPU jobs on Kubernetes.
- [GPU DWS Workloads](docs/gke/gpu-dws-workloads.md): Configure Dynamic Workload Scheduler (DWS) on GKE for GPU workloads.
- [CCC](docs/gke/ccc.md): Configure Custom Compute Classes (CCC) on GKE.

## Cleanup

To remove the WorkloadMixing configurations:
```sh
kubectl delete -f config/samples/k8s-bridge_v1alpha1_workloadmixing.yaml
```

JobSets created by `k8s-bridge` carry the `k8s-bridge.x-k8s.io/slurm-node-cleanup` finalizer. A deleted JobSet stays `Terminating`, and its Pods keep running, until `k8s-bridge` deregisters its dynamic Slurm nodes. Before uninstalling, wait until this returns no resources:
```sh
kubectl get jobsets -A -l app.kubernetes.io/managed-by=k8s-bridge
```

To uninstall the controller:
```sh
helm uninstall k8s-bridge --namespace k8s-bridge-system
```

If no running `k8s-bridge` manages a JobSet anymore (uninstalled, rolled back to a version without the finalizer, or WorkloadMixing deleted with `--cascade=orphan`), remove only this finalizer:
```sh
kubectl get jobset <name> -n <namespace> -o json \
  | jq '.metadata.finalizers -= ["k8s-bridge.x-k8s.io/slurm-node-cleanup"]' \
  | kubectl replace -f -
```

## Metrics & Monitoring

`k8s-bridge` exports Prometheus metrics for monitoring cluster synchronization throughput, poller latency, client connectivity, and active resources.

### Metrics Endpoint

The metrics server is managed by `controller-runtime` and exposes metrics at `/metrics`:
- **Secure Mode (Default):** Served over HTTPS on port `:8443` (`--metrics-secure=true`). Access requires a Kubernetes ServiceAccount token bound to the `k8s-bridge-metrics-reader` ClusterRole.
- **Insecure Mode:** Can be served over HTTP on port `:8080` by setting `--metrics-secure=false` and `--metrics-bind-address=:8080`.

### Exported Metrics

All metrics use the `k8s_bridge_` prefix and bounded, low-cardinality labels:

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `k8s_bridge_slurm_poller_duration_seconds` | Histogram | `controller` | Latency of Slurm REST API polling cycles in seconds. |
| `k8s_bridge_slurm_poller_errors_total` | Counter | `controller` | Total number of errors encountered during Slurm polling cycles. |
| `k8s_bridge_slurm_client_status` | Gauge | `controller` | Health and connectivity status of the Slurm REST client (`1` = healthy, `0` = disconnected/error). |
| `k8s_bridge_jobs_discovered_total` | Counter | `workload_mixing`, `partition` | Total number of pending Slurm jobs discovered. |
| `k8s_bridge_jobs_sync_total` | Counter | `status`, `reason`, `workload_mixing`, `partition` | Cumulative count of job synchronization attempts and outcomes (`jobset_created`, `jobset_completed`, `jobset_failed`, `translation_error`, etc.). |
| `k8s_bridge_job_translation_duration_seconds` | Histogram | `workload_mixing`, `partition` | Latency of converting Slurm jobs to Kubernetes JobSets in seconds. |
| `k8s_bridge_active_jobs` | Gauge | `workload_mixing`, `partition` | Current count of in-flight managed Slurm jobs. |
| `k8s_bridge_active_workload_mixings` | Gauge | _none_ | Total number of active `WorkloadMixing` custom resources. |
| `k8s_bridge_tracked_partitions` | Gauge | `workload_mixing` | Number of Slurm partitions actively configured and tracked per `WorkloadMixing` resource. |
| `k8s_bridge_dropped_events_total` | Counter | `workload_mixing` | Total number of events dropped due to channel buffer saturation. |
| `k8s_bridge_events_queue_depth` | Gauge | _none_ | Current depth of the internal poller event queue. |

### Prometheus Integration

A Prometheus `ServiceMonitor` manifest is provided under `config/prometheus/monitor.yaml` to enable scraping via Prometheus Operator.

## Testing

### Unit Tests
```sh
make test
```

### End-to-End (E2E) Tests

#### CPU E2E Tests (Local Kind Cluster)
```sh
make test-e2e
```

#### Resilience E2E Tests (Dedicated Kind Cluster)

The resilience suite (`test/e2e/resilience`) stops components while a job runs. It checks that the job continues, and that each component manages the job correctly after it starts again. The suite stops the k8s-bridge controller manager and slurmctld. Slurm runs with slurmctld persistence enabled.

The suite runs on its own Kind cluster, `k8s-bridge-e2e-resilience`. Kind writes the credentials of this cluster to `bin/kubeconfig-k8s-bridge-e2e-resilience`, so the current context of `~/.kube/config` does not change.

```sh
# Create the cluster, run the suite, and delete the cluster
make test-e2e-resilience

# Run both Kind suites in parallel on separate clusters
make -j2 test-e2e test-e2e-resilience

# Run the suite again on an existing resilience cluster
make run-test-e2e-resilience
```

Each Kind cluster uses inotify instances of the host. If kubelet fails with `too many open files`, raise the limit, for example with `sudo sysctl fs.inotify.max_user_instances=512`. See the [Kind known issues](https://kind.sigs.k8s.io/docs/user/known-issues/#pod-errors-due-to-too-many-open-files).

#### GPU E2E Tests on a Provided Cluster

The GPU E2E tests run against a cluster provisioned with **1x NVIDIA L4 GPU** (e.g. `g2-standard-4` machine type with `nvidia-l4` accelerator).

1. **Connect to your Cluster:**
   ```sh
   gcloud container clusters get-credentials <CLUSTER_NAME> --zone <ZONE> --project <PROJECT_ID>
   ```

2. **Build and Push the Controller Manager Image:**
   Ensure the manager image is pushed to a registry accessible by your cluster:
   ```sh
   export IMG=gcr.io/<PROJECT_ID>/k8s-bridge:test
   make docker-build docker-push IMG=$IMG
   ```

3. **Run GPU E2E Tests:**
   Run the GPU test suite against the active cluster context:
   ```sh
   make run-test-e2e-gpu IMG=$IMG
   ```
   Or execute directly using `go test`:
   ```sh
   go test -tags=e2e_gpu ./test/e2e/gpu/ -v -ginkgo.v -timeout 45m
   ```

#### GPU E2E Tests with Automated Infrastructure Provisioning (GKE)

To automatically provision a temporary GKE GPU cluster, execute tests, and tear down the infrastructure:

```sh
# Copy and configure GCP settings (zone, project, accelerator type)
cp .env.gpu.example .env

# Run automated setup, test execution, and cleanup
make test-e2e-gpu
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, development workflow, and the process for submitting pull requests.

**NOTE:** Run `make help` for more information on all potential `make` targets.

## License

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

