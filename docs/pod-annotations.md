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

# Passing Pod annotations with `--extra`

Slurm users can add Kubernetes annotations to the `slurmd` Pods of a job with Slurm's `--extra` option of `sbatch`, `srun`, and `salloc`. This lets a single partition serve jobs with different Kueue [Topology-Aware Scheduling](https://kueue.sigs.k8s.io/docs/concepts/topology_aware_scheduling/) (TAS) requests:

```bash
sbatch -p a4x -N 16 --gpus-per-node=4 \
  --extra="kueue.x-k8s.io/podset-required-topology=cloud.google.com/gce-topology-subblock" \
  train.sh
```

> **Note:** Do not enable `SchedulerParameters=extra_constraints` in `slurm.conf` (it is disabled by default). When it is enabled, `slurmctld` treats `--extra` as a node filter and the job cannot be scheduled on dynamic nodes.

## Format

`--extra` is a comma-separated list of `key=value` entries, where each key is a full Kubernetes annotation key (`[<prefix>/]<name>`).

- Whitespace around `,`, keys, and values is trimmed. An empty or whitespace-only `--extra` is ignored.
- Empty entries, for example from a trailing comma, are skipped.
- Slurm itself limits `--extra` to 1024 bytes.
- Each entry must contain `=` and a non-empty key. Values may be empty. Duplicate keys are rejected.
- The resulting annotations must be valid Kubernetes annotations.

If `--extra` is invalid, k8s-bridge cancels the Slurm job and sets its comment to one of:

- `Translation failed: invalid --extra: ...` when the `--extra` syntax is invalid.
- `Translation failed: invalid PodTemplate annotations: ...` when the resulting annotations are not valid Kubernetes annotations. The message names the offending key.

## Interaction with `WorkloadMixing.spec.podTemplate`

Annotations in `spec.podTemplate.metadata.annotations` act as defaults. `--extra` annotations override defaults with the same key, and all other defaults are kept. If the combination is invalid, for example when the defaults set `kueue.x-k8s.io/podset-preferred-topology` and `--extra` sets `kueue.x-k8s.io/podset-required-topology`, Kueue rejects the `JobSet` and the Slurm job is cancelled as described below.

## Admission errors

k8s-bridge does not check Kueue TAS rules itself. Kueue's `JobSet` validating webhook checks them when k8s-bridge creates the `JobSet`. If the webhook or the API server rejects the `JobSet` (HTTP 422, or HTTP 403 from a validating webhook), k8s-bridge cancels the Slurm job and sets its comment to `JobSet admission rejected: <reason>`. Transient errors are retried.
