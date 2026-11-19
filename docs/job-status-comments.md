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

# Job status in the Slurm job comment

k8s-bridge reports the Kubernetes side of a job on a Workload Mixing partition in the Slurm job `Comment`, which users can read with `scontrol show job <id>` or `squeue -O comment`. Your own `--comment` text is kept before the k8s-bridge status, and the comment always fits Slurm's limit of 1024 bytes.

| Status | Meaning |
|---|---|
| `Unheld by k8s-bridge: Kubernetes resource slurm-job-<id> created successfully` | The JobSet was created and Kubernetes admitted it (or the job had to wait in Kueue for a moment). |
| `Pending in Kubernetes: <reason>` | The job waits in Kubernetes. See below. |
| `Cancelled by k8s-bridge: <reason>` | k8s-bridge cancelled the job, for example because its JobSet failed or was rejected. |

## `Pending in Kubernetes:` reasons

- `JobSet creation failed, retrying: <error>`: the JobSet cannot be created yet, for example because the API server or an admission webhook is unavailable. k8s-bridge retries with backoff, and the job stays held until the JobSet exists.
- `Kueue: <message>`: the JobSet waits for Kueue admission. The message comes from the job's Kueue Workload:
  - the message of a pending admission check once quota is reserved, for example a provisioning request;
  - the eviction message, for example after a preemption, or why Kueue cannot reserve quota, for example missing quota or a topology that does not fit;
  - `waiting for <check>`: quota is reserved, and the admission check has no message yet;
  - `queued in <LocalQueue>`: Kueue has no message for the Workload yet. Kueue only explains why the Workloads at the head of a queue are not admitted, so the other jobs show this text.

When Kueue admits the JobSet, the status goes back to `Unheld by k8s-bridge: …`. Reasons longer than 200 bytes are cut, ending with `...`.

## Limitations

- Kueue reasons need `get` and `list` on `workloads.kueue.x-k8s.io` in the k8s-bridge ClusterRole, which is part of the current Helm chart and `config/rbac`. With an older ClusterRole the reasons are not shown, and k8s-bridge logs `Kueue Workload API is not available` once.
- The reason is refreshed on each poll, so it can lag behind Kueue by up to one poll interval. A job can switch between `queued in <LocalQueue>` and a Kueue message as the head of the queue changes.
- Kueue keeps the last message on a Workload until it evaluates that Workload again. A job further back in the queue can therefore show an outdated reason for a while, for example `ClusterQueue … is inactive` after the ClusterQueue was resumed.
- If the user comment is close to Slurm's 1024-byte limit, writing a long reason can shorten it, and the removed text is not restored.
- The status covers only the Kueue admission. After admission, problems with Pods (for example an unschedulable Pod) are not reported in the comment.
