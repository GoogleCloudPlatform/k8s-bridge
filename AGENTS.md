# k8s-bridge — Agent Instructions

## 1. Project Overview & Runtime Lifecycle

- **What this repository does:** `k8s-bridge` is a Kubernetes operator and Slurm plugin suite that shares compute resources between Slurm (deployed via the Slinky Slurm operator) and Kubernetes. It intercepts Slurm jobs submitted to bridged partitions and runs dynamic `slurmd` worker pods managed by Kubernetes `JobSet` and `Kueue`.
- **Runtime Workload Lifecycle:**
  1. **Submission Interception (`hack/cli_filter.lua`, `hack/job_submit.lua`):** On bridged partitions (`WORKLOAD_MIXING_PARTITIONS`), `cli_filter.lua` (login pods) validates CLI flags and `job_submit.lua` (`slurmctld`) places single-partition jobs into auto-hold (`AutoHoldMarker` in `translator.go`) and rejects multi-partition bridged jobs.
  2. **Client Sync & Polling (`internal/controller/{workloadmixing,slurmclientsync,slurmjob/poller.go}`, `internal/clientmap/`):** Reconcilers validate Slinky `Controller`/`RestApi` refs and sync JWT clients into `ClientMap`; `SlurmPoller` polls `slurmrestd` to enqueue deduplicated admission and teardown events.
  3. **Translation & Admission (`internal/controller/slurmjob/`):** `SlurmJobReconciler` and `JobSetTranslator` translate held jobs into suspended `JobSet` objects (`slurm-job-<jobID>`) mapped to a Kueue `LocalQueue` and optional `WorkloadPriorityClass`.
  4. **Dynamic Node Registration & Release:** `SlurmJobReconciler` releases the Slurm hold with per-job feature constraints (`UniqueFeaturePrefix` in `translator.go`); once Kueue admits the `JobSet`, dynamic `slurmd` pods register with `slurmctld`.
  5. **Preemption, Teardown & Metrics (`internal/controller/slurmjob/teardown_reconciler.go`, `internal/metrics/`):** Kueue preemption suspends the `JobSet`; on job termination, reconcilers delete dynamic Slurm nodes, remove `FinalizerJobSet`, delete the `JobSet`, and record Prometheus metrics.

---

## 2. Start Here — Persona Triage, Lifecycle Order & Environment Matrix

Determine which workflow track applies before running commands or editing files:

### 2.1 Two-Track Procedural Lifecycle
- **Track A — Software Contributor Lifecycle (Code, Plugins, Packaging, Tests & PRs):**
  `dev-api-and-codegen` (schemas/RBAC/packaging) -> `dev-controller-and-translator` / `dev-slurm-lua` -> `test-unit-and-lint` -> `test-e2e-cluster` -> `dev-api-and-codegen` (pre-PR parity audit)
- **Track B — Cluster Operator Lifecycle (Infra, Deployment, Config & Day-2 Ops):**
  `infra-provision-cluster` (if cloud) -> `infra-deploy-stack` -> `infra-configure-crs` -> `test-e2e-cluster` (optional validation) -> `ops-diagnose-workload` (read-only triage) -> `ops-cleanup-state` (remediation)

### 2.2 Pre-Flight Context Check (For Cluster / Cloud Operations)
Before mutating any cluster or cloud environment, use read-only commands (`kubectl config current-context`) or ask the user to verify:
1. **Target Environment** (see matrix below)
2. **Kubernetes Context & Namespace** (`kubectl config current-context`, default Slurm namespace `slurm`, operator namespace `k8s-bridge-system`)
3. **Container Image Registry & Tag** (`IMG=<registry>/k8s-bridge:<tag>`)

| Environment | Acceleration & Workload Profile | Node Provisioning Skill | Environment-Specific Configuration & Notes |
|---|---|---|---|
| **Local Kind** | CPU only / inner-loop dev & E2E | *(Managed by Kind harness)* | Default CPU partition (`k8s-partition`), Kind E2E harness (`test-e2e-cluster`), no cloud quotas |
| **Cloud CPU** | Standard CPU node pools or GKE CCC | `infra-provision-cluster` | Pre-provisioned CPU node pool (`default-flavor`) or optional GKE Node Auto-Provisioning + `ComputeClass` (`infra-configure-crs`) |
| **Cloud GPU** | Hardware GPUs (pre-provisioned or DWS) | `infra-provision-cluster` (DWS / GPU) | DWS Flex-start (`--flex-start`) or labeled GPU nodes, typed/untyped GPU `mappedResources` (`infra-configure-crs`) |

*(Base stack deployment via `infra-deploy-stack` and verification via `test-e2e-cluster` apply to all environments.)*

---

## 3. Global Guardrails & Safety Gates

### 3.1 Code & Repository Guardrails
- **Generated & Paired Artifacts (Do Not Hand-Edit Derived Outputs):** Never manually edit generated DeepCopy, CRD, RBAC, or `PROJECT` files; regenerate and sync paired artifacts via `make manifests generate` and `make version-sync` (see [skills/dev-api-and-codegen/references/parity-checklist.md](skills/dev-api-and-codegen/references/parity-checklist.md)).
- **Required Post-Edit Checks:**
  - After Go or Lua code changes: run `make fmt vet lint test` (`test-unit-and-lint`).
  - Before submitting a pull request or after API/CRD/RBAC/chart/doc edits: run `make version-sync manifests generate fmt`, verify `git diff --exit-code`, and audit parity (`dev-api-and-codegen`).

### 3.2 Cluster & Cloud Operational Guardrails
- **Confirmation Gate (Billable & Destructive Actions):** Present the planned commands and obtain user confirmation before:
  1. Creating, resizing, or deleting cloud clusters or node pools (`infra-provision-cluster`).
  2. Installing, upgrading, or uninstalling cluster-wide CRDs, webhooks, or Helm releases (`infra-deploy-stack`, `infra-configure-crs`).
  3. Cancelling active Slurm jobs, draining nodes, deleting Kubernetes resources (`ops-cleanup-state`), or mutating cluster/tenant policies and other tenants' workloads.
- **Pod Termination Safety:** Never delete dynamic `slurmd` pods with `--force --grace-period=0` (causes `BadConstraints`; see `ops-cleanup-state`).
- **Non-Root Verification:** Test Slurm submissions and modifications via `runuser -u <user> -- ...` (login pods default to `root`, which bypasses `hack/job_submit.lua` checks; see `dev-slurm-lua`).

---

## 4. Repository Layout

```text
.
├── api/v1alpha1/        # WorkloadMixing CRD Go types (canonical schema source)
├── cmd/                 # Manager binary entrypoint (cmd/main.go) and CLI flags
├── config/              # Kustomize bases (crd, rbac, manager, prometheus, network-policy, samples)
├── docs/                # User guides and scenario example manifests (docs/examples/)
├── hack/                # Slurm Lua plugins (job_submit.lua, cli_filter.lua) and helper scripts
├── helm/                # Local Helm chart (Chart.yaml, values.yaml, crds/, templates/)
├── internal/            # Go packages (clientmap, controller/{slurmclientsync,slurmjob,workloadmixing}, metrics, utils, version)
├── skills/              # On-demand Agent Skills (skills/<category>-<name>/SKILL.md)
└── test/                # Ginkgo E2E suites (test/e2e/, test/e2e/gpu/), infra scripts, and mocks (test/utils/)
```

---

## 5. Skill Routing Table

**The 1-Sentence Trigger + Pointer Rule:** When a task matches a trigger below, read the target `SKILL.md` file before taking action (all `skills/...`, `hack/...`, and `docs/...` paths and script invocations are relative to and must be run from the repository root containing `Makefile` and `PROJECT`). Do not guess procedures from memory.

**Subagent Delegation Protocol:** Include in every subagent prompt: (1) Target Environment and Kubernetes context/namespace, (2) the `skills/<skill-name>/SKILL.md` path to read first before running commands or editing files, and (3) whether access is read-only or allows state mutation after user confirmation.

### Code Development (`dev-*`)
| Task / Trigger | Canonical Skill |
|---|---|
| Modifying `WorkloadMixing` Go types in `api/`, Kubebuilder/RBAC markers, Kustomize bases, Helm chart templates/values, or auditing pre-PR parity across `config/`, `helm/`, and `docs/examples/` | [skills/dev-api-and-codegen/SKILL.md](skills/dev-api-and-codegen/SKILL.md) |
| Modifying Go reconcilers, `SlurmPoller`, `JobSetTranslator`, `ClientMap`, JWT sync, or Prometheus metrics in `internal/` | [skills/dev-controller-and-translator/SKILL.md](skills/dev-controller-and-translator/SKILL.md) |
| Modifying or testing the Slurm Lua plugins (`hack/job_submit.lua` and `hack/cli_filter.lua`) | [skills/dev-slurm-lua/SKILL.md](skills/dev-slurm-lua/SKILL.md) |

### Testing & Verification (`test-*`)
| Task / Trigger | Canonical Skill |
|---|---|
| Writing or running Go unit tests, `envtest` controller specs, Slurm REST client mocks, or linters (`make fmt vet lint test`) | [skills/test-unit-and-lint/SKILL.md](skills/test-unit-and-lint/SKILL.md) |
| Running, writing, or debugging Ginkgo E2E test suites on local Kind CPU clusters or GKE GPU clusters | [skills/test-e2e-cluster/SKILL.md](skills/test-e2e-cluster/SKILL.md) |

### Infrastructure & Cluster Configuration (`infra-*`)
| Task / Trigger | Canonical Skill |
|---|---|
| Creating, resizing, updating, or deleting GKE clusters and CPU (standard or CCC) or GPU (pre-provisioned or DWS Flex-start) node pools, or labeling GPU nodes | [skills/infra-provision-cluster/SKILL.md](skills/infra-provision-cluster/SKILL.md) |
| Installing, upgrading, or tearing down the base operators and Helm charts (`cert-manager`, `slurm-operator`, `JobSet`, `Kueue`, `k8s-bridge`) without CR configuration | [skills/infra-deploy-stack/SKILL.md](skills/infra-deploy-stack/SKILL.md) |
| Configuring, applying, or removing Kueue CRs, GKE `ComputeClass` CRs, Slinky `slurm` partitions/Lua plugins, `WorkloadMixing` CRs (CPU/GPU/priority), and non-root users | [skills/infra-configure-crs/SKILL.md](skills/infra-configure-crs/SKILL.md) |

### Day-2 Operations & Troubleshooting (`ops-*`)
| Task / Trigger | Canonical Skill |
|---|---|
| Read-only diagnosis of rejected, held, suspended, pending, preempted, or cancelled jobs and diagnostic snapshot collection | [skills/ops-diagnose-workload/SKILL.md](skills/ops-diagnose-workload/SKILL.md) |
| Draining and cleaning up wedged Slurm jobs, orphaned `JobSet` objects, dynamic `slurmd` pods, and stale Slurm nodes | [skills/ops-cleanup-state/SKILL.md](skills/ops-cleanup-state/SKILL.md) |

---

## 6. Core Architectural & Ownership Invariants

- **State Ownership (Single Source of Truth):**
  - **Slurm (`slurmctld` / `slurmrestd`):** Owns job submission, user/admin hold states, job state transitions, and task execution on dynamic `slurmd` nodes.
  - **Kubernetes & Kueue:** Own cluster resource quotas, `Workload` admission gating, priority preemption decisions, and `JobSet` pod lifecycle.
  - **`k8s-bridge`:** Stateless translator and synchronizer maintaining a 1:1 mapping between Slurm jobs on bridged partitions and Kubernetes `JobSet` (`slurm-job-<jobID>`) objects in the same namespace.
- **Configuration Parity Invariant:** Bridged partition names and `dynamicNodeFeature` values must match across `slurmctld` env, `slurm.conf` (`CliFilterParameters` and `NodeSet`), and `WorkloadMixing.spec.partitionsMapping` (see [skills/infra-configure-crs/SKILL.md](skills/infra-configure-crs/SKILL.md)).
