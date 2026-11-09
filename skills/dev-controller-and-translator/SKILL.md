---
name: dev-controller-and-translator
description: >-
  Implements or modifies Go operator reconciliation loops in
  internal/controller/, Slurm REST polling and event deduplication, JobSet
  translation, ClientMap and JWT token management, and Prometheus metric
  instruments. Use when changing how Slurm jobs are polled, translated into
  JobSets, unheld, preempted, or torn down in Go source code. Don't use for
  Slurm Lua submission plugins (use dev-slurm-lua), CRD schema or Helm
  template changes (use dev-api-and-codegen), deploying or upgrading the
  operator chart in a cluster (use infra-deploy-stack), or writing unit/E2E
  test suites (use test-unit-and-lint or test-e2e-cluster).
---

# Controller, Poller, and JobSet Translator Workflow (`dev-controller-and-translator`)

## 1. Preconditions & Inputs

Before editing Go controller, translator, or metrics packages, inspect the target package in `cmd/` or `internal/`:
- `cmd/main.go`: Manager entrypoint, CLI flags, event channels, and runnable registration.
- `internal/clientmap/` & `internal/controller/slurmclientsync/`: `ClientMap` and `SlurmClientReconciler` (JWT minting/refresh).
- `internal/controller/workloadmixing/`: `WorkloadMixingReconciler` (Slinky `Controller`/`RestApi` validation).
- `internal/controller/slurmjob/`: `SlurmPoller` (`poller.go`), `SlurmJobReconciler` (`slurmjob_controller.go`), `JobSetTranslator` (`translator.go`), `JobTeardownReconciler` (`teardown_reconciler.go`), and cleanup utilities (`slurm_utils.go`).
- `internal/metrics/metrics.go`: Prometheus metrics (paired with `README.md` `## Metrics & Monitoring`).

---

## 2. Guardrails

- **Check nil, `Set`, and `Infinite` on Slurm OpenAPI `JobInfo` fields:** Follow the `*NoValStruct` field-checking pattern in `resolveReplicas` and `resolvePodResources` (`internal/controller/slurmjob/translator.go`).
- **Preserve workqueue key deduplication (`slurm-job-<wmName>-<jobID>`):** Keep `SlurmPoller` `GenericEvent` keys and `JobSet` informer mapping (`Start`, `mapJobSetToRequest`, `parseRequestName` in `slurmjob_controller.go`) aligned on `slurm-job-<wmName>-<jobID>`.
- **Keep `SlurmPoller` channel sends non-blocking:** Never block on admission or teardown channels in `poller.go` (record drops via `k8s_bridge_dropped_events_total`).
- **Finalize `JobSet` before `r.Delete(ctx, js)`:** In `teardown_reconciler.go` and `slurmjob_controller.go`, run `finalizeJobSet` (`slurm_utils.go`) to delete Slurm nodes and remove `FinalizerJobSet` **before** calling `r.Delete(ctx, js)`.

---

## 3. Step-by-Step Procedure

> **Component Selection:** Open and modify only the package(s) matching your task:

1. **Slurm REST Client Sync & `WorkloadMixing` Status (`internal/controller/slurmclientsync/`, `internal/controller/workloadmixing/`):**
   - In `slurmclient_controller.go`, keep `slurmclient` informer caching disabled (`DisableFor` live Slurm types) and refresh JWTs in `ClientMap` ahead of expiry.
   - In `workloadmixing_controller.go`, validate both the Slinky `Controller` CR and matching `RestApi` CR (`spec.controllerRef`) before setting `Available=True`.
2. **Polling, Admission, Reconciliation & Teardown (`internal/controller/slurmjob/`):**
   - In `poller.go`, poll once per unique Slinky `ControllerRef` per tick and skip finalized `JobSet`s.
   - In `slurmjob_controller.go` and `slurm_utils.go`, preserve explicit user/admin hold deferral (`isJobExplicitlyHeld` in `translator.go`), idempotent `JobSet` creation, `JobTranslationFailed` events, and deleting-`JobSet` finalization when the Slurm job, `WorkloadMixing`, or `Controller` is gone (`fetchState` / `releaseFinalizerForDeletingJobSet`).
3. **`JobSet` Translation (`internal/controller/slurmjob/translator.go`):**
   - Keep `Label*` and `FinalizerJobSet` constants in `translator.go` as the single source of truth (select `JobSet`s/pods by `LabelSlurmJobID` rather than lossy `LabelSlurmJobName`; see `slurmJobNameLabelValue`).
   - Preserve `podTemplate` merge and injection semantics in `resolvePodResources`, `mergeContainerResources`, `injectSlurmVolumes`, `appendSlurmdArgs`, `injectPreStopHook`, and `injectSlurmdProbes` in `translator.go`.
4. **Prometheus Metrics (`internal/metrics/metrics.go`):**
   - Register metrics with `metricNamespace` and low-cardinality `label*` constants in `init()`, update `Delete*Metrics` cleanup helpers, and sync `README.md` (`## Metrics & Monitoring`).

---

## 4. Verification

1. **Automated verification (default for agents):** Run code generation, formatting, static analysis, and the unit/`envtest` suite:

```bash
make manifests generate fmt vet lint test
```

2. **Host-to-cluster debug mode (only when requested for live cluster debugging):** Port-forward `svc/slurm-restapi:6820` and run `go run ./cmd/main.go --debug --metrics-bind-address=0` (`--debug` rewrites `slurmrestd` URLs in `SlurmClientReconciler` to `http://localhost:6820`).

---

## 5. Handoffs & Related Skills

- **To write unit tests or `slurmclient` mocks for your changes:** Read [skills/test-unit-and-lint/SKILL.md](../test-unit-and-lint/SKILL.md).
- **To deploy or upgrade the operator chart in a cluster:** Read [skills/infra-deploy-stack/SKILL.md](../infra-deploy-stack/SKILL.md).
- **To run the full-stack Kind or GKE GPU E2E suite:** Read [skills/test-e2e-cluster/SKILL.md](../test-e2e-cluster/SKILL.md).
- **If you modified CRD types or `+kubebuilder:rbac` markers:** Read [skills/dev-api-and-codegen/SKILL.md](../dev-api-and-codegen/SKILL.md).
