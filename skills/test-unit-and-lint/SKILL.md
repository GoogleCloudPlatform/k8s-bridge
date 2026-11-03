---
name: test-unit-and-lint
description: >-
  Writes and runs table-driven Go unit tests, envtest controller integration
  tests, Slurm REST client mocks, and static analysis linters via make test,
  make fmt, make vet, and make lint. Use when adding or debugging package-level
  tests, mocking Slurm REST API responses, or fixing lint and formatting
  failures. Don't use for running full-cluster end-to-end Ginkgo tests on Kind
  or cloud GPU clusters (use test-e2e-cluster).
---

# Unit Testing, `envtest`, and Linting Workflow (`test-unit-and-lint`)

## 1. Preconditions & Inputs

Before writing or running unit/`envtest` tests and linters, inspect the existing test and mock helpers:
- `internal/controller/slurmjob/slurmjob_controller_helper_test.go` & `translator_helper_test.go`: Canonical fake-client/`ClientMap` test harness and fluent `JobInfo` / `WorkloadMixingSpec` builders for `slurmjob` unit tests.
- `internal/controller/slurmjob/teardown_reconciler_test.go` & `internal/controller/slurmclientsync/slurmclient_controller_test.go`: Examples using `slurmclientfake.NewClientBuilder().WithInterceptorFuncs(...)`.
- `internal/controller/workloadmixing/suite_test.go` & `workloadmixing_controller_test.go`: Ginkgo `envtest` controller suite.
- `test/utils/slurmmock.go`: Shared `&utils.SlurmClient{}` mock for job list/get/update and node delete tests.

---

## 2. Guardrails

- **Select the Slurm client mock for the target component:**
  1. Use `&utils.SlurmClient{}` (inspect `test/utils/slurmmock.go` for supported job/node state and error-injection fields) when testing `SlurmPoller`, `SlurmJobReconciler`, or `JobTeardownReconciler`. It does **not** implement controller ping or inject `List` / job-`Delete` errors.
  2. Use `slurmclientfake.NewClientBuilder().WithInterceptorFuncs(...).Build()` (`github.com/SlinkyProject/slurm-client/pkg/client/fake`) when testing controller ping in `SlurmClientReconciler` or custom `List`/job-`Delete` error interceptors.
- **Reuse the shared `slurmjob` test harness and builders:** Use the helpers in `internal/controller/slurmjob/slurmjob_controller_helper_test.go` and `translator_helper_test.go`.

---

## 3. Step-by-Step Procedure

### Step 1: Write Unit or `envtest` Specs
1. **Fake-client unit tests (`internal/controller/slurmjob/`, `internal/controller/slurmclientsync/`, `internal/clientmap/`, `internal/metrics/`):** Add table-driven tests alongside the package under test using the helpers in Section 1.
2. **`envtest` integration specs (`internal/controller/workloadmixing/`):** Add Ginkgo specs in `workloadmixing_controller_test.go` (if running `go test ./internal/controller/workloadmixing/...` directly instead of `make test`, run `make manifests setup-envtest` first).

---

## 4. Verification

Run the unit/`envtest` suite and linters (use `make lint-fix` for auto-fixable lint issues):

```bash
make fmt vet lint test
```

---

## 5. Handoffs & Related Skills

- **To run full-cluster Ginkgo E2E tests on Kind or GKE GPU clusters:** Read [skills/test-e2e-cluster/SKILL.md](../test-e2e-cluster/SKILL.md).
- **To modify controller, poller, or translator logic under test:** Read [skills/dev-controller-and-translator/SKILL.md](../dev-controller-and-translator/SKILL.md).
- **To regenerate CRDs or DeepCopy methods after API test changes:** Read [skills/dev-api-and-codegen/SKILL.md](../dev-api-and-codegen/SKILL.md).
