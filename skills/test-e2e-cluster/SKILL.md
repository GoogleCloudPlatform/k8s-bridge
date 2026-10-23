---
name: test-e2e-cluster
description: >-
  Runs, writes, and debugs full-stack end-to-end (E2E) Ginkgo test suites and
  cluster test harnesses against a local Kind CPU cluster or a GKE GPU
  Kubernetes cluster. Use when executing make test-e2e, make test-e2e-gpu, or
  make run-test-e2e-gpu, adding new E2E test specs in test/e2e/, or debugging
  E2E test failures. Don't use for local unit/envtest tests (use
  test-unit-and-lint) or interactive operator or partition deployment (use
  infra-deploy-stack or infra-configure-crs).
---

# End-to-End (E2E) Cluster Testing Workflow (`test-e2e-cluster`)

## 1. Preconditions & Inputs

Before running or modifying E2E suites, verify:
- **Local Kind CPU E2E (`test/e2e/`):** `make` installs `./bin/kind` automatically, and `BeforeSuite` applies the vendored manifests in `test/e2e/testdata/` (see `Makefile` `##@ Development`).
- **GKE GPU E2E (`test/e2e/gpu/`):** Configure the `.env` / environment variables defined in `Makefile` (`##@ E2E GPU Testing`) and `test/e2e/infra/gke-gpu-cluster-setup.sh`.

---

## 2. Guardrails

- **Confirmation gate for GKE GPU cluster creation:** `make test-e2e-gpu` and `make setup-infra-gpu` create billable GKE clusters and GPU node pools via `test/e2e/infra/gpu-cluster-setup.sh`. Obtain user confirmation before running them.
- **Use `ResetSlurmAndK8sState` in `test/utils/slurm.go` for E2E cleanup:** Never force-delete dynamic `slurmd` pods with `--grace-period=0` (see [skills/ops-cleanup-state/SKILL.md](../ops-cleanup-state/SKILL.md)).
- **Reload Lua `ConfigMap` objects when iterating on `hack/*.lua` in an existing Kind cluster:** If testing `hack/*.lua` changes against an existing Kind E2E cluster without rerunning `BeforeSuite`, reload the Lua ConfigMaps via [skills/dev-slurm-lua/SKILL.md](../dev-slurm-lua/SKILL.md) (Step 2, Option B).

---

## 3. Step-by-Step Procedure

Select the step matching your target E2E task (do not run Steps 1, 2, and 3 together):

### Step 1: Run Full Local Kind CPU E2E Suite (`make test-e2e`)
Run the full Kind CPU E2E lifecycle (cluster setup, image build/load, Ginkgo suite, and teardown):

```bash
make test-e2e
```

### Step 2: Run Focused Ginkgo Specs Against an Existing Kind Cluster
Create the Kind cluster once via `make setup-test-e2e`, run focused specs with `KIND` and `KIND_CLUSTER` set, and tear down via `make cleanup-test-e2e` when finished:

```bash
KIND=./bin/kind KIND_CLUSTER=k8s-bridge-test-e2e go test -tags=e2e ./test/e2e/ -v -ginkgo.v -ginkgo.focus="<spec pattern>" -timeout 20m
```

### Step 3: Run GKE GPU E2E Suite (`make test-e2e-gpu` vs `make run-test-e2e-gpu`)
1. Configure `.env` in the repository root with `GCP_PROJECT` and the GPU test variables listed in `Makefile` (`##@ E2E GPU Testing`). When testing local code changes on GKE (`INFRA_PROVIDER=gke`), build and push `IMG` first (`make docker-build docker-push IMG=$IMG`; see `README.md` `#### GPU E2E Tests on a Provided Cluster`).
2. **Full lifecycle (provision GKE cluster + run tests + tear down cluster):** After user confirmation, run `make test-e2e-gpu IMG=$IMG`.
3. **Existing cluster execution (skip cluster creation and teardown):** When the GKE GPU cluster is already running in `kubectl config current-context`, run `make run-test-e2e-gpu IMG=$IMG`.

### Step 4: Write New E2E Test Specs (`test/e2e/`)
Reuse the E2E job submission (`DefaultSlurmTestUser`), state verification, and cluster reset helpers in `test/utils/slurm.go`, `test/utils/login.go`, and `test/utils/jobset.go`, following the patterns in `test/e2e/e2e_test.go`, `test/e2e/slurm_flag_rejection_test.go`, and `test/e2e/gpu/gpu_test.go`.

---

## 4. Verification

1. Verify the Ginkgo suite passes and any temporary Kind/GKE test cluster created by `make test-e2e` or `make test-e2e-gpu` was cleaned up.
2. Run `make fmt vet lint` after editing any files under `test/`.

---

## 5. Handoffs & Related Skills

- **If an E2E spec fails with a stuck or cancelled Slurm job:** Read [skills/ops-diagnose-workload/SKILL.md](../ops-diagnose-workload/SKILL.md) to inspect `slurmctld`, `k8s-bridge`, and Kueue state.
- **To run fast local unit and `envtest` suites:** Read [skills/test-unit-and-lint/SKILL.md](../test-unit-and-lint/SKILL.md).
- **To install or upgrade the base operator stack in a cluster:** Read [skills/infra-deploy-stack/SKILL.md](../infra-deploy-stack/SKILL.md).
