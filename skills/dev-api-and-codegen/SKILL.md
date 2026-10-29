---
name: dev-api-and-codegen
description: >-
  Adds or modifies Kubernetes Custom Resource Definition (CRD) Go types in api/,
  Kubebuilder validation markers, RBAC annotations, Kustomize bases, and Helm
  chart templates, values, and CRD schemas; regenerates manifests, ClusterRoles,
  and DeepCopy methods via make manifests and make generate; and audits pre-PR
  repository parity across config/, helm/, VERSION, and docs/examples/ YAMLs.
  Use when changing WorkloadMixing API struct fields, updating CRD validation or
  RBAC permissions, editing Helm/Kustomize packaging templates, or verifying
  zero codegen and manifest drift before a pull request. Don't use for
  implementing Go controller reconciliation logic (use
  dev-controller-and-translator), Slurm Lua scripts (use dev-slurm-lua), or
  running unit/E2E test suites (use test-unit-and-lint or test-e2e-cluster).
---

# API, Codegen, Packaging, and Parity Audit Workflow (`dev-api-and-codegen`)

## 1. Preconditions & Inputs

Before modifying API schemas, RBAC markers, or chart templates, or running a pre-PR parity audit, read [references/parity-checklist.md](references/parity-checklist.md) for the source-to-derived parity matrix.

---

## 2. Guardrails

- **Do not hand-edit generated files, and sync manually paired artifacts:** Treat all generated outputs in [references/parity-checklist.md](references/parity-checklist.md) (plus `PROJECT`) as read-only outputs of `make manifests generate` and `make version-sync`, and manually sync all non-generated paired files in that table.
- **Preserve API identity, status subresource, and immutable Deployment selector labels:** Do not alter `WorkloadMixing` group/version (`k8s-bridge.x-k8s.io/v1alpha1`), `Namespaced` scope, or `+kubebuilder:subresource:status`, and never add `app.kubernetes.io/component` or `app.kubernetes.io/version` to `.spec.selector.matchLabels` in `helm/templates/deployment.yaml` or `config/manager/manager.yaml`.

---

## 3. Step-by-Step Procedure

### Step 1: Edit `WorkloadMixing` Go Types or Controller `+kubebuilder:rbac` Markers
1. **CRD Schema & Validation:** Edit struct fields and Kubebuilder markers in `api/v1alpha1/workloadmixing_types.go` (keep runtime `DynamicNodeFeature` validation in `internal/controller/slurmjob/translator.go` in sync).
2. **Controller RBAC Permissions:** If controller permissions changed, update the `+kubebuilder:rbac` annotations above `Reconcile` in `internal/controller/*/*_controller.go`.

### Step 2: Run Code and Manifest Generation
Regenerate CRDs, RBAC roles, `helm/crds/`, and DeepCopy methods:

```bash
make manifests generate
```

### Step 3: Synchronize Paired Artifacts & Audit Pre-PR Repository Parity
Walk through every modified row in [references/parity-checklist.md](references/parity-checklist.md) and synchronize all derived/paired artifacts.

---

## 4. Verification

Verify that version sync, code generation, formatting, static analysis, Kustomize/Helm rendering, and unit/`envtest` suites succeed with zero uncommitted drift:

```bash
make version-sync manifests generate fmt vet lint kustomize
git diff --exit-code
./bin/kustomize build config/default > /dev/null && ./bin/kustomize build config/samples > /dev/null
helm lint ./helm && helm template k8s-bridge ./helm --namespace k8s-bridge-system > /dev/null
make test
```

---

## 5. Handoffs & Related Skills

- **To implement controller reconciliation or `JobSet` translation for new CRD fields:** Read [skills/dev-controller-and-translator/SKILL.md](../dev-controller-and-translator/SKILL.md).
- **To write unit or `envtest` tests for new API fields:** Read [skills/test-unit-and-lint/SKILL.md](../test-unit-and-lint/SKILL.md).
- **To run the full-cluster Kind or GKE GPU E2E suite before merging:** Read [skills/test-e2e-cluster/SKILL.md](../test-e2e-cluster/SKILL.md).
