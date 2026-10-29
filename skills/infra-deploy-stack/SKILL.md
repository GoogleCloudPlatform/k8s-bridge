---
name: infra-deploy-stack
description: >-
  Installs, upgrades, and tears down the base Kubernetes operators and Helm
  charts (cert-manager, Slinky slurm-operator CRDs and operator, JobSet before
  Kueue, Kueue, and the k8s-bridge controller chart from OCI or local ./helm /
  Kustomize). Use when installing, upgrading, or uninstalling the base operator
  stack in a cluster. Don't use for creating cloud clusters or node pools (use
  infra-provision-cluster) or configuring and applying Kueue, ComputeClass,
  Slinky Slurm partition, and WorkloadMixing CRs (use infra-configure-crs).
---

# Base Operator & Chart Installation (`infra-deploy-stack`)

## 1. Preconditions & Inputs

Before installing, upgrading, or uninstalling the base operators, check or ask the user for:
1. **Kubernetes Context & Prerequisites:** Output of `kubectl config current-context`, plus cluster/tool version requirements in `## Prerequisites & Versions` of [docs/quickstart.md](../../docs/quickstart.md).
2. **Existing Cluster Components:** Whether any prerequisite operators (`cert-manager`, `slurm-operator`, `JobSet`, `Kueue`) are already installed (see the prerequisite installation section of [docs/existing-infra.md](../../docs/existing-infra.md)).
3. **`k8s-bridge` Installation Method:** Published OCI Helm chart (**Branch A**), local `./helm` chart with custom `IMG` (**Branch B**), or Kustomize (**Branch C**).

---

## 2. Guardrails

- **Confirmation gate:** Show the target cluster context (`kubectl config current-context`), target namespaces (`cert-manager`, `slinky`, `jobset-system`, `kueue-system`, `k8s-bridge-system`), and planned commands. Get user confirmation before installing, upgrading, or uninstalling releases.
- **Keep operator installation separate from CR and partition configuration:** Only install or tear down cluster-level operators here; hand off to [skills/infra-configure-crs/SKILL.md](../infra-configure-crs/SKILL.md) for Kueue CRs, `ComputeClass` CRs, the Slinky `slurm` release (`slurm-values.yaml` + Lua plugins), and `WorkloadMixing` CRs.
- **Install `JobSet` before `Kueue`:** If `JobSet` is installed after `Kueue`, `JobSet` objects stay suspended until you restart `kueue-controller-manager` (`kubectl -n kueue-system rollout restart deploy/kueue-controller-manager`).

---

## 3. Step-by-Step Procedure

### Step 1: Install Prerequisite Operators (`cert-manager`, `slurm-operator`, `JobSet` before `Kueue`, `Kueue`)
Follow the prerequisite installation section in [docs/quickstart.md](../../docs/quickstart.md) for a clean cluster, or [docs/existing-infra.md](../../docs/existing-infra.md) to install only missing components.

### Step 2: Install or Upgrade `k8s-bridge` (Select One Branch: A, B, or C)
- **Branch A — Published OCI Helm Chart:** Run the `helm install k8s-bridge` (or `helm upgrade --install`) command in the `k8s-bridge` installation section of [docs/quickstart.md](../../docs/quickstart.md).
- **Branch B — Build from Source & Install Local `./helm` Chart:** Build and push `IMG=${REGISTRY}/k8s-bridge:${TAG}` (`make docker-build docker-push IMG=${REGISTRY}/k8s-bridge:${TAG}`), then run `helm upgrade --install k8s-bridge ./helm --namespace k8s-bridge-system --create-namespace -f ./helm/values.yaml --set image.repository=${REGISTRY}/k8s-bridge --set image.tag=${TAG} --wait` (`make helm-install` uses default `helm/values.yaml` and ignores `IMG`).
- **Branch C — Kustomize Deploy (Alternative to Helm):** Build and push `IMG` as in Branch B, then run `make install && make deploy IMG=$IMG` (`make deploy` mutates `config/manager/kustomization.yaml`; restore via `make version-sync` or `git checkout -- config/manager/kustomization.yaml` before committing).

*(Stop here after Step 2 and proceed to Section 4 Verification when installing or upgrading. Do not run Step 3 unless the user asked to uninstall the operators.)*

### Step 3: Base Operator Stack Teardown (Run ONLY When Requested to Uninstall)
First complete CR and Slurm release teardown via [skills/infra-configure-crs/SKILL.md](../infra-configure-crs/SKILL.md) (Step 4). Then, after user confirmation, uninstall `k8s-bridge` (or `make undeploy uninstall` for Branch C) and the prerequisite operators in reverse order as shown in the cleanup section of [docs/quickstart.md](../../docs/quickstart.md) or [docs/existing-infra.md](../../docs/existing-infra.md).

---

## 4. Verification

1. Verify all operator deployments (`cert-manager`, `slurm-operator`, `jobset-controller-manager`, `kueue-controller-manager`, and `k8s-bridge-controller-manager`) have rolled out and are `Available`.
2. Verify `kubectl get crd workloadmixings.k8s-bridge.x-k8s.io` succeeds.

---

## 5. Handoffs & Related Skills

- **Next step — Configure and apply Kueue CRs, GKE `ComputeClass` CRs, the Slinky `slurm` cluster (`slurm-values.yaml` + Lua plugins), and `WorkloadMixing` CRs (CPU, GPU, priority):** Read [skills/infra-configure-crs/SKILL.md](../infra-configure-crs/SKILL.md).
- **To create or update GKE clusters and autoscaling CPU (CCC) or GPU (DWS Flex-start) node pools:** Read [skills/infra-provision-cluster/SKILL.md](../infra-provision-cluster/SKILL.md).
- **To run the automated Kind CPU or GKE GPU E2E test suites:** Read [skills/test-e2e-cluster/SKILL.md](../test-e2e-cluster/SKILL.md).
