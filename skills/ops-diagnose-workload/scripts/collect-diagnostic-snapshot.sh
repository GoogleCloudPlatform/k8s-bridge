#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Read-only diagnostic snapshot collector for k8s-bridge workloads.
# Usage: bash skills/ops-diagnose-workload/scripts/collect-diagnostic-snapshot.sh [JOB_ID] [SLURM_NS] [BRIDGE_NS]

JOB_ID="${1:-}"
SLURM_NS="${2:-slurm}"
BRIDGE_NS="${3:-k8s-bridge-system}"

echo "=== 1. Kubernetes Context & WorkloadMixing CRs (${SLURM_NS}) ==="
kubectl config current-context 2>&1 || true
kubectl -n "${SLURM_NS}" get workloadmixing,jobset,workloads,pods -o wide 2>&1 || true
kubectl -n "${SLURM_NS}" get workloadmixing -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.partitionsMapping}{"\t"}{.status.conditions}{"\n"}{end}' 2>&1 || true
kubectl get workloads,localqueue -A -o wide 2>&1 || true
kubectl get clusterqueue -o custom-columns='NAME:.metadata.name,COHORT:.spec.cohortName,STOPPOLICY:.spec.stopPolicy,ACTIVE:.status.conditions[?(@.type=="Active")].status,REASON:.status.conditions[?(@.type=="Active")].reason,MESSAGE:.status.conditions[?(@.type=="Active")].message' 2>&1 || true

echo "=== 2. Detect Slurm Login Deployment ==="
LOGIN_DEPLOY="deploy/slurm-login-slinky"
if ! kubectl -n "${SLURM_NS}" get "${LOGIN_DEPLOY}" >/dev/null 2>&1; then
  LOGIN_DEPLOY="deploy/slurm-login"
fi

echo "=== 3. Slurm Partitions, Nodes, and Queue (${LOGIN_DEPLOY}) ==="
kubectl -n "${SLURM_NS}" exec "${LOGIN_DEPLOY}" -c login -- sinfo -a -o "%P %a %l %D %N %T" 2>&1 || true
kubectl -n "${SLURM_NS}" exec "${LOGIN_DEPLOY}" -c login -- squeue -a -O "JobID:10,Partition:15,Name:20,State:12,Reason:25,NodeList:20" 2>&1 || true
kubectl -n "${SLURM_NS}" exec "${LOGIN_DEPLOY}" -c login -- scontrol -o show partitions 2>&1 || true
kubectl -n "${SLURM_NS}" exec "${LOGIN_DEPLOY}" -c login -- scontrol -o show nodes 2>&1 || true

if [ -n "${JOB_ID}" ]; then
  echo "=== 4. Specific Job Details (JobID=${JOB_ID}) ==="
  kubectl -n "${SLURM_NS}" exec "${LOGIN_DEPLOY}" -c login -- scontrol show job "${JOB_ID}" 2>&1 || true
  kubectl -n "${SLURM_NS}" get jobset "slurm-job-${JOB_ID}" -o yaml 2>&1 || true
  kubectl -n "${SLURM_NS}" describe jobset "slurm-job-${JOB_ID}" 2>&1 || true
  kubectl -n "${SLURM_NS}" describe job -l "jobset.sigs.k8s.io/jobset-name=slurm-job-${JOB_ID}" 2>&1 || true
  JS_UID="$(kubectl -n "${SLURM_NS}" get jobset "slurm-job-${JOB_ID}" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
  if [ -n "${JS_UID}" ]; then
    kubectl -n "${SLURM_NS}" describe workload -l "kueue.x-k8s.io/job-uid=${JS_UID}" 2>&1 || true
  fi
  kubectl -n "${SLURM_NS}" describe pod -l "k8s-bridge.x-k8s.io/slurm-job-id=${JOB_ID}" 2>&1 || true
  kubectl -n "${SLURM_NS}" logs -l "k8s-bridge.x-k8s.io/slurm-job-id=${JOB_ID}" -c slurmd --tail=100 2>&1 || true
fi

echo "=== 5. Translation Events & Controller Manager Logs (${BRIDGE_NS}) ==="
kubectl -n "${SLURM_NS}" get events --field-selector reason=JobTranslationFailed 2>&1 || true
kubectl -n "${BRIDGE_NS}" get deploy,pods -o wide 2>&1 || true
kubectl -n "${BRIDGE_NS}" logs deploy/k8s-bridge-controller-manager -c manager --tail=100 2>&1 || true

echo "=== 6. Slurm Controller & REST API Logs (${SLURM_NS}) ==="
kubectl -n "${SLURM_NS}" logs slurm-controller-0 -c slurmctld --tail=50 2>&1 || true
kubectl -n "${SLURM_NS}" logs deploy/slurm-restapi -c slurmrestd --tail=50 2>&1 || \
  kubectl -n "${SLURM_NS}" logs -l app.kubernetes.io/name=slurmrestd -c slurmrestd --tail=50 2>&1 || true
