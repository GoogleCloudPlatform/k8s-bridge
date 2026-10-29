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

# Drains and removes stale dynamic Slurm nodes (slurm-job-*) from slurmctld after pods terminate.
# Prerequisite: Obtain user confirmation and complete Steps 1-3 (scancel, delete jobset, delete pod) first.
# Usage: bash skills/ops-cleanup-state/scripts/cleanup-stale-nodes.sh [SLURM_NS] [OPTIONAL_JOB_ID]
set -euo pipefail

SLURM_NS="${1:-slurm}"
JOB_ID="${2:-}"

if [ -n "${JOB_ID}" ]; then
  NODE_PREFIX="slurm-job-${JOB_ID}-slurmd-"
  echo "Deregistering dynamic Slurm nodes for Job ID ${JOB_ID} in namespace ${SLURM_NS}..."
else
  NODE_PREFIX="slurm-job-"
  echo "Deregistering all k8s-bridge dynamic nodes in namespace ${SLURM_NS}..."
fi

kubectl -n "${SLURM_NS}" exec slurm-controller-0 -c slurmctld -- sh -c "
  for n in \$(scontrol -o show nodes 2>/dev/null | sed -n 's/^NodeName=\\(${NODE_PREFIX}[^ ]*\\).*/\\1/p'); do
    echo \"Deregistering dynamic Slurm node: \${n}\"
    scontrol update nodename=\"\${n}\" state=DOWN reason=cleanup || true
    scontrol delete nodename=\"\${n}\" || true
  done
"

echo "Cleanup complete. Remaining Slurm nodes:"
kubectl -n "${SLURM_NS}" exec slurm-controller-0 -c slurmctld -- scontrol -o show nodes || true
