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

# Non-root verification commands for hack/cli_filter.lua and hack/job_submit.lua
set -euo pipefail

NAMESPACE="${NAMESPACE:-slurm}"
LOGIN_DEPLOY="${LOGIN_DEPLOY:-deploy/slurm-login-slinky}"
PARTITION="${PARTITION:-k8s-partition}"
TEST_USER="${TEST_USER:-someuser}"
REJECTION_MSG="$(sed -n 's/^local REJECTION_MESSAGE = "\(.*\)"/\1/p' hack/cli_filter.lua)"
: "${REJECTION_MSG:?Failed to parse REJECTION_MESSAGE from hack/cli_filter.lua}"
AUTO_HOLD_MARKER="$(sed -n 's/^[[:space:]]*AutoHoldMarker[[:space:]]*=[[:space:]]*"\(.*\)"/\1/p' internal/controller/slurmjob/translator.go)"
: "${AUTO_HOLD_MARKER:?Failed to parse AutoHoldMarker from internal/controller/slurmjob/translator.go}"

# 1. Ensure non-root user exists on the login pod
kubectl -n "${NAMESPACE}" exec "${LOGIN_DEPLOY}" -c login -- sh -c \
  "id -u ${TEST_USER} >/dev/null 2>&1 || useradd -m -s /bin/bash ${TEST_USER}"

# 2. Verify cli_filter.lua accepts allowed options (--test-only)
OUT=$(kubectl -n "${NAMESPACE}" exec "${LOGIN_DEPLOY}" -c login -- \
  runuser -u "${TEST_USER}" -- \
  sbatch --partition="${PARTITION}" --nodes=1 --cpus-per-task=1 --mem=1G --chdir=/tmp --wrap="hostname" --test-only 2>&1 || true)
if ! grep -Fq "${REJECTION_MSG}" <<<"${OUT}" && \
   grep -Eq "to start at|Requested partition configuration not available now" <<<"${OUT}"; then
  echo "PASS: cli_filter.lua accepted allowed options (--test-only)"
else
  echo "FAIL: unexpected output from --test-only: ${OUT}" >&2
  exit 1
fi

# 3. Verify cli_filter.lua rejects disallowed options (e.g. --exclusive)
OUT=$(kubectl -n "${NAMESPACE}" exec "${LOGIN_DEPLOY}" -c login -- \
  runuser -u "${TEST_USER}" -- \
  sbatch --partition="${PARTITION}" --exclusive --chdir=/tmp --wrap="hostname" 2>&1 || true)
if grep -Fq "${REJECTION_MSG}" <<<"${OUT}"; then
  echo "PASS: cli_filter.lua rejected --exclusive"
else
  echo "FAIL: cli_filter.lua did not reject --exclusive: ${OUT}" >&2
  exit 1
fi

# 4. Verify job_submit.lua rejects multi-partition requests containing a bridged partition
OUT=$(kubectl -n "${NAMESPACE}" exec "${LOGIN_DEPLOY}" -c login -- \
  runuser -u "${TEST_USER}" -- \
  sbatch --partition="${PARTITION},other" --nodes=1 --chdir=/tmp --wrap="hostname" 2>&1 || true)
if grep -Eq "Unspecified error|Multi-partition request|Invalid partition name" <<<"${OUT}" && \
   kubectl -n "${NAMESPACE}" logs slurm-controller-0 -c slurmctld --since=2m | grep -q "Multi-partition request '${PARTITION},other'"; then
  echo "PASS: multi-partition request with ${PARTITION} rejected"
else
  echo "FAIL: multi-partition request with ${PARTITION} was not rejected: ${OUT}" >&2
  exit 1
fi

# 5. Verify job_submit.lua places held job (--hold) without k8s-bridge-auto-hold,
#    converts user release into k8s-bridge-auto-hold, and rejects subsequent user modifications
JOB_ID=$(kubectl -n "${NAMESPACE}" exec "${LOGIN_DEPLOY}" -c login -- \
  runuser -u "${TEST_USER}" -- \
  sbatch --parsable --hold --partition="${PARTITION}" --nodes=1 --chdir=/tmp --wrap="sleep 5")

if kubectl -n "${NAMESPACE}" exec "${LOGIN_DEPLOY}" -c login -- \
  scontrol show job "${JOB_ID}" | grep -q "${AUTO_HOLD_MARKER}"; then
  echo "FAIL: job ${JOB_ID} submitted with --hold should not have ${AUTO_HOLD_MARKER} before user release" >&2
  kubectl -n "${NAMESPACE}" exec slurm-controller-0 -c slurmctld -- scancel "${JOB_ID}" || true
  exit 1
fi

# Note: scontrol release may return exit code 1 (see ReleaseSlurmJob in test/utils/slurm.go)
# when 0 dynamic nodes exist; check the job comment afterwards.
kubectl -n "${NAMESPACE}" exec "${LOGIN_DEPLOY}" -c login -- \
  runuser -u "${TEST_USER}" -- \
  scontrol release "${JOB_ID}" || true

if kubectl -n "${NAMESPACE}" exec "${LOGIN_DEPLOY}" -c login -- \
  scontrol show job "${JOB_ID}" | grep -Eq "Comment=.*(${AUTO_HOLD_MARKER}|Unheld by k8s-bridge)"; then
  echo "PASS: job_submit.lua converted user release to ${AUTO_HOLD_MARKER}"
else
  echo "FAIL: job ${JOB_ID} missing ${AUTO_HOLD_MARKER} comment after release" >&2
  kubectl -n "${NAMESPACE}" exec slurm-controller-0 -c slurmctld -- scancel "${JOB_ID}" || true
  exit 1
fi

if ! kubectl -n "${NAMESPACE}" exec "${LOGIN_DEPLOY}" -c login -- \
  runuser -u "${TEST_USER}" -- \
  scontrol update JobId="${JOB_ID}" JobName=modified-name >/dev/null 2>&1; then
  echo "PASS: job_submit.lua rejected non-root scontrol update on auto-held job"
else
  echo "FAIL: job_submit.lua allowed non-root scontrol update on auto-held job" >&2
  kubectl -n "${NAMESPACE}" exec slurm-controller-0 -c slurmctld -- scancel "${JOB_ID}" || true
  exit 1
fi

kubectl -n "${NAMESPACE}" exec slurm-controller-0 -c slurmctld -- scancel "${JOB_ID}" || true
