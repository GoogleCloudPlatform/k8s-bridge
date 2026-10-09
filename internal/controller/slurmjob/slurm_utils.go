/*
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
*/

package slurmjob

import (
	"errors"
	"net/http"
	"slices"
	"strings"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

const (
	slurmErrMsgInvalidJobID    = "Invalid job id specified"
	slurmErrMsgInvalidNodeName = "Invalid node name specified"
	slurmErrMsgNodeNotFound    = "Node not found"
)

// isSlurmNotFound checks if a Slurm REST API error indicates the resource was not found.
// The controller connects directly to slurmrestd over cluster-internal networking without
// intermediary proxies, so network/routing failures produce transport errors rather than HTTP 404.
// Slinky slurm-client returns HTTP 404 or synthesizes errors.New(http.StatusText(http.StatusNotFound))
// when GET /job/{id} returns 200 with an empty job list. Slurm also returns HTTP 422 for missing nodes/jobs.
// Exact string matching is used to prevent false positives from other error messages containing "Not Found"
// (e.g., account, QOS, or partition configuration errors).
func isSlurmNotFound(err error) bool {
	if err == nil {
		return false
	}
	var agg utilerrors.Aggregate
	if errors.As(err, &agg) {
		return slices.ContainsFunc(agg.Errors(), isSlurmNotFound)
	}
	errMsg := err.Error()
	if isSlurmNotFoundString(errMsg) {
		return true
	}
	// Handle string-formatted aggregates (e.g. "[Not Found, Invalid job id specified]").
	if strings.HasPrefix(errMsg, "[") && strings.HasSuffix(errMsg, "]") {
		for part := range strings.SplitSeq(errMsg[1:len(errMsg)-1], ", ") {
			if isSlurmNotFoundString(strings.TrimSpace(part)) {
				return true
			}
		}
	}
	return false
}

func isSlurmNotFoundString(msg string) bool {
	return msg == http.StatusText(http.StatusNotFound) ||
		msg == slurmErrMsgInvalidJobID ||
		msg == slurmErrMsgInvalidNodeName ||
		msg == slurmErrMsgNodeNotFound
}

// isJobSetOwnedByWM returns true if the JobSet has an OwnerReference pointing to the WorkloadMixing.
func isJobSetOwnedByWM(js *jobsetv1alpha2.JobSet, wm *k8sbridgev1alpha1.WorkloadMixing) bool {
	for _, owner := range js.OwnerReferences {
		if owner.Kind == KindWorkloadMixing && owner.Name == wm.Name && (wm.UID == "" || owner.UID == wm.UID) {
			return true
		}
	}
	return false
}

// isTerminalSlurmJob checks whether a Slurm job is in a COMPLETED terminal state
func isTerminalSlurmJob(jobInfo *slurmclienttypes.V0044JobInfo) (bool, string) {
	if jobInfo == nil || jobInfo.GetStateAsSet().Has(slurmapi.V0044JobInfoJobStateCOMPLETING) {
		return false, ""
	}
	return classifyTerminalSlurmState(jobInfo)
}

// hasTerminalSlurmState checks whether a Slurm job is in a terminal state, regardless it it is just COMPLETING or past it
func hasTerminalSlurmState(jobInfo *slurmclienttypes.V0044JobInfo) bool {
	terminal, _ := classifyTerminalSlurmState(jobInfo)
	return terminal
}

// classifyTerminalSlurmState checks whether a Slurm job is in a terminal state, regardless it it is just COMPLETING or past it, and returns its metrics reason.
func classifyTerminalSlurmState(jobInfo *slurmclienttypes.V0044JobInfo) (bool, string) {
	if jobInfo == nil {
		return false, ""
	}
	states := jobInfo.GetStateAsSet()
	if states.HasAny(
		slurmapi.V0044JobInfoJobStatePENDING,
		slurmapi.V0044JobInfoJobStateCONFIGURING,
		slurmapi.V0044JobInfoJobStateRUNNING,
		slurmapi.V0044JobInfoJobStateSUSPENDED,
	) {
		return false, ""
	}
	if states.Has(slurmapi.V0044JobInfoJobStateCOMPLETED) {
		return true, metrics.ReasonJobCompleted
	}
	if states.Has(slurmapi.V0044JobInfoJobStateCANCELLED) {
		return true, metrics.ReasonJobCancelled
	}
	if states.HasAny(
		slurmapi.V0044JobInfoJobStateFAILED,
		slurmapi.V0044JobInfoJobStateTIMEOUT,
		slurmapi.V0044JobInfoJobStateNODEFAIL,
		slurmapi.V0044JobInfoJobStatePREEMPTED,
		slurmapi.V0044JobInfoJobStateBOOTFAIL,
		slurmapi.V0044JobInfoJobStateDEADLINE,
		slurmapi.V0044JobInfoJobStateOUTOFMEMORY,
		slurmapi.V0044JobInfoJobStateLAUNCHFAILED,
		slurmapi.V0044JobInfoJobStateRECONFIGFAIL,
		slurmapi.V0044JobInfoJobStateREVOKED,
		slurmapi.V0044JobInfoJobStateSPECIALEXIT,
	) {
		return true, metrics.ReasonJobFailed
	}
	return false, ""
}
