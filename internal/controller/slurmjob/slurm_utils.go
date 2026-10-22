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
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

const (
	defaultSlurmCallTimeout          = 5 * time.Second
	defaultReconciliationTimeout     = 30 * time.Second
	slurmErrMsgInvalidJobID          = "Invalid job id specified"
	slurmErrMsgInvalidNodeName       = "Invalid node name specified"
	slurmErrMsgNodeNotFound          = "Node not found"
	slurmErrMsgPartConfigUnavailable = "Requested partition configuration not available now"
)

// isSlurmNotFound checks if a Slurm REST API error indicates the resource was not found.
// The controller connects directly to slurmrestd over cluster-internal networking without
// intermediary proxies, so network/routing failures produce transport errors rather than HTTP 404.
// Slinky slurm-client returns HTTP 404 or synthesizes errors.New(http.StatusText(http.StatusNotFound))
// when GET /job/{id} returns 200 with an empty job list. Slurm also returns HTTP 422 for missing nodes/jobs.
// Exact string matching is used to prevent false positives from other error messages containing "Not Found"
// (e.g., account, QOS, or partition configuration errors).
func isSlurmNotFound(err error) bool {
	return slices.ContainsFunc(slurmErrorMessages(err), isSlurmNotFoundString)
}

func isSlurmNotFoundString(msg string) bool {
	return msg == http.StatusText(http.StatusNotFound) ||
		msg == slurmErrMsgInvalidJobID ||
		msg == slurmErrMsgInvalidNodeName ||
		msg == slurmErrMsgNodeNotFound
}

// isSlurmPartConfigUnavailable reports whether err is only Slurm's HTTP 422
// "Requested partition configuration not available now" warning. Slurm returns
// this warning after updating a job when its partition has no nodes.
func isSlurmPartConfigUnavailable(err error) bool {
	msgs := slurmErrorMessages(err)
	if !slices.Contains(msgs, slurmErrMsgPartConfigUnavailable) {
		return false
	}
	for _, msg := range msgs {
		if msg != slurmErrMsgPartConfigUnavailable && msg != http.StatusText(http.StatusUnprocessableEntity) {
			return false
		}
	}
	return true
}

// slurmErrorMessages extracts individual error messages from a Slurm client error,
// unwrapping utilerrors.Aggregate and "[a, b]" formatted strings.
func slurmErrorMessages(err error) []string {
	if err == nil {
		return nil
	}
	var agg utilerrors.Aggregate
	if errors.As(err, &agg) {
		var msgs []string
		for _, subErr := range agg.Errors() {
			msgs = append(msgs, slurmErrorMessages(subErr)...)
		}
		return msgs
	}
	errMsg := err.Error()
	if strings.HasPrefix(errMsg, "[") && strings.HasSuffix(errMsg, "]") {
		parts := strings.Split(errMsg[1:len(errMsg)-1], ", ")
		for i, part := range parts {
			parts[i] = strings.TrimSpace(part)
		}
		return parts
	}
	return []string{errMsg}
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

// isTerminalSlurmJob checks whether a Slurm job is in a terminal state (excluding jobs still COMPLETING)
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

// classifyTerminalSlurmState checks whether a Slurm job is in a terminal state, regardless if it is just COMPLETING or past it, and returns its metrics reason.
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

// isJobSetFinalized reports whether the JobSet is being deleted and FinalizerJobSet was already released,
// i.e. k8s-bridge has no cleanup left to do for it.
func isJobSetFinalized(js *jobsetv1alpha2.JobSet) bool {
	return isJobSetDeleting(js) && !controllerutil.ContainsFinalizer(js, FinalizerJobSet)
}

// finalizeJobSet deregisters the JobSet's dynamic slurmd nodes and then removes FinalizerJobSet.
// Nodes must be deleted first because the finalizer keeps the JobSet spec that node names are derived from.
// A nil slurmClient skips node cleanup when the Slurm cluster can no longer be reached.
func finalizeJobSet(ctx context.Context, c client.Client, slurmClient slurmclient.Client, js *jobsetv1alpha2.JobSet, callTimeout time.Duration) error {
	if js == nil {
		return nil
	}
	if err := cleanupDynamicNodes(ctx, slurmClient, js, callTimeout); err != nil {
		return fmt.Errorf("failed to cleanup dynamic nodes for JobSet %s/%s: %w", js.Namespace, js.Name, err)
	}
	return removeJobSetFinalizer(ctx, c, js)
}

// removeJobSetFinalizer removes FinalizerJobSet from the given JobSet if present.
// The patch uses optimistic locking because a JSON merge patch replaces the whole finalizers list,
// which could otherwise drop finalizers added concurrently (e.g. foregroundDeletion or orphan).
// On conflict the JobSet is re-read and the removal retried, so a concurrent finalization of the same
// JobSet by the other reconciler is not reported as an error.
func removeJobSetFinalizer(ctx context.Context, c client.Client, js *jobsetv1alpha2.JobSet) error {
	if js == nil {
		return nil
	}
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if !controllerutil.ContainsFinalizer(js, FinalizerJobSet) {
			return nil
		}
		patch := client.MergeFromWithOptions(js.DeepCopy(), client.MergeFromWithOptimisticLock{})
		controllerutil.RemoveFinalizer(js, FinalizerJobSet)
		err := c.Patch(ctx, js, patch)
		if !apierrors.IsConflict(err) {
			return client.IgnoreNotFound(err)
		}
		if getErr := c.Get(ctx, client.ObjectKeyFromObject(js), js); getErr != nil {
			return client.IgnoreNotFound(getErr)
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("failed to remove finalizer from JobSet %s/%s: %w", js.Namespace, js.Name, err)
	}
	return nil
}

// isSlurmControllerGone reports whether the Slurm Controller CR identified by key no longer exists.
func isSlurmControllerGone(ctx context.Context, c client.Reader, key types.NamespacedName) (bool, error) {
	if err := c.Get(ctx, key, &slinkyv1beta1.Controller{}); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("failed to get Slurm Controller %s: %w", key, err)
	}
	return false, nil
}
