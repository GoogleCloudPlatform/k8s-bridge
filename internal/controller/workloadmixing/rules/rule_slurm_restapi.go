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

package rules

import (
	"context"
	"fmt"

	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/utils/refresolver"
)

// ReasonSlurmRestAPINotFound indicates no non-terminating Slinky Slurm RestApi CR references the Slurm Controller.
const ReasonSlurmRestAPINotFound = "RestApiNotFound"

// SlurmRestAPIRule validates that a non-terminating Slinky Slurm RestApi CR exists for the referenced Controller.
type SlurmRestAPIRule struct{}

// Validate checks whether at least one non-terminating RestApi CR references the WorkloadMixing's Slurm Controller.
func (SlurmRestAPIRule) Validate(ctx context.Context, reader client.Reader, wm *k8sbridgev1alpha1.WorkloadMixing) (*ValidationFailure, error) {
	logger := log.FromContext(ctx)
	resolver := refresolver.New(reader)
	controllerStub := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: wm.Namespace,
			Name:      wm.Spec.ControllerRef.Name,
		},
	}

	restAPIList, err := resolver.GetRestapisForController(ctx, controllerStub)
	if err != nil {
		return nil, err
	}

	for i := range restAPIList.Items {
		if restAPIList.Items[i].DeletionTimestamp.IsZero() {
			return nil, nil
		}
	}

	logger.V(1).Info("No RestApi found for Controller", "slurmController", client.ObjectKeyFromObject(controllerStub))
	return &ValidationFailure{
		Reason:  ReasonSlurmRestAPINotFound,
		Message: fmt.Sprintf("No RestApi found for Controller %q", wm.Spec.ControllerRef.Name),
	}, nil
}

// Watch returns the watch configuration for Slinky Slurm RestApi resources.
func (SlurmRestAPIRule) Watch() *RuleWatch {
	return &RuleWatch{
		Object: &slinkyv1beta1.RestApi{},
		Opts:   []builder.WatchesOption{builder.WithPredicates(SpecOrExistencePredicate())},
		Matches: func(eventObj client.Object, wm *k8sbridgev1alpha1.WorkloadMixing) bool {
			restAPI, ok := eventObj.(*slinkyv1beta1.RestApi)
			if !ok || restAPI.Spec.ControllerRef.Name == "" {
				return false
			}
			return wm.Namespace == restAPI.Namespace &&
				wm.Spec.ControllerRef.Name == restAPI.Spec.ControllerRef.Name
		},
	}
}
