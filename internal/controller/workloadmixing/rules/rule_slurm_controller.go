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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
)

// ReasonSlurmControllerNotFound indicates the referenced Slinky Slurm Controller CR does not exist or is terminating.
const ReasonSlurmControllerNotFound = "ControllerNotFound"

// SlurmControllerRule validates that the referenced Slinky Slurm Controller CR exists and is not terminating.
type SlurmControllerRule struct{}

// Validate checks whether the referenced Slinky Slurm Controller exists in the WorkloadMixing namespace.
func (SlurmControllerRule) Validate(ctx context.Context, reader client.Reader, wm *k8sbridgev1alpha1.WorkloadMixing) (*ValidationFailure, error) {
	logger := log.FromContext(ctx)
	controllerCR := &slinkyv1beta1.Controller{}
	controllerKey := types.NamespacedName{
		Namespace: wm.Namespace,
		Name:      wm.Spec.ControllerRef.Name,
	}

	if err := reader.Get(ctx, controllerKey, controllerCR); err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(1).Info("Referenced Controller not found", "slurmController", controllerKey)
			return &ValidationFailure{
				Reason:  ReasonSlurmControllerNotFound,
				Message: fmt.Sprintf("Referenced Controller %q not found", wm.Spec.ControllerRef.Name),
			}, nil
		}
		return nil, err
	}

	if !controllerCR.DeletionTimestamp.IsZero() {
		logger.V(1).Info("Referenced Controller is terminating", "slurmController", controllerKey)
		return &ValidationFailure{
			Reason:  ReasonSlurmControllerNotFound,
			Message: fmt.Sprintf("Referenced Controller %q is being deleted", wm.Spec.ControllerRef.Name),
		}, nil
	}

	return nil, nil
}

// Watch returns the watch configuration for Slinky Slurm Controller resources.
func (SlurmControllerRule) Watch() *RuleWatch {
	return &RuleWatch{
		Object: &slinkyv1beta1.Controller{},
		Opts:   []builder.WatchesOption{builder.WithPredicates(ExistencePredicate())},
		Matches: func(eventObj client.Object, wm *k8sbridgev1alpha1.WorkloadMixing) bool {
			return wm.Namespace == eventObj.GetNamespace() &&
				wm.Spec.ControllerRef.Name == eventObj.GetName()
		},
	}
}
