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

	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
)

// ValidationRule encapsulates a validation check evaluated against a WorkloadMixing resource.
type ValidationRule interface {
	Validate(ctx context.Context, reader client.Reader, wm *k8sbridgev1alpha1.WorkloadMixing) (*ValidationFailure, error)
}

// WatchableRule is an optional interface implemented by ValidationRules that watch a Kubernetes resource for changes.
type WatchableRule interface {
	Watch() *RuleWatch
}

// ValidationFailure describes a domain validation failure returned by a ValidationRule.
type ValidationFailure struct {
	Reason  string
	Message string
}

// RuleWatch defines the secondary Kubernetes resource watch and matching predicate for a ValidationRule.
type RuleWatch struct {
	Object  client.Object
	Matches func(eventObj client.Object, wm *k8sbridgev1alpha1.WorkloadMixing) bool
	Opts    []builder.WatchesOption
}

// GetRules returns the ordered pipeline of cross-resource validation rules evaluated by WorkloadMixingReconciler.
func GetRules() []ValidationRule {
	return []ValidationRule{
		SlurmControllerRule{},
		SlurmRestAPIRule{},
	}
}

// ExistencePredicate triggers reconciliation only on object creation, deletion, or DeletionTimestamp transitions,
// ignoring status-only or spec updates that do not affect resource existence.
func ExistencePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			return e.ObjectOld.GetDeletionTimestamp().IsZero() != e.ObjectNew.GetDeletionTimestamp().IsZero()
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}

// SpecOrExistencePredicate triggers reconciliation on object creation, deletion, spec generation changes,
// or DeletionTimestamp transitions, ignoring pure status updates.
func SpecOrExistencePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return false
			}
			if e.ObjectOld.GetDeletionTimestamp().IsZero() != e.ObjectNew.GetDeletionTimestamp().IsZero() {
				return true
			}
			return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}
}
