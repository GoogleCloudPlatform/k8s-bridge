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
	"errors"
	"testing"
	"time"

	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
)

const (
	testNamespace       = "slurm"
	testControllerName  = "slurm-ctrl"
	otherControllerName = "other-ctrl"
	testRestAPIName     = "slurm-restapi"
	testWMName          = "wm-test"
	testFinalizer       = "test-finalizer"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := k8sbridgev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add k8sbridgev1alpha1 to scheme: %v", err)
	}
	if err := slinkyv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add slinkyv1beta1 to scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 to scheme: %v", err)
	}
	return scheme
}

func TestGetRules(t *testing.T) {
	defaultRules := GetRules()
	if len(defaultRules) != 2 {
		t.Fatalf("expected 2 default rules, got %d", len(defaultRules))
	}
	if _, ok := defaultRules[0].(SlurmControllerRule); !ok {
		t.Errorf("expected rule[0] to be SlurmControllerRule, got %T", defaultRules[0])
	}
	if _, ok := defaultRules[1].(SlurmRestAPIRule); !ok {
		t.Errorf("expected rule[1] to be SlurmRestAPIRule, got %T", defaultRules[1])
	}
	for i, r := range defaultRules {
		if _, ok := r.(WatchableRule); !ok {
			t.Errorf("expected default rule[%d] (%T) to implement WatchableRule", i, r)
		}
	}
}

func TestSlurmControllerRule_Validate(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	wm := &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testWMName,
			Namespace: testNamespace,
		},
		Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testControllerName,
			},
		},
	}

	t.Run("returns nil when referenced Controller exists and is not terminating", func(t *testing.T) {
		ctrlObj := &slinkyv1beta1.Controller{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testControllerName,
				Namespace: testNamespace,
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ctrlObj).Build()

		failure, err := (SlurmControllerRule{}).Validate(ctx, c, wm)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if failure != nil {
			t.Fatalf("expected nil ValidationFailure, got %+v", failure)
		}
	})

	t.Run("returns ControllerNotFound when referenced Controller does not exist", func(t *testing.T) {
		c := fake.NewClientBuilder().WithScheme(scheme).Build()

		failure, err := (SlurmControllerRule{}).Validate(ctx, c, wm)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if failure == nil {
			t.Fatal("expected ValidationFailure, got nil")
		}
		if failure.Reason != ReasonSlurmControllerNotFound {
			t.Errorf("expected reason %q, got %q", ReasonSlurmControllerNotFound, failure.Reason)
		}
		expectedMsg := `Referenced Controller "slurm-ctrl" not found`
		if failure.Message != expectedMsg {
			t.Errorf("expected message %q, got %q", expectedMsg, failure.Message)
		}
	})

	t.Run("returns ControllerNotFound when referenced Controller has DeletionTimestamp set", func(t *testing.T) {
		now := metav1.NewTime(time.Now())
		ctrlObj := &slinkyv1beta1.Controller{
			ObjectMeta: metav1.ObjectMeta{
				Name:              testControllerName,
				Namespace:         testNamespace,
				DeletionTimestamp: &now,
				Finalizers:        []string{testFinalizer},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ctrlObj).Build()

		failure, err := (SlurmControllerRule{}).Validate(ctx, c, wm)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if failure == nil {
			t.Fatal("expected ValidationFailure for terminating Controller, got nil")
		}
		if failure.Reason != ReasonSlurmControllerNotFound {
			t.Errorf("expected reason %q, got %q", ReasonSlurmControllerNotFound, failure.Reason)
		}
		expectedMsg := `Referenced Controller "slurm-ctrl" is being deleted`
		if failure.Message != expectedMsg {
			t.Errorf("expected message %q, got %q", expectedMsg, failure.Message)
		}
	})

	t.Run("propagates transient client Get error", func(t *testing.T) {
		expectedErr := errors.New("apiserver timeout")
		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return expectedErr
			},
		}).Build()

		failure, err := (SlurmControllerRule{}).Validate(ctx, c, wm)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected error %v, got %v", expectedErr, err)
		}
		if failure != nil {
			t.Errorf("expected nil failure on client error, got %+v", failure)
		}
	})
}

func TestSlurmControllerRule_Watch(t *testing.T) {
	w := (SlurmControllerRule{}).Watch()
	if w == nil {
		t.Fatal("expected non-nil RuleWatch")
	}
	if _, ok := w.Object.(*slinkyv1beta1.Controller); !ok {
		t.Errorf("expected Watch Object to be *slinkyv1beta1.Controller, got %T", w.Object)
	}

	wm := &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testWMName,
			Namespace: testNamespace,
		},
		Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
			ControllerRef: corev1.LocalObjectReference{Name: testControllerName},
		},
	}

	matchingCtrl := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{Name: testControllerName, Namespace: testNamespace},
	}
	otherNameCtrl := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{Name: otherControllerName, Namespace: testNamespace},
	}
	otherNamespaceCtrl := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{Name: testControllerName, Namespace: "other-ns"},
	}

	if !w.Matches(matchingCtrl, wm) {
		t.Error("expected Matches to return true for matching Controller namespace and name")
	}
	if w.Matches(otherNameCtrl, wm) {
		t.Error("expected Matches to return false for non-matching Controller name")
	}
	if w.Matches(otherNamespaceCtrl, wm) {
		t.Error("expected Matches to return false for Controller in different namespace")
	}
}

func TestSlurmRestAPIRule_Validate(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	wm := &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testWMName,
			Namespace: testNamespace,
		},
		Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testControllerName,
			},
		},
	}

	t.Run("returns nil when a non-terminating RestApi references the Controller (even without Controller CR)", func(t *testing.T) {
		restAPI := &slinkyv1beta1.RestApi{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testRestAPIName,
				Namespace: testNamespace,
			},
			Spec: slinkyv1beta1.RestApiSpec{
				ControllerRef: corev1.LocalObjectReference{Name: testControllerName},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(restAPI).Build()

		failure, err := (SlurmRestAPIRule{}).Validate(ctx, c, wm)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if failure != nil {
			t.Fatalf("expected nil ValidationFailure, got %+v", failure)
		}
	})

	t.Run("returns RestApiNotFound when no RestApi references the Controller", func(t *testing.T) {
		otherRestAPI := &slinkyv1beta1.RestApi{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "other-restapi",
				Namespace: testNamespace,
			},
			Spec: slinkyv1beta1.RestApiSpec{
				ControllerRef: corev1.LocalObjectReference{Name: otherControllerName},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(otherRestAPI).Build()

		failure, err := (SlurmRestAPIRule{}).Validate(ctx, c, wm)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if failure == nil {
			t.Fatal("expected ValidationFailure, got nil")
		}
		if failure.Reason != ReasonSlurmRestAPINotFound {
			t.Errorf("expected reason %q, got %q", ReasonSlurmRestAPINotFound, failure.Reason)
		}
		expectedMsg := `No RestApi found for Controller "slurm-ctrl"`
		if failure.Message != expectedMsg {
			t.Errorf("expected message %q, got %q", expectedMsg, failure.Message)
		}
	})

	t.Run("returns RestApiNotFound when matching RestApi has DeletionTimestamp set", func(t *testing.T) {
		now := metav1.NewTime(time.Now())
		terminatingRestAPI := &slinkyv1beta1.RestApi{
			ObjectMeta: metav1.ObjectMeta{
				Name:              testRestAPIName,
				Namespace:         testNamespace,
				DeletionTimestamp: &now,
				Finalizers:        []string{testFinalizer},
			},
			Spec: slinkyv1beta1.RestApiSpec{
				ControllerRef: corev1.LocalObjectReference{Name: testControllerName},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(terminatingRestAPI).Build()

		failure, err := (SlurmRestAPIRule{}).Validate(ctx, c, wm)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if failure == nil {
			t.Fatal("expected ValidationFailure for terminating RestApi, got nil")
		}
		if failure.Reason != ReasonSlurmRestAPINotFound {
			t.Errorf("expected reason %q, got %q", ReasonSlurmRestAPINotFound, failure.Reason)
		}
		expectedMsg := `RestApi for Controller "slurm-ctrl" is being deleted`
		if failure.Message != expectedMsg {
			t.Errorf("expected message %q, got %q", expectedMsg, failure.Message)
		}
	})

	t.Run("propagates transient client List error", func(t *testing.T) {
		expectedErr := errors.New("apiserver list timeout")
		c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, client client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				return expectedErr
			},
		}).Build()

		failure, err := (SlurmRestAPIRule{}).Validate(ctx, c, wm)
		if !errors.Is(err, expectedErr) {
			t.Fatalf("expected error %v, got %v", expectedErr, err)
		}
		if failure != nil {
			t.Errorf("expected nil failure on client error, got %+v", failure)
		}
	})
}

func TestSlurmRestAPIRule_Watch(t *testing.T) {
	w := (SlurmRestAPIRule{}).Watch()
	if w == nil {
		t.Fatal("expected non-nil RuleWatch")
	}
	if _, ok := w.Object.(*slinkyv1beta1.RestApi); !ok {
		t.Errorf("expected Watch Object to be *slinkyv1beta1.RestApi, got %T", w.Object)
	}

	wm := &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testWMName,
			Namespace: testNamespace,
		},
		Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
			ControllerRef: corev1.LocalObjectReference{Name: testControllerName},
		},
	}

	matchingRestAPI := &slinkyv1beta1.RestApi{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace},
		Spec:       slinkyv1beta1.RestApiSpec{ControllerRef: corev1.LocalObjectReference{Name: testControllerName}},
	}
	emptyRefRestAPI := &slinkyv1beta1.RestApi{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace},
	}
	otherRestAPI := &slinkyv1beta1.RestApi{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNamespace},
		Spec:       slinkyv1beta1.RestApiSpec{ControllerRef: corev1.LocalObjectReference{Name: otherControllerName}},
	}
	otherNamespaceRestAPI := &slinkyv1beta1.RestApi{
		ObjectMeta: metav1.ObjectMeta{Namespace: "other-ns"},
		Spec:       slinkyv1beta1.RestApiSpec{ControllerRef: corev1.LocalObjectReference{Name: testControllerName}},
	}
	nonRestAPIObj := &corev1.Secret{}

	if !w.Matches(matchingRestAPI, wm) {
		t.Error("expected Matches to return true for matching RestApi")
	}
	if w.Matches(emptyRefRestAPI, wm) {
		t.Error("expected Matches to return false when ControllerRef.Name is empty")
	}
	if w.Matches(otherRestAPI, wm) {
		t.Error("expected Matches to return false for non-matching ControllerRef.Name")
	}
	if w.Matches(otherNamespaceRestAPI, wm) {
		t.Error("expected Matches to return false for RestApi in different namespace")
	}
	if w.Matches(nonRestAPIObj, wm) {
		t.Error("expected Matches to return false for non-RestApi object")
	}
}

func TestPredicates(t *testing.T) {
	now := metav1.NewTime(time.Now())
	baseObj := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testControllerName,
			Generation: 1,
		},
	}
	statusUpdatedObj := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:            testControllerName,
			Generation:      1,
			ResourceVersion: "2",
		},
	}
	specUpdatedObj := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:       testControllerName,
			Generation: 2,
		},
	}
	deletingObj := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testControllerName,
			Generation:        1,
			DeletionTimestamp: &now,
		},
	}

	t.Run("ExistencePredicate", func(t *testing.T) {
		p := ExistencePredicate()
		if !p.Create(event.CreateEvent{Object: baseObj}) {
			t.Error("expected Create to return true")
		}
		if !p.Delete(event.DeleteEvent{Object: baseObj}) {
			t.Error("expected Delete to return true")
		}
		if p.Generic(event.GenericEvent{Object: baseObj}) {
			t.Error("expected Generic to return false")
		}
		if p.Update(event.UpdateEvent{ObjectOld: baseObj, ObjectNew: statusUpdatedObj}) {
			t.Error("expected Update on status-only change to return false")
		}
		if p.Update(event.UpdateEvent{ObjectOld: baseObj, ObjectNew: specUpdatedObj}) {
			t.Error("expected Update on generation-only change to return false for ExistencePredicate")
		}
		if !p.Update(event.UpdateEvent{ObjectOld: baseObj, ObjectNew: deletingObj}) {
			t.Error("expected Update on DeletionTimestamp transition to return true")
		}
	})

	t.Run("SpecOrExistencePredicate", func(t *testing.T) {
		p := SpecOrExistencePredicate()
		if !p.Create(event.CreateEvent{Object: baseObj}) {
			t.Error("expected Create to return true")
		}
		if !p.Delete(event.DeleteEvent{Object: baseObj}) {
			t.Error("expected Delete to return true")
		}
		if p.Generic(event.GenericEvent{Object: baseObj}) {
			t.Error("expected Generic to return false")
		}
		if p.Update(event.UpdateEvent{ObjectOld: baseObj, ObjectNew: statusUpdatedObj}) {
			t.Error("expected Update on status-only change to return false")
		}
		if !p.Update(event.UpdateEvent{ObjectOld: baseObj, ObjectNew: specUpdatedObj}) {
			t.Error("expected Update on generation change to return true for SpecOrExistencePredicate")
		}
		if !p.Update(event.UpdateEvent{ObjectOld: baseObj, ObjectNew: deletingObj}) {
			t.Error("expected Update on DeletionTimestamp transition to return true")
		}
	})
}
