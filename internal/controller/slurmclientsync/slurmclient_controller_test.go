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

package slurmclientsync

import (
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	slurmobject "github.com/SlinkyProject/slurm-client/pkg/object"
	slurmtypes "github.com/SlinkyProject/slurm-client/pkg/types"
	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/GoogleCloudPlatform/k8s-bridge/internal/clientmap"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/controller/slurmclientsync/slurmjwt"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	"github.com/GoogleCloudPlatform/k8s-bridge/test/utils"
)

const (
	testNamespace = "default"
	testCtrlName  = "my-controller"
)

func TestSlurmClientReconciler_Reconcile_NewController(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = slinkyv1beta1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}
	signingKey := make([]byte, 128)
	_, _ = rand.Read(signingKey)

	controllerObj := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.ControllerSpec{
			JwtKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: testCtrlName,
				},
				Key: slurmjwt.DefaultTokenKey,
			},
		},
	}

	secretObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Data: map[string][]byte{
			slurmjwt.DefaultTokenKey: signingKey,
		},
	}

	restapiObj := &slinkyv1beta1.RestApi{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.RestApiSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testCtrlName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerObj, secretObj, restapiObj).Build()
	cMap := clientmap.NewClientMap()

	var capturedConfig *slurmclient.Config
	var capturedOpts []slurmclient.ClientOption
	reconciler := &SlurmClientReconciler{
		Client:    fakeClient,
		Scheme:    scheme,
		ClientMap: cMap,
		NewClient: func(config *slurmclient.Config, opts ...slurmclient.ClientOption) (slurmclient.Client, error) {
			capturedConfig = config
			capturedOpts = opts
			mock := &utils.SlurmClient{}
			mock.SetToken(config.AuthToken)
			mock.SetServer(config.Server)
			return mock, nil
		},
	}

	res, err := reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: controllerKey})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if !cMap.Has(controllerKey) {
		t.Fatalf("Expected ClientMap to contain client for key %v", controllerKey)
	}

	clientObj := cMap.Get(controllerKey)
	if clientObj == nil {
		t.Fatalf("Client in ClientMap is nil")
	}

	mockClientRetrieved, ok := clientObj.(*utils.SlurmClient)
	if !ok {
		t.Fatalf("Expected mock client, got %T", clientObj)
	}

	if mockClientRetrieved.GetToken() == "" {
		t.Errorf("GetToken() = %q, want non-empty", mockClientRetrieved.GetToken())
	}

	expectedRequeue := 12 * time.Minute // 4/5 of 15m
	if res.RequeueAfter != expectedRequeue {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, expectedRequeue)
	}

	if status := testutil.ToFloat64(metrics.SlurmClientStatus.WithLabelValues(testCtrlName)); status != 1.0 {
		t.Errorf("Expected SlurmClientStatus 1.0, got %f", status)
	}

	if capturedConfig == nil {
		t.Fatal("Expected Config to be passed to NewClient")
	}
	if capturedConfig.HTTPClient == nil {
		t.Fatal("Expected Config.HTTPClient to be non-nil")
	}
	if capturedConfig.HTTPClient.Timeout != DefaultHTTPClientTimeout {
		t.Errorf("Config.HTTPClient.Timeout = %v, want %v", capturedConfig.HTTPClient.Timeout, DefaultHTTPClientTimeout)
	}

	var clientOptions *slurmclient.ClientOptions
	for _, opt := range capturedOpts {
		if o, ok := opt.(*slurmclient.ClientOptions); ok {
			clientOptions = o
			break
		}
	}
	if clientOptions == nil {
		t.Fatal("Expected ClientOptions to be passed to NewClient")
	}

	var hasNode, hasJobInfo, hasPing bool
	for _, obj := range clientOptions.DisableFor {
		switch obj.(type) {
		case *slurmtypes.V0044Node:
			hasNode = true
		case *slurmtypes.V0044JobInfo:
			hasJobInfo = true
		case *slurmtypes.V0041ControllerPing:
			hasPing = true
		}
	}
	if !hasNode {
		t.Errorf("Expected V0044Node in DisableFor")
	}
	if !hasJobInfo {
		t.Errorf("Expected V0044JobInfo in DisableFor")
	}
	if !hasPing {
		t.Errorf("Expected V0041ControllerPing in DisableFor")
	}
}

func TestSlurmClientReconciler_Reconcile_DeleteController(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = slinkyv1beta1.AddToScheme(scheme)

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	cMap := clientmap.NewClientMap()
	cMap.Add(context.Background(), controllerKey, &utils.SlurmClient{})

	reconciler := &SlurmClientReconciler{
		Client:    fakeClient,
		Scheme:    scheme,
		ClientMap: cMap,
		NewClient: func(config *slurmclient.Config, opts ...slurmclient.ClientOption) (slurmclient.Client, error) {
			mock := &utils.SlurmClient{}
			mock.SetToken(config.AuthToken)
			mock.SetServer(config.Server)
			return mock, nil
		},
	}

	metrics.SetSlurmClientStatus(testCtrlName, true)

	res, err := reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: controllerKey})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if cMap.Has(controllerKey) {
		t.Errorf("cMap.Has(%v) = true, want false", controllerKey)
	}

	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want 0", res.RequeueAfter)
	}

	if count := testutil.CollectAndCount(metrics.SlurmClientStatus); count != 0 {
		t.Errorf("Expected SlurmClientStatus series to be deleted, got %d series", count)
	}
}

func TestSlurmClientReconciler_Reconcile_StalenessCheck_NotStale(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = slinkyv1beta1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}
	signingKey := make([]byte, 128)
	_, _ = rand.Read(signingKey)

	controllerObj := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.ControllerSpec{
			JwtKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: testCtrlName,
				},
				Key: slurmjwt.DefaultTokenKey,
			},
		},
	}

	secretObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Data: map[string][]byte{
			slurmjwt.DefaultTokenKey: signingKey,
		},
	}

	restapiObj := &slinkyv1beta1.RestApi{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.RestApiSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testCtrlName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerObj, secretObj, restapiObj).Build()
	cMap := clientmap.NewClientMap()

	originalToken, err := slurmjwt.NewToken(signingKey).NewSignedToken()
	if err != nil {
		t.Fatalf("Failed to create original token: %v", err)
	}
	mockClient := &utils.SlurmClient{}
	mockClient.SetToken(originalToken)
	mockClient.SetServer("http://my-controller-restapi.default:6820")

	// Expiry in 10 minutes (which is > 3 mins staleness buffer)
	expiryTime := time.Now().Add(10 * time.Minute)
	cMap.AddWithExpiration(context.Background(), controllerKey, mockClient, expiryTime)

	reconciler := &SlurmClientReconciler{
		Client:    fakeClient,
		Scheme:    scheme,
		ClientMap: cMap,
		NewClient: func(config *slurmclient.Config, opts ...slurmclient.ClientOption) (slurmclient.Client, error) {
			mock := &utils.SlurmClient{}
			mock.SetToken(config.AuthToken)
			mock.SetServer(config.Server)
			return mock, nil
		},
	}

	res, err := reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: controllerKey})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Token should NOT have been regenerated
	retrievedClient := cMap.Get(controllerKey)
	mockClientRetrieved, ok := retrievedClient.(*utils.SlurmClient)
	if !ok {
		t.Fatalf("Expected mock client, got %T", retrievedClient)
	}
	if mockClientRetrieved.GetToken() != originalToken {
		t.Errorf("GetToken() = %q, want %q", mockClientRetrieved.GetToken(), originalToken)
	}

	// Expected requeue should be (10 minutes - 3 minutes staleness buffer) = 7 minutes
	expectedRequeueLowerBound := 6 * time.Minute
	expectedRequeueUpperBound := 8 * time.Minute
	if res.RequeueAfter < expectedRequeueLowerBound || res.RequeueAfter > expectedRequeueUpperBound {
		t.Errorf("RequeueAfter = %v, want between %v and %v", res.RequeueAfter, expectedRequeueLowerBound, expectedRequeueUpperBound)
	}
}

func TestSlurmClientReconciler_Reconcile_StalenessCheck_Stale(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = slinkyv1beta1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}
	signingKey := make([]byte, 128)
	_, _ = rand.Read(signingKey)

	controllerObj := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.ControllerSpec{
			JwtKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: testCtrlName,
				},
				Key: slurmjwt.DefaultTokenKey,
			},
		},
	}

	secretObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Data: map[string][]byte{
			slurmjwt.DefaultTokenKey: signingKey,
		},
	}

	restapiObj := &slinkyv1beta1.RestApi{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.RestApiSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testCtrlName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerObj, secretObj, restapiObj).Build()
	cMap := clientmap.NewClientMap()

	originalToken, err := slurmjwt.NewToken(signingKey).NewSignedToken()
	if err != nil {
		t.Fatalf("Failed to create original token: %v", err)
	}
	mockClient := &utils.SlurmClient{}
	mockClient.SetToken(originalToken)

	// Expiry in 2 minutes (which is <= 3 mins staleness buffer)
	expiryTime := time.Now().Add(2 * time.Minute)
	cMap.AddWithExpiration(context.Background(), controllerKey, mockClient, expiryTime)

	reconciler := &SlurmClientReconciler{
		Client:    fakeClient,
		Scheme:    scheme,
		ClientMap: cMap,
		NewClient: func(config *slurmclient.Config, opts ...slurmclient.ClientOption) (slurmclient.Client, error) {
			mock := &utils.SlurmClient{}
			mock.SetToken(config.AuthToken)
			mock.SetServer(config.Server)
			return mock, nil
		},
	}

	res, err := reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: controllerKey})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Token SHOULD have been regenerated
	retrievedClient := cMap.Get(controllerKey)
	mockClientRetrieved, ok := retrievedClient.(*utils.SlurmClient)
	if !ok {
		t.Fatalf("Expected mock client, got %T", retrievedClient)
	}
	if mockClientRetrieved.GetToken() == originalToken {
		t.Errorf("GetToken() = %q, want regenerated token != %q", mockClientRetrieved.GetToken(), originalToken)
	}

	if res.RequeueAfter != 12*time.Minute {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, 12*time.Minute)
	}
}

func TestSlurmClientReconciler_Reconcile_URLChange(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = slinkyv1beta1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}
	signingKey := make([]byte, 128)
	_, _ = rand.Read(signingKey)

	controllerObj := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.ControllerSpec{
			JwtKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: testCtrlName,
				},
				Key: slurmjwt.DefaultTokenKey,
			},
		},
	}

	secretObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Data: map[string][]byte{
			slurmjwt.DefaultTokenKey: signingKey,
		},
	}

	// Initial RestApi pointing to server-a
	restapiObj := &slinkyv1beta1.RestApi{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.RestApiSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testCtrlName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerObj, secretObj, restapiObj).Build()
	cMap := clientmap.NewClientMap()

	// Pre-populate ClientMap with a client pointing to server-a
	mockClient := &utils.SlurmClient{}
	mockClient.SetServer("http://my-controller-restapi.default:6820") // This is the expected default URL
	cMap.AddWithExpiration(context.Background(), controllerKey, mockClient, time.Now().Add(10*time.Minute))

	// Now, let's simulate a URL change by enabling Debug mode which overrides to localhost,
	// or by changing the RestApi name/namespace if that affects the FQDN.
	// For this test, we can just change the expected server in the mock or reconciler setup.
	// A simpler way with the current code is to use the Debug flag to trigger a change.

	reconciler := &SlurmClientReconciler{
		Client:    fakeClient,
		Scheme:    scheme,
		ClientMap: cMap,
		Debug:     true, // This will change the server to http://localhost:6820
		NewClient: func(config *slurmclient.Config, opts ...slurmclient.ClientOption) (slurmclient.Client, error) {
			mock := &utils.SlurmClient{}
			mock.SetToken(config.AuthToken)
			mock.SetServer(config.Server)
			return mock, nil
		},
	}

	res, err := reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: controllerKey})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Client should have been recreated with new URL
	retrievedClient := cMap.Get(controllerKey)
	mockClientRetrieved, ok := retrievedClient.(*utils.SlurmClient)
	if !ok {
		t.Fatalf("Expected mock client, got %T", retrievedClient)
	}

	expectedServer := "http://localhost:6820"
	if mockClientRetrieved.GetServer() != expectedServer {
		t.Errorf("GetServer() = %q, want %q", mockClientRetrieved.GetServer(), expectedServer)
	}

	if res.RequeueAfter != 12*time.Minute {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, 12*time.Minute)
	}
}

func TestSlurmClientReconciler_Reconcile_MissingRestApi(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = slinkyv1beta1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}

	controllerObj := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
	}

	// No RestApi object in fake client
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerObj).Build()
	cMap := clientmap.NewClientMap()

	// Pre-populate ClientMap
	mockClient := &utils.SlurmClient{}
	cMap.Add(context.Background(), controllerKey, mockClient)

	reconciler := &SlurmClientReconciler{
		Client:    fakeClient,
		Scheme:    scheme,
		ClientMap: cMap,
		NewClient: func(config *slurmclient.Config, opts ...slurmclient.ClientOption) (slurmclient.Client, error) {
			return &utils.SlurmClient{}, nil
		},
	}

	res, err := reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: controllerKey})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Client should be removed
	if cMap.Has(controllerKey) {
		t.Errorf("cMap.Has(%v) = true, want false", controllerKey)
	}

	if res.RequeueAfter != 0 {
		t.Errorf("RequeueAfter = %v, want 0", res.RequeueAfter)
	}

	if status := testutil.ToFloat64(metrics.SlurmClientStatus.WithLabelValues(testCtrlName)); status != 0.0 {
		t.Errorf("Expected SlurmClientStatus 0.0 when RestApi missing, got %f", status)
	}
}

// TestSlurmClientReconciler_Reconcile_SecretKeyRotation_ImmediateRegeneration
// verifies that when the Secret's JWT signing key is rotated, reconciliation immediately
// regenerates a new token and updates the ClientMap, bypassing the staleness check even if the
// existing token is unexpired.
func TestSlurmClientReconciler_Reconcile_SecretKeyRotation_ImmediateRegeneration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = slinkyv1beta1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}
	oldSigningKey := make([]byte, 128)
	_, _ = rand.Read(oldSigningKey)

	newSigningKey := make([]byte, 128)
	_, _ = rand.Read(newSigningKey)

	// 1. Setup Controller, RestApi, and Secret with the NEW signing key (simulating rotated secret)
	controllerObj := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.ControllerSpec{
			JwtKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: testCtrlName,
				},
				Key: slurmjwt.DefaultTokenKey,
			},
		},
	}

	secretObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Data: map[string][]byte{
			slurmjwt.DefaultTokenKey: newSigningKey,
		},
	}

	restapiObj := &slinkyv1beta1.RestApi{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.RestApiSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testCtrlName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerObj, secretObj, restapiObj).Build()
	cMap := clientmap.NewClientMap()

	// 2. Pre-populate ClientMap with a client holding a token signed by the OLD key (valid for 10 more minutes)
	oldToken, err := slurmjwt.NewToken(oldSigningKey).NewSignedToken()
	if err != nil {
		t.Fatalf("Failed to create old token: %v", err)
	}

	mockClient := &utils.SlurmClient{}
	mockClient.SetToken(oldToken)
	mockClient.SetServer("http://my-controller-restapi.default:6820")

	expiryTime := time.Now().Add(10 * time.Minute)
	cMap.AddWithExpiration(context.Background(), controllerKey, mockClient, expiryTime)

	reconciler := &SlurmClientReconciler{
		Client:    fakeClient,
		Scheme:    scheme,
		ClientMap: cMap,
		NewClient: func(config *slurmclient.Config, opts ...slurmclient.ClientOption) (slurmclient.Client, error) {
			mock := &utils.SlurmClient{}
			mock.SetToken(config.AuthToken)
			mock.SetServer(config.Server)
			return mock, nil
		},
	}

	// 3. Trigger reconciliation
	res, err := reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: controllerKey})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// 4. Verify that the token was regenerated despite being unexpired
	retrievedClient := cMap.Get(controllerKey)
	mockClientRetrieved, ok := retrievedClient.(*utils.SlurmClient)
	if !ok {
		t.Fatalf("Expected mock client, got %T", retrievedClient)
	}

	if mockClientRetrieved.GetToken() == oldToken {
		t.Errorf("GetToken() = %q, want regenerated token != %q", mockClientRetrieved.GetToken(), oldToken)
	}

	// 5. Verify the new token validates against the new signing key
	valid, err := slurmjwt.VerifyToken(mockClientRetrieved.GetToken(), newSigningKey)
	if err != nil || !valid {
		t.Errorf("VerifyToken() = (%v, %v), want (true, nil)", valid, err)
	}

	if res.RequeueAfter != 12*time.Minute {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, 12*time.Minute)
	}
}

// TestSlurmClientReconciler_Reconcile_SecretKeyUnchanged_TokenFresh_SkipsRegeneration
// verifies that when reconciliation is triggered but the Secret signing key is unchanged and the
// active token is fresh, the reconciler skips token regeneration and retains the existing client.
func TestSlurmClientReconciler_Reconcile_SecretKeyUnchanged_TokenFresh_SkipsRegeneration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = slinkyv1beta1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}
	signingKey := make([]byte, 128)
	_, _ = rand.Read(signingKey)

	// 1. Setup Controller, RestApi, and Secret with the current signing key
	controllerObj := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.ControllerSpec{
			JwtKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: testCtrlName,
				},
				Key: slurmjwt.DefaultTokenKey,
			},
		},
	}

	secretObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Data: map[string][]byte{
			slurmjwt.DefaultTokenKey: signingKey,
		},
	}

	restapiObj := &slinkyv1beta1.RestApi{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.RestApiSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testCtrlName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerObj, secretObj, restapiObj).Build()
	cMap := clientmap.NewClientMap()

	// 2. Pre-populate ClientMap with a fresh token signed by the SAME key (10 mins left)
	freshToken, err := slurmjwt.NewToken(signingKey).NewSignedToken()
	if err != nil {
		t.Fatalf("Failed to create fresh token: %v", err)
	}

	mockClient := &utils.SlurmClient{}
	mockClient.SetToken(freshToken)
	mockClient.SetServer("http://my-controller-restapi.default:6820")

	expiryTime := time.Now().Add(10 * time.Minute)
	cMap.AddWithExpiration(context.Background(), controllerKey, mockClient, expiryTime)

	reconciler := &SlurmClientReconciler{
		Client:    fakeClient,
		Scheme:    scheme,
		ClientMap: cMap,
		NewClient: func(config *slurmclient.Config, opts ...slurmclient.ClientOption) (slurmclient.Client, error) {
			mock := &utils.SlurmClient{}
			mock.SetToken(config.AuthToken)
			mock.SetServer(config.Server)
			return mock, nil
		},
	}

	// 3. Trigger reconciliation
	res, err := reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: controllerKey})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// 4. Verify that the token remained unchanged
	retrievedClient := cMap.Get(controllerKey)
	mockClientRetrieved, ok := retrievedClient.(*utils.SlurmClient)
	if !ok {
		t.Fatalf("Expected mock client, got %T", retrievedClient)
	}

	if mockClientRetrieved.GetToken() != freshToken {
		t.Errorf("GetToken() = %q, want %q", mockClientRetrieved.GetToken(), freshToken)
	}

	// 5. Verify requeue interval matches the remaining token validity minus staleness buffer (~7m)
	expectedRequeueLowerBound := 6 * time.Minute
	expectedRequeueUpperBound := 8 * time.Minute
	if res.RequeueAfter < expectedRequeueLowerBound || res.RequeueAfter > expectedRequeueUpperBound {
		t.Errorf("RequeueAfter = %v, want between %v and %v", res.RequeueAfter, expectedRequeueLowerBound, expectedRequeueUpperBound)
	}
}

// TestSlurmClientReconciler_Reconcile_InvalidToken_ImmediateRegeneration
// verifies that when an active token is invalid/malformed, reconciliation forces token regeneration
// even if metadata indicates the token is fresh.
func TestSlurmClientReconciler_Reconcile_InvalidToken_ImmediateRegeneration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = slinkyv1beta1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	controllerKey := types.NamespacedName{Namespace: testNamespace, Name: testCtrlName}
	signingKey := make([]byte, 128)
	_, _ = rand.Read(signingKey)

	controllerObj := &slinkyv1beta1.Controller{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.ControllerSpec{
			JwtKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: testCtrlName,
				},
				Key: slurmjwt.DefaultTokenKey,
			},
		},
	}

	secretObj := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Data: map[string][]byte{
			slurmjwt.DefaultTokenKey: signingKey,
		},
	}

	restapiObj := &slinkyv1beta1.RestApi{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCtrlName,
			Namespace: testNamespace,
		},
		Spec: slinkyv1beta1.RestApiSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testCtrlName,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(controllerObj, secretObj, restapiObj).Build()
	cMap := clientmap.NewClientMap()

	malformedToken := "this-is-a-malformed-token"
	mockClient := &utils.SlurmClient{}
	mockClient.SetToken(malformedToken)
	mockClient.SetServer("http://my-controller-restapi.default:6820")

	expiryTime := time.Now().Add(10 * time.Minute)
	cMap.AddWithExpiration(context.Background(), controllerKey, mockClient, expiryTime)

	reconciler := &SlurmClientReconciler{
		Client:    fakeClient,
		Scheme:    scheme,
		ClientMap: cMap,
		NewClient: func(config *slurmclient.Config, opts ...slurmclient.ClientOption) (slurmclient.Client, error) {
			mock := &utils.SlurmClient{}
			mock.SetToken(config.AuthToken)
			mock.SetServer(config.Server)
			return mock, nil
		},
	}

	res, err := reconciler.Reconcile(context.Background(), reconcile.Request{NamespacedName: controllerKey})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	retrievedClient := cMap.Get(controllerKey)
	mockClientRetrieved, ok := retrievedClient.(*utils.SlurmClient)
	if !ok {
		t.Fatalf("Expected mock client, got %T", retrievedClient)
	}

	if mockClientRetrieved.GetToken() == malformedToken {
		t.Errorf("GetToken() = %q, want regenerated token != %q", mockClientRetrieved.GetToken(), malformedToken)
	}

	valid, err := slurmjwt.VerifyToken(mockClientRetrieved.GetToken(), signingKey)
	if err != nil || !valid {
		t.Errorf("VerifyToken() = (%v, %v), want (true, nil)", valid, err)
	}

	if res.RequeueAfter != 12*time.Minute {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, 12*time.Minute)
	}
}

func TestSlurmClient_HTTPClientTimeout_AbortsHungServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	cli, err := slurmclient.NewClient(&slurmclient.Config{
		Server:    srv.URL,
		AuthToken: "test-token",
		HTTPClient: &http.Client{
			Timeout: 50 * time.Millisecond,
		},
	}, &slurmclient.ClientOptions{
		DisableFor: []slurmobject.Object{
			&slurmtypes.V0044JobInfo{},
		},
	})
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	var jobInfo slurmtypes.V0044JobInfo
	err = cli.Get(context.Background(), slurmobject.ObjectKey("12345"), &jobInfo)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Expected Get to fail with context.DeadlineExceeded when server hangs beyond HTTPClient.Timeout, got: %v", err)
	}
}
