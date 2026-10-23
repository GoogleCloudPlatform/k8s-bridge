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
	"errors"
	"fmt"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	slurmobject "github.com/SlinkyProject/slurm-client/pkg/object"
	slurmtypes "github.com/SlinkyProject/slurm-client/pkg/types"
	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"

	"github.com/GoogleCloudPlatform/k8s-bridge/internal/clientmap"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/controller/slurmclientsync/slurmjwt"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/utils/refresolver"
)

const (
	ControllerName = "slurmclient-controller"
	// TODO: Resolve the slurmrestd service port from RestApi.Spec.Service.Port instead of always using the default port.
	SlurmrestdPort           = 6820
	StalenessBuffer          = 3 * time.Minute
	DefaultHTTPClientTimeout = 30 * time.Second
)

// ErrRestApiNotFound is returned when no RestApi resources match the Controller.
var ErrRestApiNotFound = errors.New("RestApi not found")

type SlurmClientReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	ClientMap *clientmap.ClientMap
	Debug     bool

	// NewClient defines the factory function to build new Slurm clients
	NewClient func(config *slurmclient.Config, opts ...slurmclient.ClientOption) (slurmclient.Client, error)
}

// +kubebuilder:rbac:groups=slinky.slurm.net,resources=controllers,verbs=get;list;watch
// +kubebuilder:rbac:groups=slinky.slurm.net,resources=restapis,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile reconciles the SlurmClient.
func (r *SlurmClientReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.V(1).Info("Reconciling SlurmClient")

	controllerCR := &slinkyv1beta1.Controller{}
	if err := r.Get(ctx, req.NamespacedName, controllerCR); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Controller deleted, removing from ClientMap")
			r.ClientMap.Remove(req.NamespacedName)
			metrics.DeleteSlurmClientMetrics(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	server, err := r.getRestApiServer(ctx, controllerCR)
	if err != nil {
		if apierrors.IsNotFound(err) || errors.Is(err, ErrRestApiNotFound) {
			if r.ClientMap.Has(req.NamespacedName) {
				logger.Info("RestApi not found for Controller, removing from ClientMap")
				r.ClientMap.Remove(req.NamespacedName)
			}
			metrics.SetSlurmClientStatus(req.Name, false)
			return ctrl.Result{}, nil
		}
		metrics.SetSlurmClientStatus(req.Name, false)
		return ctrl.Result{}, err
	}

	// Check if we already have an active client for this controller
	existingClient, expiration := r.ClientMap.GetClientAndExpiration(req.NamespacedName)

	// Staleness & Config Check
	// 1. Do we have a client?
	// 2. Is the token still fresh?
	// 3. Is the client pointing to the CORRECT server?
	// 4. Is the token signature still valid against the current Secret key?
	isTokenFresh := !expiration.IsZero() && time.Now().Add(StalenessBuffer).Before(expiration)
	isServerCorrect := existingClient != nil && existingClient.GetServer() == server

	isTokenValid := r.verifyExistingClientToken(ctx, controllerCR, existingClient, isTokenFresh && isServerCorrect)

	if isTokenFresh && isServerCorrect && isTokenValid {
		logger.V(1).Info("Active token is fresh and server is unchanged, skipping regeneration", "expiration", expiration)
		metrics.SetSlurmClientStatus(req.Name, true)
		// Requeue for the remaining duration before expiration minus staleness buffer
		requeueTime := time.Until(expiration) - StalenessBuffer
		return ctrl.Result{RequeueAfter: requeueTime}, nil
	}

	if existingClient != nil && (!isServerCorrect || !isTokenValid) {
		logger.Info("Server URL changed or token invalid, removing old client",
			"serverChanged", !isServerCorrect, "tokenValid", isTokenValid)
		r.ClientMap.Remove(req.NamespacedName)
	}

	// Token is stale or client doesn't exist, sign new token
	token, expiresAt, refresh, err := slurmjwt.GenerateToken(ctx, r.Client, controllerCR.Namespace, controllerCR.Name)
	if err != nil {
		metrics.SetSlurmClientStatus(req.Name, false)
		return ctrl.Result{}, err
	}
	if token == "" {
		// Missing secret or token data
		logger.Info("Slurm JWT signing key is empty or missing, requeuing", "requeueAfter", 10*time.Second)
		metrics.SetSlurmClientStatus(req.Name, false)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	config := &slurmclient.Config{
		Server:    server,
		AuthToken: token,
		HTTPClient: &http.Client{
			Timeout: DefaultHTTPClientTimeout,
		},
	}
	// JobInfo and Node are listed in DisableFor as we don't want to cache queries for jobs or dynamic nodes
	options := &slurmclient.ClientOptions{
		DisableFor: []slurmobject.Object{
			&slurmtypes.V0041ControllerPing{},
			&slurmtypes.V0044JobInfo{},
			&slurmtypes.V0044Node{},
		},
	}
	newClient, err := r.NewClient(config, options)
	if err != nil {
		metrics.SetSlurmClientStatus(req.Name, false)
		return ctrl.Result{}, err
	}

	r.ClientMap.AddWithExpiration(ctx, req.NamespacedName, newClient, expiresAt)
	metrics.SetSlurmClientStatus(req.Name, true)
	logger.Info("Configured Slurm client", "server", server, "expiresAt", expiresAt, "refreshAfter", refresh)

	return ctrl.Result{RequeueAfter: refresh}, nil
}

func (r *SlurmClientReconciler) verifyExistingClientToken(
	ctx context.Context,
	controllerCR *slinkyv1beta1.Controller,
	existingClient slurmclient.Client,
	shouldVerify bool,
) bool {
	if !shouldVerify {
		return true
	}
	logger := log.FromContext(ctx)
	signingKey, err := slurmjwt.GetSigningKey(ctx, r.Client, controllerCR.Namespace, controllerCR.Name)
	if err != nil {
		logger.V(1).Info("Failed to read JWT signing key during token verification", "error", err)
		return true
	}
	if len(signingKey) == 0 {
		return true
	}
	valid, err := slurmjwt.VerifyToken(existingClient.GetToken(), signingKey)
	if err != nil || !valid {
		logger.Info("Token is invalid or verification failed, forcing regeneration")
		return false
	}
	return true
}

func (r *SlurmClientReconciler) getRestApiServer(ctx context.Context, controllerCR *slinkyv1beta1.Controller) (string, error) {
	logger := log.FromContext(ctx)
	resolver := refresolver.New(r.Client)
	restapiList, err := resolver.GetRestapisForController(ctx, controllerCR)
	if err != nil {
		return "", err
	}
	if len(restapiList.Items) == 0 {
		return "", ErrRestApiNotFound
	}

	server := fmt.Sprintf("http://%s:%d", restapiList.Items[0].ServiceFQDNShort(), SlurmrestdPort)
	if r.Debug {
		server = fmt.Sprintf("http://localhost:%d", SlurmrestdPort)
		logger.V(1).Info("Overriding RestApi URL with localhost", "server", server)
	}

	return server, nil
}

func (r *SlurmClientReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(ControllerName).
		For(&slinkyv1beta1.Controller{}).
		Watches(
			&slinkyv1beta1.RestApi{},
			handler.EnqueueRequestsFromMapFunc(r.mapRestApiToController),
		).
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.mapSecretToController),
		).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
		}).
		Complete(r)
}

func (r *SlurmClientReconciler) mapRestApiToController(ctx context.Context, obj client.Object) []reconcile.Request {
	restApi, ok := obj.(*slinkyv1beta1.RestApi)
	if !ok {
		return nil
	}
	if restApi.Spec.ControllerRef.Name == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Name:      restApi.Spec.ControllerRef.Name,
			Namespace: restApi.Namespace,
		},
	}}
}

func (r *SlurmClientReconciler) mapSecretToController(ctx context.Context, obj client.Object) []reconcile.Request {
	secret, ok := obj.(*corev1.Secret)
	if !ok {
		return nil
	}
	controllerList := &slinkyv1beta1.ControllerList{}
	if err := r.List(ctx, controllerList, client.InNamespace(secret.Namespace)); err != nil {
		log.FromContext(ctx).Error(err, "Failed to list Controllers", "namespace", secret.Namespace)
		return nil
	}
	var requests []reconcile.Request
	for _, controllerCR := range controllerList.Items {
		ref := controllerCR.AuthJwtRef()
		if ref.Name != "" && ref.Name == secret.Name {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      controllerCR.Name,
					Namespace: controllerCR.Namespace,
				},
			})
		}
	}
	return requests
}
