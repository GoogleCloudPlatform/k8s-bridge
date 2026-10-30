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

package workloadmixing

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/controller/slurmjob"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
	"github.com/prometheus/client_golang/prometheus/testutil"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

var _ = Describe("WorkloadMixing Controller", func() {
	const (
		resourceName         = "test-resource"
		resourceNamespace    = "default"
		testSlurmClusterName = "test-slurm-cluster"
		controllerNoRestapi  = "controller-no-restapi"
		wmTestName           = "wm-test"
		controllerTestName   = "controller-test"
		keySlurm             = "slurm.key"
		keyJWT               = "jwt.key"
	)

	ctx := context.Background()

	typeNamespacedName := types.NamespacedName{
		Name:      resourceName,
		Namespace: resourceNamespace,
	}

	Context("When setting up the controller with manager", func() {
		It("should register the reconciler successfully", func() {
			mgr, err := ctrl.NewManager(cfg, ctrl.Options{
				Scheme: k8sClient.Scheme(),
			})
			Expect(err).NotTo(HaveOccurred())

			controllerReconciler := &WorkloadMixingReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			Expect(controllerReconciler.SetupWithManager(mgr)).To(Succeed())
		})
	})

	Context("When reconciling WorkloadMixing resources", func() {
		createCR := func(ctx context.Context) {
			controllerCR := &slinkyv1beta1.Controller{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testSlurmClusterName,
					Namespace: resourceNamespace,
				},
				Spec: slinkyv1beta1.ControllerSpec{
					SlurmKeyRef: corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: testSlurmClusterName,
						},
						Key: keySlurm,
					},
					JwtKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: testSlurmClusterName,
						},
						Key: keyJWT,
					},
				},
			}
			Expect(k8sClient.Create(ctx, controllerCR)).To(Succeed())

			restapiCR := &slinkyv1beta1.RestApi{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testSlurmClusterName + "-restapi",
					Namespace: resourceNamespace,
				},
				Spec: slinkyv1beta1.RestApiSpec{
					ControllerRef: corev1.LocalObjectReference{
						Name: testSlurmClusterName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, restapiCR)).To(Succeed())

			workloadmixing := &k8sbridgev1alpha1.WorkloadMixing{}
			err := k8sClient.Get(ctx, typeNamespacedName, workloadmixing)
			if err != nil && errors.IsNotFound(err) {
				resource := &k8sbridgev1alpha1.WorkloadMixing{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
						ControllerRef: corev1.LocalObjectReference{
							Name: testSlurmClusterName,
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		}

		deleteCR := func(ctx context.Context) {
			resource := &k8sbridgev1alpha1.WorkloadMixing{}
			if err := k8sClient.Get(ctx, typeNamespacedName, resource); err == nil {
				_ = k8sClient.Delete(ctx, resource)
			}

			controllerCR := &slinkyv1beta1.Controller{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceNamespace, Name: testSlurmClusterName}, controllerCR); err == nil {
				_ = k8sClient.Delete(ctx, controllerCR)
			}

			restapiCR := &slinkyv1beta1.RestApi{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceNamespace, Name: testSlurmClusterName + "-restapi"}, restapiCR); err == nil {
				_ = k8sClient.Delete(ctx, restapiCR)
			}
		}

		type testCase struct {
			setup          func(ctx context.Context)
			teardown       func(ctx context.Context)
			getClient      func() client.Client
			getRecorder    func() record.EventRecorder
			req            reconcile.Request
			expectErr      bool
			expectedResult reconcile.Result
			validate       func(ctx context.Context, rec record.EventRecorder)
		}

		DescribeTable("Reconcile scenarios",
			func(tc testCase) {
				if tc.setup != nil {
					tc.setup(ctx)
				}
				if tc.teardown != nil {
					defer tc.teardown(ctx)
				}

				var rec record.EventRecorder
				if tc.getRecorder != nil {
					rec = tc.getRecorder()
				}

				c := k8sClient
				if tc.getClient != nil {
					c = tc.getClient()
				}

				reconciler := &WorkloadMixingReconciler{
					Client:   c,
					Scheme:   k8sClient.Scheme(),
					Recorder: rec,
				}

				res, err := reconciler.Reconcile(ctx, tc.req)
				if tc.expectErr {
					Expect(err).To(HaveOccurred())
				} else {
					Expect(err).NotTo(HaveOccurred())
				}
				Expect(res).To(Equal(tc.expectedResult))

				if tc.validate != nil {
					tc.validate(ctx, rec)
				}
			},
			Entry("initial reconciliation setting status to Available and recording event", testCase{
				setup:    createCR,
				teardown: deleteCR,
				getRecorder: func() record.EventRecorder {
					return record.NewFakeRecorder(10)
				},
				req:            reconcile.Request{NamespacedName: typeNamespacedName},
				expectErr:      false,
				expectedResult: reconcile.Result{},
				validate: func(ctx context.Context, rec record.EventRecorder) {
					reconciled := &k8sbridgev1alpha1.WorkloadMixing{}
					Expect(k8sClient.Get(ctx, typeNamespacedName, reconciled)).To(Succeed())
					Expect(reconciled.Status.Conditions).NotTo(BeEmpty())
					Expect(reconciled.Status.Conditions[0].Type).To(Equal("Available"))
					Expect(reconciled.Status.Conditions[0].Status).To(Equal(metav1.ConditionTrue))

					fakeRec := rec.(*record.FakeRecorder)
					Eventually(fakeRec.Events).Should(Receive(ContainSubstring("WorkloadMixing successfully reconciled")))
				},
			}),
			Entry("podTemplate problems send Warning Events once for each spec generation", testCase{
				setup: func(ctx context.Context) {
					createCR(ctx)
					wm := &k8sbridgev1alpha1.WorkloadMixing{}
					Expect(k8sClient.Get(ctx, typeNamespacedName, wm)).To(Succeed())
					wm.Spec.PodTemplate = &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
						{
							Name:  slurmjob.ContainerSlurmd,
							Image: "slurmd",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
								Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("4")},
							},
						},
						{Name: "sidecar", Image: "sidecar"},
					}}}
					Expect(k8sClient.Update(ctx, wm)).To(Succeed())
				},
				teardown:       deleteCR,
				getRecorder:    func() record.EventRecorder { return record.NewFakeRecorder(10) },
				req:            reconcile.Request{NamespacedName: typeNamespacedName},
				expectedResult: reconcile.Result{},
				validate: func(ctx context.Context, rec record.EventRecorder) {
					fakeRec := rec.(*record.FakeRecorder)
					Expect(fakeRec.Events).To(Receive(Equal("Warning MaxNodeSizeNotConfigured Maximum node size not configured: " +
						"the slurmd container in spec.podTemplate has no limit for memory")))
					Expect(fakeRec.Events).To(Receive(ContainSubstring("SlurmdRequestsIgnored")))
					Expect(fakeRec.Events).To(Receive(Equal("Warning ContainersWithoutLimits Containers in spec.podTemplate have no cpu or memory limit: " +
						"sidecar. Without pod-level resources, the pods have no cgroup limit")))
					Expect(fakeRec.Events).To(Receive(ContainSubstring("WorkloadMixing successfully reconciled")))

					r := &WorkloadMixingReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Recorder: rec}
					_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
					Expect(err).NotTo(HaveOccurred())
					Expect(fakeRec.Events).NotTo(Receive())
				},
			}),
			Entry("idempotent reconciliation when conditions are already set", testCase{
				setup: func(ctx context.Context) {
					createCR(ctx)
					r := &WorkloadMixingReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
					_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
				},
				teardown:       deleteCR,
				getRecorder:    func() record.EventRecorder { return record.NewFakeRecorder(10) },
				req:            reconcile.Request{NamespacedName: typeNamespacedName},
				expectErr:      false,
				expectedResult: reconcile.Result{},
			}),
			Entry("resource not found (NotFound)", testCase{
				getRecorder:    func() record.EventRecorder { return record.NewFakeRecorder(10) },
				req:            reconcile.Request{NamespacedName: types.NamespacedName{Name: "non-existent-resource", Namespace: resourceNamespace}},
				expectErr:      false,
				expectedResult: reconcile.Result{},
			}),
			Entry("reconciliation without event recorder (nil recorder)", testCase{
				setup:          createCR,
				teardown:       deleteCR,
				req:            reconcile.Request{NamespacedName: typeNamespacedName},
				expectErr:      false,
				expectedResult: reconcile.Result{},
			}),
			Entry("Get client call fails unexpectedly", testCase{
				getClient: func() client.Client {
					return fake.NewClientBuilder().WithScheme(k8sClient.Scheme()).WithInterceptorFuncs(interceptor.Funcs{
						Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
							return errors.NewInternalError(fmt.Errorf("fake internal error"))
						},
					}).Build()
				},
				getRecorder:    func() record.EventRecorder { return record.NewFakeRecorder(10) },
				req:            reconcile.Request{NamespacedName: typeNamespacedName},
				expectErr:      true,
				expectedResult: reconcile.Result{},
			}),
			Entry("Status update fails and emits warning event", testCase{
				getClient: func() client.Client {
					return fake.NewClientBuilder().WithScheme(k8sClient.Scheme()).WithObjects(&k8sbridgev1alpha1.WorkloadMixing{
						ObjectMeta: metav1.ObjectMeta{
							Name:      resourceName,
							Namespace: resourceNamespace,
						},
						Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
							ControllerRef: corev1.LocalObjectReference{
								Name: testSlurmClusterName,
							},
						},
					}).WithInterceptorFuncs(interceptor.Funcs{
						SubResourceUpdate: func(ctx context.Context, client client.Client, subResourceName string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
							if subResourceName == "status" {
								return errors.NewInternalError(fmt.Errorf("fake status update error"))
							}
							return client.SubResource(subResourceName).Update(ctx, obj, opts...)
						},
					}).Build()
				},
				getRecorder:    func() record.EventRecorder { return record.NewFakeRecorder(10) },
				req:            reconcile.Request{NamespacedName: typeNamespacedName},
				expectErr:      true,
				expectedResult: reconcile.Result{},
				validate: func(ctx context.Context, rec record.EventRecorder) {
					fakeRec := rec.(*record.FakeRecorder)
					Eventually(fakeRec.Events).Should(Receive(ContainSubstring("Failed to update WorkloadMixing status")))
				},
			}),
			Entry("reconciliation when referenced Controller is missing sets Available to False with Reason ControllerNotFound", testCase{
				setup: func(ctx context.Context) {
					resource := &k8sbridgev1alpha1.WorkloadMixing{
						ObjectMeta: metav1.ObjectMeta{
							Name:      resourceName,
							Namespace: resourceNamespace,
						},
						Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
							ControllerRef: corev1.LocalObjectReference{
								Name: "missing-controller",
							},
						},
					}
					Expect(k8sClient.Create(ctx, resource)).To(Succeed())
				},
				teardown: func(ctx context.Context) {
					resource := &k8sbridgev1alpha1.WorkloadMixing{}
					if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceNamespace, Name: resourceName}, resource); err == nil {
						_ = k8sClient.Delete(ctx, resource)
					}
				},
				getRecorder:    func() record.EventRecorder { return record.NewFakeRecorder(10) },
				req:            reconcile.Request{NamespacedName: typeNamespacedName},
				expectErr:      false,
				expectedResult: reconcile.Result{},
				validate: func(ctx context.Context, rec record.EventRecorder) {
					reconciled := &k8sbridgev1alpha1.WorkloadMixing{}
					Expect(k8sClient.Get(ctx, typeNamespacedName, reconciled)).To(Succeed())
					Expect(reconciled.Status.Conditions).NotTo(BeEmpty())
					Expect(reconciled.Status.Conditions[0].Type).To(Equal("Available"))
					Expect(reconciled.Status.Conditions[0].Status).To(Equal(metav1.ConditionFalse))
					Expect(reconciled.Status.Conditions[0].Reason).To(Equal("ControllerNotFound"))
				},
			}),
			Entry("reconciliation when referenced Controller exists but RestApi is missing sets Available to False with Reason RestApiNotFound", testCase{
				setup: func(ctx context.Context) {
					controllerCR := &slinkyv1beta1.Controller{
						ObjectMeta: metav1.ObjectMeta{
							Name:      controllerNoRestapi,
							Namespace: resourceNamespace,
						},
						Spec: slinkyv1beta1.ControllerSpec{
							SlurmKeyRef: corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: "test-secret",
								},
								Key: keySlurm,
							},
							JwtKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: "test-secret",
								},
								Key: keyJWT,
							},
						},
					}
					Expect(k8sClient.Create(ctx, controllerCR)).To(Succeed())

					resource := &k8sbridgev1alpha1.WorkloadMixing{
						ObjectMeta: metav1.ObjectMeta{
							Name:      resourceName,
							Namespace: resourceNamespace,
						},
						Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
							ControllerRef: corev1.LocalObjectReference{
								Name: controllerNoRestapi,
							},
						},
					}
					Expect(k8sClient.Create(ctx, resource)).To(Succeed())
				},
				teardown: func(ctx context.Context) {
					resource := &k8sbridgev1alpha1.WorkloadMixing{}
					if err := k8sClient.Get(ctx, typeNamespacedName, resource); err == nil {
						_ = k8sClient.Delete(ctx, resource)
					}
					controllerCR := &slinkyv1beta1.Controller{}
					if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: resourceNamespace, Name: controllerNoRestapi}, controllerCR); err == nil {
						_ = k8sClient.Delete(ctx, controllerCR)
					}
				},
				getRecorder:    func() record.EventRecorder { return record.NewFakeRecorder(10) },
				req:            reconcile.Request{NamespacedName: typeNamespacedName},
				expectErr:      false,
				expectedResult: reconcile.Result{},
				validate: func(ctx context.Context, rec record.EventRecorder) {
					reconciled := &k8sbridgev1alpha1.WorkloadMixing{}
					Expect(k8sClient.Get(ctx, typeNamespacedName, reconciled)).To(Succeed())
					Expect(reconciled.Status.Conditions).NotTo(BeEmpty())
					Expect(reconciled.Status.Conditions[0].Type).To(Equal("Available"))
					Expect(reconciled.Status.Conditions[0].Status).To(Equal(metav1.ConditionFalse))
					Expect(reconciled.Status.Conditions[0].Reason).To(Equal("RestApiNotFound"))
				},
			}),
			Entry("reconciliation with single, multiple distinct, and omitted workloadPriorityClass partition mappings setting status to Available", testCase{
				setup: func(ctx context.Context) {
					controllerCR := &slinkyv1beta1.Controller{
						ObjectMeta: metav1.ObjectMeta{
							Name:      testSlurmClusterName,
							Namespace: resourceNamespace,
						},
						Spec: slinkyv1beta1.ControllerSpec{
							SlurmKeyRef: corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: testSlurmClusterName,
								},
								Key: keySlurm,
							},
							JwtKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{
									Name: testSlurmClusterName,
								},
								Key: keyJWT,
							},
						},
					}
					Expect(k8sClient.Create(ctx, controllerCR)).To(Succeed())

					restapiCR := &slinkyv1beta1.RestApi{
						ObjectMeta: metav1.ObjectMeta{
							Name:      testSlurmClusterName + "-restapi",
							Namespace: resourceNamespace,
						},
						Spec: slinkyv1beta1.RestApiSpec{
							ControllerRef: corev1.LocalObjectReference{
								Name: testSlurmClusterName,
							},
						},
					}
					Expect(k8sClient.Create(ctx, restapiCR)).To(Succeed())

					resource := &k8sbridgev1alpha1.WorkloadMixing{
						ObjectMeta: metav1.ObjectMeta{
							Name:      resourceName,
							Namespace: resourceNamespace,
						},
						Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
							ControllerRef: corev1.LocalObjectReference{
								Name: testSlurmClusterName,
							},
							PartitionsMapping: []k8sbridgev1alpha1.PartitionMapping{
								{
									Partition:             "high-prio",
									LocalQueue:            resourceNamespace,
									WorkloadPriorityClass: "prod-high",
								},
								{
									Partition:             "low-prio",
									LocalQueue:            resourceNamespace,
									WorkloadPriorityClass: "batch-low",
								},
								{
									Partition:  "default-prio",
									LocalQueue: resourceNamespace,
								},
							},
						},
					}
					Expect(k8sClient.Create(ctx, resource)).To(Succeed())
				},
				teardown: deleteCR,
				getRecorder: func() record.EventRecorder {
					return record.NewFakeRecorder(10)
				},
				req:            reconcile.Request{NamespacedName: typeNamespacedName},
				expectErr:      false,
				expectedResult: reconcile.Result{},
				validate: func(ctx context.Context, rec record.EventRecorder) {
					reconciled := &k8sbridgev1alpha1.WorkloadMixing{}
					Expect(k8sClient.Get(ctx, typeNamespacedName, reconciled)).To(Succeed())
					Expect(reconciled.Status.Conditions).NotTo(BeEmpty())
					Expect(reconciled.Status.Conditions[0].Type).To(Equal("Available"))
					Expect(reconciled.Status.Conditions[0].Status).To(Equal(metav1.ConditionTrue))
					Expect(reconciled.Spec.PartitionsMapping).To(HaveLen(3))
					Expect(reconciled.Spec.PartitionsMapping[0].WorkloadPriorityClass).To(Equal("prod-high"))
					Expect(reconciled.Spec.PartitionsMapping[1].WorkloadPriorityClass).To(Equal("batch-low"))
					Expect(reconciled.Spec.PartitionsMapping[2].WorkloadPriorityClass).To(BeEmpty())
				},
			}),
		)
	})

	Context("When mapping secondary resource events", func() {
		It("should map Controller events to WorkloadMixing reconcile requests", func() {
			wm := &k8sbridgev1alpha1.WorkloadMixing{
				ObjectMeta: metav1.ObjectMeta{
					Name:      wmTestName,
					Namespace: resourceNamespace,
				},
				Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
					ControllerRef: corev1.LocalObjectReference{
						Name: controllerTestName,
					},
				},
			}
			fakeClient := fake.NewClientBuilder().WithScheme(k8sClient.Scheme()).WithObjects(wm).Build()
			r := &WorkloadMixingReconciler{Client: fakeClient, Scheme: k8sClient.Scheme()}

			ctrlObj := &slinkyv1beta1.Controller{
				ObjectMeta: metav1.ObjectMeta{
					Name:      controllerTestName,
					Namespace: resourceNamespace,
				},
			}
			reqs := r.mapControllerToWorkloadMixing(ctx, ctrlObj)
			Expect(reqs).To(HaveLen(1))
			Expect(reqs[0].NamespacedName).To(Equal(types.NamespacedName{Namespace: resourceNamespace, Name: wmTestName}))
		})

		It("should map RestApi events to WorkloadMixing reconcile requests", func() {
			wm := &k8sbridgev1alpha1.WorkloadMixing{
				ObjectMeta: metav1.ObjectMeta{
					Name:      wmTestName,
					Namespace: resourceNamespace,
				},
				Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
					ControllerRef: corev1.LocalObjectReference{
						Name: controllerTestName,
					},
				},
			}
			fakeClient := fake.NewClientBuilder().WithScheme(k8sClient.Scheme()).WithObjects(wm).Build()
			r := &WorkloadMixingReconciler{Client: fakeClient, Scheme: k8sClient.Scheme()}

			restAPIObj := &slinkyv1beta1.RestApi{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "restapi-test",
					Namespace: resourceNamespace,
				},
				Spec: slinkyv1beta1.RestApiSpec{
					ControllerRef: corev1.LocalObjectReference{
						Name: controllerTestName,
					},
				},
			}
			reqs := r.mapRestAPIToWorkloadMixing(ctx, restAPIObj)
			Expect(reqs).To(HaveLen(1))
			Expect(reqs[0].NamespacedName).To(Equal(types.NamespacedName{Namespace: resourceNamespace, Name: wmTestName}))
		})
	})

	Context("When tracking inventory metrics", func() {
		const (
			wmMetrics1 = "wm-metrics-1"
			wmMetrics2 = "wm-metrics-2"
		)

		It("should update active workload mixings and tracked partitions gauges", func() {
			wm1 := &k8sbridgev1alpha1.WorkloadMixing{
				ObjectMeta: metav1.ObjectMeta{
					Name:      wmMetrics1,
					Namespace: resourceNamespace,
				},
				Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
					ControllerRef: corev1.LocalObjectReference{
						Name: testSlurmClusterName,
					},
					PartitionsMapping: []k8sbridgev1alpha1.PartitionMapping{
						{Partition: "p1", LocalQueue: "q1"},
						{Partition: "p2", LocalQueue: "q2"},
					},
				},
			}
			wm2 := &k8sbridgev1alpha1.WorkloadMixing{
				ObjectMeta: metav1.ObjectMeta{
					Name:      wmMetrics2,
					Namespace: resourceNamespace,
				},
				Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
					ControllerRef: corev1.LocalObjectReference{
						Name: testSlurmClusterName,
					},
					PartitionsMapping: []k8sbridgev1alpha1.PartitionMapping{
						{Partition: "p3", LocalQueue: "q3"},
					},
				},
			}

			js1 := &jobsetv1alpha2.JobSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "slurm-job-1",
					Namespace: resourceNamespace,
					OwnerReferences: []metav1.OwnerReference{
						{
							Kind: slurmjob.KindWorkloadMixing,
							Name: wmMetrics1,
						},
					},
					Labels: map[string]string{
						slurmjob.LabelSlurmJobID:     "1",
						slurmjob.LabelSlurmPartition: "p1",
					},
				},
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(k8sClient.Scheme()).
				WithStatusSubresource(&k8sbridgev1alpha1.WorkloadMixing{}).
				WithObjects(wm1, wm2, js1).
				Build()
			r := &WorkloadMixingReconciler{Client: fakeClient, Scheme: k8sClient.Scheme()}

			// Reconcile wm1
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: resourceNamespace, Name: wmMetrics1}})
			Expect(err).NotTo(HaveOccurred())

			Expect(testutil.ToFloat64(metrics.TrackedPartitions.WithLabelValues(wmMetrics1))).To(Equal(2.0))
			Expect(testutil.ToFloat64(metrics.ActiveWorkloadMixings)).To(Equal(2.0))
			Expect(testutil.ToFloat64(metrics.ActiveJobs.WithLabelValues(wmMetrics1, "p1"))).To(Equal(1.0))
			Expect(testutil.ToFloat64(metrics.ActiveJobs.WithLabelValues(wmMetrics1, "p2"))).To(Equal(0.0))

			// Reconcile wm2
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: resourceNamespace, Name: wmMetrics2}})
			Expect(err).NotTo(HaveOccurred())

			Expect(testutil.ToFloat64(metrics.TrackedPartitions.WithLabelValues(wmMetrics2))).To(Equal(1.0))
			Expect(testutil.ToFloat64(metrics.ActiveWorkloadMixings)).To(Equal(2.0))

			// Delete wm1
			Expect(fakeClient.Delete(ctx, wm1)).To(Succeed())

			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: resourceNamespace, Name: wmMetrics1}})
			Expect(err).NotTo(HaveOccurred())

			Expect(testutil.ToFloat64(metrics.ActiveWorkloadMixings)).To(Equal(1.0))
			Expect(metrics.TrackedPartitions.DeleteLabelValues(wmMetrics1)).To(BeFalse())
			Expect(testutil.ToFloat64(metrics.TrackedPartitions.WithLabelValues(wmMetrics2))).To(Equal(1.0))
		})
	})
})
