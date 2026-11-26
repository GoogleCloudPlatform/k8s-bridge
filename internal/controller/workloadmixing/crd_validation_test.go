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
	"fmt"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	k8sbridgev1alpha1 "github.com/GoogleCloudPlatform/k8s-bridge/api/v1alpha1"
	"github.com/GoogleCloudPlatform/k8s-bridge/internal/controller/slurmjob"
)

const (
	testCRDControllerName = "slurm"
	testK8sGPUResource    = "nvidia.com/gpu"
	colTypeString         = "string"
)

func validWorkloadMixing(name string) *k8sbridgev1alpha1.WorkloadMixing {
	return &k8sbridgev1alpha1.WorkloadMixing{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: k8sbridgev1alpha1.WorkloadMixingSpec{
			ControllerRef: corev1.LocalObjectReference{
				Name: testCRDControllerName,
			},
			PartitionsMapping: []k8sbridgev1alpha1.PartitionMapping{
				{
					Partition:          "k8s-partition",
					LocalQueue:         "slurm-queue",
					DynamicNodeFeature: "k8s_dynamic",
				},
			},
		},
	}
}

func unstructuredWMFromYAML(rawYAML string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	ExpectWithOffset(1, yaml.Unmarshal([]byte(rawYAML), &obj.Object)).To(Succeed())
	obj.SetNamespace("default")
	return obj
}

var _ = Describe("WorkloadMixing CRD Validation", func() {
	DescribeTable("rejects invalid specs on Create with 422 Invalid",
		func(mutate func(*k8sbridgev1alpha1.WorkloadMixing), expectedErrSubstr string) {
			wm := validWorkloadMixing("invalid-wm")
			mutate(wm)

			err := k8sClient.Create(ctx, wm, client.DryRunAll)
			Expect(err).To(HaveOccurred())
			Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected 422 Invalid, got: %v", err)
			Expect(err.Error()).To(ContainSubstring(expectedErrSubstr))
		},
		Entry("empty controllerRef.name", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.ControllerRef.Name = ""
		}, "controllerRef"),
		Entry("duplicate partition in partitionsMapping", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.PartitionsMapping = []k8sbridgev1alpha1.PartitionMapping{
				{Partition: slurmjob.GRESGPU, LocalQueue: "q1"},
				{Partition: slurmjob.GRESGPU, LocalQueue: "q2"},
			}
		}, "partitionsMapping"),
		Entry("partition containing comma", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.PartitionsMapping[0].Partition = "part1,part2"
		}, "partition"),
		Entry("localQueue with invalid characters", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.PartitionsMapping[0].LocalQueue = "bad_queue"
		}, "localQueue"),
		Entry("workloadPriorityClass with invalid characters", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.PartitionsMapping[0].WorkloadPriorityClass = "bad_prio"
		}, "workloadPriorityClass"),
		Entry("dynamicNodeFeature starting with reserved k8s-id- prefix", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.PartitionsMapping[0].DynamicNodeFeature = slurmjob.UniqueFeaturePrefix + "123"
		}, slurmjob.UniqueFeaturePrefix),
		Entry("duplicate (resourceName, resourceType) pair", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.MappedResources = []k8sbridgev1alpha1.GenericResourceMapping{
				{ResourceName: slurmjob.GRESGPU, ResourceType: "l4", K8sResourceName: testK8sGPUResource},
				{ResourceName: slurmjob.GRESGPU, ResourceType: "l4", K8sResourceName: "example.com/gpu"},
			}
		}, "mappedResources"),
		Entry("mappedResources resourceName containing colon", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.MappedResources = []k8sbridgev1alpha1.GenericResourceMapping{
				{ResourceName: "gpu:l4", K8sResourceName: testK8sGPUResource},
			}
		}, "resourceName"),
		Entry("mappedResources resourceType containing colon", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.MappedResources = []k8sbridgev1alpha1.GenericResourceMapping{
				{ResourceName: slurmjob.GRESGPU, ResourceType: "l4:1", K8sResourceName: testK8sGPUResource},
			}
		}, "resourceType"),
		Entry("mappedResources resourceType containing comma", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.MappedResources = []k8sbridgev1alpha1.GenericResourceMapping{
				{ResourceName: slurmjob.GRESGPU, ResourceType: "l4,a100", K8sResourceName: testK8sGPUResource},
			}
		}, "resourceType"),
		Entry("unqualified built-in k8sResourceName", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.MappedResources = []k8sbridgev1alpha1.GenericResourceMapping{
				{ResourceName: slurmjob.GRESGPU, K8sResourceName: "cpu"},
			}
		}, "k8sResourceName"),
		Entry("reserved kubernetes.io/ domain in k8sResourceName", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.MappedResources = []k8sbridgev1alpha1.GenericResourceMapping{
				{ResourceName: slurmjob.GRESGPU, K8sResourceName: "sub.kubernetes.io/gpu"},
			}
		}, "k8sResourceName"),
		Entry("reserved requests. prefix in k8sResourceName", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.MappedResources = []k8sbridgev1alpha1.GenericResourceMapping{
				{ResourceName: slurmjob.GRESGPU, K8sResourceName: "requests.nvidia.com/gpu"},
			}
		}, "k8sResourceName"),
		Entry("podTemplate restartPolicy Always", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.PodTemplate = &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyAlways,
					Containers:    []corev1.Container{{Name: slurmjob.ContainerSlurmd, Image: "slurmd:latest"}},
				},
			}
		}, "restartPolicy"),
		Entry("podTemplate custom slurmd container with empty image", func(wm *k8sbridgev1alpha1.WorkloadMixing) {
			wm.Spec.PodTemplate = &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: slurmjob.ContainerSlurmd, Image: ""}},
				},
			}
		}, slurmjob.ContainerSlurmd),
	)

	It("rejects duplicate untyped mappedResources when one omits resourceType and one sets empty string", func() {
		obj := unstructuredWMFromYAML(`
apiVersion: k8s-bridge.x-k8s.io/v1alpha1
kind: WorkloadMixing
metadata:
  name: dup-untyped-gres
spec:
  controllerRef:
    name: slurm
  mappedResources:
    - resourceName: gpu
      k8sResourceName: nvidia.com/gpu
    - resourceName: gpu
      resourceType: ""
      k8sResourceName: nvidia.com/gpu
`)
		err := k8sClient.Create(ctx, obj, client.DryRunAll)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected 422 Invalid, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("mappedResources"))
	})

	It("accepts valid edge cases including explicit empty optionals, dotted RFC 1123 names, and optional podTemplate fields", func() {
		obj := unstructuredWMFromYAML(`
apiVersion: k8s-bridge.x-k8s.io/v1alpha1
kind: WorkloadMixing
metadata:
  name: explicit-empty-optionals
spec:
  controllerRef:
    name: slurm
  partitionsMapping:
    - partition: k8s-gpu
      localQueue: team.gpu-queue
      workloadPriorityClass: ""
  mappedResources:
    - resourceName: gpu
      resourceType: nvidia_l4
      k8sResourceName: nvidia.com/gpu
    - resourceName: gpu
      resourceType: ""
      k8sResourceName: nvidia.com/gpu
  podTemplate:
    metadata:
      labels:
        env: test
`)
		Expect(k8sClient.Create(ctx, obj, client.DryRunAll)).To(Succeed())

		wm := validWorkloadMixing("valid-edge-cases")
		wm.Spec.PartitionsMapping[0].LocalQueue = "team.gpu-q"
		wm.Spec.PartitionsMapping[0].WorkloadPriorityClass = "tier.high"
		wm.Spec.PodTemplate = &corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyOnFailure,
				Containers:    []corev1.Container{{Name: "sidecar", Image: "busybox:latest"}},
			},
		}
		Expect(k8sClient.Create(ctx, wm, client.DryRunAll)).To(Succeed())
	})

	It("enforces controllerRef immutability on Update while allowing other spec edits", func() {
		wm := validWorkloadMixing("immutable-controller-ref")
		Expect(k8sClient.Create(ctx, wm)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, wm)
		})

		// Editing mutable fields with controllerRef unchanged succeeds.
		wm.Spec.PartitionsMapping = append(wm.Spec.PartitionsMapping, k8sbridgev1alpha1.PartitionMapping{
			Partition:  "second-partition",
			LocalQueue: "slurm-queue",
		})
		Expect(k8sClient.Update(ctx, wm)).To(Succeed())

		// Changing controllerRef.name is rejected.
		wm.Spec.ControllerRef.Name = "other-slurm"
		err := k8sClient.Update(ctx, wm)
		Expect(err).To(HaveOccurred())
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "expected 422 Invalid, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("immutable"))
	})

	It("accepts every sample, example, and e2e testdata WorkloadMixing manifest", func() {
		repoRoot := filepath.Join("..", "..", "..")
		globs := []string{
			filepath.Join(repoRoot, "config", "samples", "*workloadmixing*.yaml"),
			filepath.Join(repoRoot, "docs", "examples", "*", "workloadmixing*.yaml"),
			filepath.Join(repoRoot, "test", "e2e", "testdata", "workloadmixing*.yaml"),
		}

		var manifestPaths []string
		for _, pattern := range globs {
			matches, err := filepath.Glob(pattern)
			Expect(err).NotTo(HaveOccurred())
			manifestPaths = append(manifestPaths, matches...)
		}
		Expect(manifestPaths).NotTo(BeEmpty())

		for _, path := range manifestPaths {
			raw, err := os.ReadFile(path)
			Expect(err).NotTo(HaveOccurred(), "reading %s", path)

			obj := unstructuredWMFromYAML(string(raw))
			Expect(k8sClient.Create(ctx, obj, client.DryRunAll)).To(Succeed(), "CRD admission failed for %s", path)
		}
	})

	It("exposes Controller, Available, Reason, wide Message, and Age printer columns on the CRD", func() {
		crd := &unstructured.Unstructured{}
		crd.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "apiextensions.k8s.io",
			Version: "v1",
			Kind:    "CustomResourceDefinition",
		})
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "workloadmixings.k8s-bridge.x-k8s.io"}, crd)).To(Succeed())

		versions, found, err := unstructured.NestedSlice(crd.Object, "spec", "versions")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(versions).To(HaveLen(1))

		versionMap, ok := versions[0].(map[string]any)
		Expect(ok).To(BeTrue())

		rawCols, found, err := unstructured.NestedSlice(versionMap, "additionalPrinterColumns")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue(), "expected additionalPrinterColumns on v1alpha1 CRD")

		type colSummary struct {
			name     string
			colType  string
			jsonPath string
			priority int64
		}
		got := make([]colSummary, 0, len(rawCols))
		for _, item := range rawCols {
			m, ok := item.(map[string]any)
			Expect(ok).To(BeTrue())
			prio, _, _ := unstructured.NestedInt64(m, "priority")
			got = append(got, colSummary{
				name:     fmt.Sprint(m["name"]),
				colType:  fmt.Sprint(m["type"]),
				jsonPath: fmt.Sprint(m["jsonPath"]),
				priority: prio,
			})
		}

		Expect(got).To(Equal([]colSummary{
			{name: "Controller", colType: colTypeString, jsonPath: ".spec.controllerRef.name", priority: 0},
			{name: "Available", colType: colTypeString, jsonPath: ".status.conditions[?(@.type=='Available')].status", priority: 0},
			{name: "Reason", colType: colTypeString, jsonPath: ".status.conditions[?(@.type=='Available')].reason", priority: 0},
			{name: "Message", colType: colTypeString, jsonPath: ".status.conditions[?(@.type=='Available')].message", priority: 1},
			{name: "Age", colType: "date", jsonPath: ".metadata.creationTimestamp", priority: 0},
		}))
	})
})
