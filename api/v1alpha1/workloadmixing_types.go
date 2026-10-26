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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PartitionMapping defines configuration for a specific Slurm partition.
type PartitionMapping struct {
	// Partition is the name of the Slurm partition to observe/map.
	// +required
	Partition string `json:"partition"`

	// LocalQueue is the name of the Kueue LocalQueue to map this partition to.
	// +required
	LocalQueue string `json:"localQueue"`

	// WorkloadPriorityClass is the name of the Kueue WorkloadPriorityClass to map this partition to.
	// +optional
	WorkloadPriorityClass string `json:"workloadPriorityClass,omitempty"`

	// DynamicNodeFeature is the feature name configured for dynamic nodes in this partition (e.g. "dynamic_kpartition").
	// +kubebuilder:validation:MaxLength=64
	// +kubebuilder:validation:Pattern="^[a-zA-Z0-9_-]+$"
	// +optional
	DynamicNodeFeature string `json:"dynamicNodeFeature,omitempty"`
}

type GenericResourceMapping struct {
	// ResourceName is the Slurm GRES resource name (e.g., "gpu", "tpu").
	// +required
	ResourceName string `json:"resourceName"`

	// ResourceType is the Slurm GRES type/model (e.g., "l4", "a100", "v5lite").
	// Optional if the GRES doesn't specify a sub-type.
	// +optional
	ResourceType string `json:"resourceType,omitempty"`

	// K8sResourceName is the Kubernetes resource name (e.g., "nvidia.com/gpu", "google.com/tpu").
	// +required
	K8sResourceName string `json:"k8sResourceName"`

	// NodeSelectors is a map of key-value pairs that should be added to the Pod template's nodeSelector.
	// +optional
	NodeSelectors map[string]string `json:"nodeSelectors,omitempty"`
}

// WorkloadMixingSpec defines the desired state of WorkloadMixing
type WorkloadMixingSpec struct {
	// controllerRef is a reference to the Slurm controller / cluster.
	// +required
	ControllerRef corev1.LocalObjectReference `json:"controllerRef"`

	// PartitionsMapping is a list of partition configurations.
	// +optional
	PartitionsMapping []PartitionMapping `json:"partitionsMapping,omitempty"`

	// MappedResources is a list of generic resource mappings (GRES).
	// +optional
	MappedResources []GenericResourceMapping `json:"mappedResources,omitempty"`

	// PodTemplate is an optional template for the Pods that will be created.
	// +optional
	PodTemplate *corev1.PodTemplateSpec `json:"podTemplate,omitempty"`

	// ThreadsPerCore is the number of hardware threads per core on the nodes that run the
	// slurmd pods. Slurm allocates whole cores, so k8s-bridge rounds the CPU request of each
	// pod up to a multiple of this value. Set 1 for node pools without SMT.
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	// +optional
	ThreadsPerCore *int32 `json:"threadsPerCore,omitempty"`
}

// WorkloadMixingStatus defines the observed state of WorkloadMixing.
type WorkloadMixingStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the WorkloadMixing resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// WorkloadMixing is the Schema for the workloadmixings API
type WorkloadMixing struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of WorkloadMixing
	// +required
	Spec WorkloadMixingSpec `json:"spec"`

	// status defines the observed state of WorkloadMixing
	// +optional
	Status WorkloadMixingStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WorkloadMixingList contains a list of WorkloadMixing
type WorkloadMixingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []WorkloadMixing `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &WorkloadMixing{}, &WorkloadMixingList{})
		return nil
	})
}
