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

package utils

import (
	"encoding/json"
	"fmt"
	"os/exec"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Workload is the subset of a Kueue Workload read by the e2e tests (no Kueue Go module dependency).
type Workload struct {
	metav1.ObjectMeta `json:"metadata"`
	Spec              WorkloadSpec `json:"spec"`
}

type WorkloadSpec struct {
	PodSets []WorkloadPodSet `json:"podSets"`
}

type WorkloadPodSet struct {
	TopologyRequest *WorkloadTopologyRequest `json:"topologyRequest,omitempty"`
}

// WorkloadTopologyRequest is the TAS request Kueue derives from podset-*-topology annotations.
type WorkloadTopologyRequest struct {
	Required *string `json:"required,omitempty"`
}

// WorkloadForJobSet returns the Kueue Workload owned by the named JobSet.
func WorkloadForJobSet(jobSetName, ns string) (*Workload, error) {
	out, err := Run(exec.Command("kubectl", "get", "workloads", "-n", ns, "-o", "json"))
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []Workload `json:"items"`
	}
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		return nil, fmt.Errorf("failed to parse Workloads in namespace %s: %w", ns, err)
	}
	for i := range list.Items {
		for _, ref := range list.Items[i].OwnerReferences {
			if ref.Kind == "JobSet" && ref.Name == jobSetName {
				return &list.Items[i], nil
			}
		}
	}
	return nil, fmt.Errorf("no Workload owned by JobSet %s in namespace %s", jobSetName, ns)
}
