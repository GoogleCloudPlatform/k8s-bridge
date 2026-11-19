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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkloadMixing_IsAvailable(t *testing.T) {
	now := metav1.NewTime(time.Now())

	tests := []struct {
		name string
		wm   *WorkloadMixing
		want bool
	}{
		{
			name: "nil receiver returns false",
			wm:   nil,
			want: false,
		},
		{
			name: "empty conditions returns false",
			wm: &WorkloadMixing{
				ObjectMeta: metav1.ObjectMeta{Generation: 1},
			},
			want: false,
		},
		{
			name: "Available=False returns false",
			wm: &WorkloadMixing{
				ObjectMeta: metav1.ObjectMeta{Generation: 1},
				Status: WorkloadMixingStatus{
					Conditions: []metav1.Condition{
						{
							Type:               ConditionTypeAvailable,
							Status:             metav1.ConditionFalse,
							ObservedGeneration: 1,
						},
					},
				},
			},
			want: false,
		},
		{
			name: "Available=True with stale ObservedGeneration returns false",
			wm: &WorkloadMixing{
				ObjectMeta: metav1.ObjectMeta{Generation: 2},
				Status: WorkloadMixingStatus{
					Conditions: []metav1.Condition{
						{
							Type:               ConditionTypeAvailable,
							Status:             metav1.ConditionTrue,
							ObservedGeneration: 1,
						},
					},
				},
			},
			want: false,
		},
		{
			name: "terminating resource returns false even when Available=True",
			wm: &WorkloadMixing{
				ObjectMeta: metav1.ObjectMeta{
					Generation:        1,
					DeletionTimestamp: &now,
				},
				Status: WorkloadMixingStatus{
					Conditions: []metav1.Condition{
						{
							Type:               ConditionTypeAvailable,
							Status:             metav1.ConditionTrue,
							ObservedGeneration: 1,
						},
					},
				},
			},
			want: false,
		},
		{
			name: "Available=True with matching ObservedGeneration returns true",
			wm: &WorkloadMixing{
				ObjectMeta: metav1.ObjectMeta{Generation: 3},
				Status: WorkloadMixingStatus{
					Conditions: []metav1.Condition{
						{
							Type:               ConditionTypeAvailable,
							Status:             metav1.ConditionTrue,
							ObservedGeneration: 3,
						},
					},
				},
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.wm.IsAvailable(); got != tc.want {
				t.Errorf("IsAvailable() = %v, want %v", got, tc.want)
			}
		})
	}
}
