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

package slurmjob

import (
	"context"
	"errors"
	"slices"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
)

func TestIsSlurmControllerGone(t *testing.T) {
	key := types.NamespacedName{Namespace: testNamespace, Name: "slurm"}

	tests := []struct {
		name     string
		objs     []client.Object
		getErr   error
		wantGone bool
		wantErr  bool
	}{
		{
			name:     "Controller exists",
			objs:     []client.Object{&slinkyv1beta1.Controller{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}}},
			wantGone: false,
		},
		{
			name:     "Controller not found",
			wantGone: true,
		},
		{
			name:    "Get error other than NotFound",
			getErr:  errors.New("apiserver unavailable"),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = slinkyv1beta1.AddToScheme(scheme)
			c := interceptor.NewClient(fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.objs...).Build(), interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if tc.getErr != nil {
						return tc.getErr
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})

			gone, err := isSlurmControllerGone(t.Context(), c, key)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("isSlurmControllerGone() error = %v, wantErr %v", err, tc.wantErr)
			}
			if gone != tc.wantGone {
				t.Errorf("isSlurmControllerGone() = %v, want %v", gone, tc.wantGone)
			}
		})
	}
}

func TestRemoveJobSetFinalizer(t *testing.T) {
	const otherFinalizer = "orphan"

	tests := []struct {
		name string
		// stored is the JobSet in the API server; nil means it does not exist.
		stored *jobsetv1alpha2.JobSet
		// arg is the copy passed to removeJobSetFinalizer; it is stale when it differs from stored.
		arg            *jobsetv1alpha2.JobSet
		wantFinalizers []string
		wantPatches    int
	}{
		{
			name:           "Removes only FinalizerJobSet from an up-to-date JobSet",
			stored:         newTestJobSet(testJobSet101Name).withFinalizers(FinalizerJobSet, otherFinalizer).build(),
			wantFinalizers: []string{otherFinalizer},
			wantPatches:    1,
		},
		{
			name:           "Retries on conflict and keeps concurrently added finalizers",
			stored:         newTestJobSet(testJobSet101Name).withFinalizers(FinalizerJobSet, otherFinalizer).build(),
			arg:            newTestJobSet(testJobSet101Name).withFinalizers(FinalizerJobSet).build(),
			wantFinalizers: []string{otherFinalizer},
			wantPatches:    2,
		},
		{
			name:           "Concurrently finalized JobSet is not an error",
			stored:         newTestJobSet(testJobSet101Name).withFinalizers(otherFinalizer).build(),
			arg:            newTestJobSet(testJobSet101Name).withFinalizers(FinalizerJobSet, otherFinalizer).build(),
			wantFinalizers: []string{otherFinalizer},
			wantPatches:    1,
		},
		{
			name:           "JobSet without FinalizerJobSet is a no-op",
			stored:         newTestJobSet(testJobSet101Name).withFinalizers(otherFinalizer).build(),
			wantFinalizers: []string{otherFinalizer},
			wantPatches:    0,
		},
		{
			name:        "JobSet that no longer exists returns nil",
			arg:         newTestJobSet(testJobSet101Name).withFinalizers(FinalizerJobSet).build(),
			wantPatches: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			scheme := runtime.NewScheme()
			_ = jobsetv1alpha2.AddToScheme(scheme)

			builder := fake.NewClientBuilder().WithScheme(scheme)
			if tc.stored != nil {
				builder = builder.WithObjects(tc.stored)
			}
			patches := 0
			c := interceptor.NewClient(builder.Build(), interceptor.Funcs{
				Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					patches++
					return c.Patch(ctx, obj, patch, opts...)
				},
			})

			arg := tc.arg
			if tc.stored != nil {
				// Read the stored JobSet so its resourceVersion is known, then make arg stale if requested.
				current := &jobsetv1alpha2.JobSet{}
				if err := c.Get(ctx, client.ObjectKeyFromObject(tc.stored), current); err != nil {
					t.Fatalf("Get() failed: %v", err)
				}
				if arg == nil {
					arg = current
				} else {
					arg.ResourceVersion = "1"
				}
			}

			if err := removeJobSetFinalizer(ctx, c, arg); err != nil {
				t.Fatalf("removeJobSetFinalizer() failed: %v", err)
			}
			if patches != tc.wantPatches {
				t.Errorf("Patch calls = %d, want %d", patches, tc.wantPatches)
			}
			if tc.stored == nil {
				return
			}
			got := &jobsetv1alpha2.JobSet{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(tc.stored), got); err != nil {
				t.Fatalf("Get() failed: %v", err)
			}
			if !slices.Equal(got.Finalizers, tc.wantFinalizers) {
				t.Errorf("Finalizers = %v, want %v", got.Finalizers, tc.wantFinalizers)
			}
		})
	}
}

func TestIsTerminalSlurmJob(t *testing.T) {
	tests := []struct {
		name         string
		job          *slurmclienttypes.V0044JobInfo
		wantTerminal bool
		wantReason   string
	}{
		{
			name:         "Nil job",
			job:          nil,
			wantTerminal: false,
			wantReason:   "",
		},
		{
			name: "Completed",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateCOMPLETED}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobCompleted,
		},
		{
			name: "Cancelled",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateCANCELLED}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobCancelled,
		},
		{
			name: "Failed",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateFAILED}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobFailed,
		},
		{
			name: "Timeout",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateTIMEOUT}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobFailed,
		},
		{
			name: "NodeFail",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateNODEFAIL}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobFailed,
		},
		{
			name: "Preempted",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStatePREEMPTED}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobFailed,
		},
		{
			name: "BootFail",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateBOOTFAIL}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobFailed,
		},
		{
			name: "Deadline",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateDEADLINE}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobFailed,
		},
		{
			name: "OutOfMemory",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateOUTOFMEMORY}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobFailed,
		},
		{
			name: "LaunchFailed",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateLAUNCHFAILED}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobFailed,
		},
		{
			name: "ReconfigFail",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateRECONFIGFAIL}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobFailed,
		},
		{
			name: "Revoked",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateREVOKED}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobFailed,
		},
		{
			name: "SpecialExit",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateSPECIALEXIT}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobFailed,
		},
		{
			name: "Running (non-terminal)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateRUNNING}),
				},
			},
			wantTerminal: false,
			wantReason:   "",
		},
		{
			name: "Pending (non-terminal)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStatePENDING}),
				},
			},
			wantTerminal: false,
			wantReason:   "",
		},
		{
			name: "Configuring (non-terminal)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateCONFIGURING}),
				},
			},
			wantTerminal: false,
			wantReason:   "",
		},
		{
			name: "Suspended (non-terminal)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateSUSPENDED}),
				},
			},
			wantTerminal: false,
			wantReason:   "",
		},
		{
			name: "Completing (non-terminal)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateCOMPLETING}),
				},
			},
			wantTerminal: false,
			wantReason:   "",
		},
		{
			name: "Running and Completing (non-terminal)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{
						slurmapi.V0044JobInfoJobStateRUNNING,
						slurmapi.V0044JobInfoJobStateCOMPLETING,
					}),
				},
			},
			wantTerminal: false,
			wantReason:   "",
		},
		{
			name: "Completed and Completing (non-terminal)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{
						slurmapi.V0044JobInfoJobStateCOMPLETED,
						slurmapi.V0044JobInfoJobStateCOMPLETING,
					}),
				},
			},
			wantTerminal: false,
			wantReason:   "",
		},
		{
			name: "Cancelled and Completing (non-terminal)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{
						slurmapi.V0044JobInfoJobStateCANCELLED,
						slurmapi.V0044JobInfoJobStateCOMPLETING,
					}),
				},
			},
			wantTerminal: false,
			wantReason:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTerminal, gotReason := isTerminalSlurmJob(tt.job)
			if gotTerminal != tt.wantTerminal {
				t.Errorf("isTerminalSlurmJob() gotTerminal = %v, want %v", gotTerminal, tt.wantTerminal)
			}
			if gotReason != tt.wantReason {
				t.Errorf("isTerminalSlurmJob() gotReason = %v, want %v", gotReason, tt.wantReason)
			}
		})
	}
}

func TestClassifyTerminalSlurmState(t *testing.T) {
	tests := []struct {
		name         string
		job          *slurmclienttypes.V0044JobInfo
		wantTerminal bool
		wantReason   string
	}{
		{
			name:         "Nil job",
			job:          nil,
			wantTerminal: false,
			wantReason:   "",
		},
		{
			name: "Pending (non-terminal)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStatePENDING}),
				},
			},
			wantTerminal: false,
			wantReason:   "",
		},
		{
			name: "Running (non-terminal)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateRUNNING}),
				},
			},
			wantTerminal: false,
			wantReason:   "",
		},
		{
			name: "Completing only (non-terminal)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateCOMPLETING}),
				},
			},
			wantTerminal: false,
			wantReason:   "",
		},
		{
			name: "Running and Completing (non-terminal)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{
						slurmapi.V0044JobInfoJobStateRUNNING,
						slurmapi.V0044JobInfoJobStateCOMPLETING,
					}),
				},
			},
			wantTerminal: false,
			wantReason:   "",
		},
		{
			name: "Completed",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{slurmapi.V0044JobInfoJobStateCOMPLETED}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobCompleted,
		},
		{
			name: "Completed and Completing (terminal state)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{
						slurmapi.V0044JobInfoJobStateCOMPLETED,
						slurmapi.V0044JobInfoJobStateCOMPLETING,
					}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobCompleted,
		},
		{
			name: "Cancelled and Completing (terminal state)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{
						slurmapi.V0044JobInfoJobStateCANCELLED,
						slurmapi.V0044JobInfoJobStateCOMPLETING,
					}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobCancelled,
		},
		{
			name: "Failed and Completing (terminal state)",
			job: &slurmclienttypes.V0044JobInfo{
				V0044JobInfo: slurmapi.V0044JobInfo{
					JobState: ptr.To([]slurmapi.V0044JobInfoJobState{
						slurmapi.V0044JobInfoJobStateFAILED,
						slurmapi.V0044JobInfoJobStateCOMPLETING,
					}),
				},
			},
			wantTerminal: true,
			wantReason:   metrics.ReasonJobFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTerminal, gotReason := classifyTerminalSlurmState(tt.job)
			if gotTerminal != tt.wantTerminal {
				t.Errorf("classifyTerminalSlurmState() gotTerminal = %v, want %v", gotTerminal, tt.wantTerminal)
			}
			if gotReason != tt.wantReason {
				t.Errorf("classifyTerminalSlurmState() gotReason = %v, want %v", gotReason, tt.wantReason)
			}
			if gotHas := hasTerminalSlurmState(tt.job); gotHas != tt.wantTerminal {
				t.Errorf("hasTerminalSlurmState() = %v, want %v", gotHas, tt.wantTerminal)
			}
		})
	}
}
