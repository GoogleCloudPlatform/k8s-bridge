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
	"testing"

	"k8s.io/utils/ptr"

	"github.com/GoogleCloudPlatform/k8s-bridge/internal/metrics"
	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
)

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
