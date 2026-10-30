// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package slurmjob

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"

	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
)

func TestTranslateSlurmJobToJobSet_Basic(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()
	controller := getTestController()

	t.Run("Success - Basic Translation", func(t *testing.T) {
		job := newTestJob().withNodes(2).build()

		obj, err := translator.Translate(job, wmSpec, controller)
		verifyJobSet(t, obj, err, "123", 2,
			withLabels(map[string]string{
				LabelSlurmJobID:         "123",
				LabelSlurmPartition:     testDefaultPartition,
				LabelKueueQueue:         testHighPrioQueue,
				LabelKueuePriorityClass: testHighPrioClass,
			}),
			withArgs([]string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, testPartitionHighPrio}),
			withSlurmVolume(),
		)
	})

	t.Run("Success - Omit Priority Class if not mapped", func(t *testing.T) {
		job := newTestJob().withPartition("low-prio").build()

		obj, err := translator.Translate(job, wmSpec, controller)
		verifyJobSet(t, obj, err, "123", 1,
			withLabels(map[string]string{
				LabelSlurmJobID:     "123",
				LabelSlurmPartition: "low-prio",
				LabelKueueQueue:     "low-prio-queue",
			}),
			withArgs([]string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, testPartitionLowPrio}),
		)
	})
}

func TestTranslateSlurmJobToJobSet_Filtering(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()
	controller := getTestController()

	t.Run("Skip - Unmapped Partition", func(t *testing.T) {
		job := newTestJob().withPartition("unmapped-part").build()
		obj, err := translator.Translate(job, wmSpec, controller)
		verifyTranslationSkip(t, obj, err)
	})

	t.Run("Skip - Multiple Partitions without Managed", func(t *testing.T) {
		job := newTestJob().withPartition("unmapped-1,unmapped-2").build()
		obj, err := translator.Translate(job, wmSpec, controller)
		verifyTranslationSkip(t, obj, err)
	})

	t.Run("Skip - Empty Partition", func(t *testing.T) {
		job := newTestJob().withPartition("").build()
		obj, err := translator.Translate(job, wmSpec, controller)
		verifyTranslationSkip(t, obj, err)
	})

	t.Run("Failure - Multiple Partitions with Managed", func(t *testing.T) {
		job := newTestJob().withPartition("high-prio,other-part").build()
		obj, err := translator.Translate(job, wmSpec, controller)
		verifyTranslationError(t, obj, err, "multiple partitions specified with managed partition")
	})
}

func TestTranslateSlurmJobToJobSet_Validation(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()
	controller := getTestController()

	tests := []struct {
		name    string
		job     *slurmclienttypes.V0044JobInfo
		wantErr string
	}{
		{
			name: "Failure - Unset NodeCount",
			job: newTestJob().
				withUnsetNodeCount().
				build(),
			wantErr: "job NodeCount is unset",
		},
		{
			name: "Failure - NodeCount Set to False",
			job: newTestJob().
				withNodes(2).
				withNodeCountSet(false).
				build(),
			wantErr: "job NodeCount is unset",
		},
		{
			name: "Failure - Infinite NodeCount",
			job: newTestJob().
				withInfiniteNodeCount().
				build(),
			wantErr: "infinite NodeCount is not supported",
		},
		{
			name: "Failure - Infinite NodeCount with Set",
			job: newTestJob().
				withInfiniteNodeCount().
				withNodeCountSet(true).
				build(),
			wantErr: "infinite NodeCount is not supported",
		},
		{
			name: "Failure - Zero Job ID",
			job: newTestJob().
				withJobID(0).
				build(),
			wantErr: "job ID is unset or 0",
		},
		{
			name: "Failure - Unset Job ID",
			job: newTestJob().
				withUnsetJobID().
				build(),
			wantErr: "job ID is unset or 0",
		},
		{
			name: "Failure - Zero NodeCount",
			job: newTestJob().
				withNodes(0).
				build(),
			wantErr: "invalid NodeCount: 0",
		},
		{
			name: "Failure - Negative NodeCount",
			job: newTestJob().
				withNodes(-1).
				build(),
			wantErr: "invalid NodeCount: -1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj, err := translator.Translate(tt.job, wmSpec, controller)
			verifyTranslationError(t, obj, err, tt.wantErr)
		})
	}
}

// TestTranslateSlurmJobToJobSet_Replicas verifies that replicas follow the requested node count
// (tres_req_str) rather than node_count, which slurmctld estimates for PENDING jobs.
// The NodeCount, Tasks, TasksPerNode, CpusPerTask and TresReqStr values below are what slurmrestd
// (Slurm 26.05) reported for pending jobs in an empty k8s-bridge partition while the largest node
// in the cluster had 8 CPUs.
func TestTranslateSlurmJobToJobSet_Replicas(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()
	controller := getTestController()

	// A job without task information gets 1 CPU, rounded up to 1 core (2 CPUs), and 2048MB per CPU.
	const singleTaskCPU, singleTaskMem = "2", "4096Mi"

	tests := []struct {
		name         string
		job          *slurmclienttypes.V0044JobInfo
		wantReplicas int32
		wantCPU      string
		wantMem      string
	}{
		{
			// Regression: replicas used to be 4 (8 CPUs each) while Slurm ran the job on 2 nodes.
			name: "Success - sbatch -N 2 -n 32 ignores estimated NodeCount 4",
			job: newTestJob().withNodes(4).withTasks(32).withCpusPerTask(1).
				withTresReqStr("cpu=32,node=2,billing=32").build(),
			wantReplicas: 2,
			wantCPU:      "16",      // CpusPerTask (1) * TasksPerNode (ceil(32/2) = 16)
			wantMem:      "32768Mi", // fallback 2048MB * 16 CPUs
		},
		{
			name: "Success - sbatch -N 2 -n 32 -c 2 ignores estimated NodeCount 8",
			job: newTestJob().withNodes(8).withTasks(32).withCpusPerTask(2).
				withTresReqStr("cpu=64,node=2,billing=64").build(),
			wantReplicas: 2,
			wantCPU:      "32",      // CpusPerTask (2) * TasksPerNode (ceil(32/2) = 16)
			wantMem:      "65536Mi", // fallback 2048MB * 32 CPUs
		},
		{
			name: "Success - sbatch -N 2-4 -n 32 uses minimum node count",
			job: newTestJob().withNodes(4).withTasks(32).withCpusPerTask(1).
				withTresReqStr("cpu=32,node=2,billing=32").build(),
			wantReplicas: 2,
			wantCPU:      "16",
			wantMem:      "32768Mi",
		},
		{
			name: "Success - sbatch -n 32 --ntasks-per-node=8 uses node count derived by Slurm",
			job: newTestJob().withNodes(4).withTasks(32).withTasksPerNode(8).withCpusPerTask(1).
				withTresReqStr("cpu=32,node=4,billing=32").build(),
			wantReplicas: 4,
			wantCPU:      "8",       // CpusPerTask (1) * TasksPerNode (8)
			wantMem:      "16384Mi", // fallback 2048MB * 8 CPUs
		},
		{
			name: "Success - sbatch -N 3 --ntasks-per-node=4",
			job: newTestJob().withNodes(3).withTasks(12).withTasksPerNode(4).withCpusPerTask(1).
				withTresReqStr("cpu=12,node=3,billing=12").build(),
			wantReplicas: 3,
			wantCPU:      "4",
			wantMem:      "8192Mi",
		},
		{
			name: "Success - sbatch -n 32 without -N uses a single node",
			job: newTestJob().withNodes(4).withTasks(32).withCpusPerTask(1).
				withTresReqStr("cpu=32,node=1,billing=32").build(),
			wantReplicas: 1,
			wantCPU:      "32",
			wantMem:      "65536Mi",
		},
		{
			name: "Success - Requested node count does not require NodeCount",
			job: newTestJob().withUnsetNodeCount().withTasks(4).
				withTresReqStr("cpu=4,node=2,billing=4").build(),
			wantReplicas: 2,
			wantCPU:      "2",
			wantMem:      "4096Mi",
		},
		{
			name:         "Success - Fallback to NodeCount when TresReqStr is unset",
			job:          newTestJob().withNodes(3).build(),
			wantReplicas: 3,
			wantCPU:      singleTaskCPU,
			wantMem:      singleTaskMem,
		},
		{
			name:         "Success - Fallback to NodeCount when TresReqStr has no node count",
			job:          newTestJob().withNodes(3).withTresReqStr("cpu=1,billing=1").build(),
			wantReplicas: 3,
			wantCPU:      singleTaskCPU,
			wantMem:      singleTaskMem,
		},
		{
			name:         "Success - Fallback to NodeCount when TresReqStr node count is invalid",
			job:          newTestJob().withNodes(3).withTresReqStr("cpu=1,node=0,billing=1").build(),
			wantReplicas: 3,
			wantCPU:      singleTaskCPU,
			wantMem:      singleTaskMem,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj, err := translator.Translate(tt.job, wmSpec, controller)
			verifyJobSet(t, obj, err, "123", tt.wantReplicas,
				withRequests(corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(tt.wantCPU),
					corev1.ResourceMemory: resource.MustParse(tt.wantMem),
				}),
			)
		})
	}
}

func TestTranslateSlurmJobToJobSet_Resources(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()
	controller := getTestController()

	t.Run("Success - Resource Calculation - CpusPerTask and Fallback Memory", func(t *testing.T) {
		job := newTestJob().
			withNodes(2).
			withTasks(4).
			withCpusPerTask(2).
			build()

		obj, err := translator.Translate(job, wmSpec, controller)

		// CPUs = CpusPerTask (2) * TasksPerNode (ceil(4/2) = 2) = 4
		wantCPU := resource.MustParse("4")
		// Memory = Max(fallback: 4 * 2048MB = 8192MB ~ 8192Mi) = 8192Mi
		wantMem := resource.MustParse("8192Mi")

		verifyJobSet(t, obj, err, "123", 2,
			withRequests(corev1.ResourceList{
				corev1.ResourceCPU:    wantCPU,
				corev1.ResourceMemory: wantMem,
			}),
		)
	})

	t.Run("Success - Resource Calculation - CpusPerTres (GPU) and MemoryPerNode", func(t *testing.T) {
		job := newTestJob().
			withCpusPerTres("gpu:4").
			withTresPerNode("gres/gpu=1").
			withMemoryPerNode(16384).
			build()

		obj, err := translator.Translate(job, wmSpec, controller)

		// CPUs = GPU_placeholder (1) * CpusPerTres (4) = 4
		wantCPU := resource.MustParse("4")
		// Memory = MemoryPerNode (16384MB) = 16384Mi
		wantMem := resource.MustParse("16384Mi")

		verifyJobSet(t, obj, err, "123", 1,
			withRequests(corev1.ResourceList{
				corev1.ResourceCPU:    wantCPU,
				corev1.ResourceMemory: wantMem,
			}),
		)
	})

	// slurmrestd reports cpus_per_task=1 for a job without -c. The TRES strings use the real record format.
	t.Run("Success - Resource Calculation - CpusPerTres (GPU) with reported CpusPerTask", func(t *testing.T) {
		const oneL4PerNode = "gres/gpu:l4:1"
		tests := []struct {
			name        string
			tasks       int32
			tresPerNode string
			cpusPerTres string
			wantCPU     resource.Quantity
			wantMem     resource.Quantity
			wantGPU     resource.Quantity
		}{
			{
				name:        "cpus per GPU above task CPUs",
				tasks:       1,
				tresPerNode: oneL4PerNode,
				cpusPerTres: "gres/gpu:4",
				wantCPU:     resource.MustParse("4"),      // max(1 task * 1, 1 GPU * 4)
				wantMem:     resource.MustParse("8192Mi"), // fallback 4 * 2048 MiB
				wantGPU:     resource.MustParse("1"),
			},
			{
				name:        "two GPUs per node",
				tasks:       1,
				tresPerNode: "gres/gpu:l4:2",
				cpusPerTres: "gres/gpu:4",
				wantCPU:     resource.MustParse("8"),       // max(1 task * 1, 2 GPUs * 4)
				wantMem:     resource.MustParse("16384Mi"), // fallback 8 * 2048 MiB
				wantGPU:     resource.MustParse("2"),
			},
			{
				name:        "task CPUs above cpus per GPU",
				tasks:       8,
				tresPerNode: oneL4PerNode,
				cpusPerTres: "gres/gpu:2",
				wantCPU:     resource.MustParse("8"),       // max(8 tasks * 1, 1 GPU * 2)
				wantMem:     resource.MustParse("16384Mi"), // fallback 8 * 2048 MiB
				wantGPU:     resource.MustParse("1"),
			},
			{
				name:        "odd cpus per GPU rounds up to whole cores",
				tasks:       1,
				tresPerNode: oneL4PerNode,
				cpusPerTres: "gres/gpu:3",
				wantCPU:     resource.MustParse("4"),      // max(1 task * 1, 1 GPU * 3) = 3, rounded up to 4
				wantMem:     resource.MustParse("8192Mi"), // fallback 4 * 2048 MiB
				wantGPU:     resource.MustParse("1"),
			},
			{
				name:        "no cpus per GPU falls back to task CPUs",
				tasks:       3,
				tresPerNode: oneL4PerNode,
				cpusPerTres: "",
				wantCPU:     resource.MustParse("4"),      // 3 tasks * 1 = 3, rounded up to 4
				wantMem:     resource.MustParse("8192Mi"), // fallback 4 * 2048 MiB
				wantGPU:     resource.MustParse("1"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				job := newTestJob().
					withTasks(tt.tasks).
					withCpusPerTask(1).
					withTresPerNode(tt.tresPerNode).
					withCpusPerTres(tt.cpusPerTres).
					build()

				obj, err := translator.Translate(job, wmSpec, controller)

				verifyJobSet(t, obj, err, "123", 1,
					withRequests(corev1.ResourceList{
						corev1.ResourceCPU:                       tt.wantCPU,
						corev1.ResourceMemory:                    tt.wantMem,
						corev1.ResourceName(testGpuResourceName): tt.wantGPU,
					}),
				)
			})
		}
	})

	t.Run("Success - Resource Calculation - Fallback CPU and MemoryPerCpu", func(t *testing.T) {
		job := newTestJob().
			withNodes(2).
			withTasks(2).
			withMemoryPerCpu(4096).
			build()

		obj, err := translator.Translate(job, wmSpec, controller)

		// CPUs = Fallback (1) * TasksPerNode (ceil(2/2) = 1) = 1, rounded up to 1 whole core = 2
		wantCPU := resource.MustParse("2")
		// Memory = MemoryPerCpu (4096MB) * CPUs (2) = 8192Mi
		wantMem := resource.MustParse("8192Mi")

		verifyJobSet(t, obj, err, "123", 2,
			withRequests(corev1.ResourceList{
				corev1.ResourceCPU:    wantCPU,
				corev1.ResourceMemory: wantMem,
			}),
		)
	})

	t.Run("Success - Resource Calculation - MemoryPerTres (GPU)", func(t *testing.T) {
		job := newTestJob().
			withMemoryPerTres("gpu:8192").
			withTresPerNode("gres/gpu=1").
			build()

		obj, err := translator.Translate(job, wmSpec, controller)

		// CPUs = Fallback (1) * TasksPerNode (1) = 1, rounded up to 1 whole core = 2
		wantCPU := resource.MustParse("2")
		// Memory = MemoryPerTres (8192MB) * GPU_placeholder (1) = 8192Mi
		wantMem := resource.MustParse("8192Mi")

		verifyJobSet(t, obj, err, "123", 1,
			withRequests(corev1.ResourceList{
				corev1.ResourceCPU:    wantCPU,
				corev1.ResourceMemory: wantMem,
			}),
		)
	})

	t.Run("Success - Resource Calculation - ThreadsPerCore rounding", func(t *testing.T) {
		tests := []struct {
			name                    string
			cpusPerTask             int32
			threadsPerCore          *int32
			partitionThreadsPerCore *int32
			otherPartThreadsPerCore *int32
			wantCPU                 resource.Quantity
			wantMem                 resource.Quantity
		}{
			{
				name:           "nil threadsPerCore uses default 2 and rounds 3 up to 4",
				cpusPerTask:    3,
				threadsPerCore: nil,
				wantCPU:        resource.MustParse("4"),
				wantMem:        resource.MustParse("8192Mi"),
			},
			{
				name:           "threadsPerCore 1 keeps 3 as 3",
				cpusPerTask:    3,
				threadsPerCore: ptr.To(int32(1)),
				wantCPU:        resource.MustParse("3"),
				wantMem:        resource.MustParse("6144Mi"),
			},
			{
				name:           "threadsPerCore 2 rounds 3 up to 4",
				cpusPerTask:    3,
				threadsPerCore: ptr.To(int32(2)),
				wantCPU:        resource.MustParse("4"),
				wantMem:        resource.MustParse("8192Mi"),
			},
			{
				name:           "threadsPerCore 4 rounds 5 up to 8",
				cpusPerTask:    5,
				threadsPerCore: ptr.To(int32(4)),
				wantCPU:        resource.MustParse("8"),
				wantMem:        resource.MustParse("16384Mi"),
			},
			{
				name:           "threadsPerCore 0 uses default 2 and rounds 3 up to 4",
				cpusPerTask:    3,
				threadsPerCore: ptr.To(int32(0)),
				wantCPU:        resource.MustParse("4"),
				wantMem:        resource.MustParse("8192Mi"),
			},
			{
				name:                    "partition threadsPerCore 1 overrides spec threadsPerCore 2",
				cpusPerTask:             3,
				threadsPerCore:          ptr.To(int32(2)),
				partitionThreadsPerCore: ptr.To(int32(1)),
				wantCPU:                 resource.MustParse("3"),
				wantMem:                 resource.MustParse("6144Mi"),
			},
			{
				name:                    "partition threadsPerCore 4 overrides spec threadsPerCore 1",
				cpusPerTask:             5,
				threadsPerCore:          ptr.To(int32(1)),
				partitionThreadsPerCore: ptr.To(int32(4)),
				wantCPU:                 resource.MustParse("8"),
				wantMem:                 resource.MustParse("16384Mi"),
			},
			{
				name:                    "other partition threadsPerCore does not affect target partition",
				cpusPerTask:             3,
				threadsPerCore:          nil,
				otherPartThreadsPerCore: ptr.To(int32(1)),
				wantCPU:                 resource.MustParse("4"),
				wantMem:                 resource.MustParse("8192Mi"),
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				job := newTestJob().
					withCpusPerTask(tt.cpusPerTask).
					build()
				wmBuilder := newTestWMSpec()
				if tt.threadsPerCore != nil {
					wmBuilder = wmBuilder.withThreadsPerCore(*tt.threadsPerCore)
				}
				if tt.partitionThreadsPerCore != nil {
					wmBuilder = wmBuilder.withPartitionThreadsPerCore(testDefaultPartition, *tt.partitionThreadsPerCore)
				}
				if tt.otherPartThreadsPerCore != nil {
					wmBuilder = wmBuilder.withPartitionThreadsPerCore("low-prio", *tt.otherPartThreadsPerCore)
				}

				obj, err := translator.Translate(job, wmBuilder.build(), controller)

				verifyJobSet(t, obj, err, "123", 1,
					withRequests(corev1.ResourceList{
						corev1.ResourceCPU:    tt.wantCPU,
						corev1.ResourceMemory: tt.wantMem,
					}),
				)
			})
		}
	})
}

func TestRoundUpToWholeCores(t *testing.T) {
	tests := []struct {
		cpus           int64
		threadsPerCore int64
		want           int64
	}{
		{cpus: 1, threadsPerCore: 2, want: 2},
		{cpus: 2, threadsPerCore: 2, want: 2},
		{cpus: 3, threadsPerCore: 2, want: 4},
		{cpus: 4, threadsPerCore: 2, want: 4},
		{cpus: 7, threadsPerCore: 2, want: 8},
		{cpus: 1, threadsPerCore: 1, want: 1},
		{cpus: 3, threadsPerCore: 1, want: 3},
		{cpus: 5, threadsPerCore: 4, want: 8},
	}

	for _, tt := range tests {
		if got := roundUpToWholeCores(tt.cpus, tt.threadsPerCore); got != tt.want {
			t.Errorf("roundUpToWholeCores(%d, %d) = %d, want %d", tt.cpus, tt.threadsPerCore, got, tt.want)
		}
	}
}

func TestParseTresString(t *testing.T) {
	tests := []struct {
		name    string
		tresStr string
		want    map[string]int64
	}{
		{
			name:    "empty",
			tresStr: "",
			want:    map[string]int64{},
		},
		{
			name:    "basic",
			tresStr: "cpu=4;mem=1024",
			want:    map[string]int64{tresCPU: 4, tresMEM: 1024},
		},
		{
			name:    "comma separated",
			tresStr: "cpu=4,mem=1024",
			want:    map[string]int64{tresCPU: 4, tresMEM: 1024},
		},
		{
			name:    "with feature and equal",
			tresStr: "gres/gpu:a3_ultra=2",
			want:    map[string]int64{tresGpuA3Ultra: 2},
		},
		{
			name:    "with feature and colon",
			tresStr: "gres/gpu:a3_ultra:2",
			want:    map[string]int64{tresGpuA3Ultra: 2},
		},
		{
			name:    "mixed",
			tresStr: "cpu=4,gres/gpu:a3_ultra=2;mem=1024",
			want:    map[string]int64{tresCPU: 4, tresGpuA3Ultra: 2, tresMEM: 1024},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseTresString(tt.tresStr)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("parseTresString() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTranslateSlurmJobToJobSet_GPU_Success(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()
	controller := getTestController()

	t.Run("Success_TresPerNode_WithGPUType", func(t *testing.T) {
		job := newTestJob().
			withNodes(2).
			withTresPerNode("gres/gpu:l4=2").
			build()

		obj, err := translator.Translate(job, wmSpec, controller)
		verifyJobSet(t, obj, err, "123", 2,
			withArgs([]string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, "'Features=k8s-id-123 " + testGresGpuL4 + "'"}),
			withRequests(corev1.ResourceList{
				corev1.ResourceName(testGpuResourceName): resource.MustParse("2"),
			}),
			withLimits(corev1.ResourceList{
				corev1.ResourceName(testGpuResourceName): resource.MustParse("2"),
			}),
			withNodeSelector(map[string]string{
				testGkeAcceleratorLabel: testGpuTypeL4,
			}),
		)
	})

	t.Run("Success_TresPerNode_WithUntypedGPU", func(t *testing.T) {
		job := newTestJob().
			withTresPerNode("gres/gpu=1").
			build()

		obj, err := translator.Translate(job, wmSpec, controller)
		verifyJobSet(t, obj, err, "123", 1,
			withArgs([]string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, "'Features=k8s-id-123 Gres=gpu:1'"}),
			withRequests(corev1.ResourceList{
				corev1.ResourceName(testGpuResourceName): resource.MustParse("1"),
			}),
			withNodeSelector(nil),
		)
	})

	t.Run("Success_Fallback_TresPerTask", func(t *testing.T) {
		job := newTestJob().
			withNodes(2).
			withTasks(4).
			withTresPerTask("gres/gpu:a100=1").
			build()

		obj, err := translator.Translate(job, wmSpec, controller)
		verifyJobSet(t, obj, err, "123", 2,
			withArgs([]string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, "'Features=k8s-id-123 Gres=gpu:a100:2'"}),
			withRequests(corev1.ResourceList{
				corev1.ResourceName(testGpuResourceName): resource.MustParse("2"),
			}),
			withNodeSelector(map[string]string{
				testGkeAcceleratorLabel: testGpuTypeA100,
			}),
		)
	})

	t.Run("Success_Fallback_TresPerJob", func(t *testing.T) {
		job := newTestJob().
			withNodes(2).
			withTresPerJob("gres/gpu:l4=4").
			build()

		obj, err := translator.Translate(job, wmSpec, controller)
		verifyJobSet(t, obj, err, "123", 2,
			withArgs([]string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, "'Features=k8s-id-123 " + testGresGpuL4 + "'"}),
			withRequests(corev1.ResourceList{
				corev1.ResourceName(testGpuResourceName): resource.MustParse("2"),
			}),
		)
	})

	t.Run("Success_CpusPerTres_MemoryPerTres_GPUType", func(t *testing.T) {
		job := newTestJob().
			withTresPerNode("gres/gpu:l4=1").
			withCpusPerTres("gres/gpu:l4=4").
			withMemoryPerTres("gres/gpu:l4=8192").
			build()

		obj, err := translator.Translate(job, wmSpec, controller)
		verifyJobSet(t, obj, err, "123", 1,
			withArgs([]string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, testExpectedGpuL4Args}),
			withRequests(corev1.ResourceList{
				corev1.ResourceCPU:                       resource.MustParse("4"),
				corev1.ResourceMemory:                    resource.MustParse("8192Mi"),
				corev1.ResourceName(testGpuResourceName): resource.MustParse("1"),
			}),
			withLimits(corev1.ResourceList{
				corev1.ResourceName(testGpuResourceName): resource.MustParse("1"),
			}),
			withNodeSelector(map[string]string{
				testGkeAcceleratorLabel: testGpuTypeL4,
			}),
		)
	})

	t.Run("Failure_TresPerJob_NotCleanlyDivisible", func(t *testing.T) {
		job := newTestJob().
			withNodes(2).
			withTresPerJob("gres/gpu:l4=3").
			build()

		obj, err := translator.Translate(job, wmSpec, controller)
		verifyTranslationError(t, obj, err, "is not cleanly divisible")
	})

	t.Run("Failure_HeterogeneousGPURequests", func(t *testing.T) {
		job := newTestJob().
			withTresPerNode("gres/gpu:l4=1,gres/gpu:a100=1").
			build()

		obj, err := translator.Translate(job, wmSpec, controller)
		verifyTranslationError(t, obj, err, "multiple heterogeneous gpu requests are not supported")
	})
}

func TestAppendSlurmdArgs(t *testing.T) {
	t.Run("Success_AppendToEmptyArgs", func(t *testing.T) {
		podSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: ContainerSlurmd,
				},
			},
		}
		err := appendSlurmdArgs(podSpec, "123", testConfServerDefault, nil, "")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		expected := []string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, testFeatureWithPart}
		if diff := cmp.Diff(expected, podSpec.Containers[0].Args); diff != "" {
			t.Errorf("Unexpected args (-want +got):\n%s", diff)
		}
	})

	t.Run("Success_AppendToExistingArgsWithUniqueFeature", func(t *testing.T) {
		podSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: ContainerSlurmd,
					Args: []string{"-d"},
				},
			},
		}
		err := appendSlurmdArgs(podSpec, "123", testConfServerDefault, nil, "")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		expected := []string{"-d", argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, testFeatureWithPart}
		if diff := cmp.Diff(expected, podSpec.Containers[0].Args); diff != "" {
			t.Errorf("Unexpected args (-want +got):\n%s", diff)
		}
	})

	t.Run("Success_AppendWithDynamicNodeFeature", func(t *testing.T) {
		podSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: ContainerSlurmd,
				},
			},
		}
		err := appendSlurmdArgs(podSpec, "123", testConfServerDefault, nil, "dynamic_kpartition")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		expected := []string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, "'Features=k8s-id-123,dynamic_kpartition'"}
		if diff := cmp.Diff(expected, podSpec.Containers[0].Args); diff != "" {
			t.Errorf("Unexpected args (-want +got):\n%s", diff)
		}
	})

	t.Run("Failure_SlurmdContainerNotFound", func(t *testing.T) {
		podSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "not-slurmd",
				},
			},
		}
		err := appendSlurmdArgs(podSpec, "123", testConfServerDefault, nil, "")
		verifyError(t, err, "container \"slurmd\" not found")
	})

	t.Run("Failure_InvalidDynamicNodeFeature", func(t *testing.T) {
		podSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: ContainerSlurmd,
				},
			},
		}
		err := appendSlurmdArgs(podSpec, "123", testConfServerDefault, nil, "invalid feature; rm -rf /")
		verifyError(t, err, "invalid dynamicNodeFeature")
	})

	t.Run("Success_AppendWithGRES", func(t *testing.T) {
		podSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: ContainerSlurmd,
				},
			},
		}
		gresList := []resolvedGRES{
			{name: "gpu", count: 2, resourceType: "l4"},
		}
		err := appendSlurmdArgs(podSpec, "123", testConfServerDefault, gresList, "")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		expected := []string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, "'Features=k8s-id-123 " + testGresGpuL4 + "'"}
		if diff := cmp.Diff(expected, podSpec.Containers[0].Args); diff != "" {
			t.Errorf("Unexpected args (-want +got):\n%s", diff)
		}
	})

	t.Run("Success_AppendWithMultipleGRESAndDynamicNodeFeature", func(t *testing.T) {
		podSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: ContainerSlurmd,
				},
			},
		}
		gresList := []resolvedGRES{
			{name: "gpu", count: 2, resourceType: "l4"},
			{name: "tpu", count: 1},
		}
		err := appendSlurmdArgs(podSpec, "123", testConfServerDefault, gresList, "dynamic_kpartition")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		expected := []string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, "'Features=k8s-id-123,dynamic_kpartition Gres=gpu:l4:2,tpu:1'"}
		if diff := cmp.Diff(expected, podSpec.Containers[0].Args); diff != "" {
			t.Errorf("Unexpected args (-want +got):\n%s", diff)
		}
	})
}

func envValues(envs []corev1.EnvVar, name string) []string {
	var values []string
	for _, env := range envs {
		if env.Name == name {
			values = append(values, env.Value)
		}
	}
	return values
}

func TestAppendSlurmdArgs_PodResourceEnvs(t *testing.T) {
	slurmdPodSpec := func(requests corev1.ResourceList, env ...corev1.EnvVar) *corev1.PodSpec {
		return &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:      ContainerSlurmd,
					Env:       env,
					Resources: corev1.ResourceRequirements{Requests: requests},
				},
			},
		}
	}

	tests := []struct {
		name       string
		requests   corev1.ResourceList
		wantCPUs   string
		wantMemory string
	}{
		{
			name: "whole CPUs and MiB from requests",
			requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("8192Mi"),
			},
			wantCPUs:   "4",
			wantMemory: "8192",
		},
		{
			name: "fractional CPUs round up and bytes round down to MiB",
			requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1500m"),
				corev1.ResourceMemory: resource.MustParse("1G"),
			},
			wantCPUs:   "2",
			wantMemory: "953",
		},
		{
			name: "no CPU or memory request gives 0",
			requests: corev1.ResourceList{
				corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
			},
			wantCPUs:   "0",
			wantMemory: "0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			podSpec := slurmdPodSpec(tt.requests)
			if err := appendSlurmdArgs(podSpec, "123", testConfServerDefault, nil, ""); err != nil {
				t.Fatalf("appendSlurmdArgs() returned unexpected error: %v", err)
			}
			c := podSpec.Containers[0]
			expectedArgs := []string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, testFeatureWithPart}
			if diff := cmp.Diff(expectedArgs, c.Args); diff != "" {
				t.Errorf("appendSlurmdArgs() set unexpected args (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff([]string{tt.wantCPUs}, envValues(c.Env, envPodCPUs)); diff != "" {
				t.Errorf("appendSlurmdArgs() set unexpected %s (-want +got):\n%s", envPodCPUs, diff)
			}
			if diff := cmp.Diff([]string{tt.wantMemory}, envValues(c.Env, envPodMemory)); diff != "" {
				t.Errorf("appendSlurmdArgs() set unexpected %s (-want +got):\n%s", envPodMemory, diff)
			}
		})
	}

	t.Run("ReplacesTemplateValues", func(t *testing.T) {
		const (
			templateEnv      = "TEMPLATE_ENV"
			templateEnvValue = "from-template"
		)
		podSpec := slurmdPodSpec(
			corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("8192Mi"),
			},
			corev1.EnvVar{Name: templateEnv, Value: templateEnvValue},
			corev1.EnvVar{Name: envPodCPUs, Value: "99"},
			corev1.EnvVar{Name: envPodMemory, Value: "1"},
		)
		if err := appendSlurmdArgs(podSpec, "123", testConfServerDefault, nil, ""); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c := podSpec.Containers[0]
		if diff := cmp.Diff([]string{templateEnvValue}, envValues(c.Env, templateEnv)); diff != "" {
			t.Errorf("Unexpected %s (-want +got):\n%s", templateEnv, diff)
		}
		if diff := cmp.Diff([]string{"4"}, envValues(c.Env, envPodCPUs)); diff != "" {
			t.Errorf("Unexpected %s (-want +got):\n%s", envPodCPUs, diff)
		}
		if diff := cmp.Diff([]string{"8192"}, envValues(c.Env, envPodMemory)); diff != "" {
			t.Errorf("Unexpected %s (-want +got):\n%s", envPodMemory, diff)
		}
	})
}

// TestAppendSlurmdArgs_ManagedEnvNames fails when appendSlurmdArgs and slurmdManagedEnvNames disagree.
func TestAppendSlurmdArgs_ManagedEnvNames(t *testing.T) {
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: ContainerSlurmd}}}
	if err := appendSlurmdArgs(podSpec, "123", testConfServerDefault, nil, ""); err != nil {
		t.Fatalf("appendSlurmdArgs() returned unexpected error: %v", err)
	}
	injected := make(map[string]bool, len(podSpec.Containers[0].Env))
	for _, env := range podSpec.Containers[0].Env {
		injected[env.Name] = true
	}
	if diff := cmp.Diff(slurmdManagedEnvNames, injected); diff != "" {
		t.Errorf("appendSlurmdArgs() injected env names differ from slurmdManagedEnvNames (-want +got):\n%s", diff)
	}
}

func TestTranslateSlurmJobToJobSet_PodResourceEnvs(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()
	controller := getTestController()

	t.Run("Fallback memory", func(t *testing.T) {
		// 2 tasks per pod => 4 CPUs and 4 * defaultMemoryPerCPUMB per pod.
		job := newTestJob().
			withNodes(2).
			withTasks(4).
			withCpusPerTask(2).
			build()

		obj, err := translator.Translate(job, wmSpec, controller)
		verifyJobSet(t, obj, err, "123", 2,
			withArgs([]string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, testPartitionHighPrio}),
			withRequests(corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("8192Mi"),
			}),
			withEnv(map[string]string{envPodCPUs: "4", envPodMemory: "8192"}),
		)
	})

	t.Run("Explicit memory request", func(t *testing.T) {
		job := newTestJob().
			withCpusPerTask(2).
			withMemoryPerNode(4096).
			build()

		obj, err := translator.Translate(job, wmSpec, controller)
		verifyJobSet(t, obj, err, "123", 1,
			withArgs([]string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, testPartitionHighPrio}),
			withRequests(corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("2"),
				corev1.ResourceMemory: resource.MustParse("4096Mi"),
			}),
			withEnv(map[string]string{envPodCPUs: "2", envPodMemory: "4096"}),
		)
	})
}

//nolint:goconst // Test deliberately duplicates constant literals for contract verification.
func TestAppendSlurmdArgs_NodeNameAndEnv(t *testing.T) {
	t.Run("InjectsDeterministicNodeEnvsAndNodeNameArg", func(t *testing.T) {
		podSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: ContainerSlurmd,
				},
			},
		}
		err := appendSlurmdArgs(podSpec, "123", testConfServerDefault, nil, "")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		c := podSpec.Containers[0]
		expectedEnvs := map[string]string{
			"JOBSET_NAME":         "metadata.labels['jobset.sigs.k8s.io/jobset-name']",
			"REPLICATED_JOB_NAME": "metadata.labels['jobset.sigs.k8s.io/replicatedjob-name']",
			"JOB_INDEX":           "metadata.labels['jobset.sigs.k8s.io/job-index']",
			"POD_INDEX":           "metadata.labels['batch.kubernetes.io/job-completion-index']",
		}
		for envName, wantFieldPath := range expectedEnvs {
			found := false
			for _, env := range c.Env {
				if env.Name == envName && env.ValueFrom != nil && env.ValueFrom.FieldRef != nil && env.ValueFrom.FieldRef.FieldPath == wantFieldPath {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected %s downward API env var on slurmd container with FieldPath %q, got: %v", envName, wantFieldPath, c.Env)
			}
		}

		foundNodeNameEnv := false
		wantNodeNameVal := "$(JOBSET_NAME)-$(REPLICATED_JOB_NAME)-$(JOB_INDEX)-$(POD_INDEX)"
		for _, env := range c.Env {
			if env.Name == "SLURM_NODE_NAME" && env.Value == wantNodeNameVal {
				foundNodeNameEnv = true
				break
			}
		}
		if !foundNodeNameEnv {
			t.Errorf("Expected SLURM_NODE_NAME env var with value %q, got: %v", wantNodeNameVal, c.Env)
		}

		lastNodeName := ""
		for i := 0; i < len(c.Args); i++ {
			if c.Args[i] == "-N" && i+1 < len(c.Args) {
				lastNodeName = c.Args[i+1]
			}
		}
		if lastNodeName != "$(SLURM_NODE_NAME)" {
			t.Errorf("Expected -N value to be %q, got %q", "$(SLURM_NODE_NAME)", lastNodeName)
		}
	})

	t.Run("AppendsNodeNameAndOverridesExistingDeterministicEnvs", func(t *testing.T) {
		podSpec := &corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: ContainerSlurmd,
					Args: []string{"-N", "initial-node", "-d"},
					Env: []corev1.EnvVar{
						{Name: "TEST_INITIAL_ENV", Value: "custom"},
						{Name: "JOBSET_NAME", Value: "wrong-jobset"},
						{Name: "SLURM_NODE_NAME", Value: "wrong-node"},
					},
				},
			},
		}
		err := appendSlurmdArgs(podSpec, "123", testConfServerDefault, nil, "")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		c := podSpec.Containers[0]
		expectedPrefix := []string{"-N", "initial-node", "-d"}
		if diff := cmp.Diff(expectedPrefix, c.Args[:len(expectedPrefix)]); diff != "" {
			t.Errorf("Unexpected args prefix (-want +got):\n%s", diff)
		}

		lastNodeName := ""
		for i := 0; i < len(c.Args); i++ {
			if c.Args[i] == "-N" && i+1 < len(c.Args) {
				lastNodeName = c.Args[i+1]
			}
		}
		if lastNodeName != "$(SLURM_NODE_NAME)" {
			t.Errorf("Expected last -N value to be %q, got %q", "$(SLURM_NODE_NAME)", lastNodeName)
		}

		if len(c.Env) != 8 {
			t.Fatalf("Expected 8 env vars (1 custom + 5 deterministic + 2 resource), got %d: %v", len(c.Env), c.Env)
		}
		if c.Env[0].Name != "TEST_INITIAL_ENV" {
			t.Errorf("Expected TEST_INITIAL_ENV at index 0, got %s", c.Env[0].Name)
		}
		// Deterministic env vars must be in order so SLURM_NODE_NAME can reference previous ones
		expectedOrder := []string{"JOBSET_NAME", "REPLICATED_JOB_NAME", "JOB_INDEX", "POD_INDEX", "SLURM_NODE_NAME", "POD_CPUS", "POD_MEMORY"}
		for i, wantName := range expectedOrder {
			if c.Env[i+1].Name != wantName {
				t.Errorf("Expected env at index %d to be %s, got %s", i+1, wantName, c.Env[i+1].Name)
			}
		}
	})
}

func TestTranslateSlurmJobToJobSet_TemplateMerging_BackwardCompatibility(t *testing.T) {
	translator := NewJobSetTranslator()
	wmSpec := getTestWMSpec() // has no PodTemplate
	controller := getTestController()
	job := newTestJob().
		withNodes(2).
		withCpusPerTask(2).
		build()

	obj, err := translator.Translate(job, wmSpec, controller)
	verifyJobSet(t, obj, err, "123", 2,
		withContainersCount(1),
		withSlurmdImage(DefaultSlurmdImage),
		withRequests(corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("2"),
		}),
	)
}

func TestTranslateSlurmJobToJobSet_TemplateMerging_InjectedSlurmdAndNodeSelector(t *testing.T) {
	translator := NewJobSetTranslator()
	wmSpec := newTestWMSpec().
		withContainers(corev1.Container{
			Name:  testSidecarName,
			Image: testSidecarImage,
		}).
		withNodeSelector(map[string]string{
			"environment": "prod",
		}).
		build()
	controller := getTestController()

	job := newTestJob().
		withCpusPerTask(4).
		withTresPerNode("gres/gpu:l4=1").
		build()

	obj, err := translator.Translate(job, wmSpec, controller)
	podSpec, _ := verifyJobSet(t, obj, err, "123", 1,
		withContainersCount(2),
		withSlurmdImage(DefaultSlurmdImage),
		withRequests(corev1.ResourceList{
			corev1.ResourceCPU:                       resource.MustParse("4"),
			corev1.ResourceName(testGpuResourceName): resource.MustParse("1"),
		}),
		withNodeSelector(map[string]string{
			"environment":           "prod",
			testGkeAcceleratorLabel: testGpuTypeL4,
		}),
	)

	var sidecar *corev1.Container
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == testSidecarName {
			sidecar = &podSpec.Containers[i]
			break
		}
	}

	if sidecar == nil {
		t.Fatalf("Container %q not found", testSidecarName)
		return
	}
	if sidecar.Image != testSidecarImage {
		t.Errorf("Expected sidecar image %q, got %q", testSidecarImage, sidecar.Image)
	}
}

func TestTranslateSlurmJobToJobSet_TemplateMerging_PreserveSlurmdImageAndMergeResources(t *testing.T) {
	translator := NewJobSetTranslator()
	wmSpec := newTestWMSpec().
		withContainers(
			corev1.Container{
				Name:  ContainerSlurmd,
				Image: testCustomSlurmdImage,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("1"),
					},
				},
			},
			corev1.Container{
				Name:  "logger",
				Image: "logger:latest",
			},
		).
		build()
	controller := getTestController()

	job := newTestJob().
		withCpusPerTask(2).
		build()

	obj, err := translator.Translate(job, wmSpec, controller)
	podSpec, _ := verifyJobSet(t, obj, err, "123", 1,
		withContainersCount(2),
		withSlurmdImage(testCustomSlurmdImage),
		withRequests(corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("2"),
		}),
	)

	var logger *corev1.Container
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == "logger" {
			logger = &podSpec.Containers[i]
			break
		}
	}

	if logger == nil {
		t.Fatal("Container 'logger' not found")
		return
	}
}

func TestTranslateSlurmJobToJobSet_TemplateMerging_NodeSelectorConflict(t *testing.T) {
	translator := NewJobSetTranslator()
	wmSpec := newTestWMSpec().
		withContainers(corev1.Container{
			Name: ContainerSlurmd,
		}).
		withNodeSelector(map[string]string{
			testGkeAcceleratorLabel: testGpuTypeA100,
		}).
		build()
	controller := getTestController()

	job := newTestJob().
		withTresPerNode("gres/gpu:l4=1").
		build()

	obj, err := translator.Translate(job, wmSpec, controller)
	verifyJobSet(t, obj, err, "123", 1,
		withNodeSelector(map[string]string{
			testGkeAcceleratorLabel: testGpuTypeL4,
		}),
	)
}

func TestTranslateSlurmJobToJobSet_TemplateMerging_PreserveCustomResources(t *testing.T) {
	translator := NewJobSetTranslator()
	customResourceName := corev1.ResourceName("ephemeral-storage")
	customQuantity := resource.MustParse("10Gi")
	wmSpec := newTestWMSpec().
		withContainers(corev1.Container{
			Name:  ContainerSlurmd,
			Image: testCustomSlurmdImage,
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("1"),
					customResourceName: customQuantity,
				},
				Limits: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("4"),
					customResourceName: customQuantity,
				},
			},
		}).
		build()
	controller := getTestController()

	job := newTestJob().
		withCpusPerTask(2).
		build()

	obj, err := translator.Translate(job, wmSpec, controller)
	verifyJobSet(t, obj, err, "123", 1,
		withContainersCount(1),
		withRequests(corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("2"),
			customResourceName: customQuantity,
		}),
		withLimits(corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("2"),
			customResourceName: customQuantity,
		}),
	)
}

func TestTranslateSlurmJobToJobSet_PreStopHook(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()
	controller := getTestController()

	job := newTestJob().withJobID(456).build()

	obj, err := translator.Translate(job, wmSpec, controller)
	verifyJobSet(t, obj, err, "456", 1,
		withPreStopHook([]string{"sh", "-c", "scontrol update nodename=$SLURM_NODE_NAME state=DOWN reason=\"pod_terminated\" && scontrol delete nodename=$SLURM_NODE_NAME"}),
	)
}

func TestTranslateSlurmJobToJobSet_PreStopHook_PreserveExisting(t *testing.T) {
	customCmd := []string{"sh", "-c", "echo custom preStop hook && sleep 5"}
	wmSpec := newTestWMSpec().
		withContainers(corev1.Container{
			Name: ContainerSlurmd,
			Lifecycle: &corev1.Lifecycle{
				PreStop: &corev1.LifecycleHandler{
					Exec: &corev1.ExecAction{
						Command: customCmd,
					},
				},
			},
		}).
		build()
	translator := NewJobSetTranslator()
	controller := getTestController()

	job := newTestJob().withJobID(456).build()

	obj, err := translator.Translate(job, wmSpec, controller)
	verifyJobSet(t, obj, err, "456", 1,
		withPreStopHook(customCmd),
	)
}

func TestTranslateSlurmJobToJobSet_ConfServer(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()

	job := newTestJob().build()

	t.Run("Success - In-Cluster Controller Default Port", func(t *testing.T) {
		controller := &slinkyv1beta1.Controller{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "slurm-ctld",
				Namespace: "slurm-system",
			},
			Spec: slinkyv1beta1.ControllerSpec{
				SlurmKeyRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: testSlurmAuthSecret,
					},
					Key: SlurmKeyFile,
				},
			},
		}
		obj, err := translator.Translate(job, wmSpec, controller)
		verifyJobSet(t, obj, err, "123", 1,
			withArgs([]string{argConfServer, "slurm-ctld-controller.slurm-system:6817", argNodeName, valNodeName, "-Z", argConf, testPartitionHighPrio}),
		)
	})

	t.Run("Success - In-Cluster Controller Custom Port", func(t *testing.T) {
		controller := &slinkyv1beta1.Controller{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "slurm-ctld",
				Namespace: "slurm-system",
			},
			Spec: slinkyv1beta1.ControllerSpec{
				Service: slinkyv1beta1.ServiceSpec{
					Port: 7000,
				},
				SlurmKeyRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: testSlurmAuthSecret,
					},
					Key: SlurmKeyFile,
				},
			},
		}
		obj, err := translator.Translate(job, wmSpec, controller)
		verifyJobSet(t, obj, err, "123", 1,
			withArgs([]string{argConfServer, "slurm-ctld-controller.slurm-system:7000", argNodeName, valNodeName, "-Z", argConf, testPartitionHighPrio}),
		)
	})

	t.Run("Success - External Controller", func(t *testing.T) {
		controller := &slinkyv1beta1.Controller{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testSlurmName,
				Namespace: testSlurmName,
			},
			Spec: slinkyv1beta1.ControllerSpec{
				External: true,
				ExternalConfig: slinkyv1beta1.ExternalConfig{
					Host: "10.0.0.5",
					Port: 9999,
				},
				SlurmKeyRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: testSlurmAuthSecret,
					},
					Key: SlurmKeyFile,
				},
			},
		}
		obj, err := translator.Translate(job, wmSpec, controller)
		verifyJobSet(t, obj, err, "123", 1,
			withArgs([]string{argConfServer, "10.0.0.5:9999", argNodeName, valNodeName, "-Z", argConf, testPartitionHighPrio}),
		)
	})

	t.Run("Failure - External Controller missing Host/Port", func(t *testing.T) {
		controller := &slinkyv1beta1.Controller{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testSlurmName,
				Namespace: testSlurmName,
			},
			Spec: slinkyv1beta1.ControllerSpec{
				External: true,
				SlurmKeyRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: testSlurmAuthSecret,
					},
					Key: SlurmKeyFile,
				},
			},
		}
		obj, err := translator.Translate(job, wmSpec, controller)
		verifyTranslationError(t, obj, err, "external controller is missing Host or Port")
	})

	t.Run("Success - External Controller IPv6", func(t *testing.T) {
		controller := &slinkyv1beta1.Controller{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testSlurmName,
				Namespace: testSlurmName,
			},
			Spec: slinkyv1beta1.ControllerSpec{
				External: true,
				ExternalConfig: slinkyv1beta1.ExternalConfig{
					Host: "2001:db8::1",
					Port: 8888,
				},
				SlurmKeyRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: testSlurmAuthSecret,
					},
					Key: SlurmKeyFile,
				},
			},
		}
		obj, err := translator.Translate(job, wmSpec, controller)
		verifyJobSet(t, obj, err, "123", 1,
			withArgs([]string{argConfServer, "[2001:db8::1]:8888", argNodeName, valNodeName, "-Z", argConf, testPartitionHighPrio}),
		)
	})

	t.Run("Success - Preserve user specified conf-server", func(t *testing.T) {
		customWMSpec := newTestWMSpec().
			withContainers(corev1.Container{
				Name: ContainerSlurmd,
				Args: []string{argConfServer, "custom-server.slurm:6817"},
			}).
			build()
		controller := getTestController()
		obj, err := translator.Translate(job, customWMSpec, controller)
		verifyJobSet(t, obj, err, "123", 1,
			withArgs([]string{argConfServer, "custom-server.slurm:6817", argNodeName, valNodeName, "-Z", argConf, testPartitionHighPrio}),
		)
	})
}

func TestTranslateSlurmJobToJobSet_SlurmKeyVolume(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()

	job := newTestJob().build()

	t.Run("Success - Custom SlurmKeyRef", func(t *testing.T) {
		controller := &slinkyv1beta1.Controller{
			ObjectMeta: metav1.ObjectMeta{
				Name:      testSlurmName,
				Namespace: testSlurmName,
			},
			Spec: slinkyv1beta1.ControllerSpec{
				SlurmKeyRef: corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: "custom-secret-key",
					},
					Key: "custom.key",
				},
			},
		}
		obj, err := translator.Translate(job, wmSpec, controller)
		podSpec, _ := verifyJobSet(t, obj, err, "123", 1)
		var slurmVol *corev1.Volume
		for i := range podSpec.Volumes {
			if podSpec.Volumes[i].Name == SlurmConfigVolume {
				slurmVol = &podSpec.Volumes[i]
				break
			}
		}
		if slurmVol == nil {
			t.Fatalf("Volume %s not found", SlurmConfigVolume)
			return
		}
		if len(slurmVol.Projected.Sources) != 1 || slurmVol.Projected.Sources[0].Secret == nil {
			t.Fatalf("Expected SecretProjection source")
			return
		}
		secretProj := slurmVol.Projected.Sources[0].Secret
		if secretProj.Name != "custom-secret-key" {
			t.Errorf("Expected secret name 'custom-secret-key', got %s", secretProj.Name)
		}
		if len(secretProj.Items) != 1 || secretProj.Items[0].Key != "custom.key" || secretProj.Items[0].Path != "slurm.key" {
			t.Errorf("Expected Key='custom.key' Path='slurm.key', got %v", secretProj.Items)
		}
	})

	t.Run("Success - Preserve user specified slurm-etc volume and mount", func(t *testing.T) {
		customWMSpec := newTestWMSpec().
			withVolumes(corev1.Volume{
				Name: SlurmConfigVolume,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			}).
			withContainers(corev1.Container{
				Name: ContainerSlurmd,
				VolumeMounts: []corev1.VolumeMount{
					{
						Name:      SlurmConfigVolume,
						MountPath: "/etc/slurm",
						ReadOnly:  false,
					},
				},
			}).
			build()
		controller := getTestController()
		obj, err := translator.Translate(job, customWMSpec, controller)
		podSpec, container := verifyJobSet(t, obj, err, "123", 1)

		// Should not duplicate volume or mount
		var slurmEtcVolumes int
		for _, v := range podSpec.Volumes {
			if v.Name == SlurmConfigVolume {
				slurmEtcVolumes++
				if v.EmptyDir == nil {
					t.Errorf("Expected EmptyDir volume source preserved")
				}
			}
		}
		if slurmEtcVolumes != 1 {
			t.Errorf("Expected 1 slurm-etc volume, got %d", slurmEtcVolumes)
		}

		var slurmEtcMounts int
		for _, vm := range container.VolumeMounts {
			if vm.Name == SlurmConfigVolume {
				slurmEtcMounts++
			}
		}
		if slurmEtcMounts != 1 {
			t.Errorf("Expected 1 slurm-etc volume mount, got %d", slurmEtcMounts)
		}
	})
}

func TestTranslateSlurmJobToJobSet_NilController(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()

	job := newTestJob().build()

	obj, err := translator.Translate(job, wmSpec, nil)
	verifyTranslationError(t, obj, err, "controller is nil")
}

func TestIsExplicitUserOrAdminHold(t *testing.T) {
	tests := []struct {
		reason string
		want   bool
	}{
		{ReasonJobHeldUser, true},
		{ReasonJobHeldAdmin, true},
		{"JobHeldSystem", false},
		{ReasonPriority, false},
		{"None", false},
		{"", false},
		{"OtherReason", false},
	}
	for _, tc := range tests {
		got := isExplicitUserOrAdminHold(tc.reason)
		if got != tc.want {
			t.Errorf("isExplicitUserOrAdminHold(%q) = %v, want %v", tc.reason, got, tc.want)
		}
	}
}

func TestTranslateSlurmJobToJobSet_HoldReasons(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()
	controller := getTestController()

	testCases := []struct {
		name        string
		stateReason string
		hold        bool
		priority    *int32
		comment     string
		wantNil     bool
	}{
		{name: "JobHeldUser defer translation", stateReason: ReasonJobHeldUser, hold: true, wantNil: true},
		{name: "JobHeldAdmin defer translation", stateReason: ReasonJobHeldAdmin, hold: true, wantNil: true},
		{name: "JobHeldSystem routine hold proceeds", stateReason: "JobHeldSystem", hold: true, wantNil: false},
		{name: "Priority routine hold proceeds", stateReason: ReasonPriority, hold: true, wantNil: false},
		{name: "None routine hold proceeds", stateReason: "None", hold: true, wantNil: false},
		{name: "JobHeldUser without hold proceeds", stateReason: ReasonJobHeldUser, hold: false, wantNil: false},
		{name: "JobHeldUser with auto hold comment proceeds (automatic hold)", stateReason: ReasonJobHeldUser, hold: true, comment: AutoHoldMarker, wantNil: false},
		{name: "JobHeldUser without marker defers translation (sbatch --hold)", stateReason: ReasonJobHeldUser, hold: true, comment: "user comment", wantNil: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			job := newTestJob().
				withHold(tc.hold).
				withStateReason(tc.stateReason).
				withComment(tc.comment).
				build()

			obj, err := translator.Translate(job, wmSpec, controller)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
				return
			}
			if tc.wantNil && obj != nil {
				t.Errorf("Translate() returned %T, want nil for explicit hold %s", obj, tc.stateReason)
			}
			if !tc.wantNil && obj == nil {
				t.Errorf("Translate() returned nil, want non-nil JobSet for reason %s", tc.stateReason)
			}
		})
	}
}

func TestIsJobExplicitlyHeld(t *testing.T) {
	tests := []struct {
		name        string
		hold        bool
		stateReason string
		comment     string
		want        bool
	}{
		{"empty comment with JobHeldUser (sbatch --hold)", true, ReasonJobHeldUser, "", true},
		{"auto hold marker with JobHeldUser (automatic hold)", true, ReasonJobHeldUser, AutoHoldMarker, false},
		{"user comment without marker with JobHeldUser", true, ReasonJobHeldUser, "my user comment", true},
		{"user comment with auto hold marker", true, ReasonJobHeldUser, "my user comment, " + AutoHoldMarker, false},
		{"JobHeldAdmin is always explicit even with marker", true, ReasonJobHeldAdmin, AutoHoldMarker, true},
		{"hold false is not explicit", false, ReasonJobHeldUser, "", false},
		{"other state reason is not explicit", true, ReasonPriority, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := newTestJob().
				withHold(tc.hold).
				withStateReason(tc.stateReason).
				withComment(tc.comment).
				build()
			got := isJobExplicitlyHeld(job)
			if got != tc.want {
				t.Errorf("isJobExplicitlyHeld() = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("nil job returns false", func(t *testing.T) {
		if got := isJobExplicitlyHeld(nil); got != false {
			t.Errorf("isJobExplicitlyHeld(nil) = %v, want false", got)
		}
	})
}

func TestTranslateSlurmJobToJobSet_Features(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()
	controller := getTestController()

	t.Run("Job with injected unique constraint does not override deterministic feature", func(t *testing.T) {
		job := newTestJob().
			withFeatures(testUUIDFeature).
			build()

		obj, err := translator.Translate(job, wmSpec, controller)
		expectedArgs := []string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, testPartitionHighPrio}
		verifyJobSet(t, obj, err, "123", 1, withArgs(expectedArgs))
	})

	t.Run("Job with combined boolean constraints does not override deterministic feature", func(t *testing.T) {
		job := newTestJob().
			withFeatures(testGpuArchFeature + "&highmem&" + testUUIDFeature).
			build()

		obj, err := translator.Translate(job, wmSpec, controller)
		expectedArgs := []string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, testPartitionHighPrio}
		verifyJobSet(t, obj, err, "123", 1, withArgs(expectedArgs))
	})

	t.Run("Partition mapping with DynamicNodeFeature includes it in slurmd conf", func(t *testing.T) {
		customWMSpec := getTestWMSpec()
		customWMSpec.PartitionsMapping[0].DynamicNodeFeature = "dynamic_kpartition"

		job := newTestJob().build()

		obj, err := translator.Translate(job, customWMSpec, controller)
		expectedArgs := []string{argConfServer, testConfServerDefault, argNodeName, valNodeName, "-Z", argConf, "'Features=k8s-id-123,dynamic_kpartition'"}
		verifyJobSet(t, obj, err, "123", 1, withArgs(expectedArgs))
	})
}

func TestTranslateSlurmJobToJobSet_SlurmdProbes_Default(t *testing.T) {
	wmSpec := getTestWMSpec()
	translator := NewJobSetTranslator()
	controller := getTestController()
	job := newTestJob().build()

	obj, err := translator.Translate(job, wmSpec, controller)
	_, slurmd := verifyJobSet(t, obj, err, "123", 1,
		withContainersCount(1),
	)

	expectedCmd := []string{
		"sh",
		"-c",
		`printf 'GET /livez HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n' | socat -T 2 - TCP:127.0.0.1:6818 2>/dev/null | grep -q '^HTTP/1\.[01] 20[04]' && (scontrol listpids 2>/dev/null | awk 'NR>1 {found=1; exit} END {exit !found}' || scontrol -o show node "$SLURM_NODE_NAME" 2>/dev/null | grep -Eq 'State=[^ ]*(ALLOCATED|MIXED|COMPLETING)')`,
	}

	if slurmd.StartupProbe == nil {
		t.Fatal("expected StartupProbe on slurmd container, got nil")
	}
	if slurmd.StartupProbe.Exec == nil {
		t.Fatal("expected Exec handler on StartupProbe, got nil")
	}
	if diff := cmp.Diff(expectedCmd, slurmd.StartupProbe.Exec.Command); diff != "" {
		t.Errorf("StartupProbe command mismatch (-want +got):\n%s", diff)
	}
	if slurmd.StartupProbe.PeriodSeconds != 10 {
		t.Errorf("expected StartupProbe.PeriodSeconds = 10, got %d", slurmd.StartupProbe.PeriodSeconds)
	}
	if slurmd.StartupProbe.TimeoutSeconds != 5 {
		t.Errorf("expected StartupProbe.TimeoutSeconds = 5, got %d", slurmd.StartupProbe.TimeoutSeconds)
	}
	if slurmd.StartupProbe.FailureThreshold != 30 {
		t.Errorf("expected StartupProbe.FailureThreshold = 30, got %d", slurmd.StartupProbe.FailureThreshold)
	}

	if slurmd.LivenessProbe == nil {
		t.Fatal("expected LivenessProbe on slurmd container, got nil")
	}
	if slurmd.LivenessProbe.Exec == nil {
		t.Fatal("expected Exec handler on LivenessProbe, got nil")
	}
	if diff := cmp.Diff(expectedCmd, slurmd.LivenessProbe.Exec.Command); diff != "" {
		t.Errorf("LivenessProbe command mismatch (-want +got):\n%s", diff)
	}
	if slurmd.LivenessProbe.PeriodSeconds != 10 {
		t.Errorf("expected LivenessProbe.PeriodSeconds = 10, got %d", slurmd.LivenessProbe.PeriodSeconds)
	}
	if slurmd.LivenessProbe.TimeoutSeconds != 5 {
		t.Errorf("expected LivenessProbe.TimeoutSeconds = 5, got %d", slurmd.LivenessProbe.TimeoutSeconds)
	}
	if slurmd.LivenessProbe.FailureThreshold != 6 {
		t.Errorf("expected LivenessProbe.FailureThreshold = 6, got %d", slurmd.LivenessProbe.FailureThreshold)
	}
}

func TestTranslateSlurmJobToJobSet_SlurmdProbes_PreserveExisting(t *testing.T) {
	customStartup := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sh", "-c", "custom-startup-check"},
			},
		},
		PeriodSeconds:    15,
		FailureThreshold: 20,
	}
	customLiveness := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sh", "-c", "custom-liveness-check"},
			},
		},
		PeriodSeconds:    20,
		FailureThreshold: 3,
	}
	wmSpec := newTestWMSpec().
		withContainers(corev1.Container{
			Name:          ContainerSlurmd,
			StartupProbe:  customStartup,
			LivenessProbe: customLiveness,
		}).
		build()

	translator := NewJobSetTranslator()
	controller := getTestController()
	job := newTestJob().build()

	obj, err := translator.Translate(job, wmSpec, controller)
	_, slurmd := verifyJobSet(t, obj, err, "123", 1)

	if diff := cmp.Diff(customStartup, slurmd.StartupProbe); diff != "" {
		t.Errorf("expected custom StartupProbe to be preserved (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(customLiveness, slurmd.LivenessProbe); diff != "" {
		t.Errorf("expected custom LivenessProbe to be preserved (-want +got):\n%s", diff)
	}
}

func TestTranslateSlurmJobToJobSet_UserDefinedPodTemplate(t *testing.T) {
	userVolume := corev1.Volume{
		Name: testUserVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}
	userContainer := corev1.Container{
		Name:  testSidecarName,
		Image: testSidecarImage,
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      testUserVolumeName,
				MountPath: "/data",
			},
		},
	}
	wmSpec := newTestWMSpec().
		withVolumes(userVolume).
		withContainers(userContainer).
		withNodeSelector(map[string]string{
			"custom-node": "true",
		}).
		build()

	translator := NewJobSetTranslator()
	controller := getTestController()
	job := newTestJob().build()

	obj, err := translator.Translate(job, wmSpec, controller)
	podSpec, _ := verifyJobSet(t, obj, err, "123", 1,
		withContainersCount(2),
	)

	// Verify user-defined container and volume are preserved
	var foundUserContainer bool
	for _, c := range podSpec.Containers {
		if c.Name == testSidecarName {
			foundUserContainer = true
			if len(c.VolumeMounts) != 1 || c.VolumeMounts[0].MountPath != "/data" {
				t.Errorf("User container volume mount modified: %v", c.VolumeMounts)
			}
			break
		}
	}
	if !foundUserContainer {
		t.Errorf("User container %q not found in pod spec", testSidecarName)
	}

	var foundUserVolume bool
	for _, v := range podSpec.Volumes {
		if v.Name == testUserVolumeName {
			foundUserVolume = true
			break
		}
	}
	if !foundUserVolume {
		t.Errorf("User volume %q not found in pod spec", testUserVolumeName)
	}
}

func TestTranslateSlurmJobToJobSet_RestartPolicy(t *testing.T) {
	t.Run("Defaults to RestartPolicyNever", func(t *testing.T) {
		wmSpec := getTestWMSpec()
		translator := NewJobSetTranslator()
		controller := getTestController()
		job := newTestJob().build()

		obj, err := translator.Translate(job, wmSpec, controller)
		podSpec, _ := verifyJobSet(t, obj, err, "123", 1)

		if podSpec.RestartPolicy != corev1.RestartPolicyNever {
			t.Errorf("expected RestartPolicy %v, got %v", corev1.RestartPolicyNever, podSpec.RestartPolicy)
		}
	})

	t.Run("Preserves custom RestartPolicy if set", func(t *testing.T) {
		wmSpec := newTestWMSpec().build()
		wmSpec.PodTemplate = &corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyOnFailure,
			},
		}
		translator := NewJobSetTranslator()
		controller := getTestController()
		job := newTestJob().build()

		obj, err := translator.Translate(job, wmSpec, controller)
		podSpec, _ := verifyJobSet(t, obj, err, "123", 1)

		if podSpec.RestartPolicy != corev1.RestartPolicyOnFailure {
			t.Errorf("expected RestartPolicy %v, got %v", corev1.RestartPolicyOnFailure, podSpec.RestartPolicy)
		}
	})
}

func TestTranslateSlurmJobToJobSet_PodFailurePolicy(t *testing.T) {
	t.Run("Configures FailJob on slurmd non-zero exit when RestartPolicyNever", func(t *testing.T) {
		wmSpec := getTestWMSpec()
		translator := NewJobSetTranslator()
		controller := getTestController()
		job := newTestJob().build()

		obj, err := translator.Translate(job, wmSpec, controller)
		if err != nil {
			t.Fatalf("unexpected translation error: %v", err)
		}

		js := obj.(*jobsetv1alpha2.JobSet)
		repJob := js.Spec.ReplicatedJobs[0]
		pfp := repJob.Template.Spec.PodFailurePolicy
		if pfp == nil {
			t.Fatalf("expected PodFailurePolicy on JobSpec, got nil")
		}
		if len(pfp.Rules) != 1 {
			t.Fatalf("expected 1 PodFailurePolicy rule, got %d", len(pfp.Rules))
		}
		slurmdRule := pfp.Rules[0]
		if slurmdRule.Action != batchv1.PodFailurePolicyActionFailJob {
			t.Errorf("expected slurmd rule action %v, got %v", batchv1.PodFailurePolicyActionFailJob, slurmdRule.Action)
		}
		if slurmdRule.OnExitCodes == nil {
			t.Fatalf("expected OnExitCodes on slurmd rule, got nil")
		}
		if slurmdRule.OnExitCodes.ContainerName == nil || *slurmdRule.OnExitCodes.ContainerName != "slurmd" {
			t.Errorf("expected containerName slurmd, got %v", slurmdRule.OnExitCodes.ContainerName)
		}
		if slurmdRule.OnExitCodes.Operator != batchv1.PodFailurePolicyOnExitCodesOpNotIn {
			t.Errorf("expected operator NotIn, got %v", slurmdRule.OnExitCodes.Operator)
		}
		if len(slurmdRule.OnExitCodes.Values) != 1 || slurmdRule.OnExitCodes.Values[0] != 0 {
			t.Errorf("expected values [0], got %v", slurmdRule.OnExitCodes.Values)
		}
	})

	t.Run("Does not configure PodFailurePolicy when RestartPolicy is OnFailure", func(t *testing.T) {
		wmSpec := newTestWMSpec().build()
		wmSpec.PodTemplate = &corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				RestartPolicy: corev1.RestartPolicyOnFailure,
			},
		}
		translator := NewJobSetTranslator()
		controller := getTestController()
		job := newTestJob().build()

		obj, err := translator.Translate(job, wmSpec, controller)
		if err != nil {
			t.Fatalf("unexpected translation error: %v", err)
		}

		js := obj.(*jobsetv1alpha2.JobSet)
		repJob := js.Spec.ReplicatedJobs[0]
		if repJob.Template.Spec.PodFailurePolicy != nil {
			t.Errorf("expected nil PodFailurePolicy when RestartPolicy is OnFailure, got %v", repJob.Template.Spec.PodFailurePolicy)
		}
	})
}

func TestTranslateSlurmJobToJobSet_FailurePolicyMaxRestarts(t *testing.T) {
	tests := []struct {
		name            string
		job             *testJobBuilder
		wantMaxRestarts int32
	}{
		{
			name:            "Requeue unset keeps the default restart budget",
			job:             newTestJob(),
			wantMaxRestarts: 3,
		},
		{
			name:            "Requeue true keeps the default restart budget",
			job:             newTestJob().withRequeue(true),
			wantMaxRestarts: 3,
		},
		{
			name:            "Requeue false (--no-requeue) disables JobSet restarts",
			job:             newTestJob().withRequeue(false),
			wantMaxRestarts: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			js := translateTestJobSetWithJob(t, tt.job, newTestWMSpec())
			if js.Spec.FailurePolicy == nil {
				t.Fatalf("Translate() returned FailurePolicy = nil, want MaxRestarts = %d", tt.wantMaxRestarts)
			}
			if got := js.Spec.FailurePolicy.MaxRestarts; got != tt.wantMaxRestarts {
				t.Errorf("Translate() returned FailurePolicy.MaxRestarts = %d, want %d", got, tt.wantMaxRestarts)
			}
		})
	}
}

func TestTranslate_JobSetCarriesK8sBridgeLabels(t *testing.T) {
	js := translateTestJobSet(t, newTestWMSpec())

	want := map[string]string{
		wantLabelAppName:      wantAppName,
		wantLabelAppComponent: wantComponentJobSet,
		wantLabelAppManagedBy: wantAppName,
		wantLabelSlurmJobID:   "123",
		wantLabelSlurmPart:    testDefaultPartition,
		wantLabelKueueQueue:   testHighPrioQueue,
		wantLabelKueueClass:   testHighPrioClass,
	}
	if diff := cmp.Diff(want, js.Labels); diff != "" {
		t.Errorf("JobSet labels mismatch (-want +got):\n%s", diff)
	}
}

func TestTranslate_WorkerPodTemplateCarriesK8sBridgeLabels(t *testing.T) {
	js := translateTestJobSet(t, newTestWMSpec())

	got := workerPodLabels(t, js)
	want := map[string]string{
		wantLabelAppName:      wantAppName,
		wantLabelAppComponent: wantComponentWorker,
		wantLabelAppManagedBy: wantAppName,
		wantLabelSlurmJobID:   "123",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Worker pod labels mismatch (-want +got):\n%s", diff)
	}
}

func TestTranslate_UserPodTemplateLabelsCannotOverrideK8sBridgeLabels(t *testing.T) {
	const customLabel = "team.example.com/owner"

	wmSpec := newTestWMSpec().withPodTemplateLabels(map[string]string{
		wantLabelAppName:      "not-k8s-bridge",
		wantLabelAppComponent: "something-else",
		wantLabelAppManagedBy: "someone-else",
		wantLabelSlurmJobName: "user-supplied-name",
		customLabel:           "hpc",
	})

	js := translateTestJobSet(t, wmSpec)

	got := workerPodLabels(t, js)
	want := map[string]string{
		wantLabelAppName:      wantAppName,
		wantLabelAppComponent: wantComponentWorker,
		wantLabelAppManagedBy: wantAppName,
		wantLabelSlurmJobID:   "123",
		customLabel:           "hpc",
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Worker pod labels mismatch (-want +got):\n%s", diff)
	}
}

func TestSlurmJobNameLabelValue(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "Simple name", input: "training", want: "training"},
		{name: "Script filename", input: "train.sh", want: "train.sh"},
		{name: "Preserves underscores and dots", input: "my_job.v1", want: "my_job.v1"},
		{name: "Uppercase and spaces", input: "Train Model", want: "train-model"},
		{name: "Collapses consecutive dashes", input: "a   --b", want: "a-b"},
		{name: "Trims leading and trailing non-alphanumerics", input: "_.-wrap-._", want: "wrap"},
		{name: "Keeps leading digits", input: "2nd-run", want: "2nd-run"},
		{name: "Empty name", input: "", want: ""},
		{name: "Only invalid characters", input: "***", want: ""},
		{name: "Truncates to label limit", input: strings.Repeat("a", 100), want: strings.Repeat("a", 63)},
		{name: "Trims trailing non-alphanumerics after truncation", input: strings.Repeat("a", 62) + "-b", want: strings.Repeat("a", 62)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := slurmJobNameLabelValue(tt.input)
			if got != tt.want {
				t.Errorf("slurmJobNameLabelValue(%q) = %q, want %q", tt.input, got, tt.want)
			}
			if errs := validation.IsValidLabelValue(got); len(errs) > 0 {
				t.Errorf("slurmJobNameLabelValue(%q) = %q, not a valid label value: %v", tt.input, got, errs)
			}
		})
	}
}

func TestTranslateSlurmJobToJobSet_JobNameLabel(t *testing.T) {
	t.Run("Success - Slurm job name is set as a label on JobSet and Pods", func(t *testing.T) {
		js := translateTestJobSetWithJob(t, newTestJob().withName("Train Model.sh"), newTestWMSpec())

		wantJobSetLabels := map[string]string{
			wantLabelAppName:      wantAppName,
			wantLabelAppComponent: wantComponentJobSet,
			wantLabelAppManagedBy: wantAppName,
			wantLabelSlurmJobID:   "123",
			wantLabelSlurmJobName: "train-model.sh",
			wantLabelSlurmPart:    testDefaultPartition,
			wantLabelKueueQueue:   testHighPrioQueue,
			wantLabelKueueClass:   testHighPrioClass,
		}
		if diff := cmp.Diff(wantJobSetLabels, js.Labels); diff != "" {
			t.Errorf("JobSet labels mismatch (-want +got):\n%s", diff)
		}

		wantPodLabels := map[string]string{
			wantLabelAppName:      wantAppName,
			wantLabelAppComponent: wantComponentWorker,
			wantLabelAppManagedBy: wantAppName,
			wantLabelSlurmJobID:   "123",
			wantLabelSlurmJobName: "train-model.sh",
		}
		if diff := cmp.Diff(wantPodLabels, workerPodLabels(t, js)); diff != "" {
			t.Errorf("Worker pod labels mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("Success - Job name label propagated to custom pod template and overrides user value", func(t *testing.T) {
		wmSpec := newTestWMSpec().withPodTemplateLabels(map[string]string{
			wantLabelSlurmJobName:  "user-supplied-name",
			"team.example.com/env": "simulation-env",
		})
		js := translateTestJobSetWithJob(t, newTestJob().withName("simulation"), wmSpec)

		wantPodLabels := map[string]string{
			wantLabelAppName:       wantAppName,
			wantLabelAppComponent:  wantComponentWorker,
			wantLabelAppManagedBy:  wantAppName,
			wantLabelSlurmJobID:    "123",
			wantLabelSlurmJobName:  "simulation",
			"team.example.com/env": "simulation-env",
		}
		if diff := cmp.Diff(wantPodLabels, workerPodLabels(t, js)); diff != "" {
			t.Errorf("Worker pod labels mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestTranslateSlurmJobToJobSet_MaxNodeSize(t *testing.T) {
	translator := NewJobSetTranslator()
	controller := getTestController()

	tests := []struct {
		name         string
		cpuLimit     string // An empty value means no CPU limit.
		memLimit     string // An empty value means no memory limit.
		job          *slurmclienttypes.V0044JobInfo
		wantErr      string
		wantRequests corev1.ResourceList
	}{
		{
			name:     "Failure - -N1 -n8 needs more CPUs and memory than the limits",
			cpuLimit: "4",
			memLimit: testSlurmdMemoryLimit,
			job:      newTestJob().withTasks(8).build(),
			wantErr:  "job needs 8 CPUs and 16384 MiB per node, but the maximum node size of partition high-prio is 4 CPUs and 8192 MiB",
		},
		{
			name:     "Failure - -N1 -n2 --mem=16G needs more memory than the limit",
			cpuLimit: "4",
			memLimit: testSlurmdMemoryLimit,
			job:      newTestJob().withTasks(2).withMemoryPerNode(16384).build(),
			wantErr:  "job needs 2 CPUs and 16384 MiB per node, but the maximum node size of partition high-prio is 4 CPUs and 8192 MiB",
		},
		{
			name:     "Failure - -N1 -n4 default memory is above a 6Gi limit",
			cpuLimit: "4",
			memLimit: "6Gi",
			job:      newTestJob().withTasks(4).build(),
			wantErr:  "job needs 4 CPUs and 8192 MiB per node, but the maximum node size of partition high-prio is 4 CPUs and 6144 MiB",
		},
		{
			name:     "Failure - Only a memory limit names only the memory limit",
			memLimit: testSlurmdMemoryLimit,
			job:      newTestJob().withTasks(8).build(),
			wantErr:  "job needs 8 CPUs and 16384 MiB per node, but the maximum node size of partition high-prio is 8192 MiB",
		},
		{
			name:         "Success - -N1 -n4 fits the limits exactly",
			cpuLimit:     "4",
			memLimit:     testSlurmdMemoryLimit,
			job:          newTestJob().withTasks(4).build(),
			wantRequests: cpuMemory("4", "8192Mi"),
		},
		{
			name:         "Success - Only a CPU limit does not check memory",
			cpuLimit:     "4",
			job:          newTestJob().withTasks(2).withMemoryPerNode(16384).build(),
			wantRequests: cpuMemory("2", "16384Mi"),
		},
		{
			name:         "Success - Without slurmd limits there is no maximum node size",
			job:          newTestJob().withTasks(48).build(),
			wantRequests: cpuMemory("48", "98304Mi"),
		},
		{
			// 1 CPU rounds up to 1 core, which is 2 CPUs with the default threadsPerCore.
			name:     "Success - The check does not compare GPUs",
			cpuLimit: "4",
			memLimit: testSlurmdMemoryLimit,
			job:      newTestJob().withTresPerNode("gres/gpu:l4=1").build(),
			wantRequests: corev1.ResourceList{
				corev1.ResourceCPU:                       resource.MustParse("2"),
				corev1.ResourceMemory:                    resource.MustParse("4096Mi"),
				corev1.ResourceName(testGpuResourceName): resource.MustParse("1"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wmSpec := newTestWMSpec().withContainers(slurmdWithLimits(tt.cpuLimit, tt.memLimit)).build()
			obj, err := translator.Translate(tt.job, wmSpec, controller)
			if tt.wantErr != "" {
				verifyTranslationError(t, obj, err, tt.wantErr)
				return
			}
			verifyJobSet(t, obj, err, "123", 1, withRequests(tt.wantRequests), withLimits(tt.wantRequests))
		})
	}
}

func TestFormatCPUs(t *testing.T) {
	for quantity, want := range map[string]string{"1": "1 CPU", "4": "4 CPUs", "3500m": "3.5 CPUs"} {
		if got := formatCPUs(resource.MustParse(quantity)); got != want {
			t.Errorf("formatCPUs(%q) = %q, want %q", quantity, got, want)
		}
	}
}

func TestFormatMiB(t *testing.T) {
	for quantity, want := range map[string]string{"4Gi": "4096 MiB", "16384Mi": "16384 MiB", "1G": "1G"} {
		if got := formatMiB(resource.MustParse(quantity)); got != want {
			t.Errorf("formatMiB(%q) = %q, want %q", quantity, got, want)
		}
	}
}

func TestTranslateSlurmJobToJobSet_MemoryOption(t *testing.T) {
	translator := NewJobSetTranslator()
	controller := getTestController()
	wmSpec := getTestWMSpec()

	tests := []struct {
		name       string
		job        *slurmclienttypes.V0044JobInfo
		wantMemMiB string
	}{
		{name: "--mem below the default", job: newTestJob().withTasks(4).withMemoryPerNode(4096).build(), wantMemMiB: "4096"},
		{name: "--mem=0 gets the default", job: newTestJob().withTasks(4).withMemoryPerNode(0).build(), wantMemMiB: "8192"},
		{name: "No memory option gets the default", job: newTestJob().withTasks(4).build(), wantMemMiB: "8192"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj, err := translator.Translate(tt.job, wmSpec, controller)
			verifyJobSet(t, obj, err, "123", 1,
				withRequests(corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("4"),
					corev1.ResourceMemory: resource.MustParse(tt.wantMemMiB + "Mi"),
				}),
				withEnv(map[string]string{envPodCPUs: "4", envPodMemory: tt.wantMemMiB}),
			)
		})
	}
}
