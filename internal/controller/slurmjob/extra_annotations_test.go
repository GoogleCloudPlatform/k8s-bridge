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
	"maps"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

const (
	testAnnotationRequiredTopology  = "kueue.x-k8s.io/podset-required-topology"
	testAnnotationPreferredTopology = "kueue.x-k8s.io/podset-preferred-topology"
	testAnnotationSliceTopology     = "kueue.x-k8s.io/podset-slice-required-topology"
	testAnnotationSliceSize         = "kueue.x-k8s.io/podset-slice-size"
	testTopologyBlock               = "cloud.google.com/gce-topology-block"
	testTopologySubblock            = "cloud.google.com/gce-topology-subblock"
	testAdminAnnotation             = "admin.example.com/keep"
	testAdminValue                  = "yes"
	testAnnotationsField            = "metadata.annotations"
)

func TestParseExtraAnnotations(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    map[string]string
		wantErr string
	}{
		{name: "empty", raw: "", want: nil},
		{name: "whitespace only", raw: "  \t ", want: nil},
		{name: "only separators", raw: " , ,, ", want: nil},
		{
			name: "single entry",
			raw:  testAnnotationRequiredTopology + "=" + testTopologySubblock,
			want: map[string]string{testAnnotationRequiredTopology: testTopologySubblock},
		},
		{
			name: "multiple entries",
			raw: testAnnotationRequiredTopology + "=" + testTopologyBlock + "," +
				testAnnotationSliceTopology + "=" + testTopologySubblock + "," +
				testAnnotationSliceSize + "=18",
			want: map[string]string{
				testAnnotationRequiredTopology: testTopologyBlock,
				testAnnotationSliceTopology:    testTopologySubblock,
				testAnnotationSliceSize:        "18",
			},
		},
		{
			name: "whitespace around separators keys and values is trimmed",
			raw:  "  a.example.com/k1 =  v1 ,\tk2= v2  ",
			want: map[string]string{"a.example.com/k1": "v1", "k2": "v2"},
		},
		{name: "trailing comma is skipped", raw: "k1=v1,", want: map[string]string{"k1": "v1"}},
		{name: "leading comma is skipped", raw: ",k1=v1", want: map[string]string{"k1": "v1"}},
		{name: "empty entries in the middle are skipped", raw: "k1=v1, ,,k2=v2", want: map[string]string{"k1": "v1", "k2": "v2"}},
		{name: "empty value is allowed", raw: "k1=", want: map[string]string{"k1": ""}},
		{name: "value may contain equals sign", raw: "k1=a=b", want: map[string]string{"k1": "a=b"}},
		{name: "JSON object is not in key=value format", raw: `{"k":"v"}`, wantErr: `"{\"k\":\"v\"}" is not in key=value format`},
		{name: "bare token", raw: "k1=v1,bare", wantErr: `"bare" is not in key=value format`},
		{name: "empty key", raw: "k1=v1, =v2", wantErr: `"=v2" has an empty key`},
		{name: "duplicate key", raw: "k1=v1,k1=v2", wantErr: `duplicate key "k1"`},
		{name: "duplicate key after trimming", raw: "k1=v1, k1 =v2", wantErr: `duplicate key "k1"`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseExtraAnnotations(tc.raw)
			if tc.wantErr != "" {
				verifyError(t, err, tc.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("parseExtraAnnotations(%q) returned unexpected error: %v", tc.raw, err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("parseExtraAnnotations(%q) mismatch (-want +got):\n%s", tc.raw, diff)
			}
		})
	}
}

func TestMergeExtraAnnotations(t *testing.T) {
	tests := []struct {
		name  string
		base  map[string]string
		extra map[string]string
		want  map[string]string
	}{
		{
			name:  "nil base",
			extra: map[string]string{"k1": "v1"},
			want:  map[string]string{"k1": "v1"},
		},
		{
			name: "empty extra keeps base",
			base: map[string]string{"k1": "v1"},
			want: map[string]string{"k1": "v1"},
		},
		{
			name:  "same key is overridden",
			base:  map[string]string{testAnnotationPreferredTopology: testTopologyBlock},
			extra: map[string]string{testAnnotationPreferredTopology: testTopologySubblock},
			want:  map[string]string{testAnnotationPreferredTopology: testTopologySubblock},
		},
		{
			name:  "different keys are kept, even conflicting Kueue topology modes",
			base:  map[string]string{testAnnotationPreferredTopology: testTopologyBlock, testAdminAnnotation: testAdminValue},
			extra: map[string]string{testAnnotationRequiredTopology: testTopologySubblock},
			want: map[string]string{
				testAnnotationPreferredTopology: testTopologyBlock,
				testAnnotationRequiredTopology:  testTopologySubblock,
				testAdminAnnotation:             testAdminValue,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			baseBefore := maps.Clone(tc.base)
			got := mergeExtraAnnotations(tc.base, tc.extra)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("mergeExtraAnnotations() mismatch (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(baseBefore, tc.base); diff != "" {
				t.Errorf("mergeExtraAnnotations() modified base (-before +after):\n%s", diff)
			}
		})
	}
}

func TestApplyExtraAnnotations(t *testing.T) {
	tests := []struct {
		name    string
		base    map[string]string
		extra   map[string]string
		want    map[string]string
		wantErr string
	}{
		{
			name: "no extra annotations leaves metadata untouched",
			base: map[string]string{"k1": "v1"},
			want: map[string]string{"k1": "v1"},
		},
		{
			name: "admin annotations are not validated without extra annotations",
			base: map[string]string{"invalid admin key": "v"},
			want: map[string]string{"invalid admin key": "v"},
		},
		{
			name:  "valid annotations are applied",
			base:  map[string]string{testAdminAnnotation: testAdminValue},
			extra: map[string]string{testAnnotationSliceSize: "2"},
			want:  map[string]string{testAdminAnnotation: testAdminValue, testAnnotationSliceSize: "2"},
		},
		{
			name:    "key with space is rejected",
			extra:   map[string]string{"foo bar": "v"},
			wantErr: testAnnotationsField,
		},
		{
			name:    "key with invalid prefix is rejected",
			extra:   map[string]string{"-bad.example.com/name": "v"},
			wantErr: testAnnotationsField,
		},
		{
			name:    "key with too long prefix is rejected",
			extra:   map[string]string{strings.Repeat("a", 254) + "/name": "v"},
			wantErr: testAnnotationsField,
		},
		{
			name:    "merged annotations over the total size limit are rejected",
			base:    map[string]string{testAdminAnnotation: strings.Repeat("v", 256*1024)},
			extra:   map[string]string{"k1": "v1"},
			wantErr: testAnnotationsField + ": Too long",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			meta := &metav1.ObjectMeta{Annotations: maps.Clone(tc.base)}
			err := applyExtraAnnotations(meta, tc.extra)
			if tc.wantErr != "" {
				verifyError(t, err, tc.wantErr)
				if diff := cmp.Diff(tc.base, meta.Annotations); diff != "" {
					t.Errorf("applyExtraAnnotations() modified annotations on error (-want +got):\n%s", diff)
				}
				return
			}
			if err != nil {
				t.Fatalf("applyExtraAnnotations() returned unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.want, meta.Annotations); diff != "" {
				t.Errorf("applyExtraAnnotations() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestTranslateSlurmJobToJobSet_ExtraAnnotations(t *testing.T) {
	translator := NewJobSetTranslator()
	controller := getTestController()
	requiredSubblock := testAnnotationRequiredTopology + "=" + testTopologySubblock

	tests := []struct {
		name            string
		wmSpec          *testWMSpecBuilder
		job             *testJobBuilder
		wantAnnotations map[string]string
		wantErr         string
		wantSkipped     bool
	}{
		{
			name:            "no extra and no podTemplate yields no annotations",
			wmSpec:          newTestWMSpec(),
			job:             newTestJob(),
			wantAnnotations: nil,
		},
		{
			name:            "extra without podTemplate",
			wmSpec:          newTestWMSpec(),
			job:             newTestJob().withExtra(requiredSubblock),
			wantAnnotations: map[string]string{testAnnotationRequiredTopology: testTopologySubblock},
		},
		{
			name:            "whitespace-only extra keeps podTemplate annotations",
			wmSpec:          newTestWMSpec().withPodTemplateAnnotations(map[string]string{testAnnotationPreferredTopology: testTopologyBlock}),
			job:             newTestJob().withExtra("   "),
			wantAnnotations: map[string]string{testAnnotationPreferredTopology: testTopologyBlock},
		},
		{
			name: "extra overrides same-key podTemplate annotation and keeps the others",
			wmSpec: newTestWMSpec().withPodTemplateAnnotations(map[string]string{
				testAnnotationRequiredTopology: testTopologyBlock,
				testAdminAnnotation:            testAdminValue,
			}),
			job: newTestJob().withExtra(requiredSubblock),
			wantAnnotations: map[string]string{
				testAnnotationRequiredTopology: testTopologySubblock,
				testAdminAnnotation:            testAdminValue,
			},
		},
		{
			name:    "invalid extra syntax on mapped partition fails translation",
			wmSpec:  newTestWMSpec(),
			job:     newTestJob().withExtra(requiredSubblock + ",oops"),
			wantErr: `invalid --extra: "oops" is not in key=value format`,
		},
		{
			name:    "invalid annotation key on mapped partition fails translation",
			wmSpec:  newTestWMSpec(),
			job:     newTestJob().withExtra("not a key=v"),
			wantErr: "invalid PodTemplate annotations: " + testAnnotationsField,
		},
		{
			name:        "invalid extra on unmapped partition is skipped",
			wmSpec:      newTestWMSpec(),
			job:         newTestJob().withPartition("unmapped").withExtra("{bad"),
			wantSkipped: true,
		},
		{
			name:        "invalid extra on explicitly held job is deferred",
			wmSpec:      newTestWMSpec(),
			job:         newTestJob().withHold(true).withStateReason(ReasonJobHeldUser).withExtra("{bad"),
			wantSkipped: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			obj, err := translator.Translate(tc.job.build(), tc.wmSpec.build(), controller)
			switch {
			case tc.wantErr != "":
				verifyTranslationError(t, obj, err, tc.wantErr)
				return
			case tc.wantSkipped:
				if err != nil || obj != nil {
					t.Fatalf("Translate() = (%v, %v), want (nil, nil)", obj, err)
				}
				return
			}
			verifyJobSet(t, obj, err, "123", 1)
			js := obj.(*jobsetv1alpha2.JobSet)
			got := js.Spec.ReplicatedJobs[0].Template.Spec.Template.Annotations
			if diff := cmp.Diff(tc.wantAnnotations, got); diff != "" {
				t.Errorf("PodTemplate annotations mismatch (-want +got):\n%s", diff)
			}
			if js.Annotations != nil {
				t.Errorf("JobSet annotations = %v, want none", js.Annotations)
			}
		})
	}
}
