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
	"fmt"
	"maps"
	"strings"

	apivalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

const (
	extraEntrySeparator    = ","
	extraKeyValueSeparator = "="
)

// parseExtraAnnotations parses the Slurm --extra string into PodTemplate annotations.
//
// The format is a flat, comma-separated list of key=value entries, for example
// "kueue.x-k8s.io/podset-required-topology=cloud.google.com/gce-topology-block,foo=bar".
// Whitespace around separators, keys and values is trimmed and empty entries are skipped.
// An empty or whitespace-only string yields no annotations. Values may be empty, keys may not.
// Slurm itself limits --extra to 1024 bytes, so its length is not checked here.
func parseExtraAnnotations(raw string) (map[string]string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}

	annotations := make(map[string]string)
	for entry := range strings.SplitSeq(trimmed, extraEntrySeparator) {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		key, value, found := strings.Cut(entry, extraKeyValueSeparator)
		if !found {
			return nil, fmt.Errorf("%q is not in key=value format", entry)
		}
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("%q has an empty key", entry)
		}
		if _, dup := annotations[key]; dup {
			return nil, fmt.Errorf("duplicate key %q", key)
		}
		annotations[key] = strings.TrimSpace(value)
	}
	if len(annotations) == 0 {
		return nil, nil
	}
	return annotations, nil
}

// mergeExtraAnnotations returns base annotations overridden by extra annotations with the same
// key. The base map is not modified.
func mergeExtraAnnotations(base, extra map[string]string) map[string]string {
	merged := maps.Clone(base)
	if len(extra) == 0 {
		return merged
	}
	if merged == nil {
		merged = make(map[string]string, len(extra))
	}
	maps.Copy(merged, extra)
	return merged
}

// applyExtraAnnotations merges the annotations parsed from --extra into the PodTemplate metadata
// and validates the merged result as Kubernetes object annotations. Annotations from
// WorkloadMixing.spec.podTemplate are trusted and not validated when --extra is empty.
func applyExtraAnnotations(meta *metav1.ObjectMeta, extra map[string]string) error {
	if len(extra) == 0 {
		return nil
	}
	merged := mergeExtraAnnotations(meta.Annotations, extra)
	if errs := apivalidation.ValidateAnnotations(merged, field.NewPath("metadata", "annotations")); len(errs) > 0 {
		return errs.ToAggregate()
	}
	meta.Annotations = merged
	return nil
}
