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
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/SlinkyProject/slurm-client/pkg/client"
	"github.com/SlinkyProject/slurm-client/pkg/object"
	slurmclienttypes "github.com/SlinkyProject/slurm-client/pkg/types"
	"k8s.io/utils/ptr"
)

// Assert that SlurmClient implements client.Client interface.
var _ client.Client = (*SlurmClient)(nil)

type JobUpdate struct {
	JobID int32
	Req   any
}

type SlurmClient struct {
	lock        sync.RWMutex
	server      string
	token       string
	jobs        map[string][]slurmclienttypes.V0044JobInfo
	deletedJobs []int32
	updatedJobs []JobUpdate
	GetError    error
}

func (s *SlurmClient) Start(ctx context.Context) {
}

func (s *SlurmClient) Stop() {
}

func (s *SlurmClient) SetServer(server string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.server = server
}

func (s *SlurmClient) SetToken(token string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.token = token
}

func (s *SlurmClient) GetServer() string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.server
}

func (s *SlurmClient) GetToken() string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.token
}

func (s *SlurmClient) GetInformer(objType object.ObjectType) client.InformerCache {
	return nil
}

func (s *SlurmClient) Create(ctx context.Context, obj object.Object, req any, opts ...client.CreateOption) error {
	return nil
}

func (s *SlurmClient) Get(
	ctx context.Context, key object.ObjectKey, obj object.Object, opts ...client.GetOption) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.GetError != nil {
		return s.GetError
	}

	jobID := string(key)
	if jobInfo, ok := obj.(*slurmclienttypes.V0044JobInfo); ok {
		for _, jobs := range s.jobs {
			for _, j := range jobs {
				if j.JobId != nil && fmt.Sprintf("%d", *j.JobId) == jobID {
					*jobInfo = j
					return nil
				}
			}
		}
		return fmt.Errorf("job %s not found", jobID)
	}
	return fmt.Errorf("unsupported object type: %T", obj)
}

func (s *SlurmClient) List(ctx context.Context, list object.ObjectList, opts ...client.ListOption) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if jobList, ok := list.(*slurmclienttypes.V0044JobInfoList); ok {
		for _, jobs := range s.jobs {
			jobList.Items = append(jobList.Items, jobs...)
		}
	}
	return nil
}

func (s *SlurmClient) Update(ctx context.Context, obj object.Object, req any, opts ...client.UpdateOption) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if jobInfo, ok := obj.(*slurmclienttypes.V0044JobInfo); ok {
		jobID := ptr.Deref(jobInfo.JobId, 0)
		s.updatedJobs = append(s.updatedJobs, JobUpdate{
			JobID: jobID,
			Req:   req,
		})
		return nil
	}
	return fmt.Errorf("unsupported object type: %T", obj)
}

func (s *SlurmClient) Delete(ctx context.Context, obj object.Object, opts ...client.DeleteOption) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if jobInfo, ok := obj.(*slurmclienttypes.V0044JobInfo); ok {
		jobID := ptr.Deref(jobInfo.JobId, 0)
		s.deletedJobs = append(s.deletedJobs, jobID)
		for part, jobs := range s.jobs {
			var newJobs []slurmclienttypes.V0044JobInfo
			for _, j := range jobs {
				if ptr.Deref(j.JobId, 0) != jobID {
					newJobs = append(newJobs, j)
				}
			}
			s.jobs[part] = newJobs
		}
		return nil
	}
	return fmt.Errorf("unsupported object type: %T", obj)
}

func (s *SlurmClient) SetPendingJobs(partition string, jobs []slurmclienttypes.V0044JobInfo) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.jobs == nil {
		s.jobs = make(map[string][]slurmclienttypes.V0044JobInfo)
	}
	s.jobs[partition] = slices.Clone(jobs)
}

func (s *SlurmClient) GetDeletedJobs() []int32 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return slices.Clone(s.deletedJobs)
}

func (s *SlurmClient) GetUpdatedJobs() []JobUpdate {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return slices.Clone(s.updatedJobs)
}
