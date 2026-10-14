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

package clientmap

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	"github.com/SlinkyProject/slurm-client/pkg/client"
	"github.com/SlinkyProject/slurm-client/pkg/client/fake"
)

const (
	existingNamespacedName = "existing namespaced name"
	testNamespace          = "default"
	testFooName            = "foo"
	testBarName            = "bar"
)

func TestNewClientMap(t *testing.T) {
	tests := []struct {
		name string
		want *ClientMap
	}{
		{
			name: "Test new clusters",
			want: &ClientMap{
				clients: make(map[types.NamespacedName]clientEntry),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewClientMap(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewClientMap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientMap_Get(t *testing.T) {
	testClient := fake.NewFakeClient()
	fooKey := types.NamespacedName{Namespace: testNamespace, Name: testFooName}
	c := make(map[types.NamespacedName]clientEntry)
	c[fooKey] = clientEntry{cli: testClient, cancel: func() {}}
	type fields struct {
		clients map[types.NamespacedName]clientEntry
	}
	type args struct {
		name types.NamespacedName
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   client.Client
	}{
		{
			name: existingNamespacedName,
			fields: fields{
				clients: c,
			},
			args: args{
				name: types.NamespacedName{
					Namespace: testNamespace,
					Name:      testFooName,
				},
			},
			want: testClient,
		},
		{
			name: "incorrect namespaced name",
			fields: fields{
				clients: c,
			},
			args: args{
				name: types.NamespacedName{
					Namespace: testNamespace,
					Name:      testBarName,
				},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClientMap{
				lock:    sync.RWMutex{},
				clients: tt.fields.clients,
			}
			if got := c.Get(tt.args.name); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClientMap.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientMap_add(t *testing.T) {
	testClient := fake.NewFakeClient()
	fooKey := types.NamespacedName{Namespace: testNamespace, Name: testFooName}
	c := make(map[types.NamespacedName]clientEntry)
	c[fooKey] = clientEntry{cli: testClient, cancel: func() {}}
	type fields struct {
		clients map[types.NamespacedName]clientEntry
	}
	type args struct {
		name   types.NamespacedName
		client client.Client
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "Already has NamespacedName",
			fields: fields{
				clients: c,
			},
			args: args{
				name: types.NamespacedName{
					Name:      testFooName,
					Namespace: testNamespace,
				},
				client: testClient,
			},
			want: false,
		},
		{
			name: "Add a new NamespacedName",
			fields: fields{
				clients: c,
			},
			args: args{
				name: types.NamespacedName{
					Name:      testBarName,
					Namespace: testNamespace,
				},
				client: fake.NewFakeClient(),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClientMap{
				lock:    sync.RWMutex{},
				clients: tt.fields.clients,
			}
			if got := c.add(context.TODO(), tt.args.name, tt.args.client, time.Time{}); got != tt.want {
				t.Errorf("ClientMap.add() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientMap_Add(t *testing.T) {
	testClient := fake.NewFakeClient()
	fooKey := types.NamespacedName{Namespace: testNamespace, Name: testFooName}
	c := make(map[types.NamespacedName]clientEntry)
	c[fooKey] = clientEntry{cli: testClient, cancel: func() {}}
	type fields struct {
		clients map[types.NamespacedName]clientEntry
	}
	type args struct {
		name   types.NamespacedName
		client client.Client
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "Already has NamespacedName",
			fields: fields{
				clients: c,
			},
			args: args{
				name: types.NamespacedName{
					Name:      testFooName,
					Namespace: testNamespace,
				},
				client: fake.NewFakeClient(),
			},
			want: true,
		},
		{
			name: "Add a new NamespacedName",
			fields: fields{
				clients: c,
			},
			args: args{
				name: types.NamespacedName{
					Name:      testBarName,
					Namespace: testNamespace,
				},
				client: fake.NewFakeClient(),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClientMap{
				lock:    sync.RWMutex{},
				clients: tt.fields.clients,
			}
			if got := c.Add(context.TODO(), tt.args.name, tt.args.client); got != tt.want {
				t.Errorf("ClientMap.Add() = %v, want %v", got, tt.want)
			}

			if got := c.Get(tt.args.name); got != tt.args.client {
				t.Errorf("ClientMap.Get() after Add() = %p, want %p", got, tt.args.client)
			}
		})
	}
}

func TestClientMap_Has(t *testing.T) {
	testClient := fake.NewFakeClient()
	c := make(map[types.NamespacedName]clientEntry)
	foo := types.NamespacedName{
		Namespace: testNamespace,
		Name:      testFooName,
	}
	bar := types.NamespacedName{
		Namespace: testNamespace,
		Name:      testBarName,
	}
	c[foo] = clientEntry{cli: testClient, cancel: func() {}}

	type fields struct {
		clients map[types.NamespacedName]clientEntry
	}
	type args struct {
		names []types.NamespacedName
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "Does not have NamespacedName",
			fields: fields{
				clients: c,
			},
			args: args{
				names: append([]types.NamespacedName{}, bar),
			},
			want: false,
		},
		{
			name: "Has NamespacedName",
			fields: fields{
				clients: c,
			},
			args: args{
				names: append([]types.NamespacedName{}, bar, foo),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClientMap{
				lock:    sync.RWMutex{},
				clients: tt.fields.clients,
			}
			if got := c.Has(tt.args.names...); got != tt.want {
				t.Errorf("ClientMap.Has() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientMap_Remove(t *testing.T) {
	testClient := fake.NewFakeClient()
	fooKey := types.NamespacedName{Namespace: testNamespace, Name: testFooName}
	c := make(map[types.NamespacedName]clientEntry)
	c[fooKey] = clientEntry{cli: testClient, cancel: func() {}}
	type fields struct {
		clients map[types.NamespacedName]clientEntry
	}
	type args struct {
		name types.NamespacedName
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "Remove client that exists",
			fields: fields{
				clients: c,
			},
			args: args{
				name: types.NamespacedName{
					Name:      testFooName,
					Namespace: testNamespace,
				},
			},
			want: true,
		},
		{
			name: "Remove client that does not exists",
			fields: fields{
				clients: c,
			},
			args: args{
				name: types.NamespacedName{
					Name:      testBarName,
					Namespace: testNamespace,
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClientMap{
				lock:    sync.RWMutex{},
				clients: tt.fields.clients,
			}
			if got := c.Remove(tt.args.name); got != tt.want {
				t.Errorf("ClientMap.Remove() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientMap_GetExpiration(t *testing.T) {
	testClient := fake.NewFakeClient()
	fooKey := types.NamespacedName{Namespace: testNamespace, Name: testFooName}
	now := time.Now()
	c := make(map[types.NamespacedName]clientEntry)
	c[fooKey] = clientEntry{cli: testClient, cancel: func() {}, expiration: now}

	type fields struct {
		clients map[types.NamespacedName]clientEntry
	}
	type args struct {
		name types.NamespacedName
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   time.Time
	}{
		{
			name: existingNamespacedName,
			fields: fields{
				clients: c,
			},
			args: args{
				name: fooKey,
			},
			want: now,
		},
		{
			name: "non-existing namespaced name",
			fields: fields{
				clients: c,
			},
			args: args{
				name: types.NamespacedName{Namespace: testNamespace, Name: testBarName},
			},
			want: time.Time{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClientMap{
				lock:    sync.RWMutex{},
				clients: tt.fields.clients,
			}
			if got := c.GetExpiration(tt.args.name); !got.Equal(tt.want) {
				t.Errorf("ClientMap.GetExpiration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClientMap_GetClientAndExpiration(t *testing.T) {
	testClient := fake.NewFakeClient()
	fooKey := types.NamespacedName{Namespace: testNamespace, Name: testFooName}
	now := time.Now()
	c := make(map[types.NamespacedName]clientEntry)
	c[fooKey] = clientEntry{cli: testClient, cancel: func() {}, expiration: now}

	type fields struct {
		clients map[types.NamespacedName]clientEntry
	}
	type args struct {
		name types.NamespacedName
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantClient client.Client
		wantExpiry time.Time
	}{
		{
			name: existingNamespacedName,
			fields: fields{
				clients: c,
			},
			args: args{
				name: fooKey,
			},
			wantClient: testClient,
			wantExpiry: now,
		},
		{
			name: "non-existing namespaced name",
			fields: fields{
				clients: c,
			},
			args: args{
				name: types.NamespacedName{Namespace: testNamespace, Name: testBarName},
			},
			wantClient: nil,
			wantExpiry: time.Time{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ClientMap{
				lock:    sync.RWMutex{},
				clients: tt.fields.clients,
			}
			gotClient, gotExpiry := c.GetClientAndExpiration(tt.args.name)
			if !reflect.DeepEqual(gotClient, tt.wantClient) {
				t.Errorf("ClientMap.GetClientAndExpiration() gotClient = %v, want %v", gotClient, tt.wantClient)
			}
			if !gotExpiry.Equal(tt.wantExpiry) {
				t.Errorf("ClientMap.GetClientAndExpiration() gotExpiry = %v, want %v", gotExpiry, tt.wantExpiry)
			}
		})
	}
}
