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

package fc

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// capturedRequest records one API call the fake VMM received.
type capturedRequest struct {
	method string
	path   string
	body   map[string]any
}

// fakeVMM serves the Firecracker REST API over a unix socket, recording every
// request and answering 204, the way Firecracker answers a configuration write.
type fakeVMM struct {
	mu       sync.Mutex
	requests []capturedRequest
}

func (f *fakeVMM) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if b, _ := io.ReadAll(r.Body); len(b) > 0 {
		_ = json.Unmarshal(b, &body)
	}
	f.mu.Lock()
	f.requests = append(f.requests, capturedRequest{method: r.Method, path: r.URL.Path, body: body})
	f.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

func (f *fakeVMM) last() capturedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[len(f.requests)-1]
}

// startFakeVMM serves fakeVMM on a unix socket and returns a Client bound to it.
func startFakeVMM(t *testing.T) (*Client, *fakeVMM) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "fc.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeVMM{}
	srv := &http.Server{Handler: f}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { _ = srv.Close() })
	return NewClient(sock), f
}

func TestClientConfigWireFormat(t *testing.T) {
	c, f := startFakeVMM(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tests := []struct {
		name       string
		call       func() error
		wantMethod string
		wantPath   string
		wantBody   map[string]any
	}{
		{
			name:       "machine-config",
			call:       func() error { return c.PutMachineConfig(ctx, MachineConfig{VCPUCount: 2, MemSizeMiB: 512}) },
			wantMethod: http.MethodPut,
			wantPath:   "/machine-config",
			wantBody:   map[string]any{"vcpu_count": float64(2), "mem_size_mib": float64(512), "smt": false},
		},
		{
			name: "boot-source",
			call: func() error {
				return c.PutBootSource(ctx, BootSource{KernelImagePath: "/k/vmlinux", BootArgs: "root=/dev/vda1"})
			},
			wantMethod: http.MethodPut,
			wantPath:   "/boot-source",
			wantBody:   map[string]any{"kernel_image_path": "/k/vmlinux", "boot_args": "root=/dev/vda1"},
		},
		{
			name: "root drive",
			call: func() error {
				return c.PutDrive(ctx, Drive{DriveID: "rootfs", PathOnHost: "/img", IsRootDevice: true, IsReadOnly: true})
			},
			wantMethod: http.MethodPut,
			wantPath:   "/drives/rootfs",
			wantBody:   map[string]any{"drive_id": "rootfs", "path_on_host": "/img", "is_root_device": true, "is_read_only": true},
		},
		{
			name: "network interface",
			call: func() error {
				return c.PutNetworkInterface(ctx, NetworkInterface{IfaceID: "eth0", HostDevName: "tap0", GuestMAC: "02:a8:1e:00:00:02"})
			},
			wantMethod: http.MethodPut,
			wantPath:   "/network-interfaces/eth0",
			wantBody:   map[string]any{"iface_id": "eth0", "host_dev_name": "tap0", "guest_mac": "02:a8:1e:00:00:02"},
		},
		{
			name:       "vsock",
			call:       func() error { return c.PutVsock(ctx, Vsock{GuestCID: 3, UDSPath: "/v.sock"}) },
			wantMethod: http.MethodPut,
			wantPath:   "/vsock",
			wantBody:   map[string]any{"guest_cid": float64(3), "uds_path": "/v.sock"},
		},
		{
			name:       "start action",
			call:       func() error { return c.Start(ctx) },
			wantMethod: http.MethodPut,
			wantPath:   "/actions",
			wantBody:   map[string]any{"action_type": "InstanceStart"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); err != nil {
				t.Fatalf("call: %v", err)
			}
			got := f.last()
			if got.method != tc.wantMethod || got.path != tc.wantPath {
				t.Errorf("request = %s %s, want %s %s", got.method, got.path, tc.wantMethod, tc.wantPath)
			}
			for k, want := range tc.wantBody {
				if got.body[k] != want {
					t.Errorf("body[%q] = %v (%T), want %v (%T)", k, got.body[k], got.body[k], want, want)
				}
			}
		})
	}
}

// TestPutDriveRequiresID pins the client-side guard: an empty drive id must fail
// before hitting the API, since Firecracker keys the drive resource by id.
func TestPutDriveRequiresID(t *testing.T) {
	c, f := startFakeVMM(t)
	if err := c.PutDrive(context.Background(), Drive{PathOnHost: "/img"}); err == nil {
		t.Fatal("PutDrive with empty DriveID: want error, got nil")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) != 0 {
		t.Errorf("empty-id PutDrive reached the API (%d requests)", len(f.requests))
	}
}
