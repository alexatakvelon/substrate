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
	"fmt"
)

// vmStateRequest is the body of PATCH /vm.
type vmStateRequest struct {
	State string `json:"state"` // "Paused" or "Resumed"
}

// PauseVM pauses the running guest (PATCH /vm {Paused}). A snapshot must be
// taken while paused so guest memory and device state are consistent.
func (c *Client) PauseVM(ctx context.Context) error {
	if err := c.api.patch(ctx, "/vm", vmStateRequest{State: "Paused"}); err != nil {
		return fmt.Errorf("PATCH /vm Paused: %w", err)
	}
	return nil
}

// ResumeVM unpauses the guest (PATCH /vm {Resumed}). Used to undo PauseVM when a
// snapshot is taken of an actor that keeps running, and after a snapshot load
// that did not resume inline.
func (c *Client) ResumeVM(ctx context.Context) error {
	if err := c.api.patch(ctx, "/vm", vmStateRequest{State: "Resumed"}); err != nil {
		return fmt.Errorf("PATCH /vm Resumed: %w", err)
	}
	return nil
}

// SnapshotType selects a full or diff memory snapshot. Diff snapshots require the
// VM to have been booted with MachineConfig.TrackDirtyPages.
type SnapshotType string

const (
	SnapshotFull SnapshotType = "Full"
	SnapshotDiff SnapshotType = "Diff"
)

// CreateSnapshotConfig is the body of PUT /snapshot/create: where to write the
// device/vm state file and the guest memory file. The VM must be Paused first.
type CreateSnapshotConfig struct {
	SnapshotType SnapshotType `json:"snapshot_type,omitempty"`
	SnapshotPath string       `json:"snapshot_path"`
	MemFilePath  string       `json:"mem_file_path"`
}

// CreateSnapshot writes a snapshot of the paused guest (PUT /snapshot/create):
// SnapshotPath receives the VM/device state, MemFilePath the guest RAM.
func (c *Client) CreateSnapshot(ctx context.Context, cfg CreateSnapshotConfig) error {
	if cfg.SnapshotType == "" {
		cfg.SnapshotType = SnapshotFull
	}
	if err := c.api.put(ctx, "/snapshot/create", cfg); err != nil {
		return fmt.Errorf("PUT /snapshot/create: %w", err)
	}
	return nil
}

// memBackend is the mem_backend field of PUT /snapshot/load. BackendType "File"
// eagerly loads guest RAM from BackendPath; "Uffd" hands memory faults to a
// userfaultfd handler over a unix socket (the fast, demand-paged path -- a
// follow-up; File backing is used today).
type memBackend struct {
	BackendType string `json:"backend_type"`
	BackendPath string `json:"backend_path"`
}

// LoadSnapshotConfig is the body of PUT /snapshot/load. It is issued against a
// freshly launched (unconfigured) VMM: the snapshot restores the machine config,
// devices and memory. The drive backing files, the tap, and the vsock uds named
// in the snapshot must already exist in this VMM's environment.
type LoadSnapshotConfig struct {
	SnapshotPath string `json:"snapshot_path"`
	// MemBackendPath is the guest memory file (File backing).
	MemBackendPath string `json:"-"`
	// ResumeVM resumes the guest inline; otherwise it loads Paused and a later
	// ResumeVM starts it.
	ResumeVM bool `json:"resume_vm"`
	// EnableDiffSnapshots keeps dirty-page tracking on after load, so a later
	// snapshot of the restored guest can be a diff.
	EnableDiffSnapshots bool `json:"enable_diff_snapshots,omitempty"`
}

// loadSnapshotBody is the wire form (mem_backend nests the memory file).
type loadSnapshotBody struct {
	SnapshotPath        string     `json:"snapshot_path"`
	MemBackend          memBackend `json:"mem_backend"`
	ResumeVM            bool       `json:"resume_vm"`
	EnableDiffSnapshots bool       `json:"enable_diff_snapshots,omitempty"`
}

// LoadSnapshot restores a guest from a snapshot (PUT /snapshot/load) on a bare
// VMM (launch one with LaunchVMM, then call this instead of the config
// sequence). Memory is loaded from a file (eager); demand-paging via userfaultfd
// is a follow-up.
func (c *Client) LoadSnapshot(ctx context.Context, cfg LoadSnapshotConfig) error {
	body := loadSnapshotBody{
		SnapshotPath:        cfg.SnapshotPath,
		MemBackend:          memBackend{BackendType: "File", BackendPath: cfg.MemBackendPath},
		ResumeVM:            cfg.ResumeVM,
		EnableDiffSnapshots: cfg.EnableDiffSnapshots,
	}
	if err := c.api.put(ctx, "/snapshot/load", body); err != nil {
		return fmt.Errorf("PUT /snapshot/load: %w", err)
	}
	return nil
}
