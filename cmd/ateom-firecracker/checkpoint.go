//go:build linux

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

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/fc"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CheckpointWorkload suspends the actor: pause the guest, snapshot its memory +
// device state with Firecracker, capture each container's block rootfs, then
// tear the VM down so the worker is free. atelet ships the reported files to
// object storage.
//
// Only FULL scope is supported. DATA scope captures DurableDir volumes, which
// the Firecracker class does not offer yet.
func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.setActiveRPC(rpcCheckpointWorkload, cancel)
	defer s.clearActiveRPC()

	if req.GetScope() != ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL {
		return nil, status.Errorf(codes.Unimplemented,
			"ateom-firecracker supports only FULL snapshots, got %v (DATA needs DurableDir volumes, not offered by this class yet)", req.GetScope())
	}
	actorUID := req.GetActorUid()
	ra := s.running[actorUID]
	if ra == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "no running workload for actor %q to checkpoint", actorUID)
	}

	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	checkpointDir := ateompath.CheckpointStateDir(actorUID)
	if err := os.MkdirAll(checkpointDir, 0o700); err != nil {
		return nil, fmt.Errorf("while creating checkpoint dir: %w", err)
	}

	// Pause the guest so memory + virtio queues are quiesced, then snapshot.
	if err := ra.client.PauseVM(ctx); err != nil {
		return nil, fmt.Errorf("while pausing guest: %w", err)
	}
	if err := ra.client.CreateSnapshot(ctx, fc.CreateSnapshotConfig{
		SnapshotPath: filepath.Join(checkpointDir, snapVMState),
		MemFilePath:  filepath.Join(checkpointDir, snapMemFile),
	}); err != nil {
		return nil, fmt.Errorf("while creating snapshot: %w", err)
	}

	// Capture each container's block rootfs (its disk state) alongside the memory
	// snapshot. The guest is paused, so the host-side image file is consistent.
	for _, name := range ra.containers {
		src := containerRootfsImage(actorUID, name)
		dst := filepath.Join(checkpointDir, rootfsSnapName(name))
		if err := copyFile(src, dst); err != nil {
			return nil, fmt.Errorf("while capturing rootfs for %q: %w", name, err)
		}
	}

	// The actor's state is captured; free the worker. teardownActor kills the
	// paused VMM and reclaims the host resources.
	s.activeActor.Store(nil)
	s.teardownActor(ctx, actorUID)

	files, err := listSnapshotFiles(checkpointDir)
	if err != nil {
		return nil, fmt.Errorf("while listing snapshot files: %w", err)
	}
	s.actorLogger.EmitLifecycleLog(ctx, "Actor checkpointed", actorAttribution(req))
	slog.InfoContext(ctx, "Actor checkpointed", slog.String("id", actorUID), slog.Any("files", files))
	return &ateompb.CheckpointWorkloadResponse{SnapshotFiles: files}, nil
}

// listSnapshotFiles returns the sorted relative names of the regular files in
// dir -- exactly what ateom wrote (vmstate, mem, per-container rootfs images) --
// for atelet to ship to object storage.
func listSnapshotFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.Type().IsRegular() {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	return files, nil
}

// copyFile copies src to dst (truncating dst), preserving nothing but the bytes.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
