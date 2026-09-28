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
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/fc"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/readyz"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RestoreWorkload resumes an actor from a Firecracker snapshot: rebuild the host
// networking and place the container block-rootfs images where the snapshot's
// device config references them, launch a bare VMM in the interior netns, load
// the snapshot (memory + device/vCPU state) and resume the guest, then re-dial
// the kata-agent (still listening in the restored guest).
//
// Only FULL scope is supported; DATA / DATA_ON_GOLDEN need DurableDir volumes
// and golden-snapshot handling the Firecracker class does not offer yet.
func (s *AteomService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (resp *ateompb.RestoreWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.rejectIfDraining(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.setActiveRPC(rpcRestoreWorkload, cancel)
	defer s.clearActiveRPC()

	if req.GetScope() != ateompb.SnapshotScope_SNAPSHOT_SCOPE_FULL {
		return nil, status.Errorf(codes.Unimplemented,
			"ateom-firecracker supports only FULL restores, got %v", req.GetScope())
	}
	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	actorUID := req.GetActorUid()
	attribution := actorAttribution(req)
	s.actorLogger.EmitLifecycleLog(ctx, "Actor restoring", attribution)
	s.activeActor.Store(&attribution)
	defer func() {
		if retErr != nil {
			s.activeActor.Store(nil)
		}
	}()

	restoreDir := ateompath.RestoreStateDir(actorUID)
	fcBin := firstNonEmpty(req.GetRuntimeAssetPaths()[assetFirecracker], s.fcBinary)

	egress, err := s.prepareActorEgress(ctx, req.GetAtespace(), req.GetActorName(), actorUID, req.GetEgressGateway())
	if err != nil {
		return nil, err
	}

	// Host networking: the fresh veth + tap the snapshot's virtio-net re-attaches
	// to. The fixed veth/tap MACs (see net.go) keep the guest's frozen ARP for
	// the gateway valid, so no in-guest reconfiguration is needed.
	if err := ateomnet.SetupActorNetwork(ctx, ateomnet.NetworkConfig{
		InteriorNetNS:      s.interiorNetNS,
		HostVethHWAddr:     hostVethHWAddr,
		SweepInteriorLinks: true,
		EgressRedirectPort: s.egressRedirectPort(req.GetEgressGateway() != nil),
	}); err != nil {
		return nil, fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			cleanupCtx, c := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer c()
			if err := s.deactivateActorNetworking(cleanupCtx); err != nil {
				slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after Restore failure", slog.Any("err", err))
			}
			if err := ateomnet.CleanupActorNetwork(cleanupCtx, s.interiorNetNS); err != nil {
				slog.WarnContext(cleanupCtx, "Failed to clean up actor network after Restore failure", slog.Any("err", err))
			}
		}
	}()
	if err := s.setupActorTap(ctx, tapName); err != nil {
		return nil, fmt.Errorf("while building guest tap: %w", err)
	}

	if err := os.MkdirAll(fcVMDir(actorUID), 0o700); err != nil {
		return nil, fmt.Errorf("while creating VM dir: %w", err)
	}
	// Place each container's block rootfs at the exact path the snapshot's drive
	// config references (containerRootfsImage), from the files atelet downloaded.
	for _, c := range req.GetSpec().GetContainers() {
		src := filepath.Join(restoreDir, rootfsSnapName(c.GetName()))
		dst := containerRootfsImage(actorUID, c.GetName())
		if err := copyFile(src, dst); err != nil {
			return nil, fmt.Errorf("while placing rootfs image for %q: %w", c.GetName(), err)
		}
	}

	consoleFile, err := os.OpenFile(fcConsoleLogPath(actorUID), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("while opening console log: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = consoleFile.Close()
		}
	}()

	// A bare VMM (no config); the snapshot restores the machine config + devices.
	apiSocket := fcAPISocketPath(actorUID)
	fcCmd, client, err := fc.LaunchVMM(ctx, fc.LaunchVMMOptions{
		Binary:    fcBin,
		APISocket: apiSocket,
		NetnsPath: ateompath.AteomNetNSPath(s.podUID),
		Stdout:    consoleFile,
		Stderr:    consoleFile,
	})
	if err != nil {
		return nil, fmt.Errorf("while launching VMM: %w", err)
	}
	defer func() {
		if retErr != nil {
			killVMM(fcCmd)
			logGuestBootDiagnostics(ctx, actorUID)
		}
	}()

	if err := client.LoadSnapshot(ctx, fc.LoadSnapshotConfig{
		SnapshotPath:   filepath.Join(restoreDir, snapVMState),
		MemBackendPath: filepath.Join(restoreDir, snapMemFile),
		ResumeVM:       true,
	}); err != nil {
		return nil, fmt.Errorf("while loading snapshot: %w", err)
	}
	slog.InfoContext(ctx, "Firecracker VM restored", slog.String("id", actorUID), slog.String("api", apiSocket))

	// The restored guest's kata-agent is already listening (snapshotted mid-run);
	// re-dial it so teardown can signal the containers.
	udsPath := fcVsockUDSPath(actorUID)
	if !waitForFile(udsPath, 15*time.Second) {
		return nil, fmt.Errorf("firecracker vsock socket %q did not appear after restore", udsPath)
	}
	ac, err := dialAgentRetry(ctx, udsPath, 60*time.Second)
	if err != nil {
		return nil, fmt.Errorf("while re-dialing kata-agent after restore: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = ac.Close()
		}
	}()

	if err := readyz.WaitAll(ctx, req.GetSpec().GetContainers(), ateomnet.ActorVethIP); err != nil {
		return nil, fmt.Errorf("while waiting for container readyz after restore: %w", err)
	}
	if err := s.activateActorNetworking(req.GetAtespace(), req.GetActorName(), egress); err != nil {
		return nil, err
	}

	names := make([]string, 0, len(req.GetSpec().GetContainers()))
	for _, c := range req.GetSpec().GetContainers() {
		names = append(names, c.GetName())
	}
	s.running[actorUID] = &runningActor{
		fcCmd: fcCmd, apiSocket: apiSocket, client: client, consoleFile: consoleFile,
		guestAgent: ac, containers: names,
	}
	s.actorLogger.EmitLifecycleLog(ctx, "Actor restored", attribution)
	return &ateompb.RestoreWorkloadResponse{}, nil
}
