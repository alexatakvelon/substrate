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
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/fc"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/internal/sizing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// workloadGracePeriod bounds the whole drain on shutdown. Kept well under the
// pod's termination grace period so escalation happens here, not as a kubelet
// SIGKILL of ateom itself.
const workloadGracePeriod = 30 * time.Minute

// runningActor is the live micro-VM for one actor, kept so TerminateWorkload and
// graceful shutdown can tear down the same sandbox.
type runningActor struct {
	fcCmd       *exec.Cmd
	apiSocket   string
	client      *fc.Client
	consoleFile *os.File
}

func (s *AteomService) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (resp *ateompb.RunWorkloadResponse, retErr error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if err := s.rejectIfDraining(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.setActiveRPC(rpcRunWorkload, cancel)
	defer s.clearActiveRPC()

	if err := s.deactivateActorNetworking(ctx); err != nil {
		return nil, err
	}

	attribution := resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: req.GetAtespace(), Name: req.GetActorName()},
		UID:              req.GetActorUid(),
		TemplateAtespace: req.GetActorTemplateAtespace(),
		TemplateName:     req.GetActorTemplateName(),
	}
	s.actorLogger.EmitLifecycleLog(ctx, "Actor starting", attribution)

	// Retain the attribution before the boot, so a sample taken against a
	// workload that dies mid-boot is still attributable. Dropped again if the
	// boot fails outright. Matches the other ateoms' RunWorkload.
	s.activeActor.Store(&attribution)
	defer func() {
		if retErr != nil {
			s.activeActor.Store(nil)
		}
	}()

	if err := s.coldBootActor(ctx, req); err != nil {
		return nil, err
	}
	s.actorLogger.EmitLifecycleLog(ctx, "Actor started", attribution)
	slog.InfoContext(ctx, "Actor started (bare firecracker VM; kata-agent reachable)", slog.String("id", req.GetActorUid()))
	return &ateompb.RunWorkloadResponse{}, nil
}

// coldBootActor boots the actor's Firecracker VM from scratch: host networking,
// the VMM, the kata guest (kernel + rootfs-image block device + tap NIC +
// vsock), then confirms the kata-agent is reachable. The caller holds s.lock.
//
// This is the skeleton path. It stops at kata-agent reachability: starting the
// actor's containers over the agent -- backed by a block (not virtio-fs) rootfs
// -- is the next increment. See the package doc.
func (s *AteomService) coldBootActor(ctx context.Context, req *ateompb.RunWorkloadRequest) (retErr error) {
	actorUID := req.GetActorUid()

	assetPaths := req.GetRuntimeAssetPaths()
	fcBin := firstNonEmpty(assetPaths[assetFirecracker], s.fcBinary)
	kernel, image := assetPaths[assetKernel], assetPaths[assetImage]
	if kernel == "" || image == "" {
		return status.Errorf(codes.InvalidArgument, "ateom-firecracker requires %q and %q asset paths", assetKernel, assetImage)
	}

	egress, err := s.prepareActorEgress(ctx, req.GetAtespace(), req.GetActorName(), actorUID, req.GetEgressGateway())
	if err != nil {
		return err
	}

	// Guest sizing: default, then right-size to the actor's declared limits.
	sz := sizing.FromLimits(req.GetCpuMilli(), req.GetMemoryBytes())
	vcpus := defaultVCPUs
	if v := sz.VCPUs(); v > 0 {
		vcpus = v
	}
	memMiB, err := resolveGuestMemMiB(sz.MemoryBytes, s.memReserveMiB, defaultGuestMemMiB)
	if err != nil {
		return err
	}

	// Host-side networking: per-activation veth into the interior netns, plus the
	// tap the guest's virtio-net binds to. The guest-side interface config
	// (address/MTU via the kata-agent) is part of the container-start increment;
	// here we establish the host plumbing the guest will attach to.
	if err := ateomnet.SetupActorNetwork(ctx, ateomnet.NetworkConfig{
		InteriorNetNS:      s.interiorNetNS,
		HostVethHWAddr:     hostVethHWAddr,
		SweepInteriorLinks: true,
		EgressRedirectPort: s.egressRedirectPort(req.GetEgressGateway() != nil),
	}); err != nil {
		return fmt.Errorf("while setting up actor network: %w", err)
	}
	defer func() {
		if retErr != nil {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			if cleanupErr := s.deactivateActorNetworking(cleanupCtx); cleanupErr != nil {
				slog.WarnContext(cleanupCtx, "Failed to deactivate actor networking after Run failure", slog.Any("err", cleanupErr))
			}
			if cleanupErr := ateomnet.CleanupActorNetwork(cleanupCtx, s.interiorNetNS); cleanupErr != nil {
				slog.WarnContext(cleanupCtx, "Failed to clean up actor network after Run failure", slog.Any("err", cleanupErr))
			}
		}
	}()
	if err := s.setupActorTap(ctx, tapName); err != nil {
		return fmt.Errorf("while building guest tap: %w", err)
	}

	if err := os.MkdirAll(fcVMDir(actorUID), 0o700); err != nil {
		return fmt.Errorf("while creating VM dir: %w", err)
	}

	// The VMM (and guest serial console) output goes to a per-actor console log,
	// tailed into the pod logs on a failed boot for diagnostics.
	consoleFile, err := os.OpenFile(fcConsoleLogPath(actorUID), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("while opening console log: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = consoleFile.Close()
		}
	}()

	// Launch the VMM inside the interior netns so it can open the tap by name.
	apiSocket := fcAPISocketPath(actorUID)
	fcCmd, client, err := fc.LaunchVMM(ctx, fc.LaunchVMMOptions{
		Binary:    fcBin,
		APISocket: apiSocket,
		NetnsPath: ateompath.AteomNetNSPath(s.podUID),
		Stdout:    consoleFile,
		Stderr:    consoleFile,
	})
	if err != nil {
		return fmt.Errorf("while launching VMM: %w", err)
	}
	defer func() {
		if retErr != nil {
			killVMM(fcCmd)
			logGuestBootDiagnostics(ctx, actorUID)
		}
	}()

	// Configure the VM as a set of Firecracker resources, then boot it.
	if err := client.PutMachineConfig(ctx, fc.MachineConfig{VCPUCount: int32(vcpus), MemSizeMiB: int64(memMiB)}); err != nil {
		return err
	}
	// Raise the guest agent's log level when --kata-debug is set; the guest
	// console (which carries the agent log) is already captured to the console
	// file, so this makes a boot that stalls before the agent answers legible.
	bootArgs := s.bootArgs
	if s.kataDebug {
		bootArgs += " agent.log=debug"
	}
	if err := client.PutBootSource(ctx, fc.BootSource{KernelImagePath: kernel, BootArgs: bootArgs}); err != nil {
		return err
	}
	// The kata rootfs image is the read-only root block device. Actor container
	// rootfs and durable volumes attach as additional drives once the
	// block-storage model lands.
	if err := client.PutDrive(ctx, fc.Drive{DriveID: "rootfs", PathOnHost: image, IsRootDevice: true, IsReadOnly: true}); err != nil {
		return err
	}
	if err := client.PutNetworkInterface(ctx, fc.NetworkInterface{IfaceID: "eth0", HostDevName: tapName, GuestMAC: actorGuestMAC}); err != nil {
		return err
	}
	if err := client.PutVsock(ctx, fc.Vsock{GuestCID: guestCID, UDSPath: fcVsockUDSPath(actorUID)}); err != nil {
		return err
	}
	if err := client.Start(ctx); err != nil {
		return err
	}
	slog.InfoContext(ctx, "Firecracker VM booted", slog.String("id", actorUID), slog.String("api", apiSocket))

	// Confirm the kata-agent is reachable over the hybrid-vsock. Firecracker
	// creates the base uds at PutVsock; the agent only starts listening once the
	// guest reaches its init, so poll the CONNECT until it answers.
	udsPath := fcVsockUDSPath(actorUID)
	if !waitForFile(udsPath, 15*time.Second) {
		return fmt.Errorf("firecracker vsock socket %q did not appear", udsPath)
	}
	if err := probeKataAgent(ctx, udsPath, kataAgentVsockPort, 60*time.Second); err != nil {
		return fmt.Errorf("while confirming kata-agent reachability: %w", err)
	}
	slog.InfoContext(ctx, "kata-agent reachable over vsock", slog.String("id", actorUID), slog.Int("port", kataAgentVsockPort))

	if err := s.activateActorNetworking(req.GetAtespace(), req.GetActorName(), egress); err != nil {
		return err
	}

	s.running[actorUID] = &runningActor{fcCmd: fcCmd, apiSocket: apiSocket, client: client, consoleFile: consoleFile}
	return nil
}

func (s *AteomService) TerminateWorkload(ctx context.Context, req *ateompb.TerminateWorkloadRequest) (*ateompb.TerminateWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.activeActor.Store(nil)
	if err := s.deactivateActorNetworking(ctx); err != nil {
		slog.WarnContext(ctx, "Failed to deactivate actor networking during terminate", slog.Any("err", err))
	}
	s.teardownActor(ctx, req.GetActorUid())
	return &ateompb.TerminateWorkloadResponse{}, nil
}

// teardownActor stops the actor's VMM and reclaims its host resources: network,
// VM dir, and running-map entry. Best-effort: every step is attempted even if an
// earlier one fails, since teardown is the last chance to release them.
func (s *AteomService) teardownActor(ctx context.Context, actorUID string) {
	ra := s.running[actorUID]
	if ra != nil {
		killVMM(ra.fcCmd)
		if ra.consoleFile != nil {
			_ = ra.consoleFile.Close()
		}
		delete(s.running, actorUID)
	}
	if err := ateomnet.CleanupActorNetwork(ctx, s.interiorNetNS); err != nil {
		slog.WarnContext(ctx, "Failed to clean up actor network during teardown", slog.String("id", actorUID), slog.Any("err", err))
	}
	if err := os.RemoveAll(fcVMDir(actorUID)); err != nil {
		slog.WarnContext(ctx, "Failed to remove VM dir during teardown", slog.String("id", actorUID), slog.Any("err", err))
	}
}

// gracefulShutdown tears down any running workload on SIGTERM. It cancels an
// in-flight boot rather than waiting it out, then reclaims the sandbox.
func (s *AteomService) gracefulShutdown(ctx context.Context) {
	s.shuttingDown.Store(true)
	s.cancelActiveRestoreOrRunRPC()

	deadline := time.Now().Add(workloadGracePeriod)
	lockCtx, lockCancel := context.WithDeadline(ctx, deadline)
	defer lockCancel()
	if !s.lock.LockContext(lockCtx) {
		slog.ErrorContext(ctx, "Failed to acquire lock during graceful shutdown; another RPC is still running")
		return
	}
	defer s.lock.Unlock()

	s.activeActor.Store(nil)
	if err := s.deactivateActorNetworking(ctx); err != nil {
		slog.WarnContext(ctx, "Failed to deactivate actor networking during shutdown", slog.Any("err", err))
	}
	for actorUID := range s.running {
		s.teardownActor(ctx, actorUID)
	}
	slog.InfoContext(ctx, "Graceful shutdown complete")
}

// killVMM stops the firecracker process and reaps it, tolerating the child
// reaper having collected it first (ECHILD): the reaper runs concurrently, so a
// process that has already exited leaves os/exec nothing to wait for.
func killVMM(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	if _, err := cmd.Process.Wait(); err != nil && !errors.Is(err, syscall.ECHILD) {
		slog.Warn("Failed to reap firecracker VMM", slog.Any("err", err))
	}
}

// firstNonEmpty returns the first non-empty string, or "" if all are empty.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// resolveGuestMemMiB derives the guest RAM from the actor's declared memory
// limit minus the VMM reserve, falling back to fallbackMiB when no limit is set.
// A declared limit that leaves less than minGuestMemMiB after the reserve is
// rejected rather than silently honored (too little RAM hangs the guest on boot
// instead of failing cleanly).
func resolveGuestMemMiB(declaredBytes int64, reserveMiB, fallbackMiB int) (int, error) {
	if declaredBytes <= 0 {
		return fallbackMiB, nil
	}
	declaredMiB := int(declaredBytes / (1 << 20))
	guestMiB := declaredMiB - reserveMiB
	if guestMiB < minGuestMemMiB {
		return 0, status.Errorf(codes.InvalidArgument,
			"declared memory %dMiB leaves %dMiB for the guest after the %dMiB VMM reserve, below the %dMiB floor",
			declaredMiB, guestMiB, reserveMiB, minGuestMemMiB)
	}
	return guestMiB, nil
}

// probeKataAgent confirms the kata-agent is listening by completing the
// Firecracker hybrid-vsock handshake: connect to the host uds, write
// "CONNECT <port>\n", and read back an "OK <n>" line. Retries until the agent
// answers or the timeout elapses (the agent starts listening only once the guest
// reaches its init). It closes the probe connection without exchanging agent
// traffic; the actual agent RPCs arrive with the container-start increment.
func probeKataAgent(ctx context.Context, udsPath string, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := tryConnectVsock(ctx, udsPath, port); err != nil {
			lastErr = err
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("kata-agent not reachable on vsock port %d within %s: %w", port, timeout, lastErr)
}

func tryConnectVsock(ctx context.Context, udsPath string, port int) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", udsPath)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", port); err != nil {
		return fmt.Errorf("writing CONNECT: %w", err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("reading CONNECT response: %w", err)
	}
	if !strings.HasPrefix(line, "OK") {
		return fmt.Errorf("unexpected hybrid-vsock response %q", strings.TrimSpace(line))
	}
	return nil
}

// waitForFile polls until path exists or d elapses.
func waitForFile(path string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	_, err := os.Stat(path)
	return err == nil
}

// logGuestBootDiagnostics tails the guest console log into the ateom log after a
// failed boot, so a guest that dies before the agent answers is never silent.
func logGuestBootDiagnostics(ctx context.Context, actorUID string) {
	b, err := os.ReadFile(fcConsoleLogPath(actorUID))
	if err != nil {
		slog.WarnContext(ctx, "No guest console log to report", slog.String("id", actorUID), slog.Any("err", err))
		return
	}
	slog.ErrorContext(ctx, "Guest boot diagnostics (console tail)",
		slog.String("id", actorUID), slog.String("console", tailString(string(b), 4096)))
}

func tailString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}
