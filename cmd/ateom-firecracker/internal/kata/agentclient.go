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

// Package kata is a minimal kata-agent client for the Firecracker ateom.
//
// It is deliberately a self-contained duplicate of the subset of
// cmd/ateom-microvm/internal/kata that ateom-firecracker needs (Go's internal
// visibility rule blocks importing the micro-VM copy). It carries only the
// low-level ttrpc RPCs required to start an actor's containers and configure
// guest networking directly (no kata shim); the high-level rootfs staging is
// block-device based and lives in the parent package, not here (the micro-VM
// copy's helpers are virtio-fs specific).
package kata

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-firecracker/internal/third_party/kata/agentpb"
	"github.com/containerd/ttrpc"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	// agentVsockPort is the guest port the kata-agent's ttrpc server listens on.
	agentVsockPort = 1024
	// debugConsoleVsockPort is the guest port kata's debug console listens on when
	// the guest booted with debug_console_enabled. A raw shell over hybrid vsock.
	debugConsoleVsockPort = 1026
)

// AgentClient is a thin ttrpc client for the kata-agent RPCs ateom-firecracker
// drives directly. ateom owns the Firecracker boot (no kata shim) and drives the
// agent over ttrpc itself: UpdateInterface/UpdateRoutes for guest networking and
// CreateSandbox/CreateContainer/StartContainer to run the actor's containers. It
// dials the agent through Firecracker's hybrid-vsock unix socket -- the same
// "CONNECT <port>" channel the kata shim would use, and identical to the
// cloud-hypervisor path.
type AgentClient struct {
	conn   net.Conn
	client *ttrpc.Client
}

// DialAgent connects to the kata-agent through the hybrid-vsock socket at
// udsPath: a plain-text "CONNECT <port>" handshake with the VMM, then ttrpc over
// the resulting stream.
func DialAgent(ctx context.Context, udsPath string) (*AgentClient, error) {
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "unix", udsPath)
	if err != nil {
		return nil, fmt.Errorf("dialing hybrid vsock %q: %w", udsPath, err)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	}
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", agentVsockPort); err != nil {
		conn.Close()
		return nil, fmt.Errorf("hybrid vsock CONNECT: %w", err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("hybrid vsock CONNECT response: %w", err)
	}
	if !strings.HasPrefix(line, "OK") {
		conn.Close()
		return nil, fmt.Errorf("hybrid vsock CONNECT refused: %q", strings.TrimSpace(line))
	}
	_ = conn.SetDeadline(time.Time{}) // ttrpc manages its own timeouts via ctx
	return &AgentClient{conn: conn, client: ttrpc.NewClient(conn)}, nil
}

// Close shuts the ttrpc client and the underlying connection.
func (a *AgentClient) Close() error {
	err := a.client.Close()
	_ = a.conn.Close()
	return err
}

// CreateSandboxOpts configures CreateSandboxForActor.
type CreateSandboxOpts struct {
	SandboxID string
	Hostname  string
}

// CreateSandboxForActor establishes the agent's sandbox context (id, hostname)
// before any container is created. The kata shim normally issues this once at VM
// boot; ateom (no shim) calls it itself. Unlike the cloud-hypervisor class it
// passes no shared virtio-fs storage: each container's rootfs arrives as its own
// block device, mounted per-container by CreateContainerRequest.Storages.
func (a *AgentClient) CreateSandboxForActor(ctx context.Context, opts CreateSandboxOpts) error {
	req := &agentpb.CreateSandboxRequest{
		Hostname:  opts.Hostname,
		SandboxId: opts.SandboxID,
	}
	if err := a.client.Call(ctx, "grpc.AgentService", "CreateSandbox", req, &emptypb.Empty{}); err != nil {
		return fmt.Errorf("agent CreateSandbox: %w", err)
	}
	return nil
}

// StartBlockRootfsContainer creates and starts one container whose rootfs is a
// virtio-block device. It passes a single Storage that mounts blockSource
// (fstype ext4) at the guest rootfs path, then runs the container there.
//
// blockSource is what the kata-agent's block driver resolves to a guest device.
// See the caller for how it is derived from the drive's attach order; getting it
// right is the part that needs on-node validation.
func (a *AgentClient) StartBlockRootfsContainer(ctx context.Context, opts BlockRootfsOpts) error {
	pbSpec := SpecToAgentPB(opts.Spec)
	pbSpec.Root = &agentpb.Root{Path: opts.GuestRootfsPath, Readonly: false}
	if pbSpec.Linux != nil {
		// Per-container cgroup under a shared parent, so the guest kernel accounts
		// an actor's containers hierarchically (parity with the micro-VM class).
		pbSpec.Linux.CgroupsPath = "/ateomfc/" + opts.ContainerID
	}
	req := &agentpb.CreateContainerRequest{
		ContainerId: opts.ContainerID,
		ExecId:      opts.ContainerID,
		OCI:         pbSpec,
		Storages: []*agentpb.Storage{{
			Driver:     opts.BlockDriver,
			Source:     opts.BlockSource,
			Fstype:     "ext4",
			MountPoint: opts.GuestRootfsPath,
		}},
	}
	if err := a.client.Call(ctx, "grpc.AgentService", "CreateContainer", req, &emptypb.Empty{}); err != nil {
		return fmt.Errorf("agent CreateContainer %q: %w", opts.ContainerID, err)
	}
	start := &agentpb.StartContainerRequest{ContainerId: opts.ContainerID}
	if err := a.client.Call(ctx, "grpc.AgentService", "StartContainer", start, &emptypb.Empty{}); err != nil {
		return fmt.Errorf("agent StartContainer %q: %w", opts.ContainerID, err)
	}
	return nil
}

// BlockRootfsOpts describes one container to create on a block-device rootfs.
type BlockRootfsOpts struct {
	ContainerID string
	// Spec is the container's OCI spec; its Root.Path is overwritten with
	// GuestRootfsPath (the rootfs is provided out-of-band as the storage below).
	Spec *specs.Spec
	// GuestRootfsPath is where the agent mounts the block device and runs the
	// container (e.g. /run/kata-containers/<cid>/rootfs).
	GuestRootfsPath string
	// BlockDriver is the kata-agent storage driver for the actor rootfs device
	// (Firecracker exposes virtio over MMIO, so "mmioblk").
	BlockDriver string
	// BlockSource is what the driver resolves to a guest device node.
	BlockSource string
}

// UpdateInterface configures a guest network interface (the kata shim's job,
// which ateom does itself). The agent matches the link by HwAddr, then applies
// the name/IP/MTU.
func (a *AgentClient) UpdateInterface(ctx context.Context, iface *agentpb.Interface) error {
	req := &agentpb.UpdateInterfaceRequest{Interface: iface}
	if err := a.client.Call(ctx, "grpc.AgentService", "UpdateInterface", req, &agentpb.Interface{}); err != nil {
		return fmt.Errorf("agent UpdateInterface: %w", err)
	}
	return nil
}

// UpdateRoutes replaces the guest's route table (the agent flushes and re-adds).
func (a *AgentClient) UpdateRoutes(ctx context.Context, routes []*agentpb.Route) error {
	req := &agentpb.UpdateRoutesRequest{Routes: &agentpb.Routes{Routes: routes}}
	if err := a.client.Call(ctx, "grpc.AgentService", "UpdateRoutes", req, &agentpb.Routes{}); err != nil {
		return fmt.Errorf("agent UpdateRoutes: %w", err)
	}
	return nil
}

// AddARPNeighbors installs static ARP entries in the guest -- used to pin the
// gateway to its fixed MAC so a restored guest's frozen neighbor entry stays
// valid across pods (relevant once snapshot/restore lands).
func (a *AgentClient) AddARPNeighbors(ctx context.Context, neighbors []*agentpb.ARPNeighbor) error {
	req := &agentpb.AddARPNeighborsRequest{Neighbors: &agentpb.ARPNeighbors{ARPNeighbors: neighbors}}
	if err := a.client.Call(ctx, "grpc.AgentService", "AddARPNeighbors", req, &emptypb.Empty{}); err != nil {
		return fmt.Errorf("agent AddARPNeighbors: %w", err)
	}
	return nil
}

// SignalProcess sends signal to the container's init process (ExecId ==
// ContainerId). Used during teardown to propagate SIGTERM/SIGKILL into the actor.
func (a *AgentClient) SignalProcess(ctx context.Context, containerID string, signal uint32) error {
	req := &agentpb.SignalProcessRequest{ContainerId: containerID, ExecId: containerID, Signal: signal}
	if err := a.client.Call(ctx, "grpc.AgentService", "SignalProcess", req, &emptypb.Empty{}); err != nil {
		return fmt.Errorf("agent SignalProcess %q: %w", containerID, err)
	}
	return nil
}

// WaitProcess blocks until the container's init process exits and returns its
// exit status. Used during teardown to confirm the actor stopped.
func (a *AgentClient) WaitProcess(ctx context.Context, containerID string) (int32, error) {
	resp := &agentpb.WaitProcessResponse{}
	req := &agentpb.WaitProcessRequest{ContainerId: containerID, ExecId: containerID}
	if err := a.client.Call(ctx, "grpc.AgentService", "WaitProcess", req, resp); err != nil {
		return 0, fmt.Errorf("agent WaitProcess %q: %w", containerID, err)
	}
	return resp.GetStatus(), nil
}

// DebugConsoleDump connects to the guest's kata debug console (vsock 1026) and
// runs cmd, returning its combined output. Diagnostic only (requires the guest to
// have booted with the debug console enabled). Best-effort: returns the error
// text on failure rather than failing the caller.
func DebugConsoleDump(ctx context.Context, udsPath, cmd string) string {
	d := net.Dialer{}
	dctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	conn, err := d.DialContext(dctx, "unix", udsPath)
	if err != nil {
		return "debug-console dial: " + err.Error()
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", debugConsoleVsockPort); err != nil {
		return "debug-console CONNECT: " + err.Error()
	}
	br := bufio.NewReader(conn)
	if _, err := br.ReadString('\n'); err != nil { // the "OK <n>" line
		return "debug-console CONNECT reply: " + err.Error()
	}
	// The console echoes the command before running it; split the sentinel with
	// '' (the shell strips it) so the echo does not trip the end match.
	if _, err := fmt.Fprintf(conn, "{ %s ; } 2>&1; echo __ATE''_END__\n", cmd); err != nil {
		return "debug-console write: " + err.Error()
	}
	const sentinel = "__ATE_END__"
	var out strings.Builder
	for {
		line, err := br.ReadString('\n')
		if line != "" {
			if strings.Contains(line, sentinel) {
				break
			}
			out.WriteString(line)
		}
		if err != nil {
			break
		}
	}
	return out.String()
}
