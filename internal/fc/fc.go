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

// Package fc drives a single Firecracker VMM over its REST api-socket. It is the
// Firecracker peer of internal/ch: launch the VMM process, configure the VM as a
// set of resources (machine-config, boot-source, drives, network-interfaces,
// vsock), and start it.
//
// Unlike internal/ch, this package covers only the cold-boot path today. The
// snapshot/restore half (Firecracker's PUT /snapshot/create and
// /snapshot/load, with userfaultfd demand-paging) lands with the suspend/resume
// increment of the Firecracker sandbox class; see cmd/ateom-firecracker.
package fc

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/vishvananda/netns"
)

// Client talks to one Firecracker VMM over its unix api-socket.
type Client struct {
	apiSocket string
	api       *apiClient
	info      InstanceInfo
}

// NewClient returns a Client bound to a Firecracker api-socket path. The socket
// need not exist yet; use WaitReady to block until the VMM answers.
func NewClient(apiSocket string) *Client {
	return &Client{apiSocket: apiSocket, api: newAPIClient(apiSocket)}
}

// Info returns what the VMM last reported about itself, zero until a successful
// Ping or WaitReady. A Client belongs to one actor's VMM and is used from that
// actor's goroutine, so this needs no synchronization.
func (c *Client) Info() InstanceInfo { return c.info }

// InstanceInfo is what Firecracker's "GET /" reports about the microVM. State is
// one of "Not started", "Running", "Paused"; VMMVersion is the release string
// (e.g. "1.9.0").
type InstanceInfo struct {
	ID         string `json:"id"`
	State      string `json:"state"`
	VMMVersion string `json:"vmm_version"`
	AppName    string `json:"app_name"`
}

// Ping reports what the VMM says about itself, or an error if the api-socket does
// not answer. Firecracker exposes instance info at the API root.
func (c *Client) Ping(ctx context.Context) (InstanceInfo, error) {
	var info InstanceInfo
	if err := c.api.getJSON(ctx, "/", &info); err != nil {
		return InstanceInfo{}, err
	}
	c.info = info
	return info, nil
}

// WaitReady blocks until the api-socket answers or the deadline passes,
// returning what that answer said. Callers get the VMM's version for free this
// way: the handshake already happens before every boot, so nothing has to run
// the binary again to ask.
func (c *Client) WaitReady(ctx context.Context, deadline time.Duration) (InstanceInfo, error) {
	end := time.Now().Add(deadline)
	for {
		info, err := c.Ping(ctx)
		if err == nil {
			return info, nil
		}
		if !time.Now().Before(end) {
			return InstanceInfo{}, fmt.Errorf("firecracker api socket %q not ready after %s", c.apiSocket, deadline)
		}
		select {
		case <-ctx.Done():
			return InstanceInfo{}, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// LaunchVMMOptions configures starting a bare Firecracker VMM (no VM yet).
type LaunchVMMOptions struct {
	// Binary is the firecracker executable (defaults to "firecracker").
	Binary string
	// APISocket is the api-socket path the new VMM should listen on.
	APISocket string
	// NetnsPath, when set, is the network-namespace file the Firecracker process
	// is launched into. The tap the VM's virtio-net binds to lives in that
	// namespace, and Firecracker opens the tap by name (unlike cloud-hypervisor,
	// which adopts a passed FD), so the VMM process itself must be in that
	// namespace. Bare Firecracker has no namespace flag of its own (that is a
	// jailer feature), so LaunchVMM enters the namespace before exec; see
	// startInNetns.
	NetnsPath string
	// Stdout/Stderr receive the VMM's output.
	Stdout, Stderr io.Writer
}

// LaunchVMM starts a bare Firecracker VMM and returns once its api-socket
// answers. The caller owns the returned *exec.Cmd and must stop the process on
// teardown; WaitReady honors ctx but the process itself is deliberately not tied
// to ctx, so it outlives the RPC that launched it.
func LaunchVMM(ctx context.Context, o LaunchVMMOptions) (*exec.Cmd, *Client, error) {
	if o.APISocket == "" {
		return nil, nil, fmt.Errorf("LaunchVMMOptions.APISocket is required")
	}
	bin := o.Binary
	if bin == "" {
		bin = "firecracker"
	}
	// Firecracker refuses to start if the api-socket path already exists.
	_ = os.Remove(o.APISocket)

	// Deliberately NOT exec.CommandContext: the VMM must outlive the RPC whose
	// ctx launched it. The caller owns cmd; WaitReady honors ctx.
	cmd := exec.Command(bin, "--api-sock", o.APISocket)
	cmd.Stdout = o.Stdout
	cmd.Stderr = o.Stderr
	if err := startInNetns(cmd, o.NetnsPath); err != nil {
		return nil, nil, fmt.Errorf("while starting firecracker: %w", err)
	}
	client := NewClient(o.APISocket)
	if _, err := client.WaitReady(ctx, 15*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, nil, fmt.Errorf("while waiting for VMM api-socket: %w", err)
	}
	return cmd, client, nil
}

// startInNetns starts cmd, first entering the network namespace at netnsPath
// when it is set. Bare Firecracker cannot enter a namespace itself, and it opens
// its tap by name, so the VMM process must already be in the namespace that
// holds the tap.
//
// The switch runs on a dedicated OS thread that is locked and never unlocked, so
// the child forked by cmd.Start inherits the target namespace while the change
// is confined to that thread: the Go runtime destroys a still-locked thread when
// its goroutine returns, discarding the namespace change rather than leaking it
// to the rest of the process.
func startInNetns(cmd *exec.Cmd, netnsPath string) error {
	if netnsPath == "" {
		return cmd.Start()
	}
	errc := make(chan error, 1)
	go func() {
		runtime.LockOSThread() // no UnlockOSThread: see the doc comment
		target, err := netns.GetFromPath(netnsPath)
		if err != nil {
			errc <- fmt.Errorf("opening netns %q: %w", netnsPath, err)
			return
		}
		defer target.Close()
		if err := netns.Set(target); err != nil {
			errc <- fmt.Errorf("entering netns %q: %w", netnsPath, err)
			return
		}
		errc <- cmd.Start()
	}()
	return <-errc
}
