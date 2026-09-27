//go:build linux && e2e_fc

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

// On-node e2e harness for the Firecracker sandbox class, gated behind the
// e2e_fc build tag so it never runs in normal builds/CI. It needs a KVM-capable
// host with a firecracker binary and kata guest assets, all supplied via env:
//
//	FCTEST_FCBIN    firecracker binary (default "firecracker" on PATH)
//	FCTEST_KERNEL   guest vmlinux (uncompressed ELF)
//	FCTEST_INITRD   guest initrd (used when FCTEST_MODE=initrd)
//	FCTEST_IMAGE    guest rootfs image (used when FCTEST_MODE=image)
//	FCTEST_MODE     "initrd" (default) or "image"
//	FCTEST_BOOTARGS kernel cmdline (a mode-appropriate default is used if unset)
//	FCTEST_WORKDIR  scratch dir for sockets/images (default a temp dir)
//	FCTEST_ROOTFS   (Test B) a directory tree to bake into the container ext4
//
// Build:  GOOS=linux go test -c -tags e2e_fc -o fce2e ./cmd/ateom-firecracker
// Run A:  FCTEST_KERNEL=... FCTEST_INITRD=... ./fce2e -test.run TestFCBootAgent -test.v
// Run B:  ... FCTEST_ROOTFS=/some/rootfs ./fce2e -test.run TestFCContainerStart -test.v
package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-firecracker/internal/kata"
	"github.com/agent-substrate/substrate/internal/fc"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// defaultCaps is the runc-default capability set, enough for the agent to
// accept the container. A real actor's caps come from its OCI bundle.
var defaultCaps = []string{
	"CAP_CHOWN", "CAP_DAC_OVERRIDE", "CAP_FSETID", "CAP_FOWNER", "CAP_MKNOD",
	"CAP_NET_RAW", "CAP_SETGID", "CAP_SETUID", "CAP_SETFCAP", "CAP_SETPCAP",
	"CAP_NET_BIND_SERVICE", "CAP_SYS_CHROOT", "CAP_KILL", "CAP_AUDIT_WRITE",
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

const (
	// A kata initrd boot: the initrd is the rootfs and the agent is PID 1. No
	// root= (initrd provides it); ttyS0 is Firecracker's serial console.
	defaultInitrdArgs = "console=ttyS0 reboot=k panic=1 pci=off i8042.noaux=1 " +
		"i8042.nomux=1 i8042.nopnp=1 i8042.dumbkbd=1 init=/usr/bin/kata-agent"
	// A kata rootfs-image boot from a partitioned image on /dev/vda.
	defaultImageArgs = "console=ttyS0 reboot=k panic=1 pci=off i8042.noaux=1 " +
		"i8042.nomux=1 i8042.nopnp=1 i8042.dumbkbd=1 " +
		"root=/dev/vda1 rootfstype=ext4 ro init=/usr/bin/kata-agent"
)

// bootGuest launches a Firecracker VM with the kata guest via the real
// internal/fc client, returning the vsock uds and the console-log path. containerDrive,
// when non-empty, is attached as an extra virtio-block drive after any root drive.
func bootGuest(ctx context.Context, t *testing.T, workdir, containerDrive string) (udsPath, consolePath string) {
	t.Helper()
	mode := env("FCTEST_MODE", "initrd")
	kernel := os.Getenv("FCTEST_KERNEL")
	if kernel == "" {
		t.Fatal("FCTEST_KERNEL is required")
	}
	consolePath = filepath.Join(workdir, "console.log")
	console, err := os.Create(consolePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { console.Close() })

	apiSock := filepath.Join(workdir, "fc.sock")
	cmd, client, err := fc.LaunchVMM(ctx, fc.LaunchVMMOptions{
		Binary:    env("FCTEST_FCBIN", "firecracker"),
		APISocket: apiSock,
		Stdout:    console,
		Stderr:    console,
	})
	if err != nil {
		t.Fatalf("LaunchVMM: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})

	if err := client.PutMachineConfig(ctx, fc.MachineConfig{VCPUCount: 1, MemSizeMiB: 512}); err != nil {
		t.Fatalf("PutMachineConfig: %v", err)
	}
	boot := fc.BootSource{KernelImagePath: kernel}
	switch mode {
	case "initrd":
		boot.InitrdPath = os.Getenv("FCTEST_INITRD")
		if boot.InitrdPath == "" {
			t.Fatal("FCTEST_INITRD is required for FCTEST_MODE=initrd")
		}
		boot.BootArgs = env("FCTEST_BOOTARGS", defaultInitrdArgs)
	case "image":
		boot.BootArgs = env("FCTEST_BOOTARGS", defaultImageArgs)
	default:
		t.Fatalf("unknown FCTEST_MODE %q", mode)
	}
	if err := client.PutBootSource(ctx, boot); err != nil {
		t.Fatalf("PutBootSource: %v", err)
	}
	if mode == "image" {
		if img := os.Getenv("FCTEST_IMAGE"); img != "" {
			// IsRootDevice=false on purpose: when true, Firecracker appends its own
			// "root=/dev/vda" to the cmdline, which overrides our root=/dev/vda1 and
			// makes the kernel try to mount the whole (partitioned) disk -> panic.
			// The kata rootfs is on vda1, so we keep root= in boot_args ourselves.
			if err := client.PutDrive(ctx, fc.Drive{DriveID: "rootfs", PathOnHost: img, IsRootDevice: false, IsReadOnly: true}); err != nil {
				t.Fatalf("PutDrive rootfs: %v", err)
			}
		} else {
			t.Fatal("FCTEST_IMAGE is required for FCTEST_MODE=image")
		}
	}
	if containerDrive != "" {
		if err := client.PutDrive(ctx, fc.Drive{DriveID: "ctr", PathOnHost: containerDrive, IsRootDevice: false, IsReadOnly: false}); err != nil {
			t.Fatalf("PutDrive ctr: %v", err)
		}
	}
	udsPath = filepath.Join(workdir, "vsock.sock")
	if err := client.PutVsock(ctx, fc.Vsock{GuestCID: 3, UDSPath: udsPath}); err != nil {
		t.Fatalf("PutVsock: %v", err)
	}
	if err := client.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return udsPath, consolePath
}

func dumpConsole(t *testing.T, consolePath string) {
	if b, err := os.ReadFile(consolePath); err == nil {
		t.Logf("=== guest console (%d bytes) ===\n%s", len(b), string(b))
	}
}

// TestFCBootAgent validates internal/fc: boot the kata guest and confirm the
// kata-agent answers the hybrid-vsock CONNECT (the real kata.DialAgent).
func TestFCBootAgent(t *testing.T) {
	workdir := env("FCTEST_WORKDIR", t.TempDir())
	_ = os.MkdirAll(workdir, 0o755)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	uds, console := bootGuest(ctx, t, workdir, "")

	// Poll DialAgent until the agent answers or we run out of time.
	deadline := time.Now().Add(60 * time.Second)
	var ac *kata.AgentClient
	var lastErr error
	for time.Now().Before(deadline) {
		dctx, c := context.WithTimeout(ctx, time.Second)
		ac, lastErr = kata.DialAgent(dctx, uds)
		c()
		if lastErr == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr != nil {
		dumpConsole(t, console)
		t.Fatalf("kata-agent never answered: %v", lastErr)
	}
	defer ac.Close()
	t.Log("SUCCESS: kata-agent connected over hybrid-vsock")
}

// TestFCContainerStart validates the block-rootfs path with the real code:
// buildRootfsImage bakes FCTEST_ROOTFS into an ext4 image, attached as a
// virtio-block drive; then the real kata client creates the sandbox and
// creates+starts a container on that block device.
func TestFCContainerStart(t *testing.T) {
	rootfs := os.Getenv("FCTEST_ROOTFS")
	if rootfs == "" {
		t.Skip("FCTEST_ROOTFS not set")
	}
	workdir := env("FCTEST_WORKDIR", t.TempDir())
	_ = os.MkdirAll(workdir, 0o755)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Real prod function under test: build the container's ext4 rootfs image.
	img := filepath.Join(workdir, "ctr-rootfs.ext4")
	if err := buildRootfsImage(ctx, rootfs, img); err != nil {
		t.Fatalf("buildRootfsImage: %v", err)
	}

	uds, console := bootGuest(ctx, t, workdir, img)

	// In initrd mode there is no root drive, so the container drive is the first
	// block device: /dev/vda. In image mode the kata image is /dev/vda, so it is
	// /dev/vdb (guestBlockDevice(0)).
	guestDev := "/dev/vda"
	if env("FCTEST_MODE", "initrd") == "image" {
		guestDev = guestBlockDevice(0)
	}

	deadline := time.Now().Add(60 * time.Second)
	var ac *kata.AgentClient
	var lastErr error
	for time.Now().Before(deadline) {
		dctx, c := context.WithTimeout(ctx, time.Second)
		ac, lastErr = kata.DialAgent(dctx, uds)
		c()
		if lastErr == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr != nil {
		dumpConsole(t, console)
		t.Fatalf("kata-agent never answered: %v", lastErr)
	}
	defer ac.Close()

	if err := ac.CreateSandboxForActor(ctx, kata.CreateSandboxOpts{SandboxID: "e2e", Hostname: "e2e"}); err != nil {
		dumpConsole(t, console)
		t.Fatalf("CreateSandbox: %v", err)
	}
	// A container running the workload rootfs's /bin/sh, proving the block device
	// mounted and the agent could pivot into it. A near-runc-default spec: the
	// standard mounts and namespaces the agent needs to set the container up
	// (specconv drops the network namespace so it shares the guest's).
	spec := &specs.Spec{
		Version: "1.0.0",
		Process: &specs.Process{
			Args: []string{"/bin/sh", "-c", "sleep 30"}, Cwd: "/", Env: []string{"PATH=/bin:/usr/bin"},
			// The kata-agent rejects a container whose process has no capabilities
			// ("missing process capabilities"); a real actor's spec carries them
			// from its OCI bundle. Use the runc default set here.
			Capabilities: &specs.LinuxCapabilities{
				Bounding: defaultCaps, Effective: defaultCaps,
				Permitted: defaultCaps,
			},
		},
		Root: &specs.Root{Path: "rootfs"},
		Mounts: []specs.Mount{
			{Destination: "/proc", Type: "proc", Source: "proc"},
			{Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"nosuid", "strictatime", "mode=755", "size=65536k"}},
			{Destination: "/sys", Type: "sysfs", Source: "sysfs", Options: []string{"nosuid", "noexec", "nodev", "ro"}},
		},
		Linux: &specs.Linux{
			Namespaces: []specs.LinuxNamespace{
				{Type: specs.PIDNamespace}, {Type: specs.IPCNamespace},
				{Type: specs.UTSNamespace}, {Type: specs.MountNamespace},
			},
		},
	}
	if err := ac.StartBlockRootfsContainer(ctx, kata.BlockRootfsOpts{
		ContainerID:     "e2e",
		Spec:            spec,
		GuestRootfsPath: "/run/kata-containers/e2e/rootfs",
		BlockDriver:     blockDriver,
		BlockSource:     guestDev,
	}); err != nil {
		dumpConsole(t, console)
		t.Fatalf("StartBlockRootfsContainer (driver=%s source=%s): %v", blockDriver, guestDev, err)
	}
	t.Logf("SUCCESS: container started on block rootfs (%s via %s)", guestDev, blockDriver)
}
