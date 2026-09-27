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
	"path/filepath"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

// Asset names in RunWorkloadRequest.runtime_asset_paths (set by atelet from the
// SandboxConfig assets; see manifests/ate-install/sandboxconfig-validation.yaml).
// The Firecracker class needs no virtiofsd asset -- its actor rootfs is
// virtio-block, not virtio-fs.
const (
	assetFirecracker = "firecracker"
	assetKernel      = "kata-kernel"
	assetImage       = "kata-image"
)

const (
	// guestCID is the guest's vsock context id. kata uses 3.
	guestCID = 3

	// kataAgentVsockPort is the guest vsock port the kata-agent listens on. ateom
	// reaches it over the Firecracker hybrid-vsock unix socket with a
	// "CONNECT <port>" handshake.
	kataAgentVsockPort = 1024

	// vmmMemReserveMiB is the DEFAULT guest RAM held back from the pod's memory
	// limit for the firecracker VMM, a host process in the same pod cgroup as the
	// guest RAM; without a margin the pod OOMs. Overridable via
	// --vmm-mem-reserve-mib. Firecracker's own footprint is smaller than the
	// cloud-hypervisor stack (no virtiofsd), but the margin also absorbs
	// transient page-cache from block I/O, so it is kept conservative.
	vmmMemReserveMiB = 128

	// minGuestMemMiB is the floor for guest RAM; a declared limit that leaves less
	// after the VMM reserve is rejected at cold boot rather than silently honored.
	minGuestMemMiB = 128

	// defaultGuestMemMiB / defaultVCPUs size a guest whose ActorTemplate declares
	// no limits.
	defaultGuestMemMiB = 512
	defaultVCPUs       = 1
)

// defaultKataBootArgs is the guest kernel command line for a kata rootfs-image
// boot on Firecracker. Firecracker exposes virtio-mmio (no PCI) and a serial
// console on ttyS0. root=/dev/vda1 assumes a partitioned kata image; a
// whole-disk image roots at /dev/vda -- tune via --guest-boot-args to match the
// staged kata-image. init=/usr/bin/kata-agent runs the agent as PID 1.
const defaultKataBootArgs = "console=ttyS0 reboot=k panic=1 pci=off nomodule " +
	"i8042.noaux=1 i8042.nomux=1 i8042.nopnp=1 i8042.dumbkbd=1 " +
	"root=/dev/vda1 rootfstype=ext4 rootflags=data=ordered,errors=remount-ro ro " +
	"init=/usr/bin/kata-agent"

// fcVMDir is the per-actor runtime dir holding the api-socket, the vsock uds, and
// the console log. It sits under the actor's ateom path so teardown and the
// ateom-dir sweep reclaim it.
func fcVMDir(actorUID string) string {
	return filepath.Join(ateompath.ActorPath(actorUID), "fc")
}

// fcAPISocketPath is the Firecracker REST api-socket for an actor.
func fcAPISocketPath(actorUID string) string {
	return filepath.Join(fcVMDir(actorUID), "firecracker.sock")
}

// fcVsockUDSPath is the host unix socket backing the guest's virtio-vsock. The
// kata-agent's listening ports are reached through it via the hybrid-vsock
// "CONNECT <port>" handshake.
func fcVsockUDSPath(actorUID string) string {
	return filepath.Join(fcVMDir(actorUID), "vsock.sock")
}

// fcConsoleLogPath captures the guest serial console for boot diagnostics.
func fcConsoleLogPath(actorUID string) string {
	return filepath.Join(fcVMDir(actorUID), "console.log")
}
