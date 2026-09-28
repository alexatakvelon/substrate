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
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/cmd/ateom-firecracker/internal/kata"
	"github.com/agent-substrate/substrate/cmd/ateom-firecracker/internal/third_party/kata/agentpb"
	"github.com/agent-substrate/substrate/internal/ateomnet"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/imagecache"
	"github.com/agent-substrate/substrate/internal/ocispec"
	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"golang.org/x/sys/unix"
)

const (
	// maxActorContainers caps containers per actor. Each takes one extra
	// virtio-block drive, and the guest device-name derivation below assumes the
	// count stays within a single letter suffix (/dev/vdb.. /dev/vdz).
	maxActorContainers = 24

	// blockDriver is the kata-agent Storage handler for the actor rootfs device.
	// Firecracker exposes virtio-block over MMIO; the guest agent's handler for
	// that is "mmioblk". Note this is the AGENT's handler name, which differs from
	// the kata RUNTIME's block_device_driver config value ("virtio-blk-mmio") --
	// the agent rejects the latter with "Failed to find the storage handler".
	// Validated against the kata 4.x agent on a KVM node.
	blockDriver = "mmioblk"

	// rootDriveID is the Firecracker drive_id of the kata guest OS image (the read
	// -only root, /dev/vda). Actor container rootfs drives are added after it.
	rootDriveID = "rootfs"

	// rootfsImageMinBytes / rootfsImageHeadroomBytes size a container's ext4 rootfs
	// image: the composed content plus headroom for the container's own writes,
	// floored so a tiny image still has room to run.
	rootfsImageMinBytes      = int64(512) << 20 // 512 MiB
	rootfsImageHeadroomBytes = int64(256) << 20 // 256 MiB over content
)

// actorContainer is one of an actor's containers prepared for the Firecracker
// guest: its name (also the kata container id), its OCI spec, and the host path
// of the ext4 rootfs image attached as a virtio-block drive.
type actorContainer struct {
	name      string
	spec      *specs.Spec
	rootfsImg string // host path of the ext4 image (the virtio-blk drive backing)
	driveID   string // Firecracker drive_id
	guestDev  string // /dev/vdN the guest sees this drive as
}

// buildActorContainers composes each container's rootfs from its OCI bundle and
// bakes it into an ext4 image on the host, one per container, to attach as a
// virtio-block drive. It must run before the VM boots (Firecracker configures
// drives pre-boot). The guest device name is derived from the drive's attach
// order: the kata image is /dev/vda, so the first container is /dev/vdb.
func (s *AteomService) buildActorContainers(ctx context.Context, actorUID string, containers []*ateompb.Container) ([]actorContainer, error) {
	if len(containers) == 0 {
		return nil, fmt.Errorf("actor spec has no containers")
	}
	if len(containers) > maxActorContainers {
		return nil, fmt.Errorf("ateom-firecracker supports at most %d containers, got %d", maxActorContainers, len(containers))
	}
	out := make([]actorContainer, 0, len(containers))
	for i, c := range containers {
		name := c.GetName()
		bundle := ateompath.OCIBundlePath(actorUID, name)
		// Contract with atelet: the OCI bundle (config.json + layer refs) is on
		// disk; ateom composes the rootfs (atelet runs with no mount caps).
		if err := imagecache.SetupBundleRootfs(bundle); err != nil {
			return nil, fmt.Errorf("composing rootfs for %q: %w", name, err)
		}
		spec, err := ocispec.Load(bundle)
		if err != nil {
			return nil, fmt.Errorf("loading OCI spec for %q: %w", name, err)
		}
		rootfsDir := filepath.Join(bundle, "rootfs")
		img := containerRootfsImage(actorUID, name)
		if err := buildRootfsImage(ctx, rootfsDir, img); err != nil {
			return nil, fmt.Errorf("building rootfs image for %q: %w", name, err)
		}
		out = append(out, actorContainer{
			name:      name,
			spec:      spec,
			rootfsImg: img,
			driveID:   "ctr-" + name,
			guestDev:  guestBlockDevice(i),
		})
	}
	return out, nil
}

// guestBlockDevice returns the guest device node for the i-th actor container's
// drive. Firecracker names virtio-mmio block devices /dev/vda, /dev/vdb, ... in
// the order they are added; the kata image root is added first (/dev/vda), so
// container i is /dev/vd{b+i}.
func guestBlockDevice(i int) string {
	return "/dev/vd" + string(rune('b'+i))
}

// containerRootfsImage is the host path of the ext4 rootfs image backing a
// container's virtio-block drive. It sits under the actor's VM dir; checkpoint
// copies it into the snapshot and restore places it back here (the snapshot's
// device config references this exact path).
func containerRootfsImage(actorUID, containerName string) string {
	return filepath.Join(fcVMDir(actorUID), "rootfs-"+containerName+".ext4")
}

// rootfsSnapName is the (relative) name a container's rootfs image is stored
// under inside a snapshot, alongside the memory and vmstate files.
func rootfsSnapName(containerName string) string {
	return "rootfs-" + containerName + ".ext4"
}

// Snapshot file names within a checkpoint/restore directory.
const (
	snapVMState = "vmstate" // Firecracker device + vCPU state
	snapMemFile = "mem"     // guest RAM
)

// buildRootfsImage bakes the directory tree at rootfsDir into a fresh ext4 image
// at imgPath, sized to the content plus headroom for the container's writes. It
// uses mke2fs -d, which populates the filesystem from a directory without
// mounting it (so it needs no loop device or mount privilege). The worker image
// must provide mkfs.ext4 (e2fsprogs).
func buildRootfsImage(ctx context.Context, rootfsDir, imgPath string) error {
	content, err := dirSizeBytes(rootfsDir)
	if err != nil {
		return fmt.Errorf("sizing rootfs %q: %w", rootfsDir, err)
	}
	size := content + rootfsImageHeadroomBytes
	if size < rootfsImageMinBytes {
		size = rootfsImageMinBytes
	}
	// A sparse file of the target size; mke2fs formats within it and -d populates.
	if err := os.Truncate(imgPath, 0); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("resetting %q: %w", imgPath, err)
	}
	f, err := os.OpenFile(imgPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("creating %q: %w", imgPath, err)
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return fmt.Errorf("sizing %q: %w", imgPath, err)
	}
	f.Close()
	// -F: don't prompt over a regular file. -q: quiet. -d: populate from dir.
	// root_owner=0:0 so image files are owned by root in the guest regardless of
	// the builder's uid.
	cmd := exec.CommandContext(ctx, "mkfs.ext4", "-F", "-q",
		"-E", "root_owner=0:0", "-d", rootfsDir, imgPath)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := reaper.RunCommand(cmd); err != nil {
		return fmt.Errorf("mkfs.ext4 -d %q -> %q: %w (%s)", rootfsDir, imgPath, err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// dirSizeBytes returns the apparent size of the tree at dir (sum of regular file
// sizes), used to size the ext4 image. Symlinks and special files contribute
// nothing; directory entries are covered by the image headroom.
func dirSizeBytes(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.Type().IsRegular() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// startActorContainers performs the post-boot kata-agent setup the shim normally
// does at boot: establish the sandbox, configure guest networking (eth0
// IP/MAC/MTU + routes), then create and start each container on its block-device
// rootfs.
func (s *AteomService) startActorContainers(ctx context.Context, ac *kata.AgentClient, actorUID, udsPath string, ctrs []actorContainer) error {
	sbCtx, sbCancel := context.WithTimeout(ctx, 20*time.Second)
	err := ac.CreateSandboxForActor(sbCtx, kata.CreateSandboxOpts{
		SandboxID: actorUID,
		Hostname:  ctrs[0].spec.Hostname,
	})
	sbCancel()
	if err != nil {
		return fmt.Errorf("while creating agent sandbox: %w", err)
	}

	netCtx, netCancel := context.WithTimeout(ctx, 20*time.Second)
	err = s.configureGuestNetwork(netCtx, ac, uint64(s.actorVethMTU(ctx)))
	netCancel()
	if err != nil {
		dump := kata.DebugConsoleDump(ctx, udsPath, "ip addr 2>&1; echo '== route =='; ip route 2>&1")
		slog.ErrorContext(ctx, "guest network config failed; dump", slog.String("dump", dump))
		return fmt.Errorf("while configuring guest network: %w", err)
	}

	for _, c := range ctrs {
		cCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := ac.StartBlockRootfsContainer(cCtx, kata.BlockRootfsOpts{
			ContainerID:     c.name,
			Spec:            c.spec,
			GuestRootfsPath: "/run/kata-containers/" + c.name + "/rootfs",
			BlockDriver:     blockDriver,
			BlockSource:     c.guestDev,
		})
		cancel()
		if err != nil {
			dump := kata.DebugConsoleDump(ctx, udsPath, "echo '== blk =='; ls -l /dev/vd* 2>&1; echo '== mounts =='; cat /proc/mounts 2>&1 | grep -E 'kata|vd'")
			slog.ErrorContext(ctx, "container start failed; dump", slog.String("container", c.name), slog.String("dump", dump))
			return fmt.Errorf("while starting container %q: %w", c.name, err)
		}
	}
	return nil
}

// configureGuestNetwork replicates the kata shim's guest network setup over the
// agent: configure eth0 (IP/MAC/MTU), install the connected + default routes, and
// pin the gateway's ARP entry to its fixed MAC.
func (s *AteomService) configureGuestNetwork(ctx context.Context, ac *kata.AgentClient, mtu uint64) error {
	if err := ac.UpdateInterface(ctx, &agentpb.Interface{
		Device: ateomnet.ActorVethName,
		Name:   ateomnet.ActorVethName,
		HwAddr: actorGuestMAC,
		Mtu:    mtu,
		IPAddresses: []*agentpb.IPAddress{
			{Family: agentpb.IPFamily_v4, Address: ateomnet.ActorVethIP, Mask: "30"},
		},
	}); err != nil {
		return err
	}
	if err := ac.UpdateRoutes(ctx, []*agentpb.Route{
		{Dest: ateomnet.ActorVethSubnet, Device: ateomnet.ActorVethName, Scope: uint32(unix.RT_SCOPE_LINK), Family: agentpb.IPFamily_v4},
		{Dest: "", Gateway: ateomnet.ActorVethGateway, Device: ateomnet.ActorVethName, Family: agentpb.IPFamily_v4},
	}); err != nil {
		return err
	}
	return ac.AddARPNeighbors(ctx, []*agentpb.ARPNeighbor{{
		ToIPAddress: &agentpb.IPAddress{Family: agentpb.IPFamily_v4, Address: ateomnet.ActorVethGateway},
		Device:      ateomnet.ActorVethName,
		Lladdr:      hostVethMAC,
		State:       0x80, // NUD_PERMANENT
	}})
}
