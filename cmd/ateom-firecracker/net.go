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

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	"github.com/agent-substrate/substrate/internal/ateomnet"
)

const (
	// tapName is the guest's host-side tap in the interior netns. Firecracker
	// enters the netns (--netns) and opens it by name.
	tapName = "tap0"

	// hostVethMAC is a FIXED (locally administered) gateway MAC, so that once
	// snapshot/restore lands the guest's frozen ARP entry for the gateway stays
	// valid on every pod (see the cloud-hypervisor net.go for the full rationale).
	hostVethMAC = "02:a8:1e:00:00:01"

	// actorGuestMAC is the FIXED MAC for the guest's eth0 (the virtio-net).
	actorGuestMAC = "02:a8:1e:00:00:02"
)

var hostVethHWAddr = ateomnet.MustParseMAC(hostVethMAC)

// setupActorTap creates, in the interior netns, the tap the guest's virtio-net
// binds to, cross-connected to eth0 (the actor veth peer) with TC mirred-redirect
// filters in both directions -- the same tcfilter wiring kata builds, so the
// guest shares the veth's L2/L3 with the pod network. Firecracker opens the tap
// by name after it enters the netns, so unlike the cloud-hypervisor path no FDs
// are returned. Call after ateomnet.SetupActorNetwork.
func (s *AteomService) setupActorTap(ctx context.Context, name string) error {
	return ateomnet.NetNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		eth0, err := netlink.LinkByName(ateomnet.ActorVethName)
		if err != nil {
			return fmt.Errorf("acquiring actor veth in interior netns: %w", err)
		}
		if old, lerr := netlink.LinkByName(name); lerr == nil {
			_ = netlink.LinkDel(old)
		}
		tap := &netlink.Tuntap{
			LinkAttrs: netlink.LinkAttrs{Name: name, MTU: eth0.Attrs().MTU},
			Mode:      netlink.TUNTAP_MODE_TAP,
			Flags:     netlink.TUNTAP_NO_PI | netlink.TUNTAP_VNET_HDR,
		}
		if err := netlink.LinkAdd(tap); err != nil {
			return fmt.Errorf("creating tap %q: %w", name, err)
		}
		if err := netlink.LinkSetUp(tap); err != nil {
			return fmt.Errorf("bringing up tap %q: %w", name, err)
		}
		// Cross-connect: everything arriving on the veth peer redirects out the
		// tap and vice versa (ingress qdisc + match-all u32 with a mirred
		// egress-redirect action, via U32.RedirIndex).
		for _, pair := range [][2]netlink.Link{{eth0, tap}, {tap, eth0}} {
			qdisc := &netlink.Ingress{QdiscAttrs: netlink.QdiscAttrs{
				LinkIndex: pair[0].Attrs().Index,
				Parent:    netlink.HANDLE_INGRESS,
				Handle:    netlink.MakeHandle(0xffff, 0),
			}}
			if err := netlink.QdiscReplace(qdisc); err != nil {
				return fmt.Errorf("adding ingress qdisc to %q: %w", pair[0].Attrs().Name, err)
			}
			filter := &netlink.U32{
				FilterAttrs: netlink.FilterAttrs{
					LinkIndex: pair[0].Attrs().Index,
					Parent:    netlink.MakeHandle(0xffff, 0),
					Priority:  1,
					Protocol:  unix.ETH_P_ALL,
				},
				ClassId:    netlink.MakeHandle(1, 1),
				RedirIndex: pair[1].Attrs().Index,
			}
			if err := netlink.FilterAdd(filter); err != nil {
				return fmt.Errorf("adding mirred filter %s -> %s: %w", pair[0].Attrs().Name, pair[1].Attrs().Name, err)
			}
		}
		return nil
	})
}

// actorVethMTU reads the MTU of the actor veth (eth0 in the interior netns) so
// the guest eth0 can be configured with a matching MTU via the kata-agent.
// Defaults to 1500 if the link can't be read.
func (s *AteomService) actorVethMTU(ctx context.Context) int {
	mtu := 1500
	_ = ateomnet.NetNSDo(ctx, s.interiorNetNS, func(ctx context.Context) error {
		if l, err := netlink.LinkByName(ateomnet.ActorVethName); err == nil {
			mtu = l.Attrs().MTU
		} else {
			slog.WarnContext(ctx, "Failed to read actor veth MTU; using default",
				slog.String("link", ateomnet.ActorVethName), slog.Int("default_mtu", mtu), slog.Any("err", err))
		}
		return nil
	})
	return mtu
}
