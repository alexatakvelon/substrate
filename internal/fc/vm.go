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

// MachineConfig is the body of PUT /machine-config: guest vCPU and memory
// sizing. SMT is kept off (Firecracker requires it off on arm64 and it is the
// safer default on x86 for a single-tenant guest). TrackDirtyPages is what a
// later diff-snapshot needs; it is off for a cold boot.
type MachineConfig struct {
	VCPUCount       int32 `json:"vcpu_count"`
	MemSizeMiB      int64 `json:"mem_size_mib"`
	SMT             bool  `json:"smt"`
	TrackDirtyPages bool  `json:"track_dirty_pages,omitempty"`
}

// BootSource is the body of PUT /boot-source: the guest kernel and its cmdline.
// InitrdPath is unused for the kata rootfs-image model (the guest boots from a
// virtio-block root device, not an initrd).
type BootSource struct {
	KernelImagePath string `json:"kernel_image_path"`
	BootArgs        string `json:"boot_args,omitempty"`
	InitrdPath      string `json:"initrd_path,omitempty"`
}

// Drive is the body of PUT /drives/{drive_id}: one virtio-block device. The kata
// guest rootfs image is attached as the root device (read-only); actor
// container rootfs and durable volumes attach as additional drives once the
// block-storage model lands.
type Drive struct {
	DriveID      string `json:"drive_id"`
	PathOnHost   string `json:"path_on_host"`
	IsRootDevice bool   `json:"is_root_device"`
	IsReadOnly   bool   `json:"is_read_only"`
}

// NetworkInterface is the body of PUT /network-interfaces/{iface_id}: a
// virtio-net device backed by an existing host tap. Firecracker opens the tap by
// name in its network namespace (see LaunchVMMOptions.NetnsPath), so the tap
// must already exist there.
type NetworkInterface struct {
	IfaceID     string `json:"iface_id"`
	HostDevName string `json:"host_dev_name"`
	GuestMAC    string `json:"guest_mac,omitempty"`
}

// Vsock is the body of PUT /vsock: the virtio-vsock device the kata-agent
// listens on. UDSPath is the host unix socket Firecracker creates; a host
// connection that writes "CONNECT <port>\n" to it is proxied to the guest port,
// which is how ateom reaches the agent. GuestCID is the guest's context id (kata
// uses 3).
type Vsock struct {
	GuestCID int64  `json:"guest_cid"`
	UDSPath  string `json:"uds_path"`
	VsockID  string `json:"vsock_id,omitempty"`
}

// actionRequest is the body of PUT /actions.
type actionRequest struct {
	ActionType string `json:"action_type"`
}

// PutMachineConfig sizes the guest via PUT /machine-config.
func (c *Client) PutMachineConfig(ctx context.Context, cfg MachineConfig) error {
	if err := c.api.put(ctx, "/machine-config", cfg); err != nil {
		return fmt.Errorf("PUT /machine-config: %w", err)
	}
	return nil
}

// PutBootSource sets the guest kernel + cmdline via PUT /boot-source.
func (c *Client) PutBootSource(ctx context.Context, src BootSource) error {
	if err := c.api.put(ctx, "/boot-source", src); err != nil {
		return fmt.Errorf("PUT /boot-source: %w", err)
	}
	return nil
}

// PutDrive attaches one virtio-block device via PUT /drives/{drive_id}.
func (c *Client) PutDrive(ctx context.Context, d Drive) error {
	if d.DriveID == "" {
		return fmt.Errorf("PutDrive: DriveID is required")
	}
	if err := c.api.put(ctx, "/drives/"+d.DriveID, d); err != nil {
		return fmt.Errorf("PUT /drives/%s: %w", d.DriveID, err)
	}
	return nil
}

// PutNetworkInterface attaches one virtio-net device via
// PUT /network-interfaces/{iface_id}.
func (c *Client) PutNetworkInterface(ctx context.Context, n NetworkInterface) error {
	if n.IfaceID == "" {
		return fmt.Errorf("PutNetworkInterface: IfaceID is required")
	}
	if err := c.api.put(ctx, "/network-interfaces/"+n.IfaceID, n); err != nil {
		return fmt.Errorf("PUT /network-interfaces/%s: %w", n.IfaceID, err)
	}
	return nil
}

// PutVsock attaches the virtio-vsock device via PUT /vsock.
func (c *Client) PutVsock(ctx context.Context, v Vsock) error {
	if v.UDSPath == "" {
		return fmt.Errorf("PutVsock: UDSPath is required")
	}
	if err := c.api.put(ctx, "/vsock", v); err != nil {
		return fmt.Errorf("PUT /vsock: %w", err)
	}
	return nil
}

// Start boots the configured VM via PUT /actions {InstanceStart}. All resources
// (machine-config, boot-source, at least the root drive) must be set first.
func (c *Client) Start(ctx context.Context) error {
	if err := c.api.put(ctx, "/actions", actionRequest{ActionType: "InstanceStart"}); err != nil {
		return fmt.Errorf("PUT /actions InstanceStart: %w", err)
	}
	return nil
}
