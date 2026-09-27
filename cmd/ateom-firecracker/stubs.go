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

	"github.com/agent-substrate/substrate/internal/proto/ateompb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The Ateom RPCs below are not implemented at the skeleton stage. They return
// codes.Unimplemented explicitly (rather than inheriting the embedded
// UnimplementedAteomServer default) so a caller gets a clear reason and so the
// method set documents exactly what each later increment must fill in.

// CheckpointWorkload will suspend the actor by driving Firecracker's snapshot
// API (PUT /snapshot/create) plus the block/durable-volume capture. Pending the
// suspend/resume increment.
func (s *AteomService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ateom-firecracker: CheckpointWorkload is not implemented yet (suspend/resume increment)")
}

// RestoreWorkload will resume the actor from a Firecracker snapshot
// (PUT /snapshot/load) with userfaultfd demand-paging. Pending the
// suspend/resume increment.
func (s *AteomService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (*ateompb.RestoreWorkloadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ateom-firecracker: RestoreWorkload is not implemented yet (suspend/resume increment)")
}

// GetWorkloadStats will report a resource-usage sample for the running actor,
// read from the pod cgroup and the kata-agent. Pending the container-start
// increment, which is what makes a workload to measure exist.
func (s *AteomService) GetWorkloadStats(ctx context.Context, req *ateompb.GetWorkloadStatsRequest) (*ateompb.GetWorkloadStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ateom-firecracker: GetWorkloadStats is not implemented yet")
}

// GetActiveWorkloadStats is the discovery read for a scraper that holds no
// worker-to-actor mapping. Pending the container-start increment.
func (s *AteomService) GetActiveWorkloadStats(ctx context.Context, req *ateompb.GetActiveWorkloadStatsRequest) (*ateompb.GetActiveWorkloadStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "ateom-firecracker: GetActiveWorkloadStats is not implemented yet")
}
