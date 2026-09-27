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
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestResolveGuestMemMiB(t *testing.T) {
	const reserve = 128
	const fallback = 512
	tests := []struct {
		name        string
		declaredMiB int64
		want        int
		wantErr     bool
	}{
		{name: "unset uses fallback", declaredMiB: 0, want: fallback},
		{name: "declared minus reserve", declaredMiB: 1024, want: 1024 - reserve},
		{name: "exactly at floor after reserve", declaredMiB: minGuestMemMiB + reserve, want: minGuestMemMiB},
		{name: "below floor after reserve rejected", declaredMiB: 200, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveGuestMemMiB(tc.declaredMiB*(1<<20), reserve, fallback)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveGuestMemMiB(%dMiB) = %d, want error", tc.declaredMiB, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveGuestMemMiB(%dMiB): unexpected error: %v", tc.declaredMiB, err)
			}
			if got != tc.want {
				t.Errorf("resolveGuestMemMiB(%dMiB) = %d, want %d", tc.declaredMiB, got, tc.want)
			}
		})
	}
}

// fakeVsock serves the Firecracker hybrid-vsock handshake on a unix socket: it
// reads one "CONNECT <port>\n" line and replies with reply. It is how the tests
// stand in for a guest whose kata-agent is (or is not) listening.
func fakeVsock(t *testing.T, reply string) string {
	t.Helper()
	uds := filepath.Join(t.TempDir(), "vsock.sock")
	lis, err := net.Listen("unix", uds)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
					return
				}
				_, _ = conn.Write([]byte(reply))
			}()
		}
	}()
	return uds
}

func TestProbeKataAgentReachable(t *testing.T) {
	uds := fakeVsock(t, "OK 1073741824\n")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := probeKataAgent(ctx, uds, kataAgentVsockPort, 2*time.Second); err != nil {
		t.Fatalf("probeKataAgent: want reachable, got %v", err)
	}
}

func TestProbeKataAgentNotListening(t *testing.T) {
	// The guest is up but nothing listens on the port: Firecracker answers a
	// non-OK line, which must not be read as reachable.
	uds := fakeVsock(t, "FAILED: no listener\n")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := probeKataAgent(ctx, uds, kataAgentVsockPort, 500*time.Millisecond)
	if err == nil {
		t.Fatal("probeKataAgent: want error for non-OK response, got nil")
	}
	if !strings.Contains(err.Error(), "not reachable") {
		t.Errorf("error = %v, want it to mention unreachability", err)
	}
}

func TestFirstNonEmpty(t *testing.T) {
	if got := firstNonEmpty("", "", "asset", "flag"); got != "asset" {
		t.Errorf("firstNonEmpty = %q, want %q", got, "asset")
	}
	if got := firstNonEmpty("", ""); got != "" {
		t.Errorf("firstNonEmpty(all empty) = %q, want empty", got)
	}
}
