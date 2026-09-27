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
	"os"
	"path/filepath"
	"testing"
)

func TestGuestBlockDevice(t *testing.T) {
	// The kata image root is /dev/vda; actor containers follow in attach order.
	cases := map[int]string{0: "/dev/vdb", 1: "/dev/vdc", 24: "/dev/vdz"}
	for i, want := range cases {
		if got := guestBlockDevice(i); got != want {
			t.Errorf("guestBlockDevice(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestContainerNames(t *testing.T) {
	ctrs := []actorContainer{{name: "app"}, {name: "sidecar"}}
	got := containerNames(ctrs)
	if len(got) != 2 || got[0] != "app" || got[1] != "sidecar" {
		t.Errorf("containerNames = %v, want [app sidecar]", got)
	}
	if n := containerNames(nil); len(n) != 0 {
		t.Errorf("containerNames(nil) = %v, want empty", n)
	}
}

func TestDirSizeBytes(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a"), make([]byte, 100), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "b"), make([]byte, 250), 0o644); err != nil {
		t.Fatal(err)
	}
	// A symlink contributes nothing (not a regular file).
	if err := os.Symlink("a", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	got, err := dirSizeBytes(dir)
	if err != nil {
		t.Fatalf("dirSizeBytes: %v", err)
	}
	if got != 350 {
		t.Errorf("dirSizeBytes = %d, want 350", got)
	}
}
