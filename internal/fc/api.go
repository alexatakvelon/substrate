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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
)

// apiBase is a placeholder host; the real transport always dials the unix
// api-socket, so the host portion of the URL is ignored.
const apiBase = "http://localhost"

// apiClient speaks the Firecracker REST API over its unix api-socket.
//
// Firecracker serves an HTTP/1.1 REST API on the api-socket. Unlike
// cloud-hypervisor's "/api/v1/<verb>" RPC style, Firecracker models VM
// configuration as resources mutated with PUT/PATCH (e.g. PUT /machine-config,
// PUT /boot-source, PUT /drives/{id}, PUT /vsock, PUT /network-interfaces/{id})
// and drives actions through PUT /actions.
type apiClient struct {
	http *http.Client
}

func newAPIClient(socketPath string) *apiClient {
	return &apiClient{
		http: &http.Client{
			Transport: &http.Transport{
				// Match the ch client: force a fresh connection per request. A
				// VMM heavily swapped out during reclaim can leave a kept-alive
				// connection wedged, and the API surface is low-frequency, so
				// connection reuse buys nothing here.
				DisableKeepAlives: true,
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", socketPath)
				},
			},
		},
	}
}

// getJSON issues a GET and decodes the 2xx JSON response into out.
func (c *apiClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiBase+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GET %s: %s: %s", path, resp.Status, bytes.TrimSpace(body))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(body, out)
}

// put issues a PUT with a JSON body (nil for an empty body) and requires a 2xx.
// Firecracker returns 204 No Content for successful configuration writes.
//
// The snapshot/restore increment will add a PATCH sibling: Firecracker uses
// PATCH for in-place updates such as pausing the VM state and swapping a drive's
// backing path on a running guest.
func (c *apiClient) put(ctx context.Context, path string, in any) error {
	return c.write(ctx, http.MethodPut, path, in)
}

func (c *apiClient) write(ctx context.Context, method, path string, in any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("marshaling %s %s body: %w", method, path, err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, apiBase+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, bytes.TrimSpace(respBody))
	}
	return nil
}
