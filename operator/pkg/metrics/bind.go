// SPDX-License-Identifier: Apache-2.0

package metrics

import "net"

// RestrictBindAddress rewrites all-interfaces metrics binds (":8080",
// "0.0.0.0:8080", "[::]:8080") to loopback unless public is true.
func RestrictBindAddress(addr string, public bool) string {
	if public || addr == "" || addr == "0" {
		return addr
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return net.JoinHostPort("127.0.0.1", port)
	}
	return addr
}
