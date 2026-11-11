// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func TestWaitForHostDialsControllerPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	addr := ln.Addr().String()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	if port == "9443" {
		t.Skip("listener landed on 9443; retry would not prove port derivation")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, accErr := ln.Accept()
		if accErr == nil {
			_ = conn.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := waitForHost(ctx, addr, 2*time.Second); err != nil {
		t.Fatalf("waitForHost(%q) = %v", addr, err)
	}
	<-done
}

func TestWaitForHostRejectsAddressWithoutPort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := waitForHost(ctx, "controller.example.ts.net", 200*time.Millisecond)
	if err == nil {
		t.Fatal("waitForHost accepted a host without a port")
	}
	if !strings.Contains(err.Error(), "host:port") {
		t.Fatalf("error %q does not mention host:port", err)
	}
}
