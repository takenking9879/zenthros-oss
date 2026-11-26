// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"fmt"
	"net"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

// A hub's address is a list, in the order to try: "host:9443,host:443" (the
// Gateway's own port, then Traefik's passthrough on 443, for a network that
// lets only 443 out). Both carry the same TLS name, so the agent's mTLS is the
// same on either. The chart's join.address, the attach line, zenadm join and
// the agent's unit carry the list as it is; this file reads it.

// Addresses are the list's addresses, in order.
func Addresses(list string) []string {
	var out []string
	for _, a := range strings.Split(list, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

// FirstAddress is the list's first: the one whose host names the hub.
func FirstAddress(list string) string {
	if addrs := Addresses(list); len(addrs) > 0 {
		return addrs[0]
	}
	return ""
}

// HostOf is the host the list's first address names.
func HostOf(list string) string {
	first := FirstAddress(list)
	if host, _, err := net.SplitHostPort(first); err == nil {
		return host
	}
	return first
}

// DialTarget is the gRPC target for list: one address as it is (through the
// passthrough resolver when the caller dials it itself); several through
// listScheme, which hands gRPC all of them in order. Its pick_first policy
// tries them in that order every time it connects, so an agent on a network
// that lets only 443 out reaches the hub there, and goes back to the first
// once it can.
func DialTarget(list string, ownDialer bool) string {
	addrs := Addresses(list)
	if len(addrs) == 1 {
		if ownDialer {
			return DialerTarget(addrs[0])
		}
		return addrs[0]
	}
	return listScheme + ":///" + strings.Join(addrs, ",")
}

// DialOptions are what a list's target needs besides: an authority, so TLS
// checks the hub's name, not the list. The certificate's name when the TLS
// names one (serverName, join.tlsServerName), which the TLS credentials
// already carry and gRPC refuses a second one beside; else the first
// address. An address that does not answer gives way to the next one 250 ms
// in (gRPC's pick_first tries them as Happy Eyeballs does), not after its
// connect timeout. One address needs none.
func DialOptions(list, serverName string) []grpc.DialOption {
	if len(Addresses(list)) < 2 || serverName != "" {
		return nil
	}
	return []grpc.DialOption{grpc.WithAuthority(FirstAddress(list))}
}

const listScheme = "zenthros-hub"

func init() { resolver.Register(listBuilder{}) }

type listBuilder struct{}

func (listBuilder) Scheme() string { return listScheme }

func (listBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	addrs := Addresses(target.Endpoint())
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no hub address in %q", target.URL.String())
	}
	endpoint := resolver.Endpoint{}
	for _, a := range addrs {
		endpoint.Addresses = append(endpoint.Addresses, resolver.Address{Addr: a})
	}
	if err := cc.UpdateState(resolver.State{Endpoints: []resolver.Endpoint{endpoint}}); err != nil {
		return nil, err
	}
	return listResolver{}, nil
}

// listResolver's addresses never change: each is a host:port, which the dial
// itself resolves.
type listResolver struct{}

func (listResolver) ResolveNow(resolver.ResolveNowOptions) {}
func (listResolver) Close()                                {}
