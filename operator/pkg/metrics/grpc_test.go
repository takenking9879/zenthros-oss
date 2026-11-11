// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"context"
	"net"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

// One instrumented call is counted on both ends, in the Gateway's and the
// Controller's registries, labelled by method and code only: no backend or
// job label, so the series do not grow with the fleet under test.
func TestGRPCCallsAreCountedOnBothEnds(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	server := grpc.NewServer(GRPCServerOptions()...)
	healthpb.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	opts := append([]grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, GRPCClientOptions()...)
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if _, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}

	for name, register := range map[string]func(prometheus.Registerer, ...prometheus.Collector) error{
		"gateway": RegisterGateway, "controller": RegisterController,
	} {
		reg := prometheus.NewRegistry()
		if err := register(reg); err != nil {
			t.Fatal(err)
		}
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, metric := range []string{"grpc_server_handled_total", "grpc_client_handled_total"} {
			value, labels := handled(families, metric, "Check", "OK")
			if value < 1 {
				t.Errorf("%s: %s{Health/Check,OK} = %v, want ≥ 1", name, metric, value)
			}
			if got := strings.Join(labels, ","); got != "grpc_code,grpc_method,grpc_service,grpc_type" {
				t.Errorf("%s: %s labels = %s, want method and code only", name, metric, got)
			}
		}
	}
}

func handled(families []*dto.MetricFamily, metric, method, code string) (float64, []string) {
	var total float64
	var names []string
	for _, family := range families {
		if family.GetName() != metric {
			continue
		}
		for _, m := range family.GetMetric() {
			labels := map[string]string{}
			names = names[:0]
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
				names = append(names, l.GetName())
			}
			if labels["grpc_method"] == method && labels["grpc_code"] == code {
				total += m.GetCounter().GetValue()
			}
		}
	}
	sort.Strings(names)
	return total, names
}
