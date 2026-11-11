// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
)

// gRPC request counts and latency per method, on both ends of every hub hop:
// Agent → Gateway (server), Gateway → Hub (client and server) and Controller →
// Gateway Pipe (client and server). The labels are the method and code, never
// a backend or job, so the series count does not grow with the fleet.
var (
	grpcServer = grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram())
	grpcClient = grpcprom.NewClientMetrics(grpcprom.WithClientHandlingTimeHistogram())
)

// GRPCServerOptions instruments a gRPC server. The interceptors are chained, so
// a server's own interceptor (the Hub's authorization) still runs.
func GRPCServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(grpcServer.UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(grpcServer.StreamServerInterceptor()),
	}
}

// GRPCClientOptions instruments a gRPC client connection.
func GRPCClientOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(grpcClient.UnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(grpcClient.StreamClientInterceptor()),
	}
}

func grpcCollectors() []prometheus.Collector {
	return []prometheus.Collector{grpcServer, grpcClient}
}
