// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"

	"github.com/zenthros/operator/agent/k8sagent"
	"github.com/zenthros/operator/internal/envconf"
)

// runNodeLogReader runs a node's log reader until SIGTERM, configured by
// the chart's DaemonSet.
func runNodeLogReader() int {
	raw, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "zenthros-agent node-log-reader: %v\n", err)
		return 1
	}
	logger := zapr.NewLogger(raw).WithName("node-log-reader")
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	err = k8sagent.RunNodeLogReader(ctx, k8sagent.NodeLogReaderConfig{
		Listen:    envconf.Or("ZENTHROS_NODE_LOGS_LISTEN", fmt.Sprintf(":%d", k8sagent.NodeLogPort)),
		Health:    envconf.Or("ZENTHROS_NODE_LOGS_HEALTH", ":8081"),
		Root:      envconf.Or("ZENTHROS_NODE_LOGS_ROOT", k8sagent.PodLogRoot),
		Dir:       envconf.Or("ZENTHROS_NODE_LOGS_TLS_DIR", "/etc/zenthros/node-logs"),
		Namespace: os.Getenv("POD_NAMESPACE"),
		Pod:       os.Getenv("POD_NAME"),
		UID:       os.Getenv("POD_UID"),
		Container: envconf.Or("ZENTHROS_NODE_LOGS_CONTAINER", "reader"),
	}, logger)
	if err != nil {
		logger.Error(err, "node log reader stopped")
		return 1
	}
	return 0
}
