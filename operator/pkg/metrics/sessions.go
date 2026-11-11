// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var agentSessionsRefused = prometheus.NewCounter(prometheus.CounterOpts{
	Name: "zenthros_gateway_agent_sessions_refused_total",
	Help: "Agent Register calls refused because this Gateway holds its maximum of sessions (RESOURCE_EXHAUSTED).",
})

// RecordAgentRefusedFull counts one agent session refused by a full Gateway.
func RecordAgentRefusedFull() { agentSessionsRefused.Inc() }

// AgentSessions is the gauge of agent sessions a Gateway holds, read from
// count at scrape time, with the cap it enforces (0: none).
func AgentSessions(count func() int, maxSessions int) []prometheus.Collector {
	return []prometheus.Collector{
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "zenthros_gateway_agent_sessions",
			Help: "Agent sessions this Gateway holds.",
		}, func() float64 { return float64(count()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "zenthros_gateway_agent_sessions_max",
			Help: "Agent sessions this Gateway admits at most (0: no cap).",
		}, func() float64 { return float64(maxSessions) }),
	}
}

var (
	gatewayLogChannels = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zenthros_gateway_log_channels",
		Help: "Agent LogChannel connections this Gateway holds. Each is a TCP beside the agent's Register session: open only while someone reads the machine's logs when the agent opens it on demand, always otherwise.",
	})
	gatewayLogChannelsRefused = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zenthros_gateway_log_channels_refused_total",
		Help: "LogChannels and log reads refused because this Gateway held its maximum of LogChannels.",
	})
	gatewayLogPullsBehind = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zenthros_gateway_log_reads_behind_total",
		Help: "Log reads ended because their reader fell behind a LogChannel other reads share.",
	})
	gatewayLogStreamsRefused = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zenthros_gateway_log_streams_refused_total",
		Help: "Log reads refused because this Gateway served its maximum of concurrent log streams (RESOURCE_EXHAUSTED).",
	})
	gatewayLogShed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "zenthros_gateway_log_shed_total",
		Help: "Log readers (kind=stream) and LogChannels (kind=channel) this Gateway ended because it was short of memory.",
	}, []string{"kind"})
	gatewayLogChannelOpen = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zenthros_gateway_log_channel_open_seconds",
		Help:    "How long a log reader waited for the agent to open the LogChannel it was asked for.",
		Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
	})
	gatewayLogBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zenthros_gateway_log_bytes_total",
		Help: "Log bytes this Gateway relayed from agents' LogChannels to readers.",
	})
)

// LogChannelOpened counts one agent LogChannel admitted by this Gateway.
func LogChannelOpened() { gatewayLogChannels.Inc() }

// LogChannelClosed counts one agent LogChannel gone from this Gateway.
func LogChannelClosed() { gatewayLogChannels.Dec() }

// LogChannelRefused counts one LogChannel, or one read that needed one,
// refused at the Gateway's cap.
func LogChannelRefused() { gatewayLogChannelsRefused.Inc() }

// LogPullBehind counts one log read ended for falling behind its channel.
func LogPullBehind() { gatewayLogPullsBehind.Inc() }

// LogStreamRefused counts one log read refused at the Gateway's stream cap.
func LogStreamRefused() { gatewayLogStreamsRefused.Inc() }

// LogReadersShed counts the readers and LogChannels a Gateway short of
// memory ended.
func LogReadersShed(streams, channels int) {
	gatewayLogShed.WithLabelValues("stream").Add(float64(streams))
	gatewayLogShed.WithLabelValues("channel").Add(float64(channels))
}

// ObserveLogChannelOpen records how long a reader waited for an asked
// LogChannel.
func ObserveLogChannelOpen(d time.Duration) { gatewayLogChannelOpen.Observe(d.Seconds()) }

// AddGatewayLogBytes counts log bytes relayed to a reader.
func AddGatewayLogBytes(n int) { gatewayLogBytes.Add(float64(n)) }

var gatewayReplicasExcluded = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "zenthros_gateway_replicas_excluded",
	Help: "Gateway replicas the Controller no longer waits for: silent past the exclusion bound with their pod not Ready. While one is excluded, the machines it held count as absent.",
})

// SetGatewayReplicasExcluded is how many Gateway replicas the Controller
// left out of its complete views.
func SetGatewayReplicasExcluded(n int) { gatewayReplicasExcluded.Set(float64(n)) }
