// SPDX-License-Identifier: Apache-2.0

// Package containerhosts is what each container provider declares about the
// containers it runs, for the agent and the code that attaches a running one
// (zenadm's install-container.sh) to read without naming a provider. A new
// container provider adds its line here. The values are text, as the agent
// links no provider: each line is pinned to its provider's own constants by a
// test in that provider (containerhosts_test.go there).
package containerhosts

import "time"

// The env a container provider launches the agent with (containerkit's
// LaunchEnv), which cmd/agent reads.
const (
	// EnvContainerIDName names the variable that holds the container's id
	// (a Host's IDEnv): the agent's identity document.
	EnvContainerIDName = "ZENTHROS_CONTAINER_ID_ENV"
	// EnvPreemptGrace is how long a job has between its interruption
	// notice (SIGTERM) and the provider's kill.
	EnvPreemptGrace = "ZENTHROS_PREEMPT_GRACE"
	// EnvJobGrace is how long the agent's jobs may go unheard.
	EnvJobGrace = "ZENTHROS_JOB_GRACE"
)

type Host struct {
	// Provider is the provider's registry name.
	Provider string
	// IDEnv is the variable the provider sets in each of its containers:
	// what its Managed launches set (EnvContainerIDName), and what tells
	// an attached container of it apart.
	IDEnv string
	// JobGrace is the job grace its containers need, longer than the
	// default.
	JobGrace time.Duration
}

func All() []Host {
	return []Host{
		{Provider: "runpod", IDEnv: "RUNPOD_POD_ID", JobGrace: 75 * time.Second},
		{Provider: "vastai", IDEnv: "CONTAINER_ID", JobGrace: 75 * time.Second},
	}
}
