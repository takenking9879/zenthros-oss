// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/zenthros/operator/pkg/backend"
)

const rayLogPoll = 500 * time.Millisecond

type rayLogsResponse struct {
	Logs string `json:"logs"`
}

func (e *Executor) streamRayJobLogs(ctx context.Context, namespace, name string, follow bool, tailLines int) (<-chan backend.LogChunk, error) {
	fetch := e.rayLogs
	if fetch == nil {
		fetch = e.fetchRayJobLogsHTTP
	}
	ch := make(chan backend.LogChunk, 64)
	go func() {
		defer close(ch)
		e.pipeRayJobLogs(ctx, namespace, name, follow, tailLines, fetch, ch)
	}()
	return ch, nil
}

func (e *Executor) pipeRayJobLogs(
	ctx context.Context,
	namespace, name string,
	follow bool,
	tailLines int,
	fetch func(context.Context, string, string) (string, error),
	ch chan<- backend.LogChunk,
) {
	var seenLen int
	var tail string
	var chunk string
	first := true
	unchanged := 0
	deadline := time.Now().Add(containerStartGrace)
	for {
		var rayJob rayv1.RayJob
		if err := e.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &rayJob); err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(rayLogPoll):
				continue
			}
		}
		if rayJob.Status.JobId == "" || rayJob.Status.DashboardURL == "" {
			if !follow && time.Now().After(deadline) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(rayLogPoll):
				continue
			}
		}
		text, err := fetch(ctx, rayJob.Status.DashboardURL, rayJob.Status.JobId)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(rayLogPoll):
				continue
			}
		}
		chunk, tail, seenLen = nextLogDelta(text, tail, seenLen, first, tailLines)
		first = false
		if chunk != "" {
			unchanged = 0
			pipeToChannel(ctx, strings.NewReader(chunk), ch)
		} else {
			unchanged++
		}
		if !follow {
			return
		}
		if rayv1.IsJobTerminal(rayJob.Status.JobStatus) && unchanged >= 2 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(rayLogPoll):
		}
	}
}

func (e *Executor) fetchRayJobLogsHTTP(ctx context.Context, dashboardURL, jobID string) (string, error) {
	endpoint, err := rayJobLogsURL(dashboardURL, jobID)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	client := e.httpClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ray jobs logs %s: HTTP %d", endpoint, resp.StatusCode)
	}
	var parsed rayLogsResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", fmt.Errorf("decode ray jobs logs: %w", err)
	}
	return parsed.Logs, nil
}

func rayJobLogsURL(dashboardURL, jobID string) (string, error) {
	base := strings.TrimSpace(dashboardURL)
	if base == "" || jobID == "" {
		return "", fmt.Errorf("ray dashboard URL and job id are required")
	}
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	parsed, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parse dashboard URL: %w", err)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/api/jobs/" + url.PathEscape(jobID) + "/logs"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func tailLogText(text string, tailLines int) string {
	if tailLines < 0 || text == "" {
		return text
	}
	trimmed := strings.TrimRight(text, "\n")
	lines := strings.Split(trimmed, "\n")
	if tailLines >= len(lines) {
		return text
	}
	return strings.Join(lines[len(lines)-tailLines:], "\n") + "\n"
}

const logTailFingerprint = 64

// nextLogDelta returns the new bytes since the previous poll without retaining
// the entire dashboard log. tail is a bounded suffix used as a prefix check.
func nextLogDelta(text, tail string, seenLen int, first bool, tailLines int) (chunk, nextTail string, nextLen int) {
	nextLen = len(text)
	nextTail = textFingerprint(text)
	if first && tailLines >= 0 {
		return tailLogText(text, tailLines), nextTail, nextLen
	}
	if seenLen > 0 && len(text) >= seenLen && (tail == "" || strings.HasSuffix(text[:seenLen], tail) || strings.HasPrefix(text, tail)) {
		return text[seenLen:], nextTail, nextLen
	}
	return text, nextTail, nextLen
}

func textFingerprint(text string) string {
	if len(text) <= logTailFingerprint {
		return text
	}
	return text[len(text)-logTailFingerprint:]
}
