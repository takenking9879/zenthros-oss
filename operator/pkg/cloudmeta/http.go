// SPDX-License-Identifier: Apache-2.0

package cloudmeta

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	metadataBodyLimit = 64 << 10
	// noticeTimeout bounds one notice poll: it runs every few seconds, off a
	// cloud as well, where nothing answers.
	noticeTimeout = 200 * time.Millisecond
	// factsTimeout bounds one facts read, once per agent start.
	factsTimeout = 1500 * time.Millisecond
	// identityTimeout bounds one identity read at enrollment.
	identityTimeout = 5 * time.Second
)

var errMetadataStatus = errors.New("metadata endpoint status")

// metadataClient is a small HTTP client for a link-local metadata service.
type metadataClient struct {
	client *http.Client
}

func (m metadataClient) httpClient() *http.Client {
	if m.client != nil {
		return m.client
	}
	return http.DefaultClient
}

// fetch sends one request and returns its body when the status is 2xx.
func (m metadataClient) fetch(ctx context.Context, method, url string, header map[string]string) ([]byte, error) {
	status, body, err := m.do(ctx, method, url, header)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("%w: HTTP %d from %s", errMetadataStatus, status, url)
	}
	return body, nil
}

func (m metadataClient) do(ctx context.Context, method, url string, header map[string]string) (int, []byte, error) {
	request, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return 0, nil, err
	}
	for key, value := range header {
		request.Header.Set(key, value)
	}
	response, err := m.httpClient().Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, metadataBodyLimit+1))
	if err != nil {
		return response.StatusCode, nil, err
	}
	if len(body) > metadataBodyLimit {
		return response.StatusCode, nil, fmt.Errorf("metadata response exceeds %d bytes", metadataBodyLimit)
	}
	return response.StatusCode, body, nil
}
