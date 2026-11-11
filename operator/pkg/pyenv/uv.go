// SPDX-License-Identifier: Apache-2.0

package pyenv

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/urlredact"
)

// Where uv comes from: the same artifact base and SHA256SUMS pin the agent was
// installed from. SHA256SUMS lists upstream.lock, and upstream.lock pins uv.
// An artifact named in ZENTHROS_ARTIFACT_URLS ("name=url ...", presigned URLs
// from a private bucket, say) comes from its own URL instead of the base.
const (
	EnvInstallBase  = "ZENTHROS_INSTALL_BASE"
	EnvSumsSHA256   = "ZENTHROS_SHA256SUMS_SHA256"
	EnvArtifactURLs = "ZENTHROS_ARTIFACT_URLS"
)

// maxDownload bounds every download; uv is about 20 MB.
const maxDownload = 256 << 20

// downloadRetryWaits is how long a download that failed on the network waits
// before each new try, about two minutes in all: a name that does not resolve
// for an instant (local suite, 2026-10-13) or a release host that answers 504
// for half a minute (full suite, 2026-11-05) must not leave a container without
// its Python, because the agent exits when uv cannot be fetched.
var downloadRetryWaits = []time.Duration{2 * time.Second, 5 * time.Second, 10 * time.Second, 20 * time.Second, 30 * time.Second, 60 * time.Second}

// statusError is a server's answer that is not the file.
type statusError struct {
	code   int
	status string
}

func (e *statusError) Error() string { return e.status }

// Fetch is where FetchUV finds uv and puts it.
type Fetch struct {
	// Base is the artifact base (ZENTHROS_INSTALL_BASE).
	Base string
	// ArtifactURLs are per-artifact URLs (ZENTHROS_ARTIFACT_URLS) by name;
	// they win over Base.
	ArtifactURLs map[string]string
	// SumsSHA256 is the expected SHA-256 of SHA256SUMS.
	SumsSHA256 string
	// Arch is amd64 or arm64.
	Arch string
	// Dir receives the uv binary.
	Dir string
	// Get downloads a URL; plain HTTP GET when nil.
	Get func(ctx context.Context, url string) ([]byte, error)
}

// FetchUV returns the path of a uv binary verified through the pin chain:
// SHA256SUMS against SumsSHA256, upstream.lock against SHA256SUMS, the uv
// archive against upstream.lock. Nothing unverified is written where it
// could run. A uv already fetched from the same archive is reused.
func FetchUV(ctx context.Context, f Fetch) (string, error) {
	if (f.Base == "" && len(f.ArtifactURLs) == 0) || f.SumsSHA256 == "" {
		return "", fmt.Errorf("uv: %s (or %s) and %s are required to fetch uv", EnvInstallBase, EnvArtifactURLs, EnvSumsSHA256)
	}
	get := f.Get
	if get == nil {
		get = httpGet
	}
	sums, err := getVerified(ctx, get, f.url("SHA256SUMS"), f.SumsSHA256)
	if err != nil {
		return "", err
	}
	lockSum, ok := sumsEntry(sums, "upstream.lock")
	if !ok {
		return "", errors.New("uv: SHA256SUMS does not list upstream.lock")
	}
	lock, err := getVerified(ctx, get, f.url("upstream.lock"), lockSum)
	if err != nil {
		return "", err
	}
	archiveSum, url, ok := lockRow(lock, "uv@", f.Arch)
	if !ok {
		return "", fmt.Errorf("uv: upstream.lock has no uv for %s", f.Arch)
	}
	dest := filepath.Join(f.Dir, "uv")
	marker := dest + ".sha256"
	if got, _ := os.ReadFile(marker); string(got) == archiveSum && exists(dest) {
		return dest, nil
	}
	archive, err := getVerified(ctx, get, url, archiveSum)
	if err != nil {
		return "", err
	}
	binary, err := uvFromArchive(archive)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(f.Dir, 0o755); err != nil {
		return "", err
	}
	tmp := dest + ".tmp"
	if err := os.WriteFile(tmp, binary, 0o755); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, dest); err != nil {
		return "", err
	}
	if err := os.WriteFile(marker, []byte(archiveSum), 0o644); err != nil {
		return "", err
	}
	return dest, nil
}

// url is where the artifact name comes from: its own URL, else the base.
func (f Fetch) url(name string) string {
	if url, ok := f.ArtifactURLs[name]; ok {
		return url
	}
	return strings.TrimRight(f.Base, "/") + "/" + name
}

// ParseArtifactURLs reads ZENTHROS_ARTIFACT_URLS: "name=url" pairs separated
// by white space.
func ParseArtifactURLs(raw string) map[string]string {
	urls := map[string]string{}
	for _, pair := range strings.Fields(raw) {
		if name, url, ok := strings.Cut(pair, "="); ok && name != "" && url != "" {
			urls[name] = url
		}
	}
	return urls
}

func getVerified(ctx context.Context, get func(context.Context, string) ([]byte, error), url, want string) ([]byte, error) {
	body, err := getRetrying(ctx, get, url)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", urlredact.URL(url), urlredact.Error(err))
	}
	sum := sha256.Sum256(body)
	if got := hex.EncodeToString(sum[:]); !strings.EqualFold(got, want) {
		return nil, fmt.Errorf("checksum mismatch for %s: got %s, want %s", urlredact.URL(url), got, want)
	}
	return body, nil
}

// getRetrying downloads url, again after each of downloadRetryWaits while
// the failure is the network's.
func getRetrying(ctx context.Context, get func(context.Context, string) ([]byte, error), url string) ([]byte, error) {
	for attempt := 0; ; attempt++ {
		body, err := get(ctx, url)
		if err == nil {
			return body, nil
		}
		if attempt >= len(downloadRetryWaits) || !failedOnTheNetwork(err) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(downloadRetryWaits[attempt]):
		}
	}
}

// failedOnTheNetwork reports whether a download's error is the network's or
// a moment's answer (fault.TransientHTTP: 408, 429, any 5xx), not a refusal. A
// server's refusal (no such file, no access) and a name that does not exist
// are final; so is everything this package finds wrong itself.
func failedOnTheNetwork(err error) bool {
	var status *statusError
	if errors.As(err, &status) {
		return fault.TransientHTTP(status.code)
	}
	var dns *net.DNSError
	if errors.As(err, &dns) && dns.IsNotFound {
		return false
	}
	var network net.Error
	return errors.As(err, &network) || errors.Is(err, io.ErrUnexpectedEOF)
}

// sumsEntry finds name's digest in a sha256sum listing.
func sumsEntry(sums []byte, name string) (string, bool) {
	scanner := bufio.NewScanner(bytes.NewReader(sums))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 2 && strings.TrimPrefix(fields[1], "*") == name {
			return fields[0], true
		}
	}
	return "", false
}

// lockRow is upstream.lock's "key arch sha256 url" row for the first key
// starting with prefix, on arch: the same rule as install-agent.sh lock_row.
func lockRow(lock []byte, prefix, arch string) (sum, url string, ok bool) {
	scanner := bufio.NewScanner(bytes.NewReader(lock))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 4 && strings.HasPrefix(fields[0], prefix) && (fields[1] == arch || fields[1] == "all") {
			return fields[2], fields[3], true
		}
	}
	return "", "", false
}

// uvFromArchive is the uv binary in an uv release tarball
// (uv-<triple>/uv).
func uvFromArchive(archive []byte) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return nil, fmt.Errorf("uv archive: %w", err)
	}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil, errors.New("uv archive has no uv binary")
		}
		if err != nil {
			return nil, fmt.Errorf("uv archive: %w", err)
		}
		if hdr.Typeflag == tar.TypeReg && path.Base(hdr.Name) == "uv" {
			return io.ReadAll(io.LimitReader(tr, maxDownload))
		}
	}
}

func httpGet(ctx context.Context, url string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, &statusError{code: resp.StatusCode, status: resp.Status}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxDownload+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxDownload {
		return nil, fmt.Errorf("larger than %d bytes", maxDownload)
	}
	return body, nil
}
