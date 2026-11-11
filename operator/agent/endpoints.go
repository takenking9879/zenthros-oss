// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	v1 "github.com/zenthros/operator/api/v1"
	"github.com/zenthros/operator/pkg/backend"
	"github.com/zenthros/operator/pkg/cloudmeta"
	"github.com/zenthros/operator/pkg/infrastructure/provider/traits"
)

// EndpointTemplateEnv is how a machine's provider says where a port it
// exposed is reached: a URL with {port}, {name}, {scheme} (http or tcp, by
// the port's protocol), {env:VAR} (the agent's
// environment), {publicIPv4} and {peerAddr}, and an optional ";scope=X"
// suffix. The provider set the port up, so it writes the URL; the agent
// only fills it in once the port accepts connections.
const EndpointTemplateEnv = traits.EnvEndpointTemplate

// defaultEndpointTemplate is a machine no provider described: a VM, whose
// metadata service gives its public address (expand falls back to its own
// address, scope Private, when there is none), or an attached machine.
const defaultEndpointTemplate = "{scheme}://{publicIPv4}:{port}"

// Endpoint problems: the reason a running Service publishes no endpoint.
const (
	EndpointWaitingForPort     = "WaitingForPort"
	EndpointUnresolved         = "EndpointUnresolved"
	endpointProbeTimeout       = time.Second
	serviceSpecFile            = ".json"
	defaultServiceDirMode      = 0o700
	maxEndpointTemplateLength  = 1024
	maxServiceSpecFileBytes    = 64 << 10
	endpointTemplateScopeParam = ";scope="
)

// services remembers the Service spec of each job this agent runs, by
// handle, so its endpoints are published again after the agent restarts
// (a systemd unit outlives it). Without a directory it remembers in memory.
type services struct {
	dir string
	mu  sync.Mutex
	m   map[string]*v1.ServiceSpec
}

func newServices(dir string) *services {
	return &services{dir: dir, m: make(map[string]*v1.ServiceSpec)}
}

func (s *services) path(handle string) string {
	return filepath.Join(s.dir, filepath.Base(handle)+serviceSpecFile)
}

// Put records a handle's Service. A failure to persist is returned: the
// endpoints would be lost on a restart, not now.
func (s *services) Put(handle string, spec *v1.ServiceSpec) error {
	s.mu.Lock()
	s.m[handle] = spec
	s.mu.Unlock()
	if s.dir == "" {
		return nil
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(s.dir, defaultServiceDirMode); err != nil {
		return err
	}
	tmp := s.path(handle) + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(handle))
}

// Get returns a handle's Service, reading it back from disk after a restart.
func (s *services) Get(handle string) *v1.ServiceSpec {
	s.mu.Lock()
	defer s.mu.Unlock()
	if spec, ok := s.m[handle]; ok {
		return spec
	}
	if s.dir == "" {
		return nil
	}
	f, err := os.Open(s.path(handle))
	if err != nil {
		return nil
	}
	defer f.Close()
	var spec v1.ServiceSpec
	if err := json.NewDecoder(io.LimitReader(f, maxServiceSpecFileBytes)).Decode(&spec); err != nil || len(spec.Ports) == 0 {
		return nil
	}
	s.m[handle] = &spec
	return &spec
}

// Delete forgets a handle's Service.
func (s *services) Delete(handle string) {
	s.mu.Lock()
	delete(s.m, handle)
	s.mu.Unlock()
	if s.dir != "" {
		_ = os.Remove(s.path(handle))
	}
}

// endpointResolver turns a running Service's ports into URLs.
type endpointResolver struct {
	// template is the machine's EndpointTemplateEnv; empty uses the default.
	template   string
	getenv     func(string) string
	publicIPv4 func(context.Context) string
	peerAddr   func() string
	// dial checks that a port accepts connections on this machine.
	dial func(ctx context.Context, port int32) error
}

func newEndpointResolver() *endpointResolver {
	return &endpointResolver{
		template:   strings.TrimSpace(os.Getenv(EndpointTemplateEnv)),
		getenv:     os.Getenv,
		publicIPv4: publicIPv4,
		peerAddr:   func() string { return hostFacts().PeerAddr },
		dial:       dialLocal,
	}
}

// publicIPv4 is the host's public address from its metadata service, read
// once it is asked for: only a Service needs it.
var publicIPv4 = (&addressCache{detect: func(ctx context.Context) string {
	return cloudmeta.DetectFacts(ctx, cloudmeta.FactsSources()).PublicIPv4
}, now: time.Now}).get

// addressRetry is how often an address the metadata service did not give is
// asked for again.
const addressRetry = time.Minute

// addressCache keeps an address once it is known. A first read that failed,
// or ran on a caller's context that was then cancelled, was kept for the
// agent's life, and every Service endpoint read as Private until a restart.
// It reads on its own deadline, and again after
// addressRetry while it has none.
type addressCache struct {
	detect func(context.Context) string
	now    func() time.Time

	mu sync.Mutex
	ip string
	at time.Time
}

func (c *addressCache) get(context.Context) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ip != "" || (!c.at.IsZero() && c.now().Sub(c.at) < addressRetry) {
		return c.ip
	}
	ctx, cancel := context.WithTimeout(context.Background(), sourceTimeout)
	defer cancel()
	c.ip, c.at = c.detect(ctx), c.now()
	return c.ip
}

func dialLocal(ctx context.Context, port int32) error {
	d := net.Dialer{Timeout: endpointProbeTimeout}
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port))))
	if err != nil {
		return err
	}
	return conn.Close()
}

// Resolve publishes every port's endpoint once all of them accept
// connections, or none and the reason. Never a partial set, and never a URL
// with a hole: an empty variable is a problem, not an endpoint.
func (r *endpointResolver) Resolve(ctx context.Context, spec *v1.ServiceSpec) ([]v1.Endpoint, string) {
	for _, p := range spec.Ports {
		if err := r.dial(ctx, p.Port); err != nil {
			return nil, fmt.Sprintf("%s: port %d (%s) does not accept connections yet", EndpointWaitingForPort, p.Port, p.Name)
		}
	}
	template, scope := r.template, v1.EndpointScopePublic
	if template == "" {
		template = defaultEndpointTemplate
	}
	endpoints := make([]v1.Endpoint, 0, len(spec.Ports))
	for _, p := range spec.Ports {
		u, s, err := r.expand(ctx, template, scope, p)
		if err != nil {
			return nil, fmt.Sprintf("%s: %v", EndpointUnresolved, err)
		}
		endpoints = append(endpoints, v1.Endpoint{Name: p.Name, Port: p.Port, URL: u, Scope: s})
	}
	return endpoints, ""
}

var (
	envReference = regexp.MustCompile(`\{env:([A-Za-z_][A-Za-z0-9_]*)\}`)
	knownScopes  = map[string]bool{v1.EndpointScopePublic: true, v1.EndpointScopePrivate: true, v1.EndpointScopeCluster: true}
)

// expand fills one port into the template.
func (r *endpointResolver) expand(ctx context.Context, template, scope string, p v1.ServicePort) (string, string, error) {
	if len(template) > maxEndpointTemplateLength {
		return "", "", fmt.Errorf("%s is longer than %d bytes", EndpointTemplateEnv, maxEndpointTemplateLength)
	}
	if base, s, ok := strings.Cut(template, endpointTemplateScopeParam); ok {
		if !knownScopes[s] {
			return "", "", fmt.Errorf("%s names scope %q; Public, Private or Cluster", EndpointTemplateEnv, s)
		}
		template, scope = base, s
	}
	scheme := "http"
	if p.Protocol == v1.ProtocolTCP {
		scheme = "tcp"
	}
	out := strings.NewReplacer("{port}", strconv.Itoa(int(p.Port)), "{name}", p.Name, "{scheme}", scheme).Replace(template)
	var missing []string
	out = envReference.ReplaceAllStringFunc(out, func(ref string) string {
		name := envReference.FindStringSubmatch(ref)[1]
		value := r.getenv(name)
		if value == "" {
			missing = append(missing, name)
		}
		return value
	})
	if len(missing) > 0 {
		return "", "", fmt.Errorf("%s is empty on this machine", strings.Join(missing, ", "))
	}
	if strings.Contains(out, "{publicIPv4}") {
		if ip := r.publicIPv4(ctx); ip != "" {
			out = strings.ReplaceAll(out, "{publicIPv4}", ip)
		} else {
			// No public address (a private subnet): the machine's own, and
			// only its network reaches it.
			out, scope = strings.ReplaceAll(out, "{publicIPv4}", "{peerAddr}"), v1.EndpointScopePrivate
		}
	}
	if strings.Contains(out, "{peerAddr}") {
		addr := r.peerAddr()
		if addr == "" {
			return "", "", errors.New("the machine has no address of its own (peerAddr)")
		}
		out = strings.ReplaceAll(out, "{peerAddr}", addr)
	}
	u, err := url.Parse(out)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", "", fmt.Errorf("%s does not make an absolute URL for port %d", EndpointTemplateEnv, p.Port)
	}
	return out, scope, nil
}

// withEndpoints adds a running Service's endpoints (or why it has none) to
// the state its executor observed. An executor that publishes endpoints
// itself (a Manifest's endpointFrom) is left alone, as is any other job.
func (a *Agent) withEndpoints(ctx context.Context, handle string, state backend.JobState) backend.JobState {
	if a.services == nil || state.Phase != v1.PhaseRunning || len(state.Endpoints) > 0 || state.EndpointProblem != "" {
		return state
	}
	spec := a.services.Get(handle)
	if spec == nil {
		return state
	}
	state.Endpoints, state.EndpointProblem = a.endpoints.Resolve(ctx, spec)
	return state
}

// endpointKey is what a status push compares besides the phase: a Service
// whose port opens, or whose URL changes, is pushed without a phase change.
func endpointKey(state backend.JobState) string {
	if len(state.Endpoints) == 0 {
		return state.EndpointProblem
	}
	var b strings.Builder
	for _, e := range state.Endpoints {
		fmt.Fprintf(&b, "%s|%d|%s|%s;", e.Name, e.Port, e.URL, e.Scope)
	}
	return b.String()
}
