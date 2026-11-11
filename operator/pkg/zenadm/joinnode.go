// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"k8s.io/apimachinery/pkg/api/resource"

	infrav1alpha1 "github.com/zenthros/operator/api/infrastructure/v1alpha1"
	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/bootstrap"
	"github.com/zenthros/operator/pkg/fault"
	"github.com/zenthros/operator/pkg/nodejoin"
	"github.com/zenthros/operator/pkg/tlsconfig"
)

// JoinNode makes an enrolled machine of a joined pool a node of its cluster
// (docs/design/K8S_NODE_SUPPLY.md). The machine held no cluster credential
// until now: with its machine certificate it asks the gateway for one, sealed
// by the cluster's agent to a key it makes for this request, checks that it
// reaches the cluster's API and that the API's certificate chains to the CA in
// the credential, then starts the node: a kubelet that TLS-bootstraps with
// the token (joinkubelet.go) or, on a k3s cluster, a k3s agent. The
// bootstrap token is written only to a 0600 file and works for minutes.

// k3s paths on a joined machine. Zenthros writes a config drop-in, so a
// machine image may keep its own /etc/rancher/k3s/config.yaml (a snapshotter,
// for instance).
const (
	k3sBinary        = "/usr/local/bin/k3s"
	k3sConfigDropIn  = "/etc/rancher/k3s/config.yaml.d/50-zenthros.yaml"
	k3sJoinTokenFile = "/etc/rancher/k3s/zenthros-join-token"
	k3sAgentUnitFile = "/etc/systemd/system/zenthros-k3s-agent.service"
	k3sAgentUnitName = "zenthros-k3s-agent.service"
)

// JoinNodeParams configures zenadm join-node.
type JoinNodeParams struct {
	ControllerAddr string
	TLSServerName  string
	// StateDir holds the machine certificate zenadm join wrote.
	StateDir string
	// Wait bounds how long the machine waits for its credential.
	Wait time.Duration

	// Root prefixes every file written; empty is /. Tests set it.
	Root string
	// Systemctl runs systemctl; nil runs the real one.
	Systemctl func(ctx context.Context, args ...string) error
	// Dial dials the gateway; nil dials ControllerAddr with the machine
	// certificate.
	Dial func(ctx context.Context) (*grpc.ClientConn, error)
	// Exec runs a command (modprobe); nil runs it.
	Exec func(ctx context.Context, name string, args ...string) error
	// LookPath finds the installer's containerd and runc; nil searches PATH.
	LookPath func(name string) (string, error)
	// FSType is a directory's statfs type; nil calls statfs.
	FSType func(dir string) (int64, error)
	// RuntimeReady says the installer's background runtime is in place; nil
	// reads its markers. RuntimeWait bounds the wait (10 min when zero).
	RuntimeReady func() bool
	RuntimeWait  time.Duration
	// HTTPClient downloads the pinned binaries; nil uses a client with a
	// timeout.
	HTTPClient *http.Client
	// PreflightRetry is the pause between attempts to reach the cluster's
	// API; 5 s when zero.
	PreflightRetry time.Duration
}

// JoinNode asks for the machine's node credential and joins the cluster.
func JoinNode(ctx context.Context, p JoinNodeParams, log logr.Logger) error {
	if p.StateDir == "" {
		p.StateDir = defaultStateDir
	}
	if p.Wait == 0 {
		p.Wait = 15 * time.Minute
	}
	conn, err := p.dial(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	hub := agentpb.NewNodeJoinClient(conn)

	key, err := nodejoin.NewKey()
	if err != nil {
		return err
	}
	plan, err := waitForCredential(ctx, hub, key.PublicKey().Bytes(), p.Wait, log)
	if err != nil {
		return err
	}
	credential, err := nodejoin.Open(key, plan.GetProviderId(), plan.GetSealed())
	if err != nil {
		return fmt.Errorf("open the node credential: %w", err)
	}
	log.Info("node credential received", "node", plan.GetNodeName(), "server", credential.Server,
		"k3s", plan.GetK3SVersion(), "expires", credential.ExpiresAt)

	if reason, err := preflight(ctx, credential, p.PreflightRetry); err != nil {
		if _, rerr := hub.Report(ctx, &agentpb.NodeJoinReport{Reason: reason, Message: err.Error()}); rerr != nil {
			log.Error(rerr, "could not report the failed join")
		}
		return fmt.Errorf("%s: %w", reason, err)
	}
	switch plan.GetMethod() {
	case nodejoin.MethodK3s:
		err = p.joinK3s(ctx, plan, credential)
	case nodejoin.MethodKubelet:
		err = p.joinKubelet(ctx, plan, credential, log)
	default:
		err = fmt.Errorf("join method %q is not one this zenadm knows", plan.GetMethod())
	}
	if err != nil {
		return err
	}
	log.Info("node started", "node", plan.GetNodeName(), "method", plan.GetMethod())
	return nil
}

// joinK3s installs the pinned k3s and starts its agent: k3s brings the
// cluster's network (flannel, kube-proxy) with it.
func (p JoinNodeParams) joinK3s(ctx context.Context, plan *agentpb.NodeJoinCredentialResponse, credential nodejoin.Credential) error {
	if plan.GetK3SUrl() == "" || plan.GetK3SSha256() == "" {
		return errors.New("the hub sent no pinned k3s to join the cluster with")
	}
	reserved, err := planReservations(plan)
	if err != nil {
		return err
	}
	if err := p.install(ctx, "k3s", plan.GetK3SUrl(), plan.GetK3SSha256(), p.path(k3sBinary)); err != nil {
		return err
	}
	if err := p.writeK3sAgent(plan, credential, reserved); err != nil {
		return err
	}
	if err := p.systemctl(ctx, "daemon-reload"); err != nil {
		return err
	}
	return p.systemctl(ctx, "enable", "--now", "--no-block", k3sAgentUnitName)
}

// reservations is what the node's kubelet holds back, as the hub's plan
// says (nodeshape): the template Cluster Autoscaler plans with holds back
// the same. Each is a Kubernetes quantity, or empty: the kubelet's own.
type reservations struct {
	CPU, Memory, EvictionMemory string
}

func planReservations(plan *agentpb.NodeJoinCredentialResponse) (reservations, error) {
	r := reservations{CPU: plan.GetKubeReservedCpu(), Memory: plan.GetKubeReservedMemory(), EvictionMemory: plan.GetEvictionHardMemory()}
	for name, v := range map[string]string{"kube_reserved_cpu": r.CPU, "kube_reserved_memory": r.Memory, "eviction_hard_memory": r.EvictionMemory} {
		if _, err := resource.ParseQuantity(v); v != "" && err != nil {
			return reservations{}, fmt.Errorf("the hub's %s %q: %w", name, v, err)
		}
	}
	return r, nil
}

// kubeReserved is the kubelet's kubeReserved, nil when it holds nothing back.
func (r reservations) kubeReserved() map[string]string {
	reserved := map[string]string{}
	if r.CPU != "" {
		reserved["cpu"] = r.CPU
	}
	if r.Memory != "" {
		reserved["memory"] = r.Memory
	}
	if len(reserved) == 0 {
		return nil
	}
	return reserved
}

// k3sKubeletArgs pass the reservations to k3s' kubelet. k3s sets its own
// eviction-hard (disk only); the memory threshold joins its disk ones.
func (r reservations) k3sKubeletArgs() []string {
	var args []string
	if reserved := r.kubeReserved(); reserved != nil {
		pairs := make([]string, 0, len(reserved))
		for _, name := range []string{"cpu", "memory"} {
			if v, ok := reserved[name]; ok {
				pairs = append(pairs, name+"="+v)
			}
		}
		args = append(args, "kube-reserved="+strings.Join(pairs, ","))
	}
	if r.EvictionMemory != "" {
		args = append(args, "eviction-hard=memory.available<"+r.EvictionMemory+",imagefs.available<5%,nodefs.available<5%")
	}
	return args
}

// kubeletEvictionHard is the kubelet's evictionHard with the plan's memory
// threshold. A KubeletConfiguration that sets evictionHard drops the
// signals it leaves out, so the kubelet's other Linux defaults are kept
// here.
func (r reservations) kubeletEvictionHard() map[string]string {
	if r.EvictionMemory == "" {
		return nil
	}
	return map[string]string{
		"memory.available":   r.EvictionMemory,
		"nodefs.available":   "10%",
		"nodefs.inodesFree":  "5%",
		"imagefs.available":  "15%",
		"imagefs.inodesFree": "5%",
	}
}

// waitForCredential asks until the cluster's agent issued the credential.
func waitForCredential(ctx context.Context, hub agentpb.NodeJoinClient, publicKey []byte, wait time.Duration, log logr.Logger) (*agentpb.NodeJoinCredentialResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	asked := false
	for {
		resp, err := hub.Credential(ctx, &agentpb.NodeJoinCredentialRequest{PublicKey: publicKey})
		retry := 2 * time.Second
		switch {
		case err == nil && resp.GetReady():
			return resp, nil
		case err == nil:
			if !asked {
				log.Info("asked for the node credential; waiting for the cluster to issue it")
				asked = true
			}
			if resp.GetRetryAfterSeconds() > 0 {
				retry = time.Duration(resp.GetRetryAfterSeconds()) * time.Second
			}
		case permanent(err):
			return nil, fmt.Errorf("node credential refused: %w", err)
		default:
			log.Info("node credential not available yet", "error", err.Error())
			retry = 5 * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("no node credential within %s: %w", wait, ctx.Err())
		case <-time.After(retry):
		}
	}
}

// permanent is the hub's refusal, which asking again cannot change; a check
// it could not make is asked again (fault.Retryable, c4-B4).
func permanent(err error) bool {
	return !fault.Retryable(err)
}

// preflight checks the machine reaches the cluster's API and that the API's
// certificate chains to the credential's CA. It returns the reason to report.
func preflight(ctx context.Context, credential nodejoin.Credential, retry time.Duration) (string, error) {
	if retry == 0 {
		retry = 5 * time.Second
	}
	u, err := url.Parse(credential.Server)
	if err != nil {
		return infrav1alpha1.NodeJoinReasonRejected, err
	}
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "443")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(credential.CACert) {
		return infrav1alpha1.NodeJoinReasonRejected, errors.New("the credential's CA is not PEM")
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return infrav1alpha1.NodeJoinReasonUnreachable, ctx.Err()
			case <-time.After(retry):
			}
		}
		dialer := &net.Dialer{Timeout: 10 * time.Second}
		raw, err := dialer.DialContext(ctx, "tcp", host)
		if err != nil {
			lastErr = fmt.Errorf("the cluster's API %s is unreachable from this machine (it must share the cluster's network): %w", host, err)
			continue
		}
		// The API's certificate may not name the address nodes use; its
		// chain to the cluster CA is what the node will trust.
		conn := tls.Client(raw, &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec // the chain is verified below
			VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
				return verifyChain(rawCerts, roots)
			},
		})
		err = conn.HandshakeContext(ctx)
		_ = conn.Close()
		if err != nil {
			var verify chainError
			if errors.As(err, &verify) {
				return infrav1alpha1.NodeJoinReasonRejected, fmt.Errorf("the cluster's API at %s does not present a certificate of the cluster's CA: %w", host, err)
			}
			lastErr = fmt.Errorf("TLS to the cluster's API %s: %w", host, err)
			continue
		}
		return "", nil
	}
	return infrav1alpha1.NodeJoinReasonUnreachable, lastErr
}

type chainError struct{ error }

func verifyChain(rawCerts [][]byte, roots *x509.CertPool) error {
	if len(rawCerts) == 0 {
		return chainError{errors.New("no certificate")}
	}
	certs := make([]*x509.Certificate, 0, len(rawCerts))
	for _, raw := range rawCerts {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			return chainError{err}
		}
		certs = append(certs, cert)
	}
	intermediates := x509.NewCertPool()
	for _, cert := range certs[1:] {
		intermediates.AddCert(cert)
	}
	if _, err := certs[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates}); err != nil {
		return chainError{err}
	}
	return nil
}

// install puts a pinned binary in place: only the pin says it is of the
// cluster's minor, so a binary already on the machine is never trusted.
func (p JoinNodeParams) install(ctx context.Context, component, url, sha string, target string) error {
	tmp, err := p.fetch(ctx, component, url, sha, filepath.Dir(target))
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if err := os.Chmod(tmp, 0o755); err != nil {
		return err
	}
	return os.Rename(tmp, target)
}

// fetch downloads url into a temporary file in dir and returns its path
// once its SHA-256 matches sha. A digest mismatch is not retried.
func (p JoinNodeParams) fetch(ctx context.Context, component, url, sha, dir string) (string, error) {
	if url == "" || sha == "" {
		return "", fmt.Errorf("the hub sent no pinned %s to join the cluster with", component)
	}
	client := p.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	}
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 3 * time.Second)
		}
		var tmp string
		if tmp, lastErr = download(ctx, client, url, sha, dir); lastErr == nil {
			return tmp, nil
		}
		if errors.Is(lastErr, errDigest) {
			return "", fmt.Errorf("%s: %w", component, lastErr)
		}
	}
	return "", fmt.Errorf("download %s: %w", component, lastErr)
}

var errDigest = errors.New("the download does not match its pinned SHA-256; not joining the cluster")

func download(ctx context.Context, client *http.Client, url, sha, dir string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s: %s", url, resp.Status)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(dir, ".zenthros-download-*")
	if err != nil {
		return "", err
	}
	sum := sha256.New()
	_, err = io.Copy(io.MultiWriter(tmp, sum), resp.Body)
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil && hex.EncodeToString(sum.Sum(nil)) != strings.ToLower(sha) {
		err = errDigest
	}
	if err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}

// writeK3sAgent writes the token file (0600), the agent's config drop-in and
// its unit.
func (p JoinNodeParams) writeK3sAgent(plan *agentpb.NodeJoinCredentialResponse, credential nodejoin.Credential, reserved reservations) error {
	labels := make([]string, 0, len(plan.GetLabels()))
	for k, v := range plan.GetLabels() {
		labels = append(labels, k+"="+v)
	}
	sort.Strings(labels)
	// JSON is YAML: every value is quoted, nothing a label or URL holds is
	// read as YAML.
	config, err := json.MarshalIndent(map[string]any{
		"server":      credential.Server,
		"token-file":  k3sJoinTokenFile,
		"node-name":   plan.GetNodeName(),
		"kubelet-arg": append([]string{"provider-id=" + plan.GetProviderId()}, reserved.k3sKubeletArgs()...),
		"node-label":  labels,
		"node-taint":  plan.GetTaints(),
	}, "", "  ")
	if err != nil {
		return err
	}
	unit := `[Unit]
Description=Zenthros node join (k3s agent)
Wants=network-online.target
After=network-online.target

[Service]
Type=notify
Delegate=yes
KillMode=process
LimitNOFILE=1048576
Restart=always
RestartSec=5
ExecStart=` + k3sBinary + ` agent

[Install]
WantedBy=multi-user.target
`
	for _, f := range []struct {
		path string
		data []byte
		mode os.FileMode
	}{
		{k3sJoinTokenFile, []byte(nodejoin.K3sToken(credential) + "\n"), 0o600},
		{k3sConfigDropIn, append(config, '\n'), 0o600},
		{k3sAgentUnitFile, []byte(unit), 0o644},
	} {
		if err := writeFileAtomic(p.path(f.path), f.data, f.mode); err != nil {
			return err
		}
	}
	return nil
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

func (p JoinNodeParams) path(abs string) string {
	if p.Root == "" {
		return abs
	}
	return filepath.Join(p.Root, abs)
}

func (p JoinNodeParams) systemctl(ctx context.Context, args ...string) error {
	if p.Systemctl != nil {
		return p.Systemctl(ctx, args...)
	}
	out, err := exec.CommandContext(ctx, "systemctl", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("systemctl %s: %w: %s", strings.Join(args, " "), err, out)
	}
	return nil
}

func (p JoinNodeParams) dial(ctx context.Context) (*grpc.ClientConn, error) {
	if p.Dial != nil {
		return p.Dial(ctx)
	}
	if p.ControllerAddr == "" {
		return nil, errors.New("--controller is required")
	}
	tlsCfg, err := tlsconfig.ClientConfig{
		CertFile: filepath.Join(p.StateDir, "agent.crt"), KeyFile: filepath.Join(p.StateDir, "agent.key"),
		CAFile: filepath.Join(p.StateDir, "ca.crt"), ServerName: p.TLSServerName,
	}.Config()
	if err != nil {
		return nil, fmt.Errorf("machine certificate (zenadm join first): %w", err)
	}
	return grpc.NewClient(bootstrap.DialTarget(p.ControllerAddr, false),
		append(bootstrap.DialOptions(p.ControllerAddr, tlsCfg.ServerName), grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))...)
}
