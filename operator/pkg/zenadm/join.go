// SPDX-License-Identifier: Apache-2.0

// Package zenadm implements kubeadm-style provisioning for Zenthros: a
// machine's enrollment (join, join-node and the agent's deployment) and, for
// the hub's operator, cluster preparation, node supply and cloud accounts
// (the files built !public, which link the hub's code). It is the day-0
// counterpart to zctl (day-2).
package zenadm

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zenthros/operator/pkg/bootstrap"
	"github.com/zenthros/operator/pkg/cloudmeta"
	"github.com/zenthros/operator/pkg/fault"
)

const defaultStateDir = "/var/lib/zenthros"

const tailscaleReadyTimeout = 60 * time.Second

// enroll is bootstrap.Join; tests replace it.
var enroll = bootstrap.Join

// controllerWait is how long a join waits for the controller's address
// (the gateways) to take a connection before it enrolls anyway: through an
// install or an upgrade the address refuses connections until a gateway
// is up, and a join run then failed at once. The Hub behind the gateways
// is waited for by the gateway (hubclient.Enroll).
var controllerWait = 2 * time.Minute

// shortJoinControllerWait is that wait for a join a person pasted: their hub
// is up, so an address that does not answer is the network, said soon.
var shortJoinControllerWait = 20 * time.Second

// enrollTimeout bounds the Enroll call: a hub that takes the connection and
// never answers is a failed join, not a hung one (E-08).
var enrollTimeout = 90 * time.Second

// enrollError is a failed Enroll as a person reads it after pasting a join:
// a token the hub refuses needs a new attach; a hub it cannot reach is the
// network, and the token is not spent. Anything else, as it came.
func enrollError(addr string, err error) error {
	var grpcErr interface{ GRPCStatus() *status.Status }
	if errors.As(err, &grpcErr) {
		st := grpcErr.GRPCStatus()
		switch st.Code() {
		case codes.Unauthenticated, codes.PermissionDenied, codes.AlreadyExists:
			return fmt.Errorf("the hub refused this join (%s). Run zenthros attach again for a new one "+
				"if the token is used, expired or revoked", st.Message())
		case codes.Unavailable, codes.DeadlineExceeded:
			return unreachable(addr)
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return unreachable(addr)
	}
	return fmt.Errorf("enroll: %w", err)
}

// enrollRetryFor is how long a join asks again when the hub answered with
// something to retry (a check it could not make, its rate limit): one such
// answer ended a pasted join. A hub whose address takes no connection is not
// asked again: that is the network, and the person hears it soon.
var enrollRetryFor = 30 * time.Second

// enrollRetrying is enroll, asked again while the hub says to try again
// (Transient) and its address takes connections. An answer nobody knows
// (Unknown) is not asked again blindly: the token may be spent.
func enrollRetrying(ctx context.Context, addr string, params bootstrap.JoinParams, log Logger) (*bootstrap.Credential, error) {
	until := time.Now().Add(enrollRetryFor)
	pause := time.Second
	for {
		cred, err := enroll(ctx, params)
		if err == nil || fault.Of(err) != fault.Transient || time.Now().Add(pause).After(until) || !takesConnections(addr) {
			return cred, err
		}
		log.Info("the hub asks to try again", "error", err.Error(), "in", pause.String())
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(pause):
		}
		pause = min(2*pause, 8*time.Second)
	}
}

func takesConnections(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, controllerDial)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// unreachable names every address of the hub's list, and their ports.
func unreachable(list string) error {
	addrs := bootstrap.Addresses(list)
	ports := make([]string, len(addrs))
	for i, addr := range addrs {
		ports[i] = portOf(addr)
	}
	return fmt.Errorf("cannot reach the hub at %s: check that this machine reaches it (port %s), "+
		"then paste the join again; the token is not spent", strings.Join(addrs, " or "), strings.Join(ports, " or "))
}

func portOf(addr string) string {
	if _, port, err := net.SplitHostPort(addr); err == nil {
		return port
	}
	return "9443"
}

// controllerDial bounds one connection attempt of awaitController, and
// controllerRetry is the pause between two.
const (
	controllerDial  = 2 * time.Second
	controllerRetry = 500 * time.Millisecond
)

// awaitController returns the first address of the hub's list that takes a
// TCP connection, trying them in order each round (the Gateway's port, then
// Traefik's passthrough on 443), once ctx ends or wait passes the first. It
// only waits: the token goes out with the Enroll that follows, at the address
// it returns, and that call's error is the one the user reads.
func awaitController(ctx context.Context, list string, wait time.Duration, log Logger) string {
	addrs := bootstrap.Addresses(list)
	deadline := time.Now().Add(wait)
	for told := false; ; told = true {
		var err error
		for _, addr := range addrs {
			conn, dialErr := net.DialTimeout("tcp", addr, controllerDial)
			if dialErr == nil {
				_ = conn.Close()
				return addr
			}
			err = dialErr
		}
		if time.Now().After(deadline) {
			log.Info("controller still not reachable; enrolling anyway", "controller", list, "error", err.Error())
			return addrs[0]
		}
		if !told {
			log.Info("controller not reachable yet; waiting for it to listen", "controller", list, "upTo", wait.String(), "error", err.Error())
		}
		select {
		case <-ctx.Done():
			return addrs[0]
		case <-time.After(controllerRetry):
		}
	}
}

// DeployMode selects how the enrolled agent is installed and supervised.
const (
	DeploySystemd = "systemd"
	DeployK8s     = "k8s"
)

// Agent network modes (JoinParams.Network), written to the agent's
// ZENTHROS_NETWORK. They match the agent package's constants.
const (
	NetworkTailscale = "tailscale"
	NetworkDirect    = "direct"
)

// JoinParams configures a unified enrollment.
type JoinParams struct {
	ControllerAddr string
	TLSServerName  string
	Token          string
	CACertHash     string
	CACertFile     string // optional PEM CA; when set, enroll verifies against it instead of skip-verify+pin only
	BackendID      string
	BackendType    string
	StateDir       string
	DeployMode     string
	SkipIfExisting bool

	Tailscale bool
	TSAuthKey string
	// Network is how the agent reaches the controller: NetworkTailscale
	// (default) or NetworkDirect. It is independent of Tailscale: a machine
	// may bring up Tailscale itself before joining (managed bootstrap does).
	Network          string
	TSHostname       string
	Ownership        string
	ProviderIdentity string

	// Wait, when set, keeps the join until the agent it started is
	// connected to the hub, and fails it if it is not by then (a systemd
	// deploy only). The short join waits; a machine's own bootstrap does
	// not, as other steps follow it.
	Wait time.Duration
	// Out, when set, gets the join's few lines for a person (the short
	// join's): joining, joined as, connected.
	Out io.Writer
	// Folders are the machine's folders its owner opens to jobs (--folder):
	// the only places outside their own a job writes, with what the
	// folder's permissions give its user, and where a source.hostPath may
	// be (systemd.HostRootsEnv).
	Folders []string
}

// ValidFolder refuses a folder a job cannot be given: one that is not an
// absolute, clean path, the whole machine, or one with what the agent's
// list (colon-separated) or a unit's path list cannot hold.
func ValidFolder(folder string) error {
	switch {
	case !filepath.IsAbs(folder) || filepath.Clean(folder) != folder:
		return fmt.Errorf("--folder %q: give an absolute path, like /data", folder)
	case folder == "/":
		return fmt.Errorf("--folder /: that is the whole machine; give the folder jobs need, like /data")
	case strings.ContainsAny(folder, `:\"`) || strings.ContainsFunc(folder, func(r rune) bool { return r < 0x20 || r == 0x7f }):
		return fmt.Errorf("--folder %q: a folder name cannot hold a colon, a quote, a backslash or a control character", folder)
	}
	return nil
}

// DetectDeployMode is how the agent runs on the machine whose root is root:
// a systemd unit where systemd runs, which a VM pasted on has even as a
// privileged pod with a service account; otherwise, in a pod (an
// enrollment Job), a Deployment through the cluster's API.
func DetectDeployMode(root string) string {
	if _, err := os.Stat(filepath.Join(root, "run/systemd/system")); err == nil {
		return DeploySystemd
	}
	if _, err := os.Stat(filepath.Join(root, "var/run/secrets/kubernetes.io/serviceaccount")); err == nil {
		return DeployK8s
	}
	return DeploySystemd
}

// ShortJoinWait is how long a short join (zt1_) waits for its agent to be
// connected before it fails.
const ShortJoinWait = 3 * time.Minute

// agentHealthAddr is where a systemd agent serves /readyz: on this machine
// only, for the join to wait on.
const agentHealthAddr = "127.0.0.1:8081"

// NetworkFor is how an agent reaches a hub at addr when nothing says: over
// the tailnet when the address is a tailnet name, directly otherwise.
func NetworkFor(addr string) string {
	if strings.HasSuffix(hostOf(addr), ".ts.net") {
		return NetworkTailscale
	}
	return NetworkDirect
}

// DefaultHubPort is the port a hub's gateways take agents on.
const DefaultHubPort = "9443"

// WithDefaultPort is the hub's address list with the hub's port on each
// address that names none.
func WithDefaultPort(list string) string {
	addrs := bootstrap.Addresses(list)
	for i, addr := range addrs {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			addrs[i] = net.JoinHostPort(strings.Trim(addr, "[]"), DefaultHubPort)
		}
	}
	return strings.Join(addrs, ",")
}

// short is whether the join's token is a short one, which carries the CA
// pin and stands for its backend.
func (p JoinParams) short() bool {
	_, _, ok := bootstrap.SplitJoinToken(p.Token)
	return ok
}

// Validate fails fast on missing required fields.
func (p JoinParams) Validate() error {
	required := map[string]string{"--controller": p.ControllerAddr, "--token": p.Token}
	if !p.short() {
		// A short token carries the pin and names the backend it was made for.
		required["--ca-cert-hash"], required["--backend-id"] = p.CACertHash, p.BackendID
	}
	for name, val := range required {
		if val == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if len(bootstrap.Addresses(p.ControllerAddr)) == 0 {
		return fmt.Errorf("--controller %q names no address: give host:port, or several separated by commas", p.ControllerAddr)
	}
	if p.Tailscale && p.TSAuthKey == "" {
		return fmt.Errorf("--ts-authkey is required when --tailscale is set")
	}
	switch p.Network {
	case "", NetworkTailscale:
	case NetworkDirect:
		if p.Tailscale {
			return fmt.Errorf("--tailscale contradicts --network %s", NetworkDirect)
		}
	default:
		return fmt.Errorf("--network must be %q or %q", NetworkTailscale, NetworkDirect)
	}
	if p.DeployMode != DeploySystemd && p.DeployMode != DeployK8s {
		return fmt.Errorf("--deploy must be %q or %q", DeploySystemd, DeployK8s)
	}
	if p.Ownership == "" {
		p.Ownership = "External"
	}
	if p.Ownership != "External" && p.Ownership != "Managed" {
		return fmt.Errorf("--ownership must be %q or %q", "External", "Managed")
	}
	if p.Ownership == "Managed" && p.ProviderIdentity == "" {
		return fmt.Errorf("--provider-identity is required for managed enrollment")
	}
	for _, folder := range p.Folders {
		if err := ValidFolder(folder); err != nil {
			return err
		}
	}
	return nil
}

// Join enrolls a node against the controller and deploys the agent. It is the
// single unified enrollment flow for both VM and K8s backends:
//
//  1. Optional Tailscale setup
//  2. Verify CA pin (TOFU)
//  3. CSR → Enroll RPC → mTLS certificate
//  4. Persist credentials
//  5. Deploy + start agent (systemd unit or K8s Deployment)
//  6. Exit
func Join(ctx context.Context, p JoinParams, log Logger) error {
	if p.StateDir == "" {
		p.StateDir = defaultStateDir
	}
	if p.BackendType == "" {
		p.BackendType = "vm"
	}
	if p.Ownership == "" {
		p.Ownership = "External"
	}
	if p.Network == "" {
		p.Network = NetworkFor(p.ControllerAddr)
	}
	if err := p.Validate(); err != nil {
		return err
	}
	if pin, token, ok := bootstrap.SplitJoinToken(p.Token); ok {
		if p.CACertHash != "" && !strings.EqualFold(p.CACertHash, pin) {
			return fmt.Errorf("the token's CA pin %s and --ca-cert-hash %s differ", pin, p.CACertHash)
		}
		p.Token, p.CACertHash = token, pin
		// The same line pasted again, or after a reinstall, on a machine this
		// hub enrolled with it: it is that machine, not a spent token (E-08).
		// Credentials from another hub, or from another line of this one (a
		// machine detached and attached again, or moved to another
		// Workspace), are enrolled over.
		if !p.SkipIfExisting && p.DeployMode == DeploySystemd {
			p.SkipIfExisting = enrolledBy(p.StateDir, pin, token)
		}
	}
	say := func(format string, args ...any) {
		if p.Out != nil {
			fmt.Fprintf(p.Out, format+"\n", args...)
		}
	}

	if p.SkipIfExisting {
		ready, err := existingDeploymentReady(ctx, p)
		if err != nil {
			return err
		}
		if ready {
			if p.BackendID == "" && p.DeployMode == DeploySystemd {
				// A short join again: the machine is the one its certificate names.
				id, err := existingBackendID(p.StateDir)
				if err != nil {
					return err
				}
				p.BackendID = id
			}
			log.Info("agent credentials already exist; skipping enrollment", "backendID", p.BackendID)
			if p.DeployMode == DeploySystemd {
				if err := deployUnit(p); err != nil {
					return err
				}
				return awaitAgent(ctx, p, say)
			}
			return nil
		}
	}

	// The token is spent at one address: the first that answers. The unit
	// keeps the whole list, for the agent's own reconnections.
	controllerHost, enrollAddr := hostOf(p.ControllerAddr), bootstrap.FirstAddress(p.ControllerAddr)
	renameDevice := false
	if p.Tailscale {
		if p.TSHostname == "" {
			var err error
			if p.TSHostname, renameDevice, err = deviceHostname(p.BackendID, os.Hostname); err != nil {
				return err
			}
		}
		log.Info("joining tailnet (ephemeral)", "hostname", p.TSHostname)
		if err := tailscaleUp(ctx, p.TSAuthKey, p.TSHostname); err != nil {
			return err
		}
		if err := waitForHost(ctx, enrollAddr, tailscaleReadyTimeout); err != nil {
			return fmt.Errorf("tailnet not reachable after join (is the controller online?): %w", err)
		}
	} else if strings.HasSuffix(controllerHost, ".ts.net") {
		// Enrollment Jobs join the tailnet via a sidecar; wait until MagicDNS works
		// before dialing the controller.
		log.Info("waiting for tailnet DNS", "host", controllerHost, "addr", enrollAddr)
		if err := waitForHost(ctx, enrollAddr, tailscaleReadyTimeout); err != nil {
			return fmt.Errorf("controller not reachable over tailnet (is the sidecar online?): %w", err)
		}
	} else {
		wait := controllerWait
		if p.Out != nil {
			// A person pasted the join: their hub is up, and they are
			// waiting at the terminal.
			wait = shortJoinControllerWait
			say("Joining %s…", p.ControllerAddr)
		}
		enrollAddr = awaitController(ctx, p.ControllerAddr, wait, log)
	}

	// Where the credentials go is made writable before the token is spent:
	// a one-time token enrolled, then a directory sudo could not make, was
	// a token gone and nothing written.
	if err := ensureStateDir(p.StateDir); err != nil {
		return err
	}
	if !dirUsable(p.StateDir) {
		return fmt.Errorf("state dir %s is not writable by this user; the token was not used", p.StateDir)
	}
	log.Info("enrolling with controller", "controller", p.ControllerAddr, "backendID", p.BackendID)
	if p.Out == nil || p.Tailscale || strings.HasSuffix(controllerHost, ".ts.net") {
		say("Joining %s…", p.ControllerAddr)
	}
	var identityDocument, identitySignature []byte
	var err error
	if p.ProviderIdentity != "" {
		identityDocument, identitySignature, err = cloudmeta.IdentityEvidence(ctx, p.ProviderIdentity)
		if err != nil {
			return err
		}
	}
	var caPEM []byte
	if p.CACertFile != "" {
		var readErr error
		caPEM, readErr = os.ReadFile(p.CACertFile)
		if readErr != nil {
			return fmt.Errorf("read --ca-cert: %w", readErr)
		}
	}
	enrollCtx, cancel := context.WithTimeout(ctx, enrollTimeout)
	defer cancel()
	cred, err := enrollRetrying(enrollCtx, enrollAddr, bootstrap.JoinParams{
		ControllerAddr:    enrollAddr,
		TLSServerName:     p.TLSServerName,
		Token:             p.Token,
		CACertHash:        p.CACertHash,
		CACertPEM:         caPEM,
		BackendID:         p.BackendID,
		BackendType:       p.BackendType,
		IdentityDocument:  identityDocument,
		IdentitySignature: identitySignature,
	}, log)
	if err != nil {
		return enrollError(p.ControllerAddr, err)
	}
	if p.BackendID == "" {
		// A short join named none: the hub enrolled the token's own.
		if p.BackendID, err = cred.BackendID(); err != nil {
			return fmt.Errorf("the hub's certificate: %w", err)
		}
	}
	if err := cred.Write(p.StateDir); err != nil {
		return fmt.Errorf("write credentials: %w", err)
	}
	if p.DeployMode == DeploySystemd {
		if err := writeEnrolledToken(p.StateDir, p.Token); err != nil {
			return fmt.Errorf("write credentials: %w", err)
		}
	}
	if renameDevice {
		// The token is spent: a device left with the OS's name only
		// escapes tailnet cleanup, so the join goes on.
		if err := tailscaleSetHostname(ctx, p.BackendID); err != nil {
			log.Error(err, "the tailnet device keeps the OS hostname", "hostname", p.TSHostname)
		}
	}
	log.Info("enrolled; mTLS credentials written", "stateDir", p.StateDir, "backendID", p.BackendID)
	say("✓ joined as %s", p.BackendID)

	switch p.DeployMode {
	case DeploySystemd:
		if err := deployUnit(p); err != nil {
			return err
		}
		if err := awaitAgent(ctx, p, say); err != nil {
			return err
		}
		say("%s", jobsRunAs(p.Folders))
		return nil
	case DeployK8s:
		if err := deployK8s(ctx, p); err != nil {
			return err
		}
	}
	return nil
}

// jobsRunAs is what a person who joined a machine is told of its jobs: they
// never run as root, and write only their own directories and the folders
// opened to them.
func jobsRunAs(folders []string) string {
	if len(folders) == 0 {
		return "Jobs run as a user of their Workspace, never root, and write only their own directories. " +
			"To give them a folder of this machine: zenadm join again with --folder /data."
	}
	return "Jobs run as a user of their Workspace, never root, and write only their own directories and " +
		strings.Join(folders, ", ") + ", where that folder's permissions let them (sudo chmod o+rwx, or an ACL)."
}

// deployUnit installs and starts the agent's systemd unit; tests replace it.
var deployUnit = deploySystemd

// agentReady is the URL the join waits on; tests replace it.
var agentReady = "http://" + agentHealthAddr + "/readyz"

// awaitAgent returns once the agent the join started is connected to the
// hub, when the join waits (p.Wait). It fails, with where to look, if the
// agent is not by then: a join that ends 0 is a machine that is connected.
func awaitAgent(ctx context.Context, p JoinParams, say func(string, ...any)) error {
	if p.Wait <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, p.Wait)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	last := "no answer yet"
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, agentReady, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				say("✓ connected")
				return nil
			}
			last = "not connected to the hub yet"
		} else if ctx.Err() == nil {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("the agent of %s is not connected after %s (%s): journalctl -u %s -n 30 says why",
				p.BackendID, p.Wait, last, agentUnitName)
		case <-time.After(time.Second):
		}
	}
}

// existingBackendID is the backend the certificate in stateDir names.
func existingBackendID(stateDir string) (string, error) {
	pemBytes, err := os.ReadFile(filepath.Join(stateDir, "agent.crt"))
	if err != nil {
		return "", err
	}
	return bootstrap.CertCommonName(pemBytes)
}

func existingDeploymentReady(ctx context.Context, p JoinParams) (bool, error) {
	if p.DeployMode == DeploySystemd {
		return credentialsExist(p.StateDir), nil
	}
	c, err := k8sClient()
	if err != nil {
		return false, err
	}
	return agentSecretReady(ctx, c)
}

// enrolledBy reports whether stateDir holds credentials the hub whose CA pin
// is pin signed for token: the line that enrolled them (enrolledTokenFile).
func enrolledBy(stateDir, pin, token string) bool {
	if !credentialsExist(stateDir) {
		return false
	}
	ca, err := os.ReadFile(filepath.Join(stateDir, "ca.crt"))
	if err != nil {
		return false
	}
	got, err := bootstrap.CAPinFromPEM(ca)
	if err != nil || !strings.EqualFold(got, pin) {
		return false
	}
	id, err := bootstrap.TokenID(token)
	if err != nil {
		return false
	}
	enrolled, err := os.ReadFile(filepath.Join(stateDir, enrolledTokenFile))
	return err == nil && string(enrolled) == id
}

// enrolledTokenFile holds the ID of the token the credentials beside it were
// enrolled with (bootstrap.TokenID: its public part, never the secret).
const enrolledTokenFile = "join-token-id"

func writeEnrolledToken(stateDir, token string) error {
	id, err := bootstrap.TokenID(token)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(stateDir, enrolledTokenFile), []byte(id), 0o644)
}

// credentialsExist reports whether stateDir holds credentials a join may
// reuse (unexpired).
func credentialsExist(stateDir string) bool {
	for _, name := range []string{"agent.crt", "agent.key", "ca.crt"} {
		if _, err := os.Stat(filepath.Join(stateDir, name)); err != nil {
			return false
		}
	}
	cert, err := os.ReadFile(filepath.Join(stateDir, "agent.crt"))
	return err == nil && unexpired(cert)
}

// unexpired reports whether certPEM is a certificate still valid now. An
// expired one is no credential to reuse: a join enrolls over it. A machine
// or cluster away longer than its certificate's life cannot renew it (the
// renewal needs a connection the certificate opens), and skipping the
// enrollment left it cut off for good.
func unexpired(certPEM []byte) bool {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return false
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	return err == nil && time.Now().Before(cert.NotAfter)
}

// hostOf is the host the hub's address list names (its first address).
func hostOf(list string) string { return bootstrap.HostOf(list) }

// ensureStateDir creates the credential directory before the unprivileged credential
// write. It normally lives under /var/lib (root-owned), so create it with sudo and
// hand ownership to the invoking user; when join itself runs as root the chown is a
// no-op. This lets `zenadm join` work both as a normal user (README flow) and under
// sudo (kubeadm-style). When the directory already exists and is writable by the
// current user (e.g. a mktemp -d passed by deploy.sh) no privilege escalation is
// needed, so sudo is skipped entirely.
// stateDirCmd runs the privileged commands that make the state directory;
// tests replace it.
var stateDirCmd = runCmd

func ensureStateDir(dir string) error {
	if dirUsable(dir) {
		return nil
	}
	ctx := context.Background()
	if err := stateDirCmd(ctx, "sudo", "mkdir", "-p", dir); err != nil {
		return fmt.Errorf("create state dir %s: %w", dir, err)
	}
	user := os.Getenv("SUDO_USER")
	if user == "" {
		user = os.Getenv("USER")
	}
	if user != "" && user != "root" {
		if err := stateDirCmd(ctx, "sudo", "chown", user, dir); err != nil {
			return fmt.Errorf("chown state dir %s: %w", dir, err)
		}
	}
	return nil
}

// dirUsable reports whether dir already exists and the current user can create files
// in it. It probes with a temp file rather than reading mode bits so ACLs and other
// access mechanisms are honored.
func dirUsable(dir string) bool {
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return false
	}
	f, err := os.CreateTemp(dir, ".zenadm-write-probe-")
	if err != nil {
		return false
	}
	name := f.Name()
	_ = f.Close()
	_ = os.Remove(name)
	return true
}
