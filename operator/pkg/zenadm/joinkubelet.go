// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/util/validation"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/nodejoin"
	"github.com/zenthros/operator/pkg/runtimegate"
)

// The generic join (docs/design/K8S_NODE_SUPPLY.md): the pinned kubelet of
// the cluster's minor and the pinned CNI plugins, a containerd instance of
// the node's own (its socket, root and config; the installer's containerd
// for Zenthros jobs is left alone), and TLS bootstrapping: the kubelet
// authenticates with the bootstrap token, the cluster approves its client
// certificate, the token is never used again. The cluster's network
// (CNI, kube-proxy) comes as DaemonSets, as on any kubeadm node.
const (
	kubeletBinary        = "/usr/local/lib/zenthros-node/bin/kubelet"
	cniBinDir            = "/opt/cni/bin"
	cniConfDir           = "/etc/cni/net.d"
	nodeContainerdConfig = "/etc/zenthros-node/containerd.toml"
	nodeContainerdRoot   = "/var/lib/zenthros-node/containerd"
	nodeContainerdState  = "/run/zenthros-node/containerd"
	nodeContainerdSocket = "/run/zenthros-node/containerd.sock"
	nodeContainerdUnit   = "/etc/systemd/system/zenthros-node-containerd.service"
	nodeContainerdName   = "zenthros-node-containerd.service"
	kubeletCAFile        = "/etc/kubernetes/pki/ca.crt"
	bootstrapKubeconfig  = "/etc/kubernetes/bootstrap-kubelet.conf"
	kubeletKubeconfig    = "/etc/kubernetes/kubelet.conf"
	kubeletConfigFile    = "/var/lib/kubelet/config.yaml"
	kubeletUnitFile      = "/etc/systemd/system/zenthros-kubelet.service"
	kubeletUnitName      = "zenthros-kubelet.service"
	nodeSysctlFile       = "/etc/sysctl.d/90-zenthros-node.conf"
	// systemd-resolved's stub (127.0.0.53) in a pod resolves nothing: the
	// kubelet gives pods the upstream list, as kubeadm does.
	resolvedUpstream = "/run/systemd/resolve/resolv.conf"
)

// overlayfsMagic is overlayfs' statfs type: containerd's overlayfs
// snapshotter cannot stack on it (a machine that is itself a container).
const overlayfsMagic = 0x794c7630

// nodeHost is what the machine is: how its cgroups are delegated and what
// its containerd can snapshot on.
type nodeHost struct {
	// CgroupRoot is set when PID 1 runs in a nested cgroup (a machine that
	// is a container sharing its host's cgroup namespace): systemd there
	// does not own the cgroup tree, the kubelet drives cgroupfs under it.
	CgroupRoot  string
	Snapshotter string
}

func (h nodeHost) systemdCgroup() bool { return h.CgroupRoot == "" }

// joinKubelet installs the pinned kubelet and CNI plugins, configures the
// node's containerd and kubelet and starts them.
func (p JoinNodeParams) joinKubelet(ctx context.Context, plan *agentpb.NodeJoinCredentialResponse, credential nodejoin.Credential, log logr.Logger) error {
	if len(plan.GetClusterDns()) == 0 {
		return errors.New("the hub sent no cluster DNS for the kubelet")
	}
	if errs := validation.IsDNS1123Subdomain(plan.GetNodeName()); len(errs) > 0 {
		return fmt.Errorf("node name %q: %s", plan.GetNodeName(), strings.Join(errs, "; "))
	}
	labels, err := kubeletLabels(plan.GetLabels())
	if err != nil {
		return err
	}
	taints, err := kubeletTaints(plan.GetTaints())
	if err != nil {
		return err
	}
	reserved, err := planReservations(plan)
	if err != nil {
		return err
	}
	if err := p.waitForRuntime(ctx, log); err != nil {
		return err
	}
	containerd, err := p.lookPath("containerd")
	if err != nil {
		return fmt.Errorf("the node's containerd (the installer's --container-engine containerd): %w", err)
	}
	runc, err := p.lookPath("runc")
	if err != nil {
		return fmt.Errorf("the node's runc (the installer's --container-engine containerd): %w", err)
	}
	host, err := p.detectHost()
	if err != nil {
		return err
	}
	if err := p.install(ctx, "kubelet", plan.GetKubeletUrl(), plan.GetKubeletSha256(), p.path(kubeletBinary)); err != nil {
		return err
	}
	if err := p.installCNIPlugins(ctx, plan); err != nil {
		return err
	}
	if err := p.prepareKernel(ctx); err != nil {
		return err
	}
	if err := p.writeKubeletNode(plan, credential, host, containerd, runc, labels, taints, reserved); err != nil {
		return err
	}
	if err := p.systemctl(ctx, "daemon-reload"); err != nil {
		return err
	}
	log.Info("starting the kubelet", "kubelet", plan.GetKubeletVersion(), "cni", plan.GetCniPluginsVersion(),
		"cgroupRoot", host.CgroupRoot, "snapshotter", host.Snapshotter)
	return p.systemctl(ctx, "enable", "--now", "--no-block", nodeContainerdName, kubeletUnitName)
}

// waitForRuntime waits for the installer's background runtime (containerd)
// within the credential's life.
func (p JoinNodeParams) waitForRuntime(ctx context.Context, log logr.Logger) error {
	ready := p.RuntimeReady
	if ready == nil {
		ready = runtimegate.Ready
	}
	wait := p.RuntimeWait
	if wait == 0 {
		wait = 10 * time.Minute
	}
	deadline := time.Now().Add(wait)
	for logged := false; !ready(); {
		if !logged {
			log.Info("waiting for the installer's runtime (containerd)")
			logged = true
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the installer's runtime was not ready within %s", wait)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil
}

func (p JoinNodeParams) lookPath(name string) (string, error) {
	if p.LookPath != nil {
		return p.LookPath(name)
	}
	return exec.LookPath(name)
}

// detectHost reads PID 1's cgroup and the filesystem under /var/lib.
func (p JoinNodeParams) detectHost() (nodeHost, error) {
	raw, err := os.ReadFile(p.path("/proc/1/cgroup"))
	if err != nil {
		return nodeHost{}, fmt.Errorf("read PID 1's cgroup: %w", err)
	}
	host := nodeHost{Snapshotter: "overlayfs"}
	if dir := path.Dir(pid1Cgroup(string(raw))); dir != "/" && dir != "." {
		host.CgroupRoot = dir
	}
	fsType := p.FSType
	if fsType == nil {
		fsType = statfsType
	}
	if t, err := fsType(p.path("/var/lib")); err != nil {
		return nodeHost{}, fmt.Errorf("statfs /var/lib: %w", err)
	} else if t == overlayfsMagic {
		host.Snapshotter = "native"
	}
	return host, nil
}

// pid1Cgroup is PID 1's cgroup path: the unified (v2) entry, or the
// systemd hierarchy on cgroup v1.
func pid1Cgroup(raw string) string {
	v1 := ""
	for _, line := range strings.Split(strings.TrimSpace(raw), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		switch {
		case parts[0] == "0" && parts[1] == "":
			return parts[2]
		case parts[1] == "name=systemd":
			v1 = parts[2]
		}
	}
	return v1
}

func statfsType(dir string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return int64(st.Type), nil //nolint:unconvert // int32 on some platforms
}

// installCNIPlugins unpacks the pinned plugins tarball into /opt/cni/bin:
// regular files only, by their base name.
func (p JoinNodeParams) installCNIPlugins(ctx context.Context, plan *agentpb.NodeJoinCredentialResponse) error {
	dir := p.path(cniBinDir)
	tmp, err := p.fetch(ctx, "CNI plugins", plan.GetCniPluginsUrl(), plan.GetCniPluginsSha256(), dir)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	f, err := os.Open(tmp)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("CNI plugins: %w", err)
	}
	tr := tar.NewReader(gz)
	installed := 0
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("CNI plugins: %w", err)
		}
		name := path.Base(path.Clean(h.Name))
		if h.Typeflag != tar.TypeReg || name == "." || name == ".." || strings.HasPrefix(name, ".") {
			continue
		}
		data, err := io.ReadAll(io.LimitReader(tr, 256<<20))
		if err != nil {
			return fmt.Errorf("CNI plugins %s: %w", name, err)
		}
		if err := writeFileAtomic(filepath.Join(dir, name), data, 0o755); err != nil {
			return err
		}
		installed++
	}
	if installed == 0 {
		return errors.New("the CNI plugins tarball holds no plugin")
	}
	return nil
}

// prepareKernel forwards IP and passes bridged traffic through iptables, as
// kube-proxy and every bridge CNI need. br_netfilter is loaded only when it
// is not: the module is the kernel's, the sysctls are the machine's.
func (p JoinNodeParams) prepareKernel(ctx context.Context) error {
	if _, err := os.Stat(p.path("/proc/sys/net/bridge")); err != nil {
		if err := p.exec(ctx, "modprobe", "br_netfilter"); err != nil {
			return fmt.Errorf("load br_netfilter: %w", err)
		}
	}
	sysctls := []struct{ key, value string }{
		{"net.ipv4.ip_forward", "1"},
		{"net.bridge.bridge-nf-call-iptables", "1"},
		{"net.bridge.bridge-nf-call-ip6tables", "1"},
	}
	var persist strings.Builder
	for _, s := range sysctls {
		file := p.path("/proc/sys/" + strings.ReplaceAll(s.key, ".", "/"))
		if current, err := os.ReadFile(file); err == nil && strings.TrimSpace(string(current)) == s.value {
			persist.WriteString(s.key + " = " + s.value + "\n")
			continue
		}
		if err := os.WriteFile(file, []byte(s.value+"\n"), 0o644); err != nil {
			return fmt.Errorf("set %s: %w", s.key, err)
		}
		persist.WriteString(s.key + " = " + s.value + "\n")
	}
	return writeFileAtomic(p.path(nodeSysctlFile), []byte(persist.String()), 0o644)
}

func (p JoinNodeParams) exec(ctx context.Context, name string, args ...string) error {
	if p.Exec != nil {
		return p.Exec(ctx, name, args...)
	}
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, out)
	}
	return nil
}

// kubeletLabels is the --node-labels value: sorted, each one a valid label,
// so nothing the flag or the unit would split on reaches them.
func kubeletLabels(labels map[string]string) (string, error) {
	out := make([]string, 0, len(labels))
	for k, v := range labels {
		if errs := validation.IsQualifiedName(k); len(errs) > 0 {
			return "", fmt.Errorf("node label %q: %s", k, strings.Join(errs, "; "))
		}
		if errs := validation.IsValidLabelValue(v); len(errs) > 0 {
			return "", fmt.Errorf("node label %s=%q: %s", k, v, strings.Join(errs, "; "))
		}
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return strings.Join(out, ","), nil
}

type kubeletTaint struct {
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
	Effect string `json:"effect"`
}

// kubeletTaints parses key[=value]:Effect, the form k3s takes.
func kubeletTaints(taints []string) ([]kubeletTaint, error) {
	out := make([]kubeletTaint, 0, len(taints))
	for _, raw := range taints {
		i := strings.LastIndex(raw, ":")
		if i < 0 {
			return nil, fmt.Errorf("taint %q has no effect", raw)
		}
		t := kubeletTaint{Key: raw[:i], Effect: raw[i+1:]}
		if k, v, ok := strings.Cut(t.Key, "="); ok {
			t.Key, t.Value = k, v
		}
		switch t.Effect {
		case "NoSchedule", "PreferNoSchedule", "NoExecute":
		default:
			return nil, fmt.Errorf("taint %q: effect %q", raw, t.Effect)
		}
		if errs := validation.IsQualifiedName(t.Key); len(errs) > 0 {
			return nil, fmt.Errorf("taint %q: %s", raw, strings.Join(errs, "; "))
		}
		out = append(out, t)
	}
	return out, nil
}

// writeKubeletNode writes the cluster CA, the bootstrap kubeconfig (the only
// file holding the token, 0600), the node containerd's config and unit, the
// KubeletConfiguration and the kubelet's unit. JSON is YAML: nothing a value
// holds is read as YAML.
func (p JoinNodeParams) writeKubeletNode(plan *agentpb.NodeJoinCredentialResponse, credential nodejoin.Credential,
	host nodeHost, containerd, runc, labels string, taints []kubeletTaint, reserved reservations) error {
	bootstrap, err := json.MarshalIndent(map[string]any{
		"apiVersion": "v1",
		"kind":       "Config",
		"clusters": []any{map[string]any{"name": "cluster", "cluster": map[string]any{
			"server": credential.Server, "certificate-authority": kubeletCAFile}}},
		"users": []any{map[string]any{"name": "bootstrap", "user": map[string]any{
			"token": credential.TokenID + "." + credential.TokenSecret}}},
		"contexts": []any{map[string]any{"name": "bootstrap", "context": map[string]any{
			"cluster": "cluster", "user": "bootstrap"}}},
		"current-context": "bootstrap",
	}, "", "  ")
	if err != nil {
		return err
	}
	config := map[string]any{
		"apiVersion":               "kubelet.config.k8s.io/v1beta1",
		"kind":                     "KubeletConfiguration",
		"containerRuntimeEndpoint": "unix://" + nodeContainerdSocket,
		"cgroupDriver":             "systemd",
		"providerID":               plan.GetProviderId(),
		"clusterDNS":               plan.GetClusterDns(),
		"clusterDomain":            plan.GetClusterDomain(),
		"registerWithTaints":       taints,
		"rotateCertificates":       true,
		"failSwapOn":               false,
		"authentication": map[string]any{
			"anonymous": map[string]any{"enabled": false},
			"webhook":   map[string]any{"enabled": true},
			"x509":      map[string]any{"clientCAFile": kubeletCAFile},
		},
		"authorization": map[string]any{"mode": "Webhook"},
	}
	if kubeReserved := reserved.kubeReserved(); kubeReserved != nil {
		config["kubeReserved"] = kubeReserved
	}
	if evictionHard := reserved.kubeletEvictionHard(); evictionHard != nil {
		config["evictionHard"] = evictionHard
	}
	if !host.systemdCgroup() {
		config["cgroupDriver"] = "cgroupfs"
		config["cgroupRoot"] = host.CgroupRoot
	}
	if _, err := os.Stat(p.path(resolvedUpstream)); err == nil {
		config["resolvConf"] = resolvedUpstream
	}
	kubeletConfig, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	containerdConfig := fmt.Sprintf(`version = 3
root = %q
state = %q

[grpc]
  address = %q

[plugins."io.containerd.cri.v1.images"]
  snapshotter = %q

[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runc]
  runtime_type = "io.containerd.runc.v2"

[plugins."io.containerd.cri.v1.runtime".containerd.runtimes.runc.options]
  SystemdCgroup = %t
  BinaryName = %q

[plugins."io.containerd.cri.v1.runtime".cni]
  bin_dirs = [%q]
  conf_dir = %q

[[plugins."io.containerd.transfer.v1.local".unpack_config]]
  platform = "linux/%s"
  snapshotter = %q
`, nodeContainerdRoot, nodeContainerdState, nodeContainerdSocket, host.Snapshotter,
		host.systemdCgroup(), runc, cniBinDir, cniConfDir, runtime.GOARCH, host.Snapshotter)
	containerdUnit := `[Unit]
Description=Zenthros node join (the node's containerd)
After=network.target

[Service]
Environment=PATH=` + filepath.Dir(containerd) + `:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ExecStart=` + containerd + ` --config ` + nodeContainerdConfig + `
Delegate=yes
KillMode=process
LimitNOFILE=1048576
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
`
	kubeletUnit := `[Unit]
Description=Zenthros node join (kubelet)
Wants=network-online.target
After=network-online.target ` + nodeContainerdName + `
Requires=` + nodeContainerdName + `

[Service]
ExecStart=` + kubeletBinary + ` --config=` + kubeletConfigFile + ` --bootstrap-kubeconfig=` + bootstrapKubeconfig +
		` --kubeconfig=` + kubeletKubeconfig + ` --hostname-override=` + plan.GetNodeName() + ` --node-labels=` + labels + `
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
`
	for _, f := range []struct {
		path string
		data []byte
		mode os.FileMode
	}{
		{kubeletCAFile, credential.CACert, 0o644},
		{bootstrapKubeconfig, append(bootstrap, '\n'), 0o600},
		{nodeContainerdConfig, []byte(containerdConfig), 0o644},
		{nodeContainerdUnit, []byte(containerdUnit), 0o644},
		{kubeletConfigFile, append(kubeletConfig, '\n'), 0o644},
		{kubeletUnitFile, []byte(kubeletUnit), 0o644},
	} {
		if err := writeFileAtomic(p.path(f.path), f.data, f.mode); err != nil {
			return err
		}
	}
	return os.MkdirAll(p.path(cniConfDir), 0o755)
}
