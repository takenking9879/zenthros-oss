// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	agentpb "github.com/zenthros/operator/internal/grpc/gen"
	"github.com/zenthros/operator/pkg/nodejoin"
)

// cniTarball is a plugins release: plugins under ./, plus what must never be
// installed (a directory, a link, a path out of the target).
func cniTarball(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, h := range []*tar.Header{
		{Name: "./", Typeflag: tar.TypeDir, Mode: 0o755},
		{Name: "./bridge", Typeflag: tar.TypeReg, Mode: 0o755, Size: 6},
		{Name: "./loopback", Typeflag: tar.TypeReg, Mode: 0o755, Size: 6},
		{Name: "./host-local", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"},
		{Name: "../../escape", Typeflag: tar.TypeReg, Mode: 0o755, Size: 6},
	} {
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			if _, err := tw.Write([]byte("plugin")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func sha(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// kubeletFixture is a machine of a kubeadm cluster's pool: the plan is a
// kubelet, its CNI plugins and the cluster's DNS. The host is a VM: systemd
// owns PID 1's cgroup, /var/lib is ext4, br_netfilter is loaded.
func kubeletFixture(t *testing.T, api *httptest.Server) *joinNodeFixture {
	t.Helper()
	f := newJoinNodeFixture(t, api)
	kubelet, cni := []byte("#!/bin/sh\necho kubelet\n"), cniTarball(t)
	f.downloads["/kubelet"], f.downloads["/cni.tgz"] = kubelet, cni
	f.hub.plan = &agentpb.NodeJoinCredentialResponse{
		Method: nodejoin.MethodKubelet, NodeName: "slot-0-abcdef12", ProviderId: testProviderID,
		Labels:         map[string]string{"node.kubernetes.io/instance-type": "t3.medium", "infrastructure.zenthros.io/vmpool": "pool-a"},
		Taints:         []string{"infrastructure.zenthros.io/vmpool=pool-a:NoSchedule", "dedicated:NoExecute"},
		KubeletVersion: "v1.34.0", KubeletUrl: f.downloadURL + "/kubelet", KubeletSha256: sha(kubelet),
		CniPluginsVersion: "v1.8.0", CniPluginsUrl: f.downloadURL + "/cni.tgz", CniPluginsSha256: sha(cni),
		ClusterDns: []string{"10.96.0.10"}, ClusterDomain: "cluster.local",
		KubeReservedCpu: "70m", KubeReservedMemory: "1Gi", EvictionHardMemory: "100Mi",
	}
	f.hostFile(t, "/proc/1/cgroup", "0::/init.scope\n")
	f.hostFile(t, "/proc/sys/net/ipv4/ip_forward", "0\n")
	f.hostFile(t, "/proc/sys/net/bridge/bridge-nf-call-iptables", "1\n")
	f.hostFile(t, "/proc/sys/net/bridge/bridge-nf-call-ip6tables", "1\n")
	f.fsType = 0xef53
	return f
}

func (f *joinNodeFixture) hostFile(t *testing.T, path, data string) {
	t.Helper()
	full := filepath.Join(f.params.Root, path)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func (f *joinNodeFixture) kubeletConfig(t *testing.T) map[string]any {
	t.Helper()
	raw, _ := f.file(t, kubeletConfigFile)
	var config map[string]any
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		t.Fatalf("kubelet config: %v\n%s", err, raw)
	}
	return config
}

// The generic join: the pinned kubelet and CNI plugins installed, the token
// only in the 0600 bootstrap kubeconfig, a KubeletConfiguration that
// registers the slot's node (providerID, the pool taints, the cluster's DNS),
// the node's own containerd, and both started in order.
func TestJoinNodeStartsTheKubeletWithTLSBootstrap(t *testing.T) {
	api := httptest.NewTLSServer(http.NotFoundHandler())
	defer api.Close()
	f := kubeletFixture(t, api)
	if err := JoinNode(t.Context(), f.params, logr.Discard()); err != nil {
		t.Fatalf("JoinNode: %v", err)
	}

	if kubelet, mode := f.file(t, kubeletBinary); kubelet != string(f.downloads["/kubelet"]) || mode != 0o755 {
		t.Fatalf("kubelet %o %q", mode, kubelet)
	}
	plugins, err := os.ReadDir(filepath.Join(f.params.Root, cniBinDir))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, p := range plugins {
		names = append(names, p.Name())
	}
	if strings.Join(names, ",") != "bridge,escape,loopback" {
		t.Fatalf("CNI plugins %v, want only the regular files, by base name", names)
	}
	if _, err := os.Lstat(filepath.Join(f.params.Root, "escape")); err == nil {
		t.Fatal("a tarball path left /opt/cni/bin")
	}

	bootstrap, mode := f.file(t, bootstrapKubeconfig)
	token := f.hub.credential.TokenID + "." + f.hub.credential.TokenSecret
	if mode != 0o600 || !strings.Contains(bootstrap, `"token": "`+token+`"`) || !strings.Contains(bootstrap, `"server": "`+api.URL+`"`) {
		t.Fatalf("bootstrap kubeconfig %o:\n%s", mode, bootstrap)
	}
	if ca, _ := f.file(t, kubeletCAFile); ca != string(f.hub.credential.CACert) {
		t.Fatal("the cluster CA was not written for the kubelet")
	}
	_ = filepath.WalkDir(f.params.Root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || path == filepath.Join(f.params.Root, bootstrapKubeconfig) {
			return err
		}
		if data, _ := os.ReadFile(path); bytes.Contains(data, []byte(f.hub.credential.TokenSecret)) {
			t.Errorf("the token is in %s", path)
		}
		return nil
	})

	config := f.kubeletConfig(t)
	if config["providerID"] != testProviderID || config["cgroupDriver"] != "systemd" || config["cgroupRoot"] != nil ||
		config["clusterDomain"] != "cluster.local" || config["containerRuntimeEndpoint"] != "unix://"+nodeContainerdSocket ||
		config["rotateCertificates"] != true || config["resolvConf"] != nil {
		t.Fatalf("kubelet config %v", config)
	}
	// The plan's reservations (nodeshape, as the template holds back), and
	// the kubelet's other eviction signals kept: setting evictionHard drops
	// the ones it leaves out.
	if reserved, _ := json.Marshal(config["kubeReserved"]); string(reserved) != `{"cpu":"70m","memory":"1Gi"}` {
		t.Fatalf("kubeReserved %s", reserved)
	}
	if eviction, _ := json.Marshal(config["evictionHard"]); string(eviction) != `{"imagefs.available":"15%","imagefs.inodesFree":"5%",`+
		`"memory.available":"100Mi","nodefs.available":"10%","nodefs.inodesFree":"5%"}` {
		t.Fatalf("evictionHard %s", eviction)
	}
	if dns := config["clusterDNS"].([]any); len(dns) != 1 || dns[0] != "10.96.0.10" {
		t.Fatalf("clusterDNS %v", dns)
	}
	if anonymous := config["authentication"].(map[string]any)["anonymous"].(map[string]any); anonymous["enabled"] != false {
		t.Fatal("the kubelet serves anonymous requests")
	}
	taints, _ := json.Marshal(config["registerWithTaints"])
	if string(taints) != `[{"effect":"NoSchedule","key":"infrastructure.zenthros.io/vmpool","value":"pool-a"},{"effect":"NoExecute","key":"dedicated"}]` {
		t.Fatalf("registerWithTaints %s", taints)
	}

	containerd, _ := f.file(t, nodeContainerdConfig)
	for _, want := range []string{`address = "` + nodeContainerdSocket + `"`, `snapshotter = "overlayfs"`,
		"SystemdCgroup = true", `BinaryName = "/usr/local/bin/runc"`, `bin_dirs = ["/opt/cni/bin"]`} {
		if !strings.Contains(containerd, want) {
			t.Errorf("containerd config lacks %s:\n%s", want, containerd)
		}
	}
	if unit, _ := f.file(t, nodeContainerdUnit); !strings.Contains(unit, "ExecStart=/usr/local/bin/containerd --config "+nodeContainerdConfig) {
		t.Fatalf("containerd unit:\n%s", unit)
	}
	unit, _ := f.file(t, kubeletUnitFile)
	for _, want := range []string{"--hostname-override=slot-0-abcdef12 ",
		"--node-labels=infrastructure.zenthros.io/vmpool=pool-a,node.kubernetes.io/instance-type=t3.medium\n",
		"--bootstrap-kubeconfig=" + bootstrapKubeconfig, "Requires=" + nodeContainerdName} {
		if !strings.Contains(unit, want) {
			t.Errorf("kubelet unit lacks %q:\n%s", want, unit)
		}
	}

	if forward, _ := f.file(t, "/proc/sys/net/ipv4/ip_forward"); strings.TrimSpace(forward) != "1" {
		t.Fatal("IP forwarding left off")
	}
	if len(f.execs) != 0 {
		t.Fatalf("ran %v with br_netfilter loaded", f.execs)
	}
	if len(f.systemctl) != 2 || f.systemctl[0][0] != "daemon-reload" ||
		strings.Join(f.systemctl[1], " ") != "enable --now --no-block "+nodeContainerdName+" "+kubeletUnitName {
		t.Fatalf("systemctl %v", f.systemctl)
	}
}

// A machine that is itself a container sharing its host's cgroup namespace
// (the lab's guests): systemd does not own the cgroup tree, so the kubelet
// and containerd drive cgroupfs under PID 1's parent; on overlayfs the
// snapshotter is native for CRI and for image unpacking. br_netfilter is
// loaded when missing. systemd-resolved's upstream list goes to pods.
func TestKubeletJoinOnAMachineThatIsAContainer(t *testing.T) {
	api := httptest.NewTLSServer(http.NotFoundHandler())
	defer api.Close()
	f := kubeletFixture(t, api)
	f.hostFile(t, "/proc/1/cgroup", "0::/docker/0123abcd/init.scope\n")
	f.hostFile(t, resolvedUpstream, "nameserver 10.0.0.2\n")
	if err := os.RemoveAll(filepath.Join(f.params.Root, "/proc/sys/net/bridge")); err != nil {
		t.Fatal(err)
	}
	f.params.Exec = func(_ context.Context, name string, args ...string) error {
		f.execs = append(f.execs, append([]string{name}, args...))
		f.hostFile(t, "/proc/sys/net/bridge/bridge-nf-call-iptables", "0\n")
		f.hostFile(t, "/proc/sys/net/bridge/bridge-nf-call-ip6tables", "0\n")
		return nil
	}
	f.fsType = overlayfsMagic
	if err := JoinNode(t.Context(), f.params, logr.Discard()); err != nil {
		t.Fatalf("JoinNode: %v", err)
	}
	config := f.kubeletConfig(t)
	if config["cgroupDriver"] != "cgroupfs" || config["cgroupRoot"] != "/docker/0123abcd" || config["resolvConf"] != resolvedUpstream {
		t.Fatalf("kubelet config %v", config)
	}
	containerd, _ := f.file(t, nodeContainerdConfig)
	if !strings.Contains(containerd, "SystemdCgroup = false") || strings.Count(containerd, `snapshotter = "native"`) != 2 {
		t.Fatalf("containerd config:\n%s", containerd)
	}
	if len(f.execs) != 1 || strings.Join(f.execs[0], " ") != "modprobe br_netfilter" {
		t.Fatalf("execs %v", f.execs)
	}
	if v, _ := f.file(t, "/proc/sys/net/bridge/bridge-nf-call-iptables"); strings.TrimSpace(v) != "1" {
		t.Fatal("bridged traffic skips iptables")
	}
}

// cgroup v1 names PID 1's cgroup in the systemd hierarchy.
func TestPID1Cgroup(t *testing.T) {
	for raw, want := range map[string]string{
		"0::/init.scope\n": "/init.scope",
		"12:cpu,cpuacct:/docker/x\n1:name=systemd:/docker/x/init.scope\n0::/\n": "/",
		"12:cpu,cpuacct:/docker/x\n1:name=systemd:/docker/x/init.scope\n":       "/docker/x/init.scope",
	} {
		if got := pid1Cgroup(raw); got != want {
			t.Errorf("pid1Cgroup(%q) = %q, want %q", raw, got, want)
		}
	}
}

// A kubelet or plugins off their pin, a plan without what a kubelet needs,
// or a label the kubelet flag would split never get a bootstrap kubeconfig
// written or a unit started.
func TestKubeletJoinRefusesAnIncompletePlan(t *testing.T) {
	api := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(api.Close) // after the parallel subtests
	for name, c := range map[string]struct {
		change func(*agentpb.NodeJoinCredentialResponse)
		digest bool
	}{
		"kubelet off its pin": {change: func(p *agentpb.NodeJoinCredentialResponse) { p.KubeletSha256 = strings.Repeat("0", 64) }, digest: true},
		"plugins off the pin": {change: func(p *agentpb.NodeJoinCredentialResponse) { p.CniPluginsSha256 = strings.Repeat("0", 64) }, digest: true},
		"no kubelet pin":      {change: func(p *agentpb.NodeJoinCredentialResponse) { p.KubeletUrl = "" }},
		"no plugins pin":      {change: func(p *agentpb.NodeJoinCredentialResponse) { p.CniPluginsSha256 = "" }},
		"no cluster DNS":      {change: func(p *agentpb.NodeJoinCredentialResponse) { p.ClusterDns = nil }},
		"label with a space":  {change: func(p *agentpb.NodeJoinCredentialResponse) { p.Labels["zone"] = "a b" }},
		"label with a comma":  {change: func(p *agentpb.NodeJoinCredentialResponse) { p.Labels["zone"] = "a,b=c" }},
		"taint without effect": {change: func(p *agentpb.NodeJoinCredentialResponse) {
			p.Taints = append(p.Taints, "dedicated=gpu")
		}},
		"node name with a space":  {change: func(p *agentpb.NodeJoinCredentialResponse) { p.NodeName = "slot-0 --v=9" }},
		"reserved not a quantity": {change: func(p *agentpb.NodeJoinCredentialResponse) { p.KubeReservedCpu = "70m\nfailSwapOn: true" }},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := kubeletFixture(t, api)
			c.change(f.hub.plan)
			err := JoinNode(t.Context(), f.params, logr.Discard())
			if err == nil || (c.digest && !errors.Is(err, errDigest)) {
				t.Fatalf("JoinNode = %v, want a refusal", err)
			}
			for _, path := range []string{bootstrapKubeconfig, kubeletUnitFile, kubeletConfigFile} {
				if _, err := os.Stat(filepath.Join(f.params.Root, path)); err == nil {
					t.Errorf("%s written", path)
				}
			}
			if len(f.systemctl) != 0 {
				t.Errorf("systemctl ran: %v", f.systemctl)
			}
		})
	}
}

// The installer's containerd may still be installing in the background:
// the kubelet join waits for it, and gives up when it never comes.
func TestKubeletJoinWaitsForTheInstallersRuntime(t *testing.T) {
	api := httptest.NewTLSServer(http.NotFoundHandler())
	defer api.Close()
	f := kubeletFixture(t, api)
	asked := 0
	f.params.RuntimeReady = func() bool { asked++; return asked > 1 }
	if err := JoinNode(t.Context(), f.params, logr.Discard()); err != nil || asked != 2 {
		t.Fatalf("JoinNode = %v after %d looks", err, asked)
	}

	f = kubeletFixture(t, api)
	f.params.RuntimeReady = func() bool { return false }
	f.params.RuntimeWait = time.Millisecond
	if err := JoinNode(t.Context(), f.params, logr.Discard()); err == nil || len(f.systemctl) != 0 {
		t.Fatalf("JoinNode = %v (systemctl %v), want it to give up", err, f.systemctl)
	}
}
