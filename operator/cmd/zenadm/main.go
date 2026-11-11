// SPDX-License-Identifier: Apache-2.0

// Command zenadm provisions Zenthros nodes (kubeadm equivalent): on a
// machine it enrolls the agent (join, join-node, pin, container-installer);
// for the hub's operator it creates bootstrap tokens and prepares clusters
// (admin.go, left out of a public build). Day-2 operations live in zctl
// (kubectl).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"go.uber.org/zap/zapcore"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/zenthros/operator/pkg/bootstrap"
	"github.com/zenthros/operator/pkg/cloudmeta"
	"github.com/zenthros/operator/pkg/containerinstall"
	"github.com/zenthros/operator/pkg/zenadm"
)

// adminCommands are the hub operator's commands, by name, and adminUsage
// their usage lines (admin.go): none in a public build (-tags public).
var (
	adminCommands = map[string]func([]string) error{}
	adminUsage    string
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	var err error
	switch os.Args[1] {
	case "join":
		err = runJoin(os.Args[2:])
	case "join-node":
		err = runJoinNode(os.Args[2:])
	case "pin":
		err = runPin(os.Args[2:])
	case "container-installer":
		// install-container.sh, which install/stage.sh publishes with the
		// agent and writes where into (or the base given here).
		base := ""
		if len(os.Args) > 2 {
			base = os.Args[2]
		}
		_, err = fmt.Print(containerinstall.Script(base))
	case "help", "-h", "--help":
		usage()
		return
	default:
		run, ok := adminCommands[os.Args[1]]
		if !ok {
			fmt.Fprintf(os.Stderr, "zenadm: unknown command %q\n", os.Args[1])
			usage()
			os.Exit(1)
		}
		err = run(os.Args[2:])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "zenadm:", err)
		os.Exit(1)
	}
}

// runJoinNode makes an enrolled machine of a joined pool a node of its

// cluster: the managed bootstrap runs it after zenadm join.

func runJoinNode(args []string) error {
	fs := flag.NewFlagSet("join-node", flag.ExitOnError)
	var p zenadm.JoinNodeParams
	fs.StringVar(&p.ControllerAddr, "controller", "", "gateway control-channel address host:port (required)")
	fs.StringVar(&p.TLSServerName, "tls-server-name", "", "gateway certificate name (defaults to the address host)")
	fs.StringVar(&p.StateDir, "state-dir", "/var/lib/zenthros", "directory holding the machine certificate zenadm join wrote")
	fs.DurationVar(&p.Wait, "wait", 15*time.Minute, "how long to wait for the cluster to issue the node credential")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctrl.SetLogger(zap.New(zap.UseDevMode(false)))
	return zenadm.JoinNode(context.Background(), p, ctrl.Log.WithName("zenadm-join-node"))
}

func runJoin(args []string) error {
	fs := flag.NewFlagSet("join", flag.ExitOnError)
	var p zenadm.JoinParams
	fs.StringVar(&p.ControllerAddr, "controller", "", "controller control-channel address host:port (required)")
	fs.StringVar(&p.TLSServerName, "tls-server-name", "", "controller certificate name (defaults to the controller address host)")
	fs.StringVar(&p.Token, "token", "", "one-time bootstrap token (required)")
	fs.StringVar(&p.CACertHash, "ca-cert-hash", "", "discovery pin sha256:<hex> of the controller CA (required)")
	fs.StringVar(&p.CACertFile, "ca-cert", "", "PEM file of the controller CA (optional; used as TLS roots)")
	fs.StringVar(&p.BackendID, "backend-id", "", "backend id this agent fronts (required)")
	fs.StringVar(&p.BackendType, "backend-type", "vm", "backend implementation type (vm or k8s; a container agent joins by itself, see token create --container)")
	fs.StringVar(&p.StateDir, "state-dir", "/var/lib/zenthros", "directory to write agent mTLS material")
	fs.StringVar(&p.DeployMode, "deploy", "", "how to deploy the agent: \"systemd\" or \"k8s\" (auto-detected if empty)")
	fs.BoolVar(&p.SkipIfExisting, "skip-if-existing", false, "reuse existing agent credentials and only reconcile the deployment")
	fs.BoolVar(&p.Tailscale, "tailscale", false, "join the Tailscale tailnet before enrolling")
	fs.StringVar(&p.TSAuthKey, "ts-authkey", "", "ephemeral Tailscale auth key (required with --tailscale)")
	fs.StringVar(&p.TSHostname, "ts-hostname", "", "tailscale hostname (defaults to the backend ID)")
	fs.StringVar(&p.Network, "network", zenadm.NetworkTailscale, "how the agent reaches the controller: tailscale or direct (no Tailscale interface to wait for)")
	fs.StringVar(&p.Ownership, "ownership", "External", "backend ownership: External or Managed")
	fs.StringVar(&p.ProviderIdentity, "provider-identity", "", "provider identity evidence to present: one of "+strings.Join(cloudmeta.Names(), ", "))
	fs.DurationVar(&p.Wait, "wait", 0, "wait this long for the agent to be connected, and fail if it is not (default with a zt1_ token: "+zenadm.ShortJoinWait.String()+")")
	fs.Func("folder", "a folder of this machine jobs may write and use as source.hostPath, like /data (repeat for more); jobs never run as root and write nowhere else but their own directories",
		func(folder string) error { p.Folders = append(p.Folders, folder); return nil })
	if err := fs.Parse(joinArgs(args, &p.ControllerAddr)); err != nil {
		return err
	}
	if p.DeployMode == "" {
		p.DeployMode = zenadm.DetectDeployMode("/")
	}
	given := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { given[f.Name] = true })
	logger := zap.New(zap.UseDevMode(false))
	if _, _, short := bootstrap.SplitJoinToken(p.Token); short {
		// The short join a person pastes: `zenadm join HUB --token zt1_...`.
		// The rest is the hub's (the backend, in the token) or follows from
		// the address (the network); it waits until connected, and says so
		// in three lines instead of logs.
		p.ControllerAddr = zenadm.WithDefaultPort(p.ControllerAddr)
		if !given["network"] {
			p.Network = ""
		}
		if !given["wait"] {
			p.Wait = zenadm.ShortJoinWait
		}
		p.Out = os.Stdout
		logger = zap.New(zap.UseDevMode(false), zap.Level(zapcore.ErrorLevel))
	}
	ctrl.SetLogger(logger)
	return zenadm.Join(context.Background(), p, ctrl.Log.WithName("zenadm-join"))
}

// joinArgs is args with a leading address taken as --controller: `zenadm

// join HUB --token ...`, as k3s and kubeadm take theirs. The flag package

// stops at the first argument that is not a flag.

func joinArgs(args []string, controller *string) []string {
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		*controller = args[0]
		return args[1:]
	}
	return args
}

func runPin(args []string) error {
	fs := flag.NewFlagSet("pin", flag.ContinueOnError)
	var name, url, sha, dest string
	fs.StringVar(&name, "name", "", "binary name (PATH lookup and default dest basename)")
	fs.StringVar(&url, "url", "", "download URL used only when the installed sha does not match")
	fs.StringVar(&sha, "sha256", "", "required sha256 hex of the pinned binary")
	fs.StringVar(&dest, "dest", "", "install path (default /usr/local/bin/<name>)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if name == "" || url == "" || sha == "" {
		return fmt.Errorf("usage: zenadm pin --name NAME --url URL --sha256 HEX [--dest PATH]")
	}
	return zenadm.EnsurePinnedBinaryDest(context.Background(), name, url, sha, dest)
}

// runNodeSupply: which providers may supply nodes to a Kubernetes backend's

// cluster (docs/design/K8S_NODE_SUPPLY.md).

// runCloudAccount shows a registered cloud account's permissions: check asks

// the provisioner to try them now, status shows what it last found.

func usage() {
	fmt.Fprint(os.Stderr, `zenadm — Zenthros node provisioning (kubeadm equivalent)

Usage:
`+adminUsage+`  zenadm container-installer [INSTALL_BASE]   prints install-container.sh, which install/stage.sh publishes
  zenadm pin --name NAME --url URL --sha256 HEX [--dest PATH]
  zenadm join HUB --token zt1_...                       the short join zenthros attach prints: waits until connected
  zenadm join --controller H:P --token T --ca-cert-hash sha256:H --backend-id ID [flags]
  zenadm join-node --controller H:P [--state-dir DIR] [--wait 15m]   after join, on a joined pool's machine: become its cluster's node

Join flags:
  --backend-type string   backend type: vm or k8s (default "vm")
  --deploy string         deploy mode: systemd or k8s (auto-detected)
  --skip-if-existing      reuse existing credentials and only reconcile the deployment
  --state-dir string      credential directory (default "/var/lib/zenthros")
  --tailscale             join Tailscale tailnet before enrolling
  --ts-authkey string     ephemeral Tailscale auth key (required with --tailscale)
  --network string        tailscale (default) or direct: how the agent reaches the controller
  --ts-hostname string    tailscale hostname (defaults to OS hostname)
`)
}
