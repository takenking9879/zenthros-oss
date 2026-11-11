// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"fmt"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/zenthros/operator/pkg/capability"
)

// capabilityRows is docs/capabilities.md's table as the executors declare it:
// the VM agent with containerd present, the container agent, and this
// executor's translators.
func capabilityRows() string {
	vm := capability.VM(true, capability.RuntimeSystemd)
	container := capability.Container()
	k8s := map[string]string{}
	for _, tr := range translators {
		if tr.resource.Empty() {
			k8s[tr.kind] = "always"
		} else {
			k8s[tr.kind] = fmt.Sprintf("`%s` %s", tr.resource.GroupVersion(), tr.resource.Resource)
		}
	}
	kinds := vm.Kinds()
	for _, kind := range container.Kinds() {
		if !slices.Contains(kinds, kind) {
			kinds = append(kinds, kind)
		}
	}
	for kind := range k8s {
		if !slices.Contains(kinds, kind) {
			kinds = append(kinds, kind)
		}
	}
	sort.Strings(kinds)
	var b strings.Builder
	b.WriteString("| Kind | Machines (VM agent) | Containers (container agent) | Kubernetes (in-cluster agent) | Lifecycle |\n|---|---|---|---|---|\n")
	for _, kind := range kinds {
		machines := "no"
		if w, ok := vm.Find(kind); ok {
			machines = strings.Join(w.Runtimes, ", ")
		}
		inContainer := "no"
		if w, ok := container.Find(kind); ok {
			inContainer = strings.Join(w.Runtimes, ", ")
		}
		cluster := k8s[kind]
		if cluster == "" {
			cluster = "no"
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", kind, machines, inContainer, cluster, capability.LifecycleOf(kind))
	}
	return b.String()
}

// The table in docs/capabilities.md is what the executors declare.
func TestCapabilitiesDocMatchesExecutors(t *testing.T) {
	raw, err := os.ReadFile("../../docs/capabilities.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)
	const begin, end = "<!-- capabilities:begin -->\n", "<!-- capabilities:end -->"
	i, j := strings.Index(doc, begin), strings.Index(doc, end)
	if i < 0 || j < i {
		t.Fatalf("docs/capabilities.md lacks the %q ... %q markers", begin, end)
	}
	if got, want := doc[i+len(begin):j], capabilityRows(); got != want {
		t.Fatalf("docs/capabilities.md table drifted from the executors; replace it with:\n%s", want)
	}
}

// Every kind the CRD accepts is run by some executor: the VM agent or a
// translator here. A kind in the enum that nothing runs passes admission
// and then fails on every backend.
func TestEveryEnumKindHasExecutor(t *testing.T) {
	raw, err := os.ReadFile("../../api/v1/workload_types.go")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`\+kubebuilder:validation:Enum=(Job;[A-Za-z;]+)`).FindSubmatch(raw)
	if m == nil {
		t.Fatal("no workload kind enum in api/v1/workload_types.go")
	}
	run := capability.VM(true, capability.RuntimeSystemd).Kinds()
	for _, tr := range translators {
		run = append(run, tr.kind)
	}
	for _, kind := range strings.Split(string(m[1]), ";") {
		if !slices.Contains(run, kind) {
			t.Errorf("the CRD accepts kind %s, which no executor runs", kind)
		}
	}
}
