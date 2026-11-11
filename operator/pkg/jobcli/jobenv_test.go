// SPDX-License-Identifier: Apache-2.0

package jobcli

import (
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/zenthros/operator/internal/testutil/jobenv"
)

// The protocol's doc names every variable a job is given, so the list a
// job's author reads is the one the tests hold the code to.
func TestTheProtocolDocNamesEveryJobVariable(t *testing.T) {
	raw, err := os.ReadFile("../../docs/interfaces/job-protocol.md")
	if err != nil {
		t.Fatal(err)
	}
	_, section, _ := strings.Cut(string(raw), "\n## Env\n")
	section, _, _ = strings.Cut(section, "\n## ")
	all, err := jobenv.All()
	if err != nil {
		t.Fatal(err)
	}
	var missing []string
	for name := range all {
		if !strings.Contains(section, "`"+name+"`") {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Fatalf("docs/interfaces/job-protocol.md's Env does not name %v", missing)
	}
}
