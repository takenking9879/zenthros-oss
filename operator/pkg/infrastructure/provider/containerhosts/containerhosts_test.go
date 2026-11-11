// SPDX-License-Identifier: Apache-2.0

package containerhosts

import "testing"

// Each container provider pins its own line (containerhosts_test.go in its
// package); here, what the lines must keep among themselves.
func TestEveryHostIsToldApart(t *testing.T) {
	providers, envs := map[string]bool{}, map[string]string{}
	for _, h := range All() {
		if providers[h.Provider] {
			t.Errorf("%s has two lines", h.Provider)
		}
		providers[h.Provider] = true
		if other, seen := envs[h.IDEnv]; seen {
			t.Errorf("%s and %s both set %s", other, h.Provider, h.IDEnv)
		}
		envs[h.IDEnv] = h.Provider
		if h.JobGrace <= 0 {
			t.Errorf("%s declares job grace %s", h.Provider, h.JobGrace)
		}
	}
}
