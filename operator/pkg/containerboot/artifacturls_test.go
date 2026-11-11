// SPDX-License-Identifier: Apache-2.0

package containerboot

import (
	"testing"

	"github.com/zenthros/operator/pkg/pyenv"
)

// The boot and uv read the artifact URLs from one variable: two constants
// name it, in packages that do not import each other (07#9).
func TestBootAndUVReadTheSameArtifactURLs(t *testing.T) {
	if EnvArtifactURLs != pyenv.EnvArtifactURLs {
		t.Fatalf("containerboot reads %s, uv %s", EnvArtifactURLs, pyenv.EnvArtifactURLs)
	}
}
