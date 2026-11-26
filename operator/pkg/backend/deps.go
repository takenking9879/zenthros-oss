// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Deps carries the shared dependencies a backend implementation needs.
type Deps struct {
	Client client.Client
	Scheme *runtime.Scheme
	Logger logr.Logger

	// Reader is a direct, uncached reader (the manager's APIReader). Backends use it
	// to resolve foreign objects such as Secrets without caching them.
	Reader client.Reader

	// WorkspaceSecretsNamespace keeps the Workspaces' secrets; empty is
	// v1.DefaultWorkspaceSecretsNamespace.
	WorkspaceSecretsNamespace string
}
