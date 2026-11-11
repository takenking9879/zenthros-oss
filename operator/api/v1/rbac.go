// SPDX-License-Identifier: Apache-2.0

package v1

// RBAC names for what users do with a Workspace. Kubernetes RBAC is the one
// authority for users, whether they reach the cluster with a kubeconfig or
// through zenthros-api; these are the strings both doors are checked against.
//
// Jobs are a real resource: create, get, list, watch, patch (cancel) and
// delete are checked by the kube-apiserver on the request itself. The
// subresources and objects below are not served by the kube-apiserver. RBAC
// still evaluates them, so zenthros-api asks Kubernetes about them with a
// SelfSubjectAccessReview made with the caller's own credential, and the
// ValidatingAdmissionPolicy asks about spec edits the same way.
const (
	ResourceZenthrosJobs = "zenthrosjobs"

	// SubresourceSpec, verb update: edit spec fields other than cancel. A
	// patch without it may only set spec.cancel.
	SubresourceSpec = "spec"
	// SubresourceLog, verb get: read a job's log (zenthros-api → Gateway).
	SubresourceLog = "log"
	// SubresourceReports, verb get: read a job's ExecutionReports.
	SubresourceReports = "reports"

	// ResourceZenthrosObjects is the Workspace's prefix in the call store:
	// get downloads (a result), create uploads (a call), list lists them.
	ResourceZenthrosObjects = "zenthrosobjects"

	// ResourceZenthrosSecrets is the Workspace's secrets (workspace-secret):
	// create sets one, update replaces it, delete removes it, list names
	// them. zenthros-api keeps them, with its own ServiceAccount, in the
	// Workspace's namespace; no verb ever reads a value back.
	ResourceZenthrosSecrets = "zenthrossecrets"

	// ResourceZenthrosBackends is the backends a Workspace may use (its
	// BackendAccess, the machines of pools it may use), as the Gateway holds
	// them. list reads them (workspace.backends.read); the custom verb drain
	// drains and undrains one (workspace.backends.drain). Deleting a backend
	// is an administrator's, through the Gateway.
	ResourceZenthrosBackends = "zenthrosbackends"
	VerbDrain                = "drain"
	// VerbJoin on zenthrosbackends is workspace.backends.join: a one-use
	// token for a container that joins Private to the Workspace and Named
	// (only jobs that name it reach it). VerbRetire on zenthrosbackends/<id>
	// is workspace.backends.retire: revoke it, then remove its access. Their
	// own role, zenthros:workspace-backend-owner, grants them; the editor's
	// does not.
	VerbJoin   = "join"
	VerbRetire = "retire"

	// ResourceZenthrosMachines is the machines a Workspace member made from a
	// template (zenthros.create): create makes one (workspace.machines.create),
	// delete ends it (workspace.machines.delete), list reads them. Their own
	// role, zenthros:workspace-provisioner, grants them.
	ResourceZenthrosMachines = "zenthrosmachines"

	// ResourceZenthrosPools is the pools a Workspace may name in a job (the
	// VMPools whose access lists it). list reads them (workspace.pools.read);
	// zenthros-api then reads the pools with its own ServiceAccount.
	ResourceZenthrosPools = "zenthrospools"

	// The ClusterRoles the chart installs; a RoleBinding in a Workspace's
	// namespace grants one to a user, a group or a ServiceAccount.
	ClusterRoleWorkspaceViewer = "zenthros:workspace-viewer"
	ClusterRoleWorkspaceEditor = "zenthros:workspace-editor"
	// ClusterRoleWorkspaceBackendOwner joins containers to the Workspace and
	// retires its Private backends (zenthros.LocalBackend in the SDK).
	ClusterRoleWorkspaceBackendOwner = "zenthros:workspace-backend-owner"
	// ClusterRoleWorkspaceProvisioner creates machines for the Workspace
	// from the administrator's templates, within their limits, and deletes
	// them (zenthros.create and zenthros.delete in the SDK).
	ClusterRoleWorkspaceProvisioner = "zenthros:workspace-provisioner"
	// ClusterRoleSignupKeys issues tokens for SignupOwner and nothing else.
	// The controller binds it to zenthros-api in each signup Workspace's
	// namespace, so the API can key a person's own Workspace and no other.
	ClusterRoleSignupKeys = "zenthros:signup-keys"
)
