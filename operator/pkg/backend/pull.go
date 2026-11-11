// SPDX-License-Identifier: Apache-2.0

package backend

import "strings"

// PullRefused reports a pull's failure that is the registry's answer, in the
// words of ctr (pkg/containerd, on a machine) or of a kubelet (containerd's,
// through the CRI, in a pod's waiting state): the image or its platform is
// not there, the reference is malformed or names a registry that does not
// exist, or access is denied. Trying again gets the same answer, so it ends
// the job. A content digest or a snapshot not found is the local store's (a
// blob or a layer another pull's cleanup took on a shared machine), not the
// registry's. Anything else (the network however it broke, a registry's 5xx
// or 429) passes, and the next pull may work: a list of the network's
// failures left out a connection cut a way nobody listed (R4 ronda 3, G1).
func PullRefused(text string) bool {
	msg := strings.ToLower(text)
	if strings.Contains(msg, "content digest") || strings.Contains(msg, "snapshot") {
		return false
	}
	for _, marker := range []string{
		"not found", "manifest unknown", "name unknown", "no match for platform", "invalid reference format",
		"no such host", "401 unauthorized", "403 forbidden", "unauthorized", "denied",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}
