// SPDX-License-Identifier: Apache-2.0

package nodejoin

import (
	"crypto/sha256"
	"encoding/hex"
)

// K3sToken is the credential as a k3s agent's token: K10<CA hash>::<id>.<secret>.
// The hash is the SHA-256 of the CA bundle the server serves at /cacerts,
// which is the cluster CA the issuing agent read: the agent checks the server
// against it before it sends the token.
func K3sToken(credential Credential) string {
	sum := sha256.Sum256(credential.CACert)
	return "K10" + hex.EncodeToString(sum[:]) + "::" + credential.TokenID + "." + credential.TokenSecret
}

// K3sBootstrapGroup is the group k3s lets join as an agent.
const K3sBootstrapGroup = "system:bootstrappers:k3s:default-node-token"
