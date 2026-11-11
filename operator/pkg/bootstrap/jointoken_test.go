// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"context"
	"strings"
	"testing"
)

const testPin = "sha256:0f1e2d3c4b5a69788796a5b4c3d2e1f00f1e2d3c4b5a69788796a5b4c3d2e1f0"

// The short join token carries the CA pin with the bearer token: the hub is
// still authenticated on first contact by the pin, only it travels inside
// the token (U-05: "the CA hash stays, it is what stops a fake hub").
func TestJoinTokenCarriesThePinAndTheToken(t *testing.T) {
	short := JoinToken(testPin, "abcdef.0123456789abcdef")
	if want := "zt1_" + strings.TrimPrefix(testPin, "sha256:") + "_abcdef.0123456789abcdef"; short != want {
		t.Fatalf("JoinToken = %q, want %q", short, want)
	}
	pin, token, ok := SplitJoinToken(short)
	if !ok || pin != testPin || token != "abcdef.0123456789abcdef" {
		t.Fatalf("SplitJoinToken = %q %q %v", pin, token, ok)
	}
	if JoinToken(strings.ToUpper(testPin), "abcdef.0123456789abcdef") != short {
		t.Fatal("the pin's case changed the token")
	}
}

func TestSplitJoinTokenRefusesWhatIsNotOne(t *testing.T) {
	hex := strings.TrimPrefix(testPin, "sha256:")
	for _, bad := range []string{
		"abcdef.0123456789abcdef",                      // a bare token
		"zt1_" + hex + "abcdef.0123456789abcdef",       // no separator
		"zt1_" + hex[:62] + "_abcdef.0123456789abcdef", // a short pin
		"zt1_" + strings.ToUpper(hex) + "_abcdef.0123456789abcdef",
		"zt1_" + hex[:63] + "g_abcdef.0123456789abcdef", // not hex
		"zt1_" + hex + "_abcdef0123456789abcdef",        // not a bootstrap token
		"zt2_" + hex + "_abcdef.0123456789abcdef",
	} {
		if _, _, ok := SplitJoinToken(bad); ok {
			t.Errorf("SplitJoinToken(%q) took it", bad)
		}
	}
}

// A pin given beside a short token must be the token's: two pins that
// differ are refused before anything is dialled.
func TestJoinRefusesAShortTokenWithAnotherPin(t *testing.T) {
	other := "sha256:" + strings.Repeat("ab", 32)
	_, err := Join(context.Background(), JoinParams{
		ControllerAddr: "127.0.0.1:1", Token: JoinToken(testPin, "abcdef.0123456789abcdef"), CACertHash: other,
	})
	if err == nil || !strings.Contains(err.Error(), "differ") {
		t.Fatalf("Join with two pins = %v, want them refused", err)
	}
}
