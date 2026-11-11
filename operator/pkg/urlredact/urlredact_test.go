// SPDX-License-Identifier: Apache-2.0

package urlredact

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestURLKeepsNoCredential(t *testing.T) {
	for raw, want := range map[string]string{
		"https://bucket.test/p/key?X-Amz-Signature=SECRET&a=b": "https://bucket.test/p/key",
		"https://user:SECRET@host.test/p#SECRET":               "https://host.test/p",
		"https://bucket.test/key?X-Amz-Signature=SECRET\x7f":   "https://bucket.test/key",
	} {
		if got := URL(raw); got != want {
			t.Errorf("URL(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestErrorRedactsTheURLItQuotes(t *testing.T) {
	_, err := http.NewRequest(http.MethodGet, "https://bucket.test/key?X-Amz-Signature=SECRET\x7f", nil)
	if err == nil {
		t.Fatal("fixture: the URL parsed")
	}
	if got := Error(err).Error(); strings.Contains(got, "SECRET") {
		t.Fatalf("Error = %s", got)
	}
	plain := errors.New("no URL here")
	if Error(plain) != plain {
		t.Fatal("an error with no URL changed")
	}
}

func TestQuotedNamesTheURLWithoutItsToken(t *testing.T) {
	raw := "https://user:ghp-token@github.com/o/r.git"
	cause := errors.New("exit 128")
	err := Quoted(fmt.Errorf("git clone '%s': %w: fatal: could not read from %s", raw, cause, raw), raw)
	if strings.Contains(err.Error(), "ghp-token") || !strings.Contains(err.Error(), "https://github.com/o/r.git") {
		t.Fatalf("err = %q", err)
	}
	if !errors.Is(err, cause) {
		t.Fatal("the cause left the chain")
	}
}

func TestAnUnparsableURLLosesItsUserInfo(t *testing.T) {
	if got := URL("https://user:ghp-token@github.com/o/r\x7f.git?x=1"); strings.Contains(got, "ghp-token") || !strings.Contains(got, "github.com/o/r") {
		t.Fatalf("URL = %q", got)
	}
}
