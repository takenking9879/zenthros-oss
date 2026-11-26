// SPDX-License-Identifier: Apache-2.0

package metrics

import "testing"

func TestRestrictBindAddressLoopbackByDefault(t *testing.T) {
	cases := []struct {
		addr   string
		public bool
		want   string
	}{
		{addr: ":8080", want: "127.0.0.1:8080"},
		{addr: "0.0.0.0:8080", want: "127.0.0.1:8080"},
		{addr: "[::]:8080", want: "127.0.0.1:8080"},
		{addr: "127.0.0.1:8080", want: "127.0.0.1:8080"},
		{addr: ":8080", public: true, want: ":8080"},
		{addr: "0.0.0.0:8080", public: true, want: "0.0.0.0:8080"},
		{addr: "0", want: "0"},
		{addr: "", want: ""},
	}
	for _, testCase := range cases {
		got := RestrictBindAddress(testCase.addr, testCase.public)
		if got != testCase.want {
			t.Errorf("RestrictBindAddress(%q, public=%v) = %q, want %q", testCase.addr, testCase.public, got, testCase.want)
		}
	}
}
