// SPDX-License-Identifier: Apache-2.0

// Package urlredact names a URL without what makes it a credential: the
// query (a presigned URL's signature), the fragment and the user info.
// Errors that quote a URL end up in logs and statuses.
package urlredact

import (
	"errors"
	"net/url"
	"strings"
)

// URL is raw without its query, fragment and user info. A URL that does
// not parse loses everything after its first '?' or '#', and what comes
// before an '@' in its authority.
func URL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		base, _, _ := strings.Cut(raw, "?")
		base, _, _ = strings.Cut(base, "#")
		if scheme, rest, ok := strings.Cut(base, "://"); ok {
			authority, path, hasPath := strings.Cut(rest, "/")
			if at := strings.LastIndex(authority, "@"); at >= 0 {
				base = scheme + "://" + authority[at+1:]
				if hasPath {
					base += "/" + path
				}
			}
		}
		return base
	}
	u.RawQuery, u.ForceQuery, u.Fragment, u.RawFragment, u.User = "", false, "", "", nil
	return u.String()
}

// Error redacts the URL the HTTP client or url.Parse quote in their own
// error: the one asked for, or the one a server redirected to. The error
// stays in the chain.
func Error(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		urlErr.URL = URL(urlErr.URL)
	}
	return err
}

// Quoted is err with raw, wherever its text quotes it (a command line, a
// tool's own output), named by URL. err stays in the chain: errors.Is and
// errors.As see it as before.
func Quoted(err error, raw string) error {
	if err == nil || raw == "" {
		return err
	}
	return quoted{err: err, text: strings.ReplaceAll(err.Error(), raw, URL(raw))}
}

type quoted struct {
	err  error
	text string
}

func (q quoted) Error() string { return q.text }
func (q quoted) Unwrap() error { return q.err }
