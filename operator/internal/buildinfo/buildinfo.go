// SPDX-License-Identifier: Apache-2.0

// Package buildinfo is what a binary knows of its build.
package buildinfo

// Release is the release a binary was built for (install/release.sh): the
// build sets it with -ldflags "-X github.com/zenthros/operator/internal/buildinfo.Release=<name>",
// and a binary built otherwise is "dev".
var Release = "dev"
