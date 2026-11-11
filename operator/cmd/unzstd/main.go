// SPDX-License-Identifier: Apache-2.0

// Command zenthros-unzstd unpacks zstd from standard input to standard
// output. The installers and a container's boot script download it first,
// check it against SHA256SUMS, and unpack every .zst with it: a machine
// needs neither zstd nor root to install it (install/stage.sh).
//
//	zenthros-unzstd < zenthros-agent-linux-amd64.zst > zenthros-agent
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

// maxWindow bounds what one frame may ask for: zstd -19 asks 8 MiB; the
// artifacts are zstd -3.
const maxWindow = 64 << 20

func main() {
	if len(os.Args) > 1 {
		fmt.Fprintln(os.Stderr, "usage: zenthros-unzstd < FILE.zst > FILE")
		os.Exit(2)
	}
	if err := unpack(os.Stdout, os.Stdin); err != nil {
		fmt.Fprintf(os.Stderr, "zenthros-unzstd: %v\n", err)
		os.Exit(1)
	}
}

func unpack(dst io.Writer, src io.Reader) error {
	d, err := zstd.NewReader(bufio.NewReaderSize(src, 1<<20), zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxWindow(maxWindow))
	if err != nil {
		return err
	}
	defer d.Close()
	w := bufio.NewWriterSize(dst, 1<<20)
	if _, err := io.Copy(w, d); err != nil {
		return err
	}
	return w.Flush()
}
