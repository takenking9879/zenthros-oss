// SPDX-License-Identifier: Apache-2.0

package zenadm

import (
	"fmt"
	"os"
	"path/filepath"
)

func readCredentialFiles(stateDir string) (certPEM, keyPEM, caPEM []byte, err error) {
	files := map[string]*[]byte{
		"agent.crt": &certPEM,
		"agent.key": &keyPEM,
		"ca.crt":    &caPEM,
	}
	for name, dst := range files {
		data, readErr := os.ReadFile(filepath.Join(stateDir, name))
		if readErr != nil {
			return nil, nil, nil, fmt.Errorf("read %s: %w", name, readErr)
		}
		*dst = data
	}
	return certPEM, keyPEM, caPEM, nil
}
