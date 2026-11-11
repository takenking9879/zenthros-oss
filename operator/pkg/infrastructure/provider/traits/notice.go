// SPDX-License-Identifier: Apache-2.0

package traits

import "time"

// Sources of a cloud machine's interruption notice, as its agent hears it
// (pkg/cloudmeta).
const (
	NoticeAWSIMDS     = "aws-imds"
	NoticeGCPMetadata = "gcp-metadata"
)

// gcpSpotNotice is provider/gcp's SpotNotice: the notice it creates every
// Spot instance with.
const gcpSpotNotice = 2 * time.Minute

// drainCaps is how long an interrupted worker may still commit checkpoints
// after each cloud's notice: the notice less 10 s, for the last ack to land
// before the machine goes. AWS warns two minutes ahead.
var drainCaps = map[string]time.Duration{
	NoticeAWSIMDS:     2*time.Minute - 10*time.Second,
	NoticeGCPMetadata: gcpSpotNotice - 10*time.Second,
}

// DrainCap is the longest a worker may drain after a notice from source,
// and false for a source no cloud declares.
func DrainCap(source string) (time.Duration, bool) {
	limit, ok := drainCaps[source]
	return limit, ok
}
