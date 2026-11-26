// SPDX-License-Identifier: Apache-2.0

package k8sagent

import (
	"context"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// eventCacheTTL is how long a job's event read answers its later status
// polls. Events only diagnose a job whose pod has not started (quota,
// scheduling; image errors come from the pod itself), and that diagnosis
// changes slowly: reading them on every 2-second poll was two apiserver
// LISTs per waiting job per sweep, 600 a sweep with 300 jobs waiting, and
// on a remote cluster each costs a round trip.
const eventCacheTTL = 10 * time.Second

// eventCache keeps event LISTs by namespace and field selector.
type eventCache struct {
	mu      sync.Mutex
	entries map[string]cachedEvents
	now     func() time.Time
}

type cachedEvents struct {
	at    time.Time
	items []corev1.Event
}

func (c *eventCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// list answers from a read younger than eventCacheTTL, or reads through
// and keeps the answer. A failed read is not kept.
func (c *eventCache) list(ctx context.Context, through client.Reader, list *corev1.EventList, opts ...client.ListOption) error {
	options := (&client.ListOptions{}).ApplyOptions(opts)
	fields := ""
	if options.FieldSelector != nil {
		fields = options.FieldSelector.String()
	}
	key := options.Namespace + "|" + fields
	now := c.clock()
	c.mu.Lock()
	if hit, ok := c.entries[key]; ok && now.Sub(hit.at) < eventCacheTTL {
		list.Items = append([]corev1.Event(nil), hit.items...)
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	if err := through.List(ctx, list, opts...); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[string]cachedEvents{}
	}
	for k, entry := range c.entries {
		if now.Sub(entry.at) >= eventCacheTTL {
			delete(c.entries, k) // a job that started or ended stops asking
		}
	}
	c.entries[key] = cachedEvents{at: now, items: append([]corev1.Event(nil), list.Items...)}
	return nil
}
