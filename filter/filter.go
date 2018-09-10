// package filter manages a bloom filter on the endpoint keys.  Code is very simple, but it
// is broken out to its own package to simplify testing.
package filter

import (
	"log"

	"github.com/willf/bloom"
	"google.golang.org/appengine/datastore"
)

// EndpointFilter encapsulates a Bloom filter, and implements Populate function.
type EndpointFilter struct {
	*bloom.BloomFilter
}

// New creates an empty endpoint filter.
func New(n uint) EndpointFilter {
	m, k := bloom.EstimateParameters(n, 0.001)
	log.Println("Using", m, k)
	return EndpointFilter{bloom.New(m, k)}
}

// Populate populates the filter from a slice of keys
func (ef *EndpointFilter) Populate(keys []*datastore.Key) {
	for k := range keys {
		ef.AddString(keys[k].StringID())
	}
}
