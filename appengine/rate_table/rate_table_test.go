package rate_table_test

import (
	"context"
	"encoding/json"
	"log"
	"testing"

	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/memcache"

	"github.com/m-lab/mlab-ns-rate-limit/endpoint"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

var (
	gctx context.Context
	gkey string
)

func Inner(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := memcache.Get(gctx, gkey); err != nil {
			b.Fatal(err)
		}
	}
}

// This runs against aetest local environment, and shows that memcache hit takes about 400 usec.
// In actual standard appengine environment, it is generally a bit over 1 msec/hit.
func BenchmarkMemcacheGet(b *testing.B) {
	ctx, done, err := aetest.NewContext()
	if err != nil {
		b.Fatal(err)
	}
	defer done()
	ctx, err = appengine.Namespace(ctx, "memcache_requests")
	if err != nil {
		b.Fatal(err)
	}

	ep := endpoint.Stats{
		RequestsPerDay: 1234,
		Probability:    6 / 1234.0,
	}
	epJson, err := json.Marshal(ep)
	if err != nil {
		b.Fatal(err)
	}
	key := "foobar"
	// Set the item, unconditionally
	if err := memcache.Set(ctx, &memcache.Item{Key: key, Value: epJson}); err != nil {
		b.Fatalf("error setting item: %v", err)
	}

	// Get the item from the memcache
	if _, err := memcache.Get(ctx, key); err == memcache.ErrCacheMiss {
		b.Fatal("item not in the cache")
	} else if err != nil {
		b.Fatalf("error getting item: %v", err)
	}

	gkey = key
	gctx = ctx
	b.Run("MemcacheRead", Inner)
}
