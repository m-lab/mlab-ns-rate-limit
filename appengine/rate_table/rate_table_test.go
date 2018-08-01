package rate_table_test

import (
	"context"
	"encoding/json"
	"log"
	"testing"

	"cloud.google.com/go/datastore"
	"google.golang.org/appengine"
	"google.golang.org/appengine/aetest"
	"google.golang.org/appengine/memcache"

	"github.com/m-lab/go/bqext"
	"github.com/m-lab/mlab-ns-rate-limit/endpoint"
)

func xTestDSEntity(t *testing.T) {
	ctx := context.Background()

	// Set your Google Cloud Platform project ID.
	projectID := "mlab-nstesting"

	// Creates a client.
	client, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	// Sets the kind for the new entity.
	kind := "requests"
	// Sets the name/ID for the new entity.
	//       127.0.0.1#Davlik 2.1.0 (blah blah blah)#ndt_ssl#format#geo_options#af#ip#metro#lat#lon"
	name := "127.0.0.1#Davlik 2.1.0 (blah blah blah)#ndt_ssl##geo_options#####"
	// Creates a Key instance.
	key := datastore.NameKey(kind, name, nil)
	key.Namespace = "endpoint_stats"

	// Creates a Task instance.
	ep := endpoint.Stats{}

	// Saves the new entity.
	if _, err := client.Put(ctx, key, &ep); err != nil {
		log.Fatalf("Failed to save task: %v", err)
	}
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

// This runs against aetest local environment, andshows that memcache hit takes about 400 usec.
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

func getClient() (*datastore.Client, error) {
	ctx := context.Background()

	// Set your Google Cloud Platform project ID.
	projectID := "mlab-nstesting"

	return datastore.NewClient(ctx, projectID)
}

func xTestDeleteAllKeys(t *testing.T) {
	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}

	err = endpoint.DeleteAllKeys(client, "endpoint_stats", "requests")
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateTestEntries(t *testing.T) {
	dsExt, err := bqext.NewDataset("mlab-ns", "exports")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := endpoint.QueryAndFetch(&dsExt, 500)
	if err != nil {
		t.Fatal(err)
	}
	log.Println(len(rows))
	for i := range rows {
		if i > 10 {
			break
		}
		log.Println(rows[i])
	}

	keys, endpoints, err := endpoint.MakeKeysAndStats(rows, 300)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}
	log.Println(len(keys), "rows over threshold")

	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}

	qkeys, err := endpoint.GetAllKeys(client, "endpoint_stats", "requests")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Found", len(qkeys))

	/*
		ctx := context.Background()

		err = client.DeleteMulti(ctx, keys)
		if err != nil {
			log.Fatal(err)
		}*/

	// Saves the new entity.
	err = endpoint.Saveall(client, keys, endpoints)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}
	log.Println("Wrote", len(keys), "of", len(rows))
}
