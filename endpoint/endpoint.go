// Package endpoint contains tools for dealing with endpoints
package endpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/datastore"
	"google.golang.org/appengine"
	"google.golang.org/appengine/memcache"
)

// Stats contains all information about an endpoint.
type Stats struct {
	Probability    float32 `datastore:"probability"`
	RequestsPerDay int32   `datastore:"requests_per_day"`

	// These fields are characteristics of the endpoint.
	AF        string `datastore:"af"`
	Format    string `datastore:"format"`
	Latitude  string `datastore:"latitude"`
	Longitude string `datastore:"longitude"`
	Metro     string `datastore:"metro"`
	Policy    string `datastore:"policy"`
	Path      string `datastore:"path"`
	TargetIP  string `datastore:"target_ip"`

	// RequesterIP specifies the requester, if different from the target.
	RequesterIP string `datastore:"requester_ip"`
}

func getClient() (*datastore.Client, error) {
	ctx := context.Background()

	// Set your Google Cloud Platform project ID.
	projectID := "mlab-nstesting"

	return datastore.NewClient(ctx, projectID)
}

// Key creates the datastore or memcache key for an EndpointStats object.
func (ep *Stats) Key(agent string) string {
	//       127.0.0.1#Davlik 2.1.0 (blah blah blah)#ndt_ssl#format#geo_options#af#ip#metro#lat#lon"
	name := fmt.Sprintf("%s#%s#%s#%s#%s#%s#%s#%s#%s", ep.TargetIP, agent, ep.Path, ep.Format, ep.Policy, ep.AF, ep.Metro, ep.Latitude, ep.Longitude)
	return name
}

// Save saves an endpoint to datastore
func (ep *Stats) Save(client datastore.Client, agent string) error {
	ctx := context.Background()

	// Sets the kind for the new entity.
	kind := "requests"
	name := ep.Key(agent)
	// Creates a Key instance.
	key := datastore.NameKey(kind, name, nil)
	key.Namespace = "endpoint_stats"

	// Saves the new entity.
	if _, err := client.Put(ctx, key, &ep); err != nil {
		return err
	}
	return nil
}

// This shows that memcache read, with aetest environment, takes about 400 usec.
func testMemcache() {
	ctx := context.Background()
	ctx, err := appengine.Namespace(ctx, "memcache_requests")
	if err != nil {
		log.Fatal(err)
	}

	ep := Stats{
		Path:     "ndt_ssl",
		Policy:   "geo_options",
		TargetIP: "127.0.0.1",
	}
	epJSON, err := json.Marshal(ep)
	if err != nil {
		log.Fatal(err)
	}
	key := ep.Key("foobar")

	// Set the item, unconditionally
	if err := memcache.Set(ctx, &memcache.Item{Key: key, Value: epJSON}); err != nil {
		log.Fatalf("error setting item: %v", err)
	}

	// Get the item from the memcache
	if item, err := memcache.Get(ctx, key); err == memcache.ErrCacheMiss {
		log.Fatal("item not in the cache")
	} else if err != nil {
		log.Fatalf("error getting item: %v", err)
	} else {
		log.Printf("the lyric is %q", item.Value)
	}
}
