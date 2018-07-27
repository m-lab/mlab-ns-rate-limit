// Package endpoint contains stats for client endpoints.  An endpoint corresponds
// to an mlab-ns request signature that we expect may represent an individual
// requester endpoint.  (IP alone is insufficient, because of CG-NAT and use
// of proxies).
package endpoint

import (
	"context"
	"fmt"

	"cloud.google.com/go/datastore"
)

// Stats contains information about request rate for an endpoint, and probability
// for mlab-ns to use in routing new requests.
type Stats struct {
	RequestsPerDay int32   // Number of requests made per day.
	Probability    float32 // Fraction of requests that should be sent to standard backend.
}

func getClient() (*datastore.Client, error) {
	ctx := context.Background()

	// Set your Google Cloud Platform project ID.
	projectID := "mlab-nstesting"

	return datastore.NewClient(ctx, projectID)
}

// Key creates the memcache key.
func Key(userAgent string, resource string, IP string) string {
	name := fmt.Sprintf("%s#%s#%s", userAgent, resource, IP)
	return name
}

// Save saves an endpoint to datastore
func (ep *Stats) Save(client datastore.Client, key string) error {
	ctx := context.Background()

	// Sets the kind for the new entity.
	kind := "requests"
	// Creates a Key instance.
	dsKey := datastore.NameKey(kind, key, nil)
	dsKey.Namespace = "endpoint_stats"

	// Saves the new entity.
	if _, err := client.Put(ctx, dsKey, &ep); err != nil {
		return err
	}
	return nil
}
