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

// Stats contains information required to create endpoint keys (for datastore
// and memcache), and
type Stats struct {
	// Coarse endpoint characteristics.
	AgentPrefix string // The simple agent string, without build info.
	Path        string // Root path or URL without params.

	// IP for which the request is being made.  This usually the IP of the
	// requester, but also may be specified explicitly in request parameters.
	TargetIP string

	RequestsPerDay int32   // Number of requests made per day.
	Probability    float32 // Fraction of requests that should be sent to standard backend.

	// Additional resource parameters associated with the endpoint.
	AF        string // Address family
	Format    string // Format, e.g. json
	Latitude  string
	Longitude string
	Metro     string // Metro specified with metro=
	Policy    string // Policy specified with policy=, e.g. geo_options

	// RemoteIP specifies the IP address of the requester.  It is usually
	// empty, indicating that the TargetIP is also the remote IP.  If the
	// target IP is specified in a request parameter, then this field will
	// be set to the IP of the requester.
	RemoteIP string
}

func getClient() (*datastore.Client, error) {
	ctx := context.Background()

	// Set your Google Cloud Platform project ID.
	projectID := "mlab-nstesting"

	return datastore.NewClient(ctx, projectID)
}

// Key creates the datastore or memcache key for a Stats object.
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
