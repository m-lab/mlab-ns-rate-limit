// Package endpoint contains tools for dealing with endpoints
package endpoint

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/datastore"
)

func getClient() (*datastore.Client, error) {
	ctx := context.Background()

	// Set your Google Cloud Platform project ID.
	projectID := "mlab-nstesting"

	return datastore.NewClient(ctx, projectID)
}

// Stats contains all information about an endpoint.
type Stats struct {
	// Coarse endpoint characteristics.
	AgentPrefix string // The simple agent string, without build info.
	Path        string // Root path or URL without params.
	TargetIP    string // IP for which the request is being made.

	RequestsPerDay int64   // Number of requests made per day.
	Probability    float32 // Fraction of requests that should be sent to standard backend.

	// Additional resource parameters associated with the endpoint.
	AF        string // Address family
	Format    string // Format, e.g. json
	Latitude  string
	Longitude string
	Metro     string // Metro specified with metro=
	Policy    string // Policy specified with policy=, e.g. geo_options

	// RequesterIP specifies the requester, if different from the target.
	RequesterIP string
}

func StatsFromMap(row map[string]bigquery.Value) Stats {
	stats := Stats{}

	for k := range row {
		v, ok := row[k]
		if ok && v != nil {
			switch k {
			case "AF":
				stats.AF = v.(string)
			case "Format":
				stats.Format = v.(string)
			case "Latitude":
				stats.Latitude = fmt.Sprintf("%-8.4f", v.(float32))
			case "Longitude":
				stats.Latitude = fmt.Sprintf("%-8.4f", v.(float32))
			case "Metro":
				stats.Metro = v.(string)
			case "Policy":
				stats.Policy = v.(string)
			case "Path":
				stats.Path = v.(string)
			case "IPParam":
				stats.TargetIP = v.(string)
			case "RequesterIP":
				stats.RequesterIP = v.(string)
			case "AgentPrefix":
				stats.AgentPrefix = v.(string)
			case "RequestsPerDay":
				stats.RequestsPerDay = v.(int64)
			default:
			}
		}
	}
	if stats.TargetIP == "" {
		stats.TargetIP = stats.RequesterIP
		stats.RequesterIP = ""
	}
	if stats.RequestsPerDay > 0 {
		stats.Probability = 6.0 / float32(stats.RequestsPerDay)
	}
	return stats
}

// DSKey creates a datastore key for this object, using Stats fields and userAgent string.
func (ep *Stats) DSKey(userAgent string) *datastore.Key {
	// Sets the kind for the new entity.
	kind := "requests"
	//       127.0.0.1#Davlik 2.1.0 (blah blah blah)#ndt_ssl#format#geo_options#af#ip#metro#lat#lon"
	name := fmt.Sprintf("%s#%s#%s#%s#%s#%s#%s#%s#%s", ep.TargetIP, userAgent, ep.Path, ep.Format, ep.Policy, ep.AF, ep.Metro, ep.Latitude, ep.Longitude)
	// Creates a Key instance.
	key := datastore.NameKey(kind, name, nil)
	key.Namespace = "endpoint_stats"

	return key
}

// Save saves an endpoint to datastore
func (ep *Stats) Save(client datastore.Client, agent string) error {
	key := ep.DSKey(agent)

	// Saves the new entity.
	// TODO use timeout and handle errors
	ctx := context.Background()
	if _, err := client.Put(ctx, key, ep); err != nil {
		return err
	}
	return nil
}
