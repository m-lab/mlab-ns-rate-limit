// Package endpoint contains stats for client endpoints.  An endpoint corresponds
// to an mlab-ns request signature that we expect may represent an individual
// requester endpoint.  (IP alone is insufficient, because of CG-NAT and use
// of proxies).  Currently, we use the useAgent, resource string, and IP address.
package endpoint

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/datastore"
	"github.com/m-lab/go/bqext"
	"google.golang.org/api/iterator"
)

const (
	endpointKind      = "requests"
	endpointNamespace = "endpoint_stats"
)

// Stats contains information about request rate for an endpoint, and probability
// for mlab-ns to use in routing new requests.
type Stats struct {
	RequestsPerDay int64   // Number of requests made per day.
	Probability    float32 // Fraction of requests that should be sent to standard backend.
}

// StatsFromMap creates a Key and Stats object from a bigquery row.
func StatsFromMap(row map[string]bigquery.Value) (string, Stats) {
	var stats Stats
	rpd, ok := row["RequestsPerDay"]

	if ok && rpd != nil {
		stats.RequestsPerDay = rpd.(int64)
		if stats.RequestsPerDay > 0 {
			stats.Probability = 6.0 / float32(stats.RequestsPerDay)
		}
	}

	userAgent := row["userAgent"]
	if userAgent == nil {
		userAgent = ""
	}
	resource := row["resource"]
	if resource == nil {
		resource = ""
	}
	ip := row["RequesterIP"]
	if ip == nil {
		ip = ""
	}
	key := fmt.Sprintf("%s#%s#%s", userAgent, resource, ip)
	return key, stats
}

// DSKey creates a Datastore Key by adding namespace and kind to name string.
func DSKey(name string) *datastore.Key {
	// Creates a Key instance.
	key := datastore.NameKey(endpointKind, name, nil)
	key.Namespace = endpointNamespace

	return key
}

// Save saves an endpoint to datastore
func (ep *Stats) Save(ctx context.Context, client datastore.Client, key string) error {
	// Sets the kind for the new entity.
	// Creates a Key instance.
	dsKey := datastore.NameKey(endpointKind, key, nil)
	dsKey.Namespace = endpointNamespace

	// Saves the new entity.
	if _, err := client.Put(ctx, dsKey, &ep); err != nil {
		return err
	}
	return nil
}

// MakeKeysAndStats converts slice of bigquery rows into DSKeys and Stats objects.
func MakeKeysAndStats(rows []map[string]bigquery.Value, threshold int64) ([]*datastore.Key, []Stats, error) {
	keys := make([]*datastore.Key, 0, 200)
	endpoints := make([]Stats, 0, 200)
	count := 0
	for i := range rows {
		key, ep := StatsFromMap(rows[i])
		if ep.RequestsPerDay >= threshold {
			count++
			endpoints = append(endpoints, ep)
			keys = append(keys, DSKey(key))
		}
	}

	return keys, endpoints, nil
}

// FetchEndpointStats executes simpleQuery, and returns a slice of rows containing
// endpoint signatures and request counts.
// TODO - move the body (excluding simpleQuery) into go/bqext
func FetchEndpointStats(dsExt *bqext.Dataset, threshold int64) ([]map[string]bigquery.Value, error) {
	query := dsExt.ResultQuery(simpleQuery, false)
	it, err := query.Read(context.Background())
	if err != nil {
		return nil, err
	}

	var rows = make([]map[string]bigquery.Value, 0, 1000)
	row := make(map[string]bigquery.Value, 20)

	for err = it.Next(&row); err == nil; err = it.Next(&row) {
		rows = append(rows, row)
		row = make(map[string]bigquery.Value, 20)
	}
	if err != iterator.Done {
		return nil, err
	}

	return rows, nil
}

// GetAllKeys fetches all keys from Datastore for a namespace and kind.
func GetAllKeys(ctx context.Context, client *datastore.Client, namespace string, kind string) ([]*datastore.Key, error) {
	query := datastore.NewQuery(kind).Namespace(namespace)
	query = query.KeysOnly()

	return client.GetAll(ctx, query, nil)
}

// DeleteAllKeys deletes all keys for a namespace and kind from Datastore.
func DeleteAllKeys(ctx context.Context, client *datastore.Client, namespace string, kind string) (int, error) {
	qkeys, err := GetAllKeys(ctx, client, namespace, kind)
	if err != nil {
		return 0, err
	}

	err = client.DeleteMulti(ctx, qkeys)
	if err != nil {
		return 0, err
	}
	log.Println("Deleted", len(qkeys), "keys from datastore.")

	return len(qkeys), nil
}

// simpleQuery queries the stackdriver request log table, and extracts the count
// of requests from each requester signature (RequesterIP, userAgent, request (resource) string)
var simpleQuery = `
SELECT RequesterIP, resource, userAgent, RequestsPerDay
FROM
(
SELECT
protopayload.userAgent,
protoPayload.resource,
count(*) as RequestsPerDay,
protoPayload.ip as RequesterIP
FROM ` + "`mlab-ns.exports.appengine_googleapis_com_request_log_*`" + `
WHERE (_table_suffix = '20180720'
OR _table_suffix = '20180721')
AND protoPayload.starttime > "2018-07-20 12:00:00"
AND protoPayload.starttime < "2018-07-21 12:00:00"
GROUP BY RequesterIP, userAgent, resource
)
WHERE RequestsPerDay > 6
GROUP BY RequesterIP, userAgent, resource, RequestsPerDay
ORDER BY RequestsPerDay DESC`
