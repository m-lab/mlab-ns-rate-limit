// Package endpoint contains stats for client endpoints.  An endpoint corresponds
// to an mlab-ns request signature that we expect may represent an individual
// requester.  (IP alone is insufficient, because of CG-NAT and use
// of proxies).  We use the userAgent, resource string, and IP address.
package endpoint

import (
	"context"
	"fmt"
	"log"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/datastore"
	"github.com/m-lab/go/bqext"
	"google.golang.org/api/iterator"
)

const (
	endpointKind      = "Requests"
	endpointNamespace = "endpoint_stats"
)

// Stats contains information about request rate for an endpoint, and probability
// for mlab-ns to use in routing new requests.
type Stats struct {
	RequestsPerDay int64   `datastore:"requests_per_day"` // Number of requests made per day.
	Probability    float32 `datastore:"probability"`      // Fraction of requests that should be sent to standard backend.
}

// StatsFromMap creates a Key and Stats object from a bigquery result map.
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
func MakeKeysAndStats(rows []map[string]bigquery.Value) ([]*datastore.Key, []Stats, error) {
	// preallocate to match number of rows, to avoid reallocation.
	keys := make([]*datastore.Key, 0, len(rows))
	endpoints := make([]Stats, 0, len(rows))
	for i := range rows {
		key, ep := StatsFromMap(rows[i])
		endpoints = append(endpoints, ep)
		keys = append(keys, DSKey(key))
	}

	return keys, endpoints, nil
}

// FetchEndpointStats executes simpleQuery, and returns a slice of rows containing
// endpoint signatures and request counts.
// TODO - move the body (excluding simpleQuery) into go/bqext
func FetchEndpointStats(dsExt *bqext.Dataset, threshold int) ([]map[string]bigquery.Value, error) {
	qString := strings.Replace(simpleQuery, "${THRESHOLD}", fmt.Sprint(threshold), 1)
	qString = strings.Replace(qString, "${DATE}", fmt.Sprint(threshold), 1)

	query := dsExt.ResultQuery(qString, false)
	it, err := query.Read(context.Background())
	if err != nil {
		return nil, err
	}

	var rows = make([]map[string]bigquery.Value, 0, 1000)
	newRow := make(map[string]bigquery.Value, 20)

	for err = it.Next(&newRow); err == nil; err = it.Next(&newRow) {
		rows = append(rows, newRow)
		newRow = make(map[string]bigquery.Value, 20)
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
SELECT
  RequesterIP, resource, userAgent, RequestsPerDay
FROM (
  SELECT
    protopayload.userAgent, protoPayload.resource, COUNT(*) AS RequestsPerDay, protoPayload.ip AS RequesterIP
  FROM
    ` + "`mlab-ns.exports.appengine_googleapis_com_request_log_*`" + `
  WHERE
    (_table_suffix = FORMAT_DATE("%Y%m%d", CURRENT_DATE())
    OR _table_suffix = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)))
    AND protoPayload.starttime > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
  GROUP BY
    RequesterIP, userAgent, resource )
WHERE
  RequestsPerDay > ${THRESHOLD}
GROUP BY
  RequesterIP, userAgent, resource, RequestsPerDay
ORDER BY
  RequestsPerDay DESC`
