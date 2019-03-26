// Package endpoint contains stats for client endpoints.  An endpoint corresponds
// to an mlab-ns request signature that we expect may represent an individual requester.
// (IP alone is insufficient, because of CG-NAT and use of proxies).  We use the userAgent,
// resource string, and IP address.
// NOTE: We currently limit results to 20K endpoints, and as of Sept 2018, we are seeing about
// 8K endpoints with more than 12 requests per day.  The limit is imposed because we don't have
// enough experience to predict how mlab-ns might behave if bad endpoints grew to 100K or 200K.
package endpoint

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

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
			// TODO: make probability proportional to usage,
			// e.g. 6.0 / float32(stats.RequestsPerDay)
			// Probability of zero guarantees that all requests are offloaded or blocked.
			stats.Probability = 0.0
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
func FetchEndpointStats(ctx context.Context, dsExt *bqext.Dataset, threshold int) ([]map[string]bigquery.Value, error) {
	qString := strings.Replace(sixHourQuery, "${THRESHOLD}", fmt.Sprint(threshold), 1)
	qString = strings.Replace(qString, "${DATE}", fmt.Sprint(threshold), 1)

	query := dsExt.ResultQuery(qString, false)
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	it, err := query.Read(ctx)
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

// DeleteAllKeys deletes all keys for a namespace and kind from Datastore, dividing into blocks of 500
// to satisfy datastore API constraint.  If there are errors, it returns the last error message, but the
// length returned will not reflect the failures.
func DeleteAllKeys(ctx context.Context, client *datastore.Client, namespace string, kind string) (int, error) {
	qkeys, err := GetAllKeys(ctx, client, namespace, kind)
	if err != nil {
		return 0, err
	}

	count := 0
	errChan := make(chan error)
	for start := 0; start < len(qkeys); start = start + 500 {
		count++
		end := start + 500
		if end > len(qkeys) {
			end = len(qkeys)
		}

		// TODO - add latency metric here.
		go func(errChan chan error, start, end int) {
			err := client.DeleteMulti(ctx, qkeys[start:end])
			errChan <- err
		}(errChan, start, end)
	}

	var lastError error = nil
	for ; count > 0; count-- {
		err := <-errChan
		if err != nil {
			lastError = err
			log.Println(err)
		}
	}

	return len(qkeys), lastError
}

// PutMulti writes a set of keys and endpoints to datastore, dividing into blocks of 500
// to satisfy datastore API constraint.
func PutMulti(ctx context.Context, client *datastore.Client, keys []*datastore.Key, endpoints []Stats) error {
	count := 0
	errChan := make(chan error)
	for start := 0; start < len(keys); start = start + 500 {
		count++
		end := start + 500
		if end > len(keys) {
			end = len(keys)
		}

		// TODO - add latency metric here.
		go func(errChan chan error, start, end int) {
			_, err := client.PutMulti(ctx, keys[start:end], endpoints[start:end])
			errChan <- err
		}(errChan, start, end)
	}

	var lastError error = nil
	for ; count > 0; count-- {
		err := <-errChan
		if err != nil {
			lastError = err
			log.Println(err)
		}
	}

	log.Println("Put", len(keys), "entities")
	return lastError
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
  RequestsPerDay DESC
LIMIT 20000`

// sixHourQuery looks for clients that run every six hours and issue requests
// to both /ndt and /neubot.
var sixHourQuery = `
WITH
  clientsInSixHourPeriods AS (
    SELECT
      protoPayload.ip as ip,
      protoPayload.resource as resource,
      COUNT(*) AS requestsPerDay,
      CASE
        WHEN protoPayload.startTime BETWEEN TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 19 MINUTE) AND TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 81 MINUTE) THEN 1
        WHEN protoPayload.startTime BETWEEN TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 379 MINUTE) AND TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 441 MINUTE) THEN 2
        WHEN protoPayload.startTime BETWEEN TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 739 MINUTE) AND TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 801 MINUTE) THEN 4
        WHEN protoPayload.startTime BETWEEN TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 1099 MINUTE) AND TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 1161 MINUTE) THEN 8
        ELSE 0
      END AS period
    FROM
  ` + "`mlab-ns.exports.appengine_googleapis_com_request_log_*`" + `
    WHERE
         (_table_suffix = FORMAT_DATE("%Y%m%d", CURRENT_DATE())
      OR  _table_suffix = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      OR  _table_suffix = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
      AND protoPayload.starttime > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))
      AND (protoPayload.resource = '/neubot' OR protoPayload.resource = '/ndt')
      AND protoPayload.userAgent is NULL
    GROUP BY
      -- Also group by 'period' to guarantee that we only have one representative
      -- request from each ip and resource.
      ip, resource, period
  ),
  clientsOutsideSixHourPeriods AS (
    SELECT
      protoPayload.ip as ip
    FROM
  ` + "`mlab-ns.exports.appengine_googleapis_com_request_log_*`" + `
    WHERE
         (_table_suffix = FORMAT_DATE("%Y%m%d", CURRENT_DATE())
      OR  _table_suffix = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      OR  _table_suffix = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
      AND protoPayload.starttime > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))
      AND (protoPayload.resource = '/neubot' OR protoPayload.resource = '/ndt')
      AND protoPayload.userAgent is NULL
      AND (
          protoPayload.startTime BETWEEN TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 0 MINUTE) AND TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 19 MINUTE) OR
          protoPayload.startTime BETWEEN TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 81 MINUTE) AND TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 379 MINUTE) OR
          protoPayload.startTime BETWEEN TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 441 MINUTE) AND TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 739 MINUTE) OR
          protoPayload.startTime BETWEEN TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 801 MINUTE) AND TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 1099 MINUTE) OR
          protoPayload.startTime BETWEEN TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 1161 MINUTE) AND TIMESTAMP_ADD(TIMESTAMP_TRUNC(protoPayload.startTime, DAY), INTERVAL 1440 MINUTE)
      )
    GROUP BY
      ip
  ),
  nsRequestsInSixHourPeriods AS (
    SELECT
      ip, resource, SUM(period) as total
    FROM
      clientsInSixHourPeriods
    GROUP BY
      ip, resource
    HAVING
      -- Guarantee that each client runs in each period by totaling the 'period'
      -- values. If all periods are represented, then the sum(1 + 2 + 4 + 8) = 15.
      total != 0 AND total = 15
  ),
  uniqueIPsInSixHourPeriods AS (
    (SELECT ip FROM nsRequestsInSixHourPeriods WHERE resource = "/neubot" AND ip NOT IN ( SELECT ip FROM clientsOutsideSixHourPeriods )
     intersect DISTINCT
     SELECT ip FROM nsRequestsInSixHourPeriods WHERE Resource = "/ndt" AND ip NOT IN ( SELECT ip FROM clientsOutsideSixHourPeriods ))
  )

SELECT
    protoPayload.ip AS RequesterIP,
    protoPayload.resource as resource,
    protoPayload.userAgent as userAgent,
    COUNT(*) as RequestsPerDay
FROM
  ` + "`mlab-ns.exports.appengine_googleapis_com_request_log_*`" + `
WHERE
     (_table_suffix = FORMAT_DATE("%Y%m%d", CURRENT_DATE())
  OR  _table_suffix = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  OR  _table_suffix = FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
  AND protoPayload.starttime > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))
  AND (protoPayload.resource = '/neubot' OR protoPayload.resource = '/ndt')
  AND protoPayload.userAgent IS NULL
  AND protoPayload.ip IN ( SELECT ip FROM uniqueIPsInSixHourPeriods )
GROUP BY
  RequesterIP, protoPayload.resource, protoPayload.userAgent
ORDER BY
  RequesterIP, RequestsPerDay DESC
`
