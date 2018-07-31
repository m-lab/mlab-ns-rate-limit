// Package endpoint contains stats for client endpoints.  An endpoint corresponds
// to an mlab-ns request signature that we expect may represent an individual
// requester endpoint.  (IP alone is insufficient, because of CG-NAT and use
// of proxies).
package endpoint

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/datastore"
	"github.com/m-lab/go/bqext"
	"google.golang.org/api/iterator"
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

// Key creates the memcache key.
func Key(userAgent string, resource string, IP string) string {
	name := fmt.Sprintf("%s#%s#%s", userAgent, resource, IP)
	return name
}

// DSKey creates a Datastore Key by adding namespace and kind to name string.
func DSKey(name string) *datastore.Key {
	// Sets the kind for the new entity.
	kind := "requests"
	// Creates a Key instance.
	key := datastore.NameKey(kind, name, nil)
	key.Namespace = "endpoint_stats"

	return key
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

func Saveall(client *datastore.Client, keys []*datastore.Key, stats []Stats) error {
	ctx := context.Background()

	// Saves the new entity.
	_, err := client.PutMulti(ctx, keys, stats)
	return err
}

// MakeKeysAndStats converts bigquery rows into DSKeys and Stats objects.
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

// QueryAndFetch executes a query that should return one ore more rows
// TODO - move this into go/bqext
func QueryAndFetch(dsExt *bqext.Dataset, threshold int64) ([]map[string]bigquery.Value, error) {
	query := dsExt.ResultQuery(SimpleQuery, false)
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
		return nil, errors.New("multiple row data")
	}

	return rows, nil
}

func GetAllFromDS(client *datastore.Client) ([]*datastore.Key, error) {
	query := datastore.NewQuery("alt_requests").Namespace("endpoint_stats")
	query = query.KeysOnly()

	ctx := context.Background()
	return client.GetAll(ctx, query, nil)
}

var Query1 = `select *,
regexp_extract(resource, "/([^?]+)") as path,
regexp_extract(resource, "[?&]?format=([^&]+)") as format,
regexp_extract(resource, "[?&]+policy=([^&]+)") as policy,
regexp_extract(resource, "[?&]+address_family=([^&]+)") as af,
regexp_extract(resource, "[?&]+ip=([^&]+)") as ip_param,
regexp_extract(resource, "[?&]+metro=([^&]+)") as metro,
cast(regexp_extract(resource, "[?&]+lat[a-z]*=([^&]+)") as float64) as lat,
cast(regexp_extract(resource, "[?&]+lon[a-z]*=([^&]+)") as float64) as long,
regexp_extract(userAgent, "([a-zA-Z0-9_/.+-]+)") as agent
from
(
SELECT count(*) as tests,
protoPayload.ip,
protopayload.userAgent,
protoPayload.resource
FROM ` + "`mlab-ns.exports.appengine_googleapis_com_request_log_20180718`" + `
group by ip, userAgent, resource
)
where tests > 48
group by ip, ip_param, userAgent, resource, tests
order by tests DESC
`

var Query = `SELECT
regexp_extract(userAgent, "([a-zA-Z0-9_/.+-]+)") AS AgentPrefix,
regexp_extract(resource, "/([^?]+)") AS Path,
RequesterIP,
regexp_extract(resource, "[?&]+ip=([^&]+)") AS IPParam,
resource, userAgent,
RequestsPerDay,
regexp_extract(resource, "[?&]?format=([^&]+)") AS Format,
regexp_extract(resource, "[?&]+policy=([^&]+)") AS Policy,
regexp_extract(resource, "[?&]+address_family=([^&]+)") AS AF,
regexp_extract(resource, "[?&]+metro=([^&]+)") AS Metro,
cast(regexp_extract(resource, "[?&]+lat[a-z]*=([^&]+)") as float64) AS Latitude,
cast(regexp_extract(resource, "[?&]+lon[a-z]*=([^&]+)") as float64) AS Longitude
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
GROUP BY RequesterIP, IPParam, userAgent, resource, RequestsPerDay
ORDER BY RequestsPerDay DESC`

var SimpleQuery = `SELECT RequesterIP, resource, userAgent, RequestsPerDay
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
