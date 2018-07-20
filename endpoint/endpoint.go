// Package endpoint contains tools for dealing with endpoints
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

// QueryAndFetch executes a query that should return one ore more rows
// TODO - move this into go/bqext
func QueryAndFetch(dsExt *bqext.Dataset, q string) ([]map[string]bigquery.Value, error) {
	query := dsExt.ResultQuery(q, false)
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

var Query = `
--select count(tests) as endpoints, sum(tests) as sum, userAgent, resource, agent from
select ip, ip_param, tests, userAgent, resource, agent from
(
SELECT count(*) as tests,
regexp_extract(protoPayload.resource, "/([^?]+)") as rsrc,
regexp_extract(protoPayload.resource, "[?&]?format=([^&]+)") as format,
regexp_extract(protoPayload.resource, "[?&]+policy=([^&]+)") as policy,
regexp_extract(protoPayload.resource, "[?&]+address_family=([^&]+)") as af,
regexp_extract(protoPayload.resource, "[?&]+ip=([^&]+)") as ip_param,
regexp_extract(protoPayload.resource, "[?&]+metro=([^&]+)") as metro,
regexp_extract(protoPayload.resource, "[?&]+lat[a-z]*=([^&]+)") as lat,
regexp_extract(protoPayload.resource, "[?&]+lon[a-z]*=([^&]+)") as long,
regexp_extract(protoPayload.userAgent, "([a-zA-Z0-9_/.+-]+)") as agent,
protoPayload.ip,
protopayload.userAgent,
protoPayload.resource
FROM ` + "`mlab-ns.exports.appengine_googleapis_com_request_log_20180718`" + `
group by ip, userAgent, resource, agent, ip_param
)
where tests > 6
group by ip, ip_param, userAgent, resource, agent, tests
order by tests DESC

--and protoPayload.ip like "%2620:%"
`
