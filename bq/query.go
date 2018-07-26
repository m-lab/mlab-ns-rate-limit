package bq

import (
	"context"
	"errors"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	"github.com/m-lab/go/bqext"
)

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
