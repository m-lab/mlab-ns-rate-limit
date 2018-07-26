package endpoint_test

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/mlab-ns-rate-limit/endpoint"
)

func TestStatsFromMap(t *testing.T) {
	input := make(map[string]bigquery.Value, 20)
	input["AF"] = "ipv4"
	input["Format"] = "json"
	input["RequestsPerDay"] = int64(1234)

	stats := endpoint.StatsFromMap(input)
	expect := endpoint.Stats{AF: "ipv4", Format: "json", RequestsPerDay: 1234, Probability: 6 / 1234.0}

	if stats != expect {
		t.Error("mismatch", stats, "!=", expect)
	}
}
