package endpoint_test

// TODO maybe test endpoint.FetchEndpointStats()

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/datastore"
	"github.com/m-lab/go/bqext"
	"github.com/m-lab/mlab-ns-rate-limit/endpoint"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

// getClient creates a test client that uses mlab-testing project.
func getClient() (*datastore.Client, error) {
	ctx := context.Background()
	projectID := "mlab-testing"
	return datastore.NewClient(ctx, projectID)
}

func TestDeleteAllKeys(t *testing.T) {
	// This should only be run when using the emulator.
	_, usingEmulator := os.LookupEnv("DATASTORE_EMULATOR_HOST")
	if !usingEmulator {
		t.Skip("Skipping - only run with emulator")
	}

	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}

	// Just verify that the call completes successfully.
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = endpoint.DeleteAllKeys(ctx, client, "endpoint_stats", "Requests")
	if err != nil {
		t.Fatal(err)
	}

	qkeys, err := endpoint.GetAllKeys(ctx, client, "endpoint_stats", "Requests")
	if err != nil {
		t.Fatal(err)
	}
	if len(qkeys) > 0 {
		t.Error("Expected zero keys, found:", len(qkeys))
	}
}

// NOTE: This test depends on the real data in the mlab-ns logs.  If the number of
// ill-behaved clients drops, this test may start failing.
func TestLiveBQQuery(t *testing.T) {
	testLive, _ := os.LookupEnv("TEST_LIVE_BQ")
	if testLive != "true" {
		t.Skip("Skipping - set TEST_LIVE_BQ to run")
	}

	// Using the real mlab-ns table!
	dsExt, err := bqext.NewDataset("mlab-ns", "exports")
	if err != nil {
		t.Fatal(err)
	}
	// Fetch all client signatures with more than 200 requests in past day.
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	rows, err := endpoint.FetchEndpointStats(ctx, &dsExt, 200)
	if err != nil {
		t.Fatal(err)
	}

	keys, _, err := endpoint.MakeKeysAndStats(rows)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}
	log.Println(len(keys), "rows over threshold")
	// Test is meaningless if there are no interesting clients
	if len(keys) < 10 {
		t.Fatal("Not enough high rate clients", len(keys))
	}

}

func TestCreateTestEntries(t *testing.T) {
	var rows []map[string]bigquery.Value
	var err error
	rows, err = testRows()
	if err != nil {
		t.Fatal(err)
	}

	keys, endpoints, err := endpoint.MakeKeysAndStats(rows)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}
	log.Println(len(keys), "rows over threshold")
	// Test is meaningless if there are no interesting clients
	if len(keys) < 10 {
		t.Fatal("Not enough high rate clients", len(keys))
	}

	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// This is OK, because we are using client ProjectID mlab-testing.
	_, err = endpoint.DeleteAllKeys(ctx, client, "endpoint_stats", "Requests")
	if err != nil {
		log.Fatal(err)
	}

	// Save all the keys
	_, err = client.PutMulti(ctx, keys, endpoints)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}

	// Even the datastore emulator takes a little while to become consistent, so
	// backoff until success, up to about 1 second.
	var found []*datastore.Key
	for delay := 33 * time.Millisecond; delay < time.Second; delay = 2 * delay {
		time.Sleep(delay)
		found, err = endpoint.GetAllKeys(ctx, client, "endpoint_stats", "Requests")
		if err != nil {
			log.Fatal(err)
		}
		if len(found) == len(keys) {
			return
		}
	}
	t.Error("Expected", len(keys), "Found", len(found))
}

func TestRowGenerator(t *testing.T) {
	rows, err := testRows()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 10 {
		t.Error("Should be 10 rows, got", len(rows))
	}
	compare, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(compare, jsonTestRows) != 0 {
		t.Error("json not identical")
	}
}

func testRows() ([]map[string]bigquery.Value, error) {
	rows := make([]map[string]bigquery.Value, 0, 20)
	err := json.Unmarshal(jsonTestRows, &rows)

	if err != nil {
		return nil, err
	}
	// Actual BQ result has int64, not float64, so convert those fields.
	for i := range rows {
		for k := range rows[i] {
			switch v := rows[i][k].(type) {

			case float64:
				rows[i][k] = int64(v)
			default:
			}
		}
	}
	return rows, nil
}

var jsonTestRows = []byte(`[{"RequesterIP":"0.1.0.1","RequestsPerDay":4494,"resource":"/cron/check_status","userAgent":"AppEngine-Google; (+http://code.google.com/appengine)"},{"RequesterIP":"2601:406:302:6a50:b2fc:dff:fec8:eaad","RequestsPerDay":1883,"resource":"/ndt_ssl?policy=geo_options","userAgent":"Dalvik/2.1.0 (Linux; U; Android 5.1.1; AFTT Build/LVY48F)"},{"RequesterIP":"24.214.47.89","RequestsPerDay":1858,"resource":"/ndt?policy=geo_options","userAgent":"Dalvik/2.1.0 (Linux; U; Android 5.1.1; AFTT Build/LVY48F)"},{"RequesterIP":"2600:1700:4ef0:e10:b67c:9cff:fe54:4c08","RequestsPerDay":1834,"resource":"/ndt_ssl?policy=geo_options","userAgent":"Dalvik/2.1.0 (Linux; U; Android 5.1.1; AFTT Build/LVY48F)"},{"RequesterIP":"98.28.34.74","RequestsPerDay":1791,"resource":"/ndt?policy=geo_options","userAgent":"Dalvik/2.1.0 (Linux; U; Android 5.1.1; AFTT Build/LVY48F)"},{"RequesterIP":"24.165.204.134","RequestsPerDay":1531,"resource":"/ndt_ssl?policy=geo_options","userAgent":"Dalvik/2.1.0 (Linux; U; Android 5.1.1; AFTT Build/LVY48F)"},{"RequesterIP":"2600:8801:2d04:4e00:2fc:8bff:fe30:94dd","RequestsPerDay":1398,"resource":"/ndt_ssl?policy=geo_options","userAgent":"Dalvik/2.1.0 (Linux; U; Android 5.1.1; AFTT Build/LVY48F)"},{"RequesterIP":"209.107.214.55","RequestsPerDay":1371,"resource":"/ndt_ssl?policy=geo_options","userAgent":"Dalvik/2.1.0 (Linux; U; Android 5.1.1; AFTT Build/LVY48F)"},{"RequesterIP":"2604:2d80:840a:84b9:2fc:8bff:fe23:7760","RequestsPerDay":1345,"resource":"/ndt_ssl?policy=geo_options","userAgent":"Dalvik/2.1.0 (Linux; U; Android 5.1.1; AFTT Build/LVY48F)"},{"RequesterIP":"96.19.226.136","RequestsPerDay":1323,"resource":"/ndt?policy=geo_options","userAgent":"Dalvik/2.1.0 (Linux; U; Android 5.1.1; AFTT Build/LVY48F)"}]`)
