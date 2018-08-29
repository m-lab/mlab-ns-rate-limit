package endpoint_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/m-lab/go/bqext"
	"github.com/m-lab/mlab-ns-rate-limit/endpoint"
)

func getClient() (*datastore.Client, error) {
	ctx := context.Background()
	projectID := "mlab-testing"
	return datastore.NewClient(ctx, projectID)
}

func TestDeleteAllKeys(t *testing.T) {
	// This should only be run when using the emulator.
	_, usingEmulator := os.LookupEnv("DATASTORE_EMULATOR_HOST")
	if !usingEmulator {
		return
	}

	client, err := getClient()
	if err != nil {
		log.Fatal(err)
	}

	// Just verify that the call completes successfully.
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err = endpoint.DeleteAllKeys(ctx, client, "endpoint_stats", "requests")
	if err != nil {
		t.Fatal(err)
	}

	qkeys, err := endpoint.GetAllKeys(ctx, client, "endpoint_stats", "requests")
	if err != nil {
		t.Fatal(err)
	}
	if len(qkeys) != 0 {
		t.Error("Expected zero keys, found:", len(qkeys))
	}

}

func TestCreateTestEntries(t *testing.T) {
	// This test queries the real mlab-ns stackdriver table, so we skip it when
	// test is invoked with -short
	if testing.Short() {
		log.Println("Skipping TestCreateTestEntries")
		return
	}

	// Using the real mlab-ns table!
	dsExt, err := bqext.NewDataset("mlab-ns", "exports")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := endpoint.FetchEndpointStats(&dsExt, 500)
	if err != nil {
		t.Fatal(err)
	}
	log.Println(len(rows))
	for i := range rows {
		if i > 10 {
			break
		}
		log.Println(rows[i])
	}

	keys, endpoints, err := endpoint.MakeKeysAndStats(rows, 300)
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

	err = client.DeleteMulti(ctx, keys)
	if err != nil {
		log.Fatal(err)
	}

	qkeys, err := endpoint.GetAllKeys(ctx, client, "endpoint_stats", "requests")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Found", len(qkeys))

	// Saves the new entity.
	_, err = client.PutMulti(ctx, keys, endpoints)
	if err != nil {
		log.Fatalf("Failed: %v", err)
	}
	log.Println("Wrote", len(keys), "of", len(rows))
}
