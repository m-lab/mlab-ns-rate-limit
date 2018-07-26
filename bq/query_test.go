package bq_test

import (
	"log"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/GoogleCloudPlatform/google-cloud-go-testing/bigquery/bqiface"
	"golang.org/x/net/context"
)

func TestAdaptClient(t *testing.T) {
	ctx := context.Background()
	c, err := bigquery.NewClient(ctx, "my-project")
	if err != nil {
		// TODO: Handle error.
	}
	client := bqiface.AdaptClient(c)
	defer client.Close()
	ds := client.Dataset("my_dataset")
	md, err := ds.Metadata(ctx)
	if err != nil {
		t.Error(err)
	}
	log.Println(md)
}
