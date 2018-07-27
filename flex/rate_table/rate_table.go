// Package rate_table contains the top level app-engine flex code to run the rate_table app.
package rate_table

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/m-lab/mlab-ns-rate-limit/endpoint"
	"google.golang.org/appengine"
	"google.golang.org/appengine/memcache"
)

// 1.  Datastore stuff
// 2.  Memcache stuff
// 3.  Bigquery stuff
// 4.  Other logic

// Design elements:
//  a. This will run in appengine standard, triggered by an appengine cron request.
//  b. Need to determine whether cron jobs may run concurrently, which could cause headaches.
//  c. Memcache entries will be set to expire in twice the cron interval, so that
//     we don't have to delete endpoint signatures that are no longer abusive.
//     We still have to delete them from datastore, though.
//  d. We will handle the BQ query, and directly build the table in memcache and datastore as
//     we read the query result.

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	http.HandleFunc("/", defaultHandler)
	http.HandleFunc("/benchmark", benchmark)
	http.HandleFunc("/status", Status)
}

// Status writes an instance summary into the response.
// TODO(gfr) Add either a black list or a white list for the environment
// variables, so we can hide sensitive vars. https://github.com/m-lab/etl/issues/384
func Status(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "<html><body>\n")
	fmt.Fprintf(w, "<p>NOTE: This is just one of potentially many instances.</p>\n")
	commit := os.Getenv("COMMIT_HASH")
	if len(commit) >= 8 {
		fmt.Fprintf(w, "Release: %s <br>  Commit: <a href=\"https://github.com/m-lab/etl/tree/%s\">%s</a><br>\n",
			os.Getenv("RELEASE_TAG"), os.Getenv("COMMIT_HASH"), os.Getenv("COMMIT_HASH")[0:7])
	} else {
		fmt.Fprintf(w, "Release: %s   Commit: unknown\n", os.Getenv("RELEASE_TAG"))
	}

	env := os.Environ()
	for i := range env {
		fmt.Fprintf(w, "%s</br>\n", env[i])
	}
	fmt.Fprintf(w, "</body></html>\n")
}

const defaultMessage = "<html><body>This is not the app you're looking for.</body></html>"

// A default handler for root path.
func defaultHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"message": "Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	fmt.Fprintf(w, defaultMessage)
}

func benchmark(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	benchmarkMemcacheGet(ctx)
}

// TODO Remove this when development complete.  Only needed for development
// performance validation.
func benchmarkMemcacheGet(ctx context.Context) {
	ctx, err := appengine.Namespace(ctx, "memcache_requests")
	if err != nil {
		log.Fatal(err)
	}

	ep := endpoint.Stats{
		RequestsPerDay: 1234,
		Probability:    6 / 1234.0,
	}
	epJSON, err := json.Marshal(ep)
	if err != nil {
		log.Fatal(err)
	}
	key := "foobar"
	// Set the item, unconditionally
	if err := memcache.Set(ctx, &memcache.Item{Key: key, Value: epJSON}); err != nil {
		log.Fatalf("error setting item: %v", err)
	}

	for i := 0; i < 1000; i++ {
		if _, err := memcache.Get(ctx, key); err != nil {
			log.Fatal(err)
		}
	}
}
