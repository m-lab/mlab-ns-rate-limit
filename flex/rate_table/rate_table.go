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
//  a. Only this singleton app will write to datastore or memcache.  mlab-ns will be read only.
//  b. Since we update memcache, expiration time can be indefinite.  Any item in memcache will
//     be up to date.
//  c. We must remove any items in memcache that are not present in the newest table.
//  d. We will handle the BQ query, and directly build the table in memcache and datastore as
//     we read the query result.

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	http.HandleFunc("/", defaultHandler)
	http.HandleFunc("/receiver", receiver)
}

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
		// TODO - this is actually returning StatusOK.  Weird!
		http.Error(w, `{"message": "Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	fmt.Fprintf(w, defaultMessage)
}

// receiver accepts a GET request, and transforms the given parameters into a TaskQueue Task.
func receiver(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)

	benchmarkMemcacheGet(ctx)
}

// This shows that memcache read, with aetest environment, takes about 400 usec.
func benchmarkMemcacheGet(ctx context.Context) {
	ctx, err := appengine.Namespace(ctx, "memcache_requests")
	if err != nil {
		log.Fatal(err)
	}

	ep := endpoint.Stats{
		Path:     "ndt_ssl",
		Policy:   "geo_options",
		TargetIP: "127.0.0.1",
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

	// Get the item from the memcache
	if item, err := memcache.Get(ctx, key); err == memcache.ErrCacheMiss {
		log.Fatal("item not in the cache")
	} else if err != nil {
		log.Fatalf("error getting item: %v", err)
	} else {
		log.Printf("the lyric is %q", item.Value)
	}

	for i := 0; i < 1000; i++ {
		if _, err := memcache.Get(ctx, key); err != nil {
			log.Fatal(err)
		}
	}
}

/*
// TODO(gfr) unify counting for http and pubsub paths?
func worker(rwr http.ResponseWriter, rq *http.Request) {
	// This will add metric count and log message from any panic.
	// The panic will still propagate, and http will report it.
	defer func() {
		etl.CountPanics(recover(), "worker")
	}()

	rwr.WriteHeader(http.StatusOK)
	//, `{"message": "Success"}`
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	// TODO(soltesz): provide a real health check.
	fmt.Fprint(w, "ok")
}

func main() {
	// Define a custom serve mux for prometheus to listen on a separate port.
	// We listen on a separate port so we can forward this port on the host VM.
	// We cannot forward port 8080 because it is used by AppEngine.
	mux := http.NewServeMux()
	// Assign the default prometheus handler to the standard exporter path.
	mux.Handle("/metrics", promhttp.Handler())
	// Assign the pprof handling paths to the external port to access individual
	// instances.
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.HandleFunc("/", Status)
	mux.HandleFunc("/status", Status)
	go http.ListenAndServe(":9090", mux)

	http.HandleFunc("/", Status)
	http.HandleFunc("/status", Status)
	http.HandleFunc("/worker", worker)
	http.HandleFunc("/_ah/health", healthCheckHandler)

	// Enable block profiling
	runtime.SetBlockProfileRate(1000000) // One event per msec.

	// We also setup another prometheus handler on a non-standard path. This
	// path name will be accessible through the AppEngine service address,
	// however it will be served by a random instance.
	http.Handle("/random-metrics", promhttp.Handler())
	http.ListenAndServe(":8080", nil)
}
*/