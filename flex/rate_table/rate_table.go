// Package main contains the top level app-engine flex code to run the rate_table app.
package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"

	"github.com/m-lab/etl/etl"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
