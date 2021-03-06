package metrics

import "github.com/prometheus/client_golang/prometheus"

func init() {
	// Register the metrics defined with Prometheus's default registry.
	prometheus.MustRegister(FailCount)
	prometheus.MustRegister(WarningCount)
	prometheus.MustRegister(BadEndpointCount)
}

var (
	// FailCount counts the number of failed requests.
	// These occur when a request cannot be completed.
	//
	// Provides metrics:
	//   rate_table_fail_count{type}
	// Example usage:
	// metrics.FailCount.WithLabelValues("BadTableName").Inc()
	FailCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rate_table_fail_total",
			Help: "Number of processing failures.",
		},
		[]string{"type"},
	)

	// WarningCount counts all warnings encountered during processing a request.
	//
	// Provides metrics:
	//   rate_table_warning_count{type}
	// Example usage:
	// metrics.WarningCount.WithLabelValues("No bad clients found").Inc()
	WarningCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rate_table_warning_total",
			Help: "Number of processing warnings.",
		},
		[]string{"type"},
	)

	// BadEndpointCount tracks number of bad endpoints in most recent update.
	//
	// Provides metrics:
	//   rate_table_bad_endpoint_count{}
	// Example usage:
	// metrics.BadEndpointCount.Inc()
	BadEndpointCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "rate_table_bad_endpoint_count",
			Help: "Current number of bad endpoints.",
		},
	)
)
