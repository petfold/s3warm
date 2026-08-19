// Package metrics defines the gateway's Prometheus instrumentation
// (design §16), served at /_s3warm/metrics.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3warm_requests_total",
		Help: "S3 API requests by method and status code.",
	}, []string{"method", "code"})

	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3warm_request_duration_seconds",
		Help:    "S3 API request duration.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method"})

	ObjectBytesIn = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3warm_object_bytes_in_total",
		Help: "Object payload bytes received (PutObject).",
	})

	ObjectBytesOut = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3warm_object_bytes_out_total",
		Help: "Object payload bytes served (GetObject).",
	})

	BeeRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3warm_bee_requests_total",
		Help: "Upstream Bee API requests by operation and status code (0 = transport error).",
	}, []string{"op", "code"})

	BeeRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3warm_bee_request_duration_seconds",
		Help:    "Upstream Bee API request duration.",
		Buckets: prometheus.DefBuckets,
	}, []string{"op"})

	StampTTLSeconds = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "s3warm_stamp_ttl_seconds",
		Help: "Estimated remaining life of a tracked postage batch.",
	}, []string{"batch"})

	StampUtilizationRatio = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "s3warm_stamp_utilization_ratio",
		Help: "Fill ratio (0-1) of a tracked postage batch.",
	}, []string{"batch"})
)

// Handler serves the Prometheus exposition endpoint.
func Handler() http.Handler { return promhttp.Handler() }
