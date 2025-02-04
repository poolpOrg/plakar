package agent

import "github.com/prometheus/client_golang/prometheus"

var (
	// Define a counter
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "my_exporter_requests_total",
			Help: "Total number of processed requests",
		},
		[]string{"method", "status"},
	)

	// Define a gauge
	upGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "my_exporter_up",
			Help: "Exporter up status",
		},
	)

	disconnectsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "my_exporter_disconnects_total",
			Help: "Total number of client disconnections",
		},
	)
)

func init() {
	// Register metrics with Prometheus
	prometheus.MustRegister(requestsTotal)
	prometheus.MustRegister(upGauge)
	prometheus.MustRegister(disconnectsTotal)
}

func trackRequest(method, status string) {
	requestsTotal.WithLabelValues(method, status).Inc()
}

func setUpGauge(value float64) {
	upGauge.Set(value)
}

func handleDisconnect() {
	disconnectsTotal.Inc() // Increment disconnect counter
}
