package tenantpool

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics wraps the Prometheus collectors the Registry updates. Callers
// register them with their own Registerer; the package never touches
// prometheus.DefaultRegisterer so it cannot double-register when
// imported from multiple modules.
type Metrics struct {
	PoolsActive  prometheus.Gauge
	PoolsCreated prometheus.Counter
	PoolsEvicted prometheus.Counter
	PoolErrors   *prometheus.CounterVec
	Acquire      prometheus.Histogram
}

// NewMetrics returns a Metrics bundle. Pass the result to
// Registry.WithMetrics; call MustRegister on your Registerer to expose
// them. The optional constLabels map (e.g. {"module":"auth"}) lets
// multi-module deployments distinguish pools in the same Prometheus
// scrape.
func NewMetrics(constLabels map[string]string) *Metrics {
	return &Metrics{
		PoolsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "tenantpool_pools_active",
			Help:        "Number of live tenant pools held by the Registry.",
			ConstLabels: constLabels,
		}),
		PoolsCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "tenantpool_pools_created_total",
			Help:        "Tenant pools opened.",
			ConstLabels: constLabels,
		}),
		PoolsEvicted: prometheus.NewCounter(prometheus.CounterOpts{
			Name:        "tenantpool_pools_evicted_total",
			Help:        "Tenant pools closed by LRU or idle eviction.",
			ConstLabels: constLabels,
		}),
		PoolErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name:        "tenantpool_pool_errors_total",
			Help:        "Tenant pool resolution errors, labelled by sentinel.",
			ConstLabels: constLabels,
		}, []string{"reason"}),
		Acquire: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:        "tenantpool_pool_acquire_duration_seconds",
			Help:        "Pool acquire latency (Registry.Get including dial).",
			ConstLabels: constLabels,
			Buckets:     prometheus.DefBuckets,
		}),
	}
}

// Collectors returns the slice needed to register all metrics with a
// single MustRegister call.
func (m *Metrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.PoolsActive,
		m.PoolsCreated,
		m.PoolsEvicted,
		m.PoolErrors,
		m.Acquire,
	}
}

// metricsState wires Registry counters into Prometheus. The Registry
// holds a *Metrics pointer (nil-safe) and increments through these
// helpers so business code does not branch on "metrics enabled".
type metricsState struct {
	mu sync.Mutex
	m  *Metrics
}

// WithMetrics attaches a Metrics bundle to the Registry. Call before
// Get is invoked; changing metrics at runtime is unsupported.
func (r *Registry) WithMetrics(m *Metrics) *Registry {
	r.metrics.mu.Lock()
	r.metrics.m = m
	r.metrics.mu.Unlock()
	return r
}

func (s *metricsState) incCreated() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m != nil {
		s.m.PoolsCreated.Inc()
		s.m.PoolsActive.Inc()
	}
}
func (s *metricsState) incEvicted(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m != nil && n > 0 {
		s.m.PoolsEvicted.Add(float64(n))
		s.m.PoolsActive.Sub(float64(n))
	}
}
func (s *metricsState) incError(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m != nil {
		s.m.PoolErrors.WithLabelValues(reason).Inc()
	}
}
