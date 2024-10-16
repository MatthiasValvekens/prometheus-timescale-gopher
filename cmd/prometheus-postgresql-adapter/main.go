// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The main package for the Prometheus server executable.
package main

// Based on the Prometheus remote storage example:
// documentation/examples/remote_storage/remote_storage_adapter/main.go

import (
	"flag"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sync/atomic"
	"time"

	"github.com/timescale/prometheus-postgresql-adapter/pkg/log"
	pgprometheus "github.com/timescale/prometheus-postgresql-adapter/pkg/postgresql"
	"github.com/timescale/prometheus-postgresql-adapter/pkg/util"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/jamiealquiza/envy"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	ioprometheusclient "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"

	"database/sql"
	"fmt"
)

type config struct {
	remoteTimeout      time.Duration
	listenAddr         string
	telemetryPath      string
	pgPrometheusConfig pgprometheus.Config
	logLevel           string
	haGroupLockID      int
	restElection       bool
	prometheusTimeout  time.Duration
	electionInterval   time.Duration
}

const (
	tickInterval      = time.Second
	promLivenessCheck = time.Second
)

var (
	receivedSamples = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "received_samples_total",
			Help: "Total number of received samples.",
		},
	)
	sentSamples = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sent_samples_total",
			Help: "Total number of processed samples sent to remote storage.",
		},
		[]string{"remote"},
	)
	failedSamples = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "failed_samples_total",
			Help: "Total number of processed samples which failed on send to remote storage.",
		},
		[]string{"remote"},
	)
	sentBatchDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sent_batch_duration_seconds",
			Help:    "Duration of sample batch send calls to the remote storage.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"remote"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_ms",
			Help:    "Duration of HTTP request in milliseconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"path"},
	)
	writeThroughput     = util.NewThroughputCalc(tickInterval)
	elector             *util.Elector
	lastRequestUnixNano = time.Now().UnixNano()
)

func init() {
	prometheus.MustRegister(receivedSamples)
	prometheus.MustRegister(sentSamples)
	prometheus.MustRegister(failedSamples)
	prometheus.MustRegister(sentBatchDuration)
	prometheus.MustRegister(httpRequestDuration)
	writeThroughput.Start()
}

func main() {
	cfg := parseFlags()
	log.Init(cfg.logLevel)
	log.Info("config", fmt.Sprintf("%+v", cfg))

	http.Handle(cfg.telemetryPath, promhttp.Handler())

	pgClient := buildClients(cfg)
	elector = initElector(cfg, pgClient.DB)

	http.Handle("/write", timeHandler("write", write(pgClient)))
	http.Handle("/healthz", health(pgClient))

	log.Info("msg", "Starting up...")
	log.Info("msg", "Listening", "addr", cfg.listenAddr)

	err := http.ListenAndServe(cfg.listenAddr, nil)

	if err != nil {
		log.Error("msg", "Listen failure", "err", err)
		os.Exit(1)
	}
}

func parseFlags() *config {

	cfg := &config{}

	pgprometheus.ParseFlags(&cfg.pgPrometheusConfig)

	flag.DurationVar(&cfg.remoteTimeout, "adapter-send-timeout", 30*time.Second, "The timeout to use when sending samples to the remote storage.")
	flag.StringVar(&cfg.listenAddr, "web-listen-address", ":9201", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.telemetryPath, "web-telemetry-path", "/metrics", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.logLevel, "log-level", "debug", "The log level to use [ \"error\", \"warn\", \"info\", \"debug\" ].")
	flag.IntVar(&cfg.haGroupLockID, "leader-election-pg-advisory-lock-id", 0, "Unique advisory lock id per adapter high-availability group. Set it if you want to use leader election implementation based on PostgreSQL advisory lock.")
	flag.DurationVar(&cfg.prometheusTimeout, "leader-election-pg-advisory-lock-prometheus-timeout", -1, "Adapter will resign if there are no requests from Prometheus within a given timeout (0 means no timeout). "+
		"Note: make sure that only one Prometheus instance talks to the adapter. Timeout value should be co-related with Prometheus scrape interval but add enough `slack` to prevent random flips.")
	flag.BoolVar(&cfg.restElection, "leader-election-rest", false, "Enable REST interface for the leader election")
	flag.DurationVar(&cfg.electionInterval, "scheduled-election-interval", 5*time.Second, "Interval at which scheduled election runs. This is used to select a leader and confirm that we still holding the advisory lock.")

	envy.Parse("TS_PROM")
	flag.Parse()

	return cfg
}

type writer interface {
	Write(samples model.Samples) error
	Name() string
}

func buildClients(cfg *config) *pgprometheus.Client {
	pgClient := pgprometheus.NewClient(&cfg.pgPrometheusConfig)
	return pgClient
}

func initElector(cfg *config, db *sql.DB) *util.Elector {
	if cfg.restElection && cfg.haGroupLockID != 0 {
		log.Error("msg", "Use either REST or PgAdvisoryLock for the leader election")
		os.Exit(1)
	}
	if cfg.restElection {
		return util.NewElector(util.NewRestElection())
	}
	if cfg.haGroupLockID == 0 {
		log.Warn("msg", "No adapter leader election. Group lock id is not set. Possible duplicate write load if running adapter in high-availability mode")
		return nil
	}
	if cfg.prometheusTimeout == -1 {
		log.Error("msg", "Prometheus timeout configuration must be set when using PG advisory lock")
		os.Exit(1)
	}
	lock, err := util.NewPgAdvisoryLock(cfg.haGroupLockID, db)
	if err != nil {
		log.Error("msg", "Error creating advisory lock", "haGroupLockId", cfg.haGroupLockID, "err", err)
		os.Exit(1)
	}
	scheduledElector := util.NewScheduledElector(lock, cfg.electionInterval)
	log.Info("msg", "Initialized leader election based on PostgreSQL advisory lock")
	if cfg.prometheusTimeout != 0 {
		go func() {
			ticker := time.NewTicker(promLivenessCheck)
			for {
				select {
				case <-ticker.C:
					lastReq := atomic.LoadInt64(&lastRequestUnixNano)
					scheduledElector.PrometheusLivenessCheck(lastReq, cfg.prometheusTimeout)
				}
			}
		}()
	}
	return &scheduledElector.Elector
}

func write(writer writer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		compressed, err := io.ReadAll(r.Body)
		if err != nil {
			log.Error("msg", "Read error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			log.Error("msg", "Decode error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			log.Error("msg", "Unmarshal error", "err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		samples := protoToSamples(&req)
		receivedSamples.Add(float64(len(samples)))

		err = sendSamples(writer, samples)
		if err != nil {
			log.Warn("msg", "Error sending samples to remote storage", "err", err, "storage", writer.Name(), "num_samples", len(samples))
		}

		counter, err := sentSamples.GetMetricWithLabelValues(writer.Name())
		if err != nil {
			log.Warn("msg", "Couldn't get a counter", "labelValue", writer.Name(), "err", err)
		}
		writeThroughput.SetCurrent(getCounterValue(counter))

		select {
		case d := <-writeThroughput.Values:
			log.Info("msg", "Samples write throughput", "samples/sec", d)
		default:
		}
	})
}

func getCounterValue(counter prometheus.Counter) float64 {
	dtoMetric := &ioprometheusclient.Metric{}
	if err := counter.Write(dtoMetric); err != nil {
		log.Warn("msg", "Error reading counter value", "err", err, "sentSamples", sentSamples)
	}
	return dtoMetric.GetCounter().GetValue()
}

func health(writer *pgprometheus.Client) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := writer.HealthCheck()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", "0")
	})
}

func protoToSamples(req *prompb.WriteRequest) model.Samples {
	var samples model.Samples
	for _, ts := range req.Timeseries {
		metric := make(model.Metric, len(ts.Labels))
		for _, l := range ts.Labels {
			metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		}

		for _, s := range ts.Samples {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(s.Value),
				Timestamp: model.Time(s.Timestamp),
			})
		}
	}
	return samples
}

func sendSamples(w writer, samples model.Samples) error {
	atomic.StoreInt64(&lastRequestUnixNano, time.Now().UnixNano())
	begin := time.Now()
	shouldWrite := true
	var err error
	if elector != nil {
		shouldWrite, err = elector.IsLeader()
		if err != nil {
			log.Error("msg", "IsLeader check failed", "err", err)
			return err
		}
	}
	if shouldWrite {
		err = w.Write(samples)
	} else {
		log.Debug("msg", fmt.Sprintf("Election id %v: Instance is not a leader. Can't write data", elector.ID()))
		return nil
	}
	duration := time.Since(begin).Seconds()
	if err != nil {
		failedSamples.WithLabelValues(w.Name()).Add(float64(len(samples)))
		return err
	}
	sentSamples.WithLabelValues(w.Name()).Add(float64(len(samples)))
	sentBatchDuration.WithLabelValues(w.Name()).Observe(duration)
	return nil
}

// timeHandler uses Prometheus histogram to track request time
func timeHandler(path string, handler http.Handler) http.Handler {
	f := func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		handler.ServeHTTP(w, r)
		elapsedMs := time.Since(start).Nanoseconds() / int64(time.Millisecond)
		httpRequestDuration.WithLabelValues(path).Observe(float64(elapsedMs))
	}
	return http.HandlerFunc(f)
}
