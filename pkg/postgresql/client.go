package pgprometheus

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/timescale/prometheus-postgresql-adapter/pkg/log"
	"github.com/timescale/prometheus-postgresql-adapter/pkg/util"

	_ "github.com/lib/pq"
	"github.com/prometheus/common/model"
)

// Config for the database
type Config struct {
	host                   string
	port                   int
	user                   string
	passwordFile           string
	database               string
	schema                 string
	sslMode                string
	table                  string
	maxOpenConns           int
	maxIdleConns           int
	pgPrometheusLogSamples bool
	dbConnectRetries       int
}

// ParseFlags parses the configuration flags specific to PostgreSQL and TimescaleDB
func ParseFlags(cfg *Config) *Config {
	flag.StringVar(&cfg.host, "pg-host", "localhost", "The PostgreSQL host")
	flag.IntVar(&cfg.port, "pg-port", 5432, "The PostgreSQL port")
	flag.StringVar(&cfg.user, "pg-user", "postgres", "The PostgreSQL user")
	flag.StringVar(&cfg.passwordFile, "pg-password-file", "", "File to read the PostgreSQL password from")
	flag.StringVar(&cfg.database, "pg-database", "postgres", "The PostgreSQL database")
	flag.StringVar(&cfg.sslMode, "pg-ssl-mode", "disable", "The PostgreSQL connection ssl mode")
	flag.StringVar(&cfg.table, "pg-table", "metrics", "Override prefix for internal tables. It is also a view name used for querying")
	flag.IntVar(&cfg.maxOpenConns, "pg-max-open-conns", 50, "The max number of open connections to the database")
	flag.IntVar(&cfg.maxIdleConns, "pg-max-idle-conns", 10, "The max number of idle connections to the database")
	flag.BoolVar(&cfg.pgPrometheusLogSamples, "pg-prometheus-log-samples", false, "Log raw samples to stdout")
	flag.IntVar(&cfg.dbConnectRetries, "pg-db-connect-retries", 0, "How many times to retry connecting to the database")
	return cfg
}

// Client sends Prometheus samples to PostgreSQL
type Client struct {
	DB  *sql.DB
	cfg *Config
}

// noinspection SqlNoDataSourceInspection
const (
	sqlCreateTmpTable = "CREATE TEMPORARY TABLE IF NOT EXISTS %s_tmp(sample prom_sample) ON COMMIT DELETE ROWS;"
	sqlCopyTable      = "COPY \"%s\" FROM STDIN"
	sqlInsertLabels   = "insert into %s_labels (metric_name, labels) select distinct sample.metric_name, sample.labels from %s_tmp sample on conflict do nothing;"
	sqlInsertValues   = "insert into %s_values (time, value, labels_id) select sample.time, sample.value, lbl.id from %s_tmp sample left join %s_labels lbl on lbl.metric_name = sample.metric_name and lbl.labels = sample.labels;"
	sqlHealthCheck    = "SELECT 1"
)

var (
	createTmpTableStmt *sql.Stmt
)

// NewClient creates a new PostgreSQL client
func NewClient(cfg *Config) *Client {
	content, err := os.ReadFile(cfg.passwordFile)
	if err != nil {
		log.Error("msg", "Password file for establishing postgresql connection not found", "err", err)
		os.Exit(1)
	}
	password := string(content)
	connStr := fmt.Sprintf("host=%v port=%v user=%v dbname=%v password='%v' sslmode=%v connect_timeout=10",
		cfg.host, cfg.port, cfg.user, cfg.database, password, cfg.sslMode)

	wrappedDb, err := util.RetryWithFixedDelay(uint(cfg.dbConnectRetries), time.Second, func() (interface{}, error) {
		return sql.Open("postgres", connStr)
	})

	log.Info("msg", regexp.MustCompile("password='(.+?)'").ReplaceAllLiteralString(connStr, "password='****'"))

	if err != nil {
		log.Error("err", err)
		os.Exit(1)
	}

	db := wrappedDb.(*sql.DB)

	db.SetMaxOpenConns(cfg.maxOpenConns)
	db.SetMaxIdleConns(cfg.maxIdleConns)

	client := &Client{
		DB:  db,
		cfg: cfg,
	}

	createTmpTableStmt, err = db.Prepare(fmt.Sprintf(sqlCreateTmpTable, cfg.table))
	if err != nil {
		log.Error("msg", "Error on preparing create tmp table statement", "err", err)
		os.Exit(1)
	}

	return client
}

func metricMetaJson(m model.Metric) (string, string) {
	metricName, hasName := m[model.MetricNameLabel]
	numLabels := len(m) - 1
	if !hasName {
		numLabels = len(m)
	}
	labelStrings := make([]string, 0, numLabels)
	for label, value := range m {
		if label != model.MetricNameLabel {
			escapedLabel, err := json.Marshal(string(label))
			if err != nil {
				log.Warn("msg", fmt.Sprintf("Could not format label '%s', skipping", string(label)), "err", err)
				continue
			}
			escapedValue, err := json.Marshal(string(value))
			if err != nil {
				log.Warn("msg", fmt.Sprintf("Could not format value '%s', skipping", string(value)), "err", err)
				continue
			}
			labelStrings = append(labelStrings, fmt.Sprintf("%s: %s", escapedLabel, escapedValue))
		}
	}

	switch numLabels {
	case 0:
		if hasName {
			return string(metricName), "{}"
		}
		return "", "{}"
	default:
		sort.Strings(labelStrings)
		return string(metricName), fmt.Sprintf("{%s}", strings.Join(labelStrings, ","))
	}
}

// Write implements the Writer interface and writes metric samples to the database
func (c *Client) Write(samples model.Samples) error {
	begin := time.Now()
	tx, err := c.DB.Begin()

	if err != nil {
		log.Error("msg", "Error on Begin when writing samples", "err", err)
		return err
	}

	defer func(tx *sql.Tx) {
		err = tx.Rollback()
		if err != nil {
			log.Warn("msg", "Rollback failed", "err", err)
		}
	}(tx)

	_, err = tx.Stmt(createTmpTableStmt).Exec()
	if err != nil {
		log.Error("msg", "Error executing create tmp table", "err", err)
		return err
	}

	copyTable := fmt.Sprintf("%s_tmp", c.cfg.table)
	copyStmt, err := tx.Prepare(fmt.Sprintf(sqlCopyTable, copyTable))

	if err != nil {
		log.Error("msg", "Error on COPY prepare", "err", err)
		return err
	}

	for _, sample := range samples {
		timestampStr := sample.Timestamp.Time().UTC().Format(time.RFC3339)
		metricName, metricJson := metricMetaJson(sample.Metric)
		line := fmt.Sprintf("%v\t%v\t%v\t%v", timestampStr, sample.Value, metricName, metricJson)
		if c.cfg.pgPrometheusLogSamples {
			fmt.Println(line)
		}

		_, err = copyStmt.Exec(line)
		if err != nil {
			log.Error("msg", "Error executing COPY statement", "stmt", line, "err", err)
			return err
		}
	}

	_, err = copyStmt.Exec()
	if err != nil {
		log.Error("msg", "Error executing COPY statement", "err", err)
		return err
	}

	if copyTable == fmt.Sprintf("%s_tmp", c.cfg.table) {
		stmtLabels, err := tx.Prepare(fmt.Sprintf(sqlInsertLabels, c.cfg.table, c.cfg.table))
		if err != nil {
			log.Error("msg", "Error on preparing labels statement", "err", err)
			return err
		}
		_, err = stmtLabels.Exec()
		if err != nil {
			log.Error("msg", "Error executing labels statement", "err", err)
			return err
		}

		stmtValues, err := tx.Prepare(fmt.Sprintf(sqlInsertValues, c.cfg.table, c.cfg.table, c.cfg.table))
		if err != nil {
			log.Error("msg", "Error on preparing values statement", "err", err)
			return err
		}
		_, err = stmtValues.Exec()
		if err != nil {
			log.Error("msg", "Error executing values statement", "err", err)
			return err
		}

		err = stmtLabels.Close()
		if err != nil {
			log.Error("msg", "Error on closing labels statement", "err", err)
			return err
		}

		err = stmtValues.Close()
		if err != nil {
			log.Error("msg", "Error on closing values statement", "err", err)
			return err
		}
	}

	err = copyStmt.Close()
	if err != nil {
		log.Error("msg", "Error on COPY Close when writing samples", "err", err)
		return err
	}

	err = tx.Commit()

	if err != nil {
		log.Error("msg", "Error on Commit when writing samples", "err", err)
		return err
	}

	duration := time.Since(begin).Seconds()

	log.Debug("msg", "Wrote samples", "count", len(samples), "duration", duration)

	return nil
}

func (c *Client) Close() {
	if c.DB != nil {
		if err := c.DB.Close(); err != nil {
			log.Error("msg", err.Error())
		}
	}
}

// HealthCheck implements the healtcheck interface
func (c *Client) HealthCheck() error {
	rows, err := c.DB.Query(sqlHealthCheck)

	if err != nil {
		log.Debug("msg", "Health check error", "err", err)
		return err
	}

	_ = rows.Close()
	return nil
}

// Name identifies the client as a PostgreSQL client.
func (c *Client) Name() string {
	return "PostgreSQL"
}
