package pgprometheus

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/timescale/prometheus-postgresql-adapter/pkg/log"

	pgx_stdlib "github.com/jackc/pgx/v5/stdlib"
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
	sqlCreateTmpTable   = "create temporary table %s_tmp (time timestamp with time zone, value double precision, metric_name text, labels jsonb) on commit preserve rows;"
	sqlTempTableCleanup = "drop table %s_tmp;"
	sqlInsertLabels     = "insert into %s_labels (metric_name, labels) select distinct sample.metric_name, sample.labels from %s_tmp sample on conflict do nothing;"
	sqlInsertValues     = "insert into %s_values (time, value, labels_id) select sample.time, sample.value, lbl.id from %s_tmp sample left join %s_labels lbl on lbl.metric_name = sample.metric_name and lbl.labels = sample.labels;"
	sqlHealthCheck      = "SELECT 1"
)

func readPassword(cfg *Config) string {
	content, err := os.ReadFile(cfg.passwordFile)
	if err != nil {
		log.Error("msg", "Password file for establishing postgresql connection not found", "err", err)
		os.Exit(1)
	}
	return string(content)
}

// NewClient creates a new PostgreSQL client
func NewClient(cfg *Config) *Client {
	baseConnStr := fmt.Sprintf("host=%v port=%v user=%v dbname=%v sslmode=%v connect_timeout=10",
		cfg.host, cfg.port, cfg.user, cfg.database, cfg.sslMode)

	config, err := pgx.ParseConfig(baseConnStr)
	if err != nil {
		log.Error("err", err)
		os.Exit(1)
	}
	beforeConnectHook := func(ctx context.Context, connConfig *pgx.ConnConfig) error {
		if connConfig != nil {
			log.Debug("msg", "Re-reading password before establishing new connection...")
			password := readPassword(cfg)
			connConfig.Password = password
		}
		return nil
	}
	connector := pgx_stdlib.GetConnector(*config, pgx_stdlib.OptionBeforeConnect(beforeConnectHook))

	db := sql.OpenDB(connector)

	log.Info("msg", baseConnStr)

	db.SetMaxOpenConns(cfg.maxOpenConns)
	db.SetMaxIdleConns(cfg.maxIdleConns)

	client := &Client{
		DB:  db,
		cfg: cfg,
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

func copyFromTmpTableInTransaction(ctx context.Context, conn *sql.Conn, query string, queryDescription string) error {
	tx, err := conn.BeginTx(ctx, nil)

	if err != nil {
		log.Error("msg", "Error on transaction setup", "err", err, "desc", queryDescription)
		return err
	}

	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	stmtLabels, err := tx.Prepare(query)
	if err != nil {
		log.Error("msg", "Error on preparing statement", "err", err, "desc", queryDescription)
		return err
	}
	_, err = stmtLabels.Exec()
	if err != nil {
		log.Error("msg", "Error executing statement", "err", err, "desc", queryDescription)
		return err
	}

	err = stmtLabels.Close()
	if err != nil {
		log.Error("msg", "Error on closing statement", "err", err, "desc", queryDescription)
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Error("msg", "Error on Commit", "err", err, "desc", queryDescription)
		return err
	}
	return nil
}

func (c *Client) insertLabels(ctx context.Context, conn *sql.Conn) error {
	query := fmt.Sprintf(sqlInsertLabels, c.cfg.table, c.cfg.table)
	return copyFromTmpTableInTransaction(ctx, conn, query, "labels")
}

func (c *Client) insertValues(ctx context.Context, conn *sql.Conn) error {
	query := fmt.Sprintf(sqlInsertValues, c.cfg.table, c.cfg.table, c.cfg.table)
	return copyFromTmpTableInTransaction(ctx, conn, query, "values")
}

func (c *Client) cleanup(ctx context.Context, conn *sql.Conn) {
	// not 100% sure if this is necessary, but AFAICT there's no reason why returning
	// a connection to the pool would clean session-local data like temporary tables
	_, err := conn.ExecContext(ctx, fmt.Sprintf(sqlTempTableCleanup, c.cfg.table))
	if err != nil {
		log.Error("msg", "Failed to clean up temp table", "err", err)
	}
	_ = conn.Close()
}

// Write implements the Writer interface and writes metric samples to the database
func (c *Client) Write(samples model.Samples) error {
	begin := time.Now()
	ctx := context.Background()
	conn, err := c.DB.Conn(ctx)
	if err != nil {
		log.Error("msg", "Failed to acquire database connection", "err", err)
		return err
	}
	defer c.cleanup(ctx, conn)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(sqlCreateTmpTable, c.cfg.table))
	if err != nil {
		log.Error("msg", "Error executing create tmp table", "err", err)
		return err
	}

	copyTable := fmt.Sprintf("%s_tmp", c.cfg.table)
	var inputRows [][]interface{} = nil

	for _, sample := range samples {
		timestamp := sample.Timestamp.Time().UTC()
		metricName, metricJson := metricMetaJson(sample.Metric)
		line := fmt.Sprintf("%v\t%v\t%v\t%v", timestamp.Format(time.RFC3339), sample.Value, metricName, metricJson)
		if c.cfg.pgPrometheusLogSamples {
			fmt.Println(line)
		}
		inputRows = append(inputRows, []interface{}{timestamp, float64(sample.Value), metricName, metricJson})
	}
	err = conn.Raw(func(driverConn any) error {
		conn := driverConn.(*pgx_stdlib.Conn).Conn()
		_, err := conn.CopyFrom(ctx, []string{copyTable}, []string{"time", "value", "metric_name", "labels"}, pgx.CopyFromRows(inputRows))
		return err
	})
	if err != nil {
		log.Error("msg", "Error on copy", "err", err)
		return err
	}

	err = c.insertLabels(ctx, conn)
	if err != nil {
		return err
	}

	err = c.insertValues(ctx, conn)
	if err != nil {
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
