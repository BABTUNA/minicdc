// Package bench measures end-to-end pipeline latency the way Artie's own
// benchmarks repo does: compare the source commit time carried on each row
// against the time the writer applied it. Both timestamps live in the
// destination's metadata columns, so one query over the destination tells the
// whole story.
package bench

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Bucket is one minute of latency stats, mirroring the columns in
// artie-labs/benchmarks/data/*.csv.
type Bucket struct {
	Minute   time.Time
	RowCount int64
	AvgSec   float64
	P95Sec   float64
	MaxSec   float64
}

// Report queries per-minute latency for a destination table, writes a CSV, and
// returns an overall summary line.
func Report(ctx context.Context, destDSN, table, csvPath string) (string, error) {
	pool, err := pgxpool.New(ctx, destDSN)
	if err != nil {
		return "", err
	}
	defer pool.Close()

	query := fmt.Sprintf(`
SELECT date_trunc('minute', __minicdc_updated_at) AS minute,
       count(*),
       avg(extract(epoch FROM __minicdc_updated_at - __minicdc_commit_ts)),
       percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM __minicdc_updated_at - __minicdc_commit_ts)),
       max(extract(epoch FROM __minicdc_updated_at - __minicdc_commit_ts))
FROM %s
WHERE __minicdc_updated_at IS NOT NULL AND __minicdc_commit_ts IS NOT NULL
GROUP BY 1 ORDER BY 1`, quoteTable(table))

	rows, err := pool.Query(ctx, query)
	if err != nil {
		return "", fmt.Errorf("latency query: %w", err)
	}
	defer rows.Close()

	var buckets []Bucket
	var totalRows int64
	var weightedAvg, overallMax float64
	for rows.Next() {
		var b Bucket
		if err := rows.Scan(&b.Minute, &b.RowCount, &b.AvgSec, &b.P95Sec, &b.MaxSec); err != nil {
			return "", err
		}
		buckets = append(buckets, b)
		totalRows += b.RowCount
		weightedAvg += b.AvgSec * float64(b.RowCount)
		if b.MaxSec > overallMax {
			overallMax = b.MaxSec
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if totalRows == 0 {
		return "", fmt.Errorf("no rows with latency metadata in %s", table)
	}

	if err := writeCSV(csvPath, buckets); err != nil {
		return "", err
	}

	avg := weightedAvg / float64(totalRows)
	var p95 float64
	for _, b := range buckets {
		if b.P95Sec > p95 {
			p95 = b.P95Sec
		}
	}
	return fmt.Sprintf("BENCH: %s, %d rows, avg %.1fs / p95 %.0fs / max %.0fs end-to-end latency (csv: %s)",
		table, totalRows, avg, p95, overallMax, csvPath), nil
}

func writeCSV(path string, buckets []Bucket) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintln(f, "minute,row_count,avg_latency_s,p95_latency_s,max_latency_s")
	for _, b := range buckets {
		fmt.Fprintf(f, "%s,%d,%.3f,%.3f,%.3f\n",
			b.Minute.Format("2006-01-02 15:04:05"), b.RowCount, b.AvgSec, b.P95Sec, b.MaxSec)
	}
	return nil
}

func quoteTable(t string) string {
	// Reuse the same schema-qualified quoting the rest of the project uses.
	// Kept local to avoid a dependency on internal/writer.
	if i := indexDot(t); i >= 0 {
		return `"` + t[:i] + `"."` + t[i+1:] + `"`
	}
	return `"` + t + `"`
}

func indexDot(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			return i
		}
	}
	return -1
}
