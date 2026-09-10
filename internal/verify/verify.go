package verify

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BABTUNA/bartie/internal/config"
)

// Run compares every replicated table on the source against the destination,
// retrying until they match or the timeout expires. Retrying exists because a
// live pipeline has in-flight events: failure is never converging, not a
// single mismatched snapshot.
//
// It knows nothing about the reader, writer, or Kafka: it reads both ends and
// nothing else, which is what makes its MATCH trustworthy.
func Run(ctx context.Context, cfg config.Config, timeout time.Duration) error {
	source, err := pgxpool.New(ctx, cfg.SourceDSN)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer source.Close()
	dest, err := pgxpool.New(ctx, cfg.DestDSN)
	if err != nil {
		return fmt.Errorf("connect dest: %w", err)
	}
	defer dest.Close()

	tables, err := listTables(ctx, source, cfg.Publication)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("publication %q covers no tables", cfg.Publication)
	}

	shapes := make([]tableShape, 0, len(tables))
	for _, tbl := range tables {
		shape, err := loadShape(ctx, source, tbl)
		if err != nil {
			return err
		}
		shapes = append(shapes, shape)
	}

	deadline := time.Now().Add(timeout)
	for {
		mismatches, err := compareAll(ctx, source, dest, shapes)
		if err != nil {
			return err
		}
		if len(mismatches) == 0 {
			fmt.Printf("VERIFY: all %d tables MATCH\n", len(shapes))
			return nil
		}
		if time.Now().After(deadline) {
			for _, m := range mismatches {
				fmt.Println("MISMATCH:", m)
			}
			return fmt.Errorf("verify failed: %d of %d tables did not converge within %s", len(mismatches), len(shapes), timeout)
		}
		time.Sleep(2 * time.Second)
	}
}

func compareAll(ctx context.Context, source, dest *pgxpool.Pool, shapes []tableShape) ([]string, error) {
	var mismatches []string
	for _, shape := range shapes {
		srcCS, err := checksum(ctx, source, shape)
		if err != nil {
			return nil, err
		}
		destCS, err := checksum(ctx, dest, shape)
		if err != nil {
			// The dest table may not exist yet (no events seen): a mismatch,
			// not a fatal error, so the retry loop can wait it out.
			mismatches = append(mismatches, fmt.Sprintf("%s: dest not readable (%v)", shape.Table, err))
			continue
		}
		if srcCS != destCS {
			mismatches = append(mismatches, fmt.Sprintf("%s: source{n=%d %s} dest{n=%d %s}",
				shape.Table, srcCS.Count, short(srcCS.Digest), destCS.Count, short(destCS.Digest)))
		}
	}
	return mismatches, nil
}

func listTables(ctx context.Context, source *pgxpool.Pool, publication string) ([]string, error) {
	rows, err := source.Query(ctx,
		`SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY 1`, publication)
	if err != nil {
		return nil, fmt.Errorf("list publication tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func short(digest string) string {
	if len(digest) > 8 {
		return digest[:8]
	}
	return digest
}
