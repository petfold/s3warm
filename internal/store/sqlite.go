package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // cgo-free sqlite driver
)

// SQLite is the persistent metadata index (design §5). It is safe for
// concurrent use; WAL mode plus a busy timeout let readers and the single
// writer coexist.
type SQLite struct {
	db *sql.DB
}

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS buckets (
	name       TEXT PRIMARY KEY,
	created_at TEXT NOT NULL,
	batch_id   TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS objects (
	bucket        TEXT NOT NULL,
	key           TEXT NOT NULL,
	swarm_ref     TEXT NOT NULL,
	size          INTEGER NOT NULL,
	etag          TEXT NOT NULL,
	content_type  TEXT NOT NULL DEFAULT '',
	storage_class TEXT NOT NULL DEFAULT '',
	user_meta     TEXT NOT NULL DEFAULT 'null',
	last_modified TEXT NOT NULL,
	PRIMARY KEY (bucket, key)
);
`

// OpenSQLite opens (creating if needed) the index database at path.
func OpenSQLite(path string) (*SQLite, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(sqliteSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("initializing schema: %w", err)
	}
	return &SQLite{db: db}, nil
}

func (s *SQLite) Close() error { return s.db.Close() }

const timeLayout = time.RFC3339Nano

func (s *SQLite) CreateBucket(ctx context.Context, b Bucket) error {
	if b.CreatedAt.IsZero() {
		b.CreatedAt = time.Now().UTC()
	}
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO buckets (name, created_at, batch_id) VALUES (?, ?, ?)
		 ON CONFLICT (name) DO NOTHING`,
		b.Name, b.CreatedAt.UTC().Format(timeLayout), b.BatchID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrBucketExists
	}
	return nil
}

func (s *SQLite) GetBucket(ctx context.Context, name string) (*Bucket, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT name, created_at, batch_id FROM buckets WHERE name = ?`, name)
	var b Bucket
	var created string
	if err := row.Scan(&b.Name, &created, &b.BatchID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrBucketNotFound
		}
		return nil, err
	}
	b.CreatedAt, _ = time.Parse(timeLayout, created)
	return &b, nil
}

func (s *SQLite) ListBuckets(ctx context.Context) ([]Bucket, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, created_at, batch_id FROM buckets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Bucket
	for rows.Next() {
		var b Bucket
		var created string
		if err := rows.Scan(&b.Name, &created, &b.BatchID); err != nil {
			return nil, err
		}
		b.CreatedAt, _ = time.Parse(timeLayout, created)
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *SQLite) DeleteBucket(ctx context.Context, name string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	var one int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM buckets WHERE name = ?`, name).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrBucketNotFound
	}
	if err != nil {
		return err
	}
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM objects WHERE bucket = ? LIMIT 1`, name).Scan(&one)
	if err == nil {
		return ErrBucketNotEmpty
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM buckets WHERE name = ?`, name); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLite) PutObject(ctx context.Context, o Object) error {
	meta, err := json.Marshal(o.UserMetadata)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO objects
		 (bucket, key, swarm_ref, size, etag, content_type, storage_class, user_meta, last_modified)
		 SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
		 WHERE EXISTS (SELECT 1 FROM buckets WHERE name = ?1)`,
		o.Bucket, o.Key, o.SwarmRef, o.Size, o.ETag, o.ContentType,
		o.StorageClass, string(meta), o.LastModified.UTC().Format(timeLayout))
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrBucketNotFound
	}
	return nil
}

const objectColumns = `bucket, key, swarm_ref, size, etag, content_type, storage_class, user_meta, last_modified`

func scanObject(row interface{ Scan(...any) error }) (*Object, error) {
	var o Object
	var meta, modified string
	if err := row.Scan(&o.Bucket, &o.Key, &o.SwarmRef, &o.Size, &o.ETag,
		&o.ContentType, &o.StorageClass, &meta, &modified); err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(meta), &o.UserMetadata) //nolint:errcheck // written by us
	o.LastModified, _ = time.Parse(timeLayout, modified)
	return &o, nil
}

func (s *SQLite) GetObject(ctx context.Context, bucket, key string) (*Object, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE bucket = ? AND key = ?`, bucket, key)
	o, err := scanObject(row)
	if err == nil {
		return o, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if _, berr := s.GetBucket(ctx, bucket); berr != nil {
		return nil, berr
	}
	return nil, ErrObjectNotFound
}

func (s *SQLite) DeleteObject(ctx context.Context, bucket, key string) error {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM objects WHERE bucket = ? AND key = ?`, bucket, key)
	return err
}

func (s *SQLite) ListObjects(ctx context.Context, bucket, prefix, after string, limit int) ([]Object, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	// Prefix filtering as a key range: [prefix, prefixEnd). SQLite compares
	// TEXT bytewise (memcmp), matching Go string order, so `key > after`
	// and the range bound compose correctly.
	end := prefixEnd(prefix)
	q := `SELECT ` + objectColumns + ` FROM objects
	      WHERE bucket = ? AND key > ? AND key >= ?`
	args := []any{bucket, after, prefix}
	if end != "" {
		q += ` AND key < ?`
		args = append(args, end)
	}
	q += ` ORDER BY key LIMIT ?`
	if limit < 0 {
		limit = -1 // sqlite: LIMIT -1 means unlimited
	}
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Object
	for rows.Next() {
		o, err := scanObject(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *o)
	}
	return out, rows.Err()
}

// prefixEnd returns the smallest string greater than every string with the
// given prefix, or "" when no upper bound exists (empty prefix, or all 0xFF).
func prefixEnd(prefix string) string {
	if prefix == "" {
		return ""
	}
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}

var _ Store = (*SQLite)(nil)
