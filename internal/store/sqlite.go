package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	batch_id   TEXT NOT NULL DEFAULT '',
	sse        TEXT NOT NULL DEFAULT '',
	head_root  TEXT NOT NULL DEFAULT '',
	commit_seq INTEGER NOT NULL DEFAULT 0,
	cors       TEXT NOT NULL DEFAULT '',
	versioning TEXT NOT NULL DEFAULT '',
	tags       TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS snapshots (
	bucket     TEXT NOT NULL,
	label      TEXT NOT NULL,
	root       TEXT NOT NULL,
	seq        INTEGER NOT NULL,
	created_at TEXT NOT NULL,
	PRIMARY KEY (bucket, label)
);
CREATE TABLE IF NOT EXISTS objects (
	bucket        TEXT NOT NULL,
	key           TEXT NOT NULL,
	version_id    TEXT NOT NULL DEFAULT 'null',
	vseq          INTEGER NOT NULL DEFAULT 0,
	is_latest     INTEGER NOT NULL DEFAULT 1,
	delete_marker INTEGER NOT NULL DEFAULT 0,
	swarm_ref     TEXT NOT NULL,
	batch_id      TEXT NOT NULL DEFAULT '',
	size          INTEGER NOT NULL,
	etag          TEXT NOT NULL,
	content_type  TEXT NOT NULL DEFAULT '',
	storage_class TEXT NOT NULL DEFAULT '',
	user_meta     TEXT NOT NULL DEFAULT 'null',
	last_modified TEXT NOT NULL,
	parts         TEXT NOT NULL DEFAULT '',
	checksum_alg  TEXT NOT NULL DEFAULT '',
	checksum      TEXT NOT NULL DEFAULT '',
	content_enc   TEXT NOT NULL DEFAULT '',
	encrypted     INTEGER NOT NULL DEFAULT 0,
	tags          TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (bucket, key, version_id)
);
CREATE INDEX IF NOT EXISTS objects_latest ON objects (bucket, key) WHERE is_latest = 1;
CREATE TABLE IF NOT EXISTS multipart_uploads (
	upload_id     TEXT PRIMARY KEY,
	bucket        TEXT NOT NULL,
	key           TEXT NOT NULL,
	initiated     TEXT NOT NULL,
	content_type  TEXT NOT NULL DEFAULT '',
	storage_class TEXT NOT NULL DEFAULT '',
	user_meta     TEXT NOT NULL DEFAULT 'null',
	batch_id      TEXT NOT NULL DEFAULT '',
	encrypted     INTEGER NOT NULL DEFAULT 0,
	tags          TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS multipart_parts (
	upload_id     TEXT NOT NULL,
	part_number   INTEGER NOT NULL,
	swarm_ref     TEXT NOT NULL,
	size          INTEGER NOT NULL,
	etag          TEXT NOT NULL,
	last_modified TEXT NOT NULL,
	PRIMARY KEY (upload_id, part_number)
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
	// Column migrations for older databases; a duplicate-column error means
	// the schema is already current.
	for _, mig := range []string{
		`ALTER TABLE objects ADD COLUMN batch_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE objects ADD COLUMN parts TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE objects ADD COLUMN checksum_alg TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE objects ADD COLUMN checksum TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE objects ADD COLUMN content_enc TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE objects ADD COLUMN encrypted INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE buckets ADD COLUMN sse TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE multipart_uploads ADD COLUMN encrypted INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE buckets ADD COLUMN head_root TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE buckets ADD COLUMN commit_seq INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE buckets ADD COLUMN cors TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE buckets ADD COLUMN versioning TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE buckets ADD COLUMN tags TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE objects ADD COLUMN tags TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE multipart_uploads ADD COLUMN tags TEXT NOT NULL DEFAULT ''`,
	} {
		if _, err := db.Exec(mig); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			db.Close()
			return nil, fmt.Errorf("migrating schema: %w", err)
		}
	}
	if err := migrateObjectsToVersioned(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating objects to versioned schema: %w", err)
	}
	return &SQLite{db: db}, nil
}

// migrateObjectsToVersioned rebuilds a pre-versioning objects table (PK
// (bucket, key)) into the versioned shape (PK (bucket, key, version_id));
// existing rows become the "null" latest version. The primary key cannot be
// altered in place, hence the copy.
func migrateObjectsToVersioned(db *sql.DB) error {
	var n int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM pragma_table_info('objects') WHERE name = 'version_id'`).Scan(&n)
	if err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	stmts := []string{
		`ALTER TABLE objects RENAME TO objects_v1`,
		`CREATE TABLE objects (
			bucket TEXT NOT NULL, key TEXT NOT NULL,
			version_id TEXT NOT NULL DEFAULT 'null', vseq INTEGER NOT NULL DEFAULT 0,
			is_latest INTEGER NOT NULL DEFAULT 1, delete_marker INTEGER NOT NULL DEFAULT 0,
			swarm_ref TEXT NOT NULL, batch_id TEXT NOT NULL DEFAULT '',
			size INTEGER NOT NULL, etag TEXT NOT NULL,
			content_type TEXT NOT NULL DEFAULT '', storage_class TEXT NOT NULL DEFAULT '',
			user_meta TEXT NOT NULL DEFAULT 'null', last_modified TEXT NOT NULL,
			parts TEXT NOT NULL DEFAULT '', checksum_alg TEXT NOT NULL DEFAULT '',
			checksum TEXT NOT NULL DEFAULT '', content_enc TEXT NOT NULL DEFAULT '',
			encrypted INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (bucket, key, version_id))`,
		`INSERT INTO objects
			(bucket, key, swarm_ref, batch_id, size, etag, content_type, storage_class, user_meta, last_modified, parts, checksum_alg, checksum, content_enc, encrypted, tags)
		 SELECT bucket, key, swarm_ref, batch_id, size, etag, content_type, storage_class, user_meta, last_modified, parts, checksum_alg, checksum, content_enc, encrypted, tags
		 FROM objects_v1`,
		`DROP TABLE objects_v1`,
		`CREATE INDEX IF NOT EXISTS objects_latest ON objects (bucket, key) WHERE is_latest = 1`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLite) Close() error { return s.db.Close() }

const timeLayout = time.RFC3339Nano

func (s *SQLite) CreateBucket(ctx context.Context, b Bucket) error {
	if b.CreatedAt.IsZero() {
		b.CreatedAt = time.Now().UTC()
	}
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO buckets (name, created_at, batch_id, sse, head_root, commit_seq, cors, versioning, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT (name) DO NOTHING`,
		b.Name, b.CreatedAt.UTC().Format(timeLayout), b.BatchID, b.Encryption, b.HeadRoot, b.CommitSeq, b.CORS, b.Versioning, b.Tags)
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
		`SELECT name, created_at, batch_id, sse, head_root, commit_seq, cors, versioning, tags FROM buckets WHERE name = ?`, name)
	var b Bucket
	var created string
	if err := row.Scan(&b.Name, &created, &b.BatchID, &b.Encryption, &b.HeadRoot, &b.CommitSeq, &b.CORS, &b.Versioning, &b.Tags); err != nil {
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
		`SELECT name, created_at, batch_id, sse, head_root, commit_seq, cors, versioning, tags FROM buckets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Bucket
	for rows.Next() {
		var b Bucket
		var created string
		if err := rows.Scan(&b.Name, &created, &b.BatchID, &b.Encryption, &b.HeadRoot, &b.CommitSeq, &b.CORS, &b.Versioning, &b.Tags); err != nil {
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

// SetBucketEncryption sets the bucket-default SSE algorithm.
func (s *SQLite) SetBucketEncryption(ctx context.Context, bucket, algorithm string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE buckets SET sse = ? WHERE name = ?`, algorithm, bucket)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrBucketNotFound
	}
	return nil
}

// SetBucketVersioning sets the bucket versioning status.
func (s *SQLite) SetBucketVersioning(ctx context.Context, bucket, status string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE buckets SET versioning = ? WHERE name = ?`, status, bucket)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrBucketNotFound
	}
	return nil
}

// SetBucketCORS sets the bucket's CORS rules JSON.
func (s *SQLite) SetBucketCORS(ctx context.Context, bucket, corsJSON string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE buckets SET cors = ? WHERE name = ?`, corsJSON, bucket)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrBucketNotFound
	}
	return nil
}

// SetBucketTagging sets the bucket tag set JSON.
func (s *SQLite) SetBucketTagging(ctx context.Context, bucket, tagsJSON string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE buckets SET tags = ? WHERE name = ?`, tagsJSON, bucket)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrBucketNotFound
	}
	return nil
}

// SetObjectTags replaces one version's tag set in place.
func (s *SQLite) SetObjectTags(ctx context.Context, bucket, key, versionID, tagsJSON string) error {
	q := `UPDATE objects SET tags = ? WHERE bucket = ? AND key = ? AND is_latest = 1`
	args := []any{tagsJSON, bucket, key}
	if versionID != "" {
		q = `UPDATE objects SET tags = ? WHERE bucket = ? AND key = ? AND version_id = ?`
		args = append(args, versionID)
	}
	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		if _, berr := s.GetBucket(ctx, bucket); berr != nil {
			return berr
		}
		return ErrObjectNotFound
	}
	return nil
}

// SetBucketHead records the bucket's latest commit root and sequence.
func (s *SQLite) SetBucketHead(ctx context.Context, bucket, root string, seq int64) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE buckets SET head_root = ?, commit_seq = ? WHERE name = ?`, root, seq, bucket)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrBucketNotFound
	}
	return nil
}

func (s *SQLite) PutSnapshot(ctx context.Context, snap Snapshot) error {
	if _, err := s.GetBucket(ctx, snap.Bucket); err != nil {
		return err
	}
	if snap.CreatedAt.IsZero() {
		snap.CreatedAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO snapshots (bucket, label, root, seq, created_at) VALUES (?, ?, ?, ?, ?)`,
		snap.Bucket, snap.Label, snap.Root, snap.Seq, snap.CreatedAt.UTC().Format(timeLayout))
	return err
}

func (s *SQLite) GetSnapshot(ctx context.Context, bucket, label string) (*Snapshot, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT bucket, label, root, seq, created_at FROM snapshots WHERE bucket = ? AND label = ?`,
		bucket, label)
	var snap Snapshot
	var created string
	if err := row.Scan(&snap.Bucket, &snap.Label, &snap.Root, &snap.Seq, &created); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrSnapshotNotFound
		}
		return nil, err
	}
	snap.CreatedAt, _ = time.Parse(timeLayout, created)
	return &snap, nil
}

func (s *SQLite) ListSnapshots(ctx context.Context, bucket string) ([]Snapshot, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT bucket, label, root, seq, created_at FROM snapshots WHERE bucket = ? ORDER BY label`, bucket)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Snapshot
	for rows.Next() {
		var snap Snapshot
		var created string
		if err := rows.Scan(&snap.Bucket, &snap.Label, &snap.Root, &snap.Seq, &created); err != nil {
			return nil, err
		}
		snap.CreatedAt, _ = time.Parse(timeLayout, created)
		out = append(out, snap)
	}
	return out, rows.Err()
}

// RestoreBucket atomically replaces the bucket's object set and head.
func (s *SQLite) RestoreBucket(ctx context.Context, bucket string, objects []Object, root string, seq int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	res, err := tx.ExecContext(ctx,
		`UPDATE buckets SET head_root = ?, commit_seq = ? WHERE name = ?`, root, seq, bucket)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrBucketNotFound
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE bucket = ?`, bucket); err != nil {
		return err
	}
	for _, o := range objects {
		o.Bucket = bucket
		meta, err := json.Marshal(o.UserMetadata)
		if err != nil {
			return err
		}
		parts := ""
		if len(o.Parts) > 0 {
			b, err := json.Marshal(o.Parts)
			if err != nil {
				return err
			}
			parts = string(b)
		}
		if o.VersionID == "" {
			o.VersionID = "null"
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO objects
			 (bucket, key, version_id, vseq, is_latest, delete_marker, swarm_ref, batch_id, size, etag, content_type, storage_class, user_meta, last_modified, parts, checksum_alg, checksum, content_enc, encrypted, tags)
			 VALUES (?, ?, ?, ?, 1, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			o.Bucket, o.Key, o.VersionID, o.VSeq,
			o.SwarmRef, o.BatchID, o.Size, o.ETag, o.ContentType,
			o.StorageClass, string(meta), o.LastModified.UTC().Format(timeLayout), parts,
			o.ChecksumAlgorithm, o.Checksum, o.ContentEncoding, o.Encrypted, o.Tags); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SQLite) PutObject(ctx context.Context, o Object, cond *PutCondition) error {
	meta, err := json.Marshal(o.UserMetadata)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	var one int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM buckets WHERE name = ?`, o.Bucket).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrBucketNotFound
	}
	if err != nil {
		return err
	}
	if cond != nil {
		var etag string
		var marker bool
		err := tx.QueryRowContext(ctx,
			`SELECT etag, delete_marker FROM objects WHERE bucket = ? AND key = ? AND is_latest = 1`,
			o.Bucket, o.Key).Scan(&etag, &marker)
		exists := err == nil && !marker
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if !cond.Ok(exists, etag) {
			return ErrPreconditionFailed
		}
	}
	parts := ""
	if len(o.Parts) > 0 {
		b, err := json.Marshal(o.Parts)
		if err != nil {
			return err
		}
		parts = string(b)
	}
	if o.VersionID == "" {
		o.VersionID = "null"
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE objects SET is_latest = 0 WHERE bucket = ? AND key = ? AND is_latest = 1`,
		o.Bucket, o.Key); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT OR REPLACE INTO objects
		 (bucket, key, version_id, vseq, is_latest, delete_marker, swarm_ref, batch_id, size, etag, content_type, storage_class, user_meta, last_modified, parts, checksum_alg, checksum, content_enc, encrypted, tags)
		 VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		o.Bucket, o.Key, o.VersionID, o.VSeq, o.DeleteMarker,
		o.SwarmRef, o.BatchID, o.Size, o.ETag, o.ContentType,
		o.StorageClass, string(meta), o.LastModified.UTC().Format(timeLayout), parts,
		o.ChecksumAlgorithm, o.Checksum, o.ContentEncoding, o.Encrypted, o.Tags); err != nil {
		return err
	}
	// A same-version replace (suspended "null" overwrites) may have removed
	// the previous latest row entirely; promote the newest survivor.
	if _, err := tx.ExecContext(ctx,
		`UPDATE objects SET is_latest = 1 WHERE bucket = ?1 AND key = ?2 AND vseq =
		   (SELECT MAX(vseq) FROM objects WHERE bucket = ?1 AND key = ?2)`,
		o.Bucket, o.Key); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLite) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (*Object, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE bucket = ? AND key = ? AND version_id = ?`,
		bucket, key, versionID)
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

func (s *SQLite) DeleteVersion(ctx context.Context, bucket, key, versionID string) (*Object, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	row := tx.QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE bucket = ? AND key = ? AND version_id = ?`,
		bucket, key, versionID)
	o, err := scanObject(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if _, berr := s.GetBucket(ctx, bucket); berr != nil {
				return nil, berr
			}
			return nil, ErrObjectNotFound
		}
		return nil, err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM objects WHERE bucket = ? AND key = ? AND version_id = ?`,
		bucket, key, versionID); err != nil {
		return nil, err
	}
	if o.IsLatest {
		if _, err := tx.ExecContext(ctx,
			`UPDATE objects SET is_latest = 1 WHERE bucket = ?1 AND key = ?2 AND vseq =
			   (SELECT MAX(vseq) FROM objects WHERE bucket = ?1 AND key = ?2)`,
			bucket, key); err != nil {
			return nil, err
		}
	}
	return o, tx.Commit()
}

func (s *SQLite) ListVersions(ctx context.Context, bucket, prefix, keyMarker, versionMarker string, limit int) ([]Object, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	end := prefixEnd(prefix)
	q := `SELECT ` + objectColumns + ` FROM objects WHERE bucket = ? AND key >= ?`
	args := []any{bucket, prefix}
	if end != "" {
		q += ` AND key < ?`
		args = append(args, end)
	}
	if keyMarker != "" {
		markerVSeq := int64(-1)
		if versionMarker != "" {
			// Position strictly after that version of keyMarker.
			err := s.db.QueryRowContext(ctx,
				`SELECT vseq FROM objects WHERE bucket = ? AND key = ? AND version_id = ?`,
				bucket, keyMarker, versionMarker).Scan(&markerVSeq)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
		}
		if versionMarker != "" && markerVSeq >= 0 {
			q += ` AND (key > ? OR (key = ? AND vseq < ?))`
			args = append(args, keyMarker, keyMarker, markerVSeq)
		} else {
			q += ` AND key > ?`
			args = append(args, keyMarker)
		}
	}
	if limit < 0 {
		limit = -1
	}
	q += ` ORDER BY key ASC, vseq DESC LIMIT ?`
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

const objectColumns = `bucket, key, version_id, vseq, is_latest, delete_marker, swarm_ref, batch_id, size, etag, content_type, storage_class, user_meta, last_modified, parts, checksum_alg, checksum, content_enc, encrypted, tags`

func scanObject(row interface{ Scan(...any) error }) (*Object, error) {
	var o Object
	var meta, modified, parts string
	if err := row.Scan(&o.Bucket, &o.Key, &o.VersionID, &o.VSeq, &o.IsLatest, &o.DeleteMarker,
		&o.SwarmRef, &o.BatchID, &o.Size, &o.ETag,
		&o.ContentType, &o.StorageClass, &meta, &modified, &parts,
		&o.ChecksumAlgorithm, &o.Checksum, &o.ContentEncoding, &o.Encrypted, &o.Tags); err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(meta), &o.UserMetadata) //nolint:errcheck // written by us
	if parts != "" {
		json.Unmarshal([]byte(parts), &o.Parts) //nolint:errcheck // written by us
	}
	o.LastModified, _ = time.Parse(timeLayout, modified)
	return &o, nil
}

func (s *SQLite) GetObject(ctx context.Context, bucket, key string) (*Object, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE bucket = ? AND key = ? AND is_latest = 1`, bucket, key)
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
	      WHERE bucket = ? AND is_latest = 1 AND delete_marker = 0 AND key > ? AND key >= ?`
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

func (s *SQLite) CreateMultipartUpload(ctx context.Context, u MultipartUpload) error {
	if _, err := s.GetBucket(ctx, u.Bucket); err != nil {
		return err
	}
	if u.Initiated.IsZero() {
		u.Initiated = time.Now().UTC()
	}
	meta, err := json.Marshal(u.UserMetadata)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO multipart_uploads
		 (upload_id, bucket, key, initiated, content_type, storage_class, user_meta, batch_id, encrypted, tags)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		u.UploadID, u.Bucket, u.Key, u.Initiated.UTC().Format(timeLayout),
		u.ContentType, u.StorageClass, string(meta), u.BatchID, u.Encrypted, u.Tags)
	return err
}

func (s *SQLite) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*MultipartUpload, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT upload_id, bucket, key, initiated, content_type, storage_class, user_meta, batch_id, encrypted, tags
		 FROM multipart_uploads WHERE upload_id = ? AND bucket = ? AND key = ?`,
		uploadID, bucket, key)
	var u MultipartUpload
	var initiated, meta string
	if err := row.Scan(&u.UploadID, &u.Bucket, &u.Key, &initiated,
		&u.ContentType, &u.StorageClass, &meta, &u.BatchID, &u.Encrypted, &u.Tags); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrUploadNotFound
		}
		return nil, err
	}
	json.Unmarshal([]byte(meta), &u.UserMetadata) //nolint:errcheck // written by us
	u.Initiated, _ = time.Parse(timeLayout, initiated)
	return &u, nil
}

func (s *SQLite) PutPart(ctx context.Context, uploadID string, p Part) error {
	res, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO multipart_parts
		 (upload_id, part_number, swarm_ref, size, etag, last_modified)
		 SELECT ?, ?, ?, ?, ?, ?
		 WHERE EXISTS (SELECT 1 FROM multipart_uploads WHERE upload_id = ?1)`,
		uploadID, p.PartNumber, p.SwarmRef, p.Size, p.ETag,
		p.LastModified.UTC().Format(timeLayout))
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrUploadNotFound
	}
	return nil
}

func (s *SQLite) ListParts(ctx context.Context, uploadID string, afterPart, limit int) ([]Part, error) {
	var one int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM multipart_uploads WHERE upload_id = ?`, uploadID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrUploadNotFound
	}
	if err != nil {
		return nil, err
	}
	if limit < 0 {
		limit = -1
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT part_number, swarm_ref, size, etag, last_modified
		 FROM multipart_parts WHERE upload_id = ? AND part_number > ?
		 ORDER BY part_number LIMIT ?`, uploadID, afterPart, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Part
	for rows.Next() {
		var p Part
		var modified string
		if err := rows.Scan(&p.PartNumber, &p.SwarmRef, &p.Size, &p.ETag, &modified); err != nil {
			return nil, err
		}
		p.LastModified, _ = time.Parse(timeLayout, modified)
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *SQLite) ListMultipartUploads(ctx context.Context, bucket, prefix string) ([]MultipartUpload, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	end := prefixEnd(prefix)
	q := `SELECT upload_id, bucket, key, initiated, content_type, storage_class, user_meta, batch_id, encrypted, tags
	      FROM multipart_uploads WHERE bucket = ? AND key >= ?`
	args := []any{bucket, prefix}
	if end != "" {
		q += ` AND key < ?`
		args = append(args, end)
	}
	q += ` ORDER BY key, upload_id`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MultipartUpload
	for rows.Next() {
		var u MultipartUpload
		var initiated, meta string
		if err := rows.Scan(&u.UploadID, &u.Bucket, &u.Key, &initiated,
			&u.ContentType, &u.StorageClass, &meta, &u.BatchID, &u.Encrypted, &u.Tags); err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(meta), &u.UserMetadata) //nolint:errcheck // written by us
		u.Initiated, _ = time.Parse(timeLayout, initiated)
		out = append(out, u)
	}
	return out, rows.Err()
}

func (s *SQLite) DeleteMultipartUpload(ctx context.Context, uploadID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	res, err := tx.ExecContext(ctx, `DELETE FROM multipart_uploads WHERE upload_id = ?`, uploadID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrUploadNotFound
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM multipart_parts WHERE upload_id = ?`, uploadID); err != nil {
		return err
	}
	return tx.Commit()
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
