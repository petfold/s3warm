package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver
)

// Postgres is the shared metadata index for multi-gateway deployments
// (design §10): every s3warm instance points at the same database and gets
// the same read-after-write and list-after-write guarantees a single
// gateway has, because every mutation is one transaction here.
//
// Two things differ from the SQLite port on purpose:
//
//   - Key columns are COLLATE "C": S3 listings are ordered by byte value
//     (Go string order), and Postgres TEXT would otherwise sort by the
//     database's locale.
//   - Multi-writer safety: SQLite has a single writer, so read-modify-write
//     sequences inside a transaction were serialized for free. Here,
//     per-key transactions take an advisory lock
//     (pg_advisory_xact_lock over hash(bucket/key)) so concurrent PUTs,
//     conditional writes and version deletes of the same key serialize
//     across gateways.
type Postgres struct {
	db *sql.DB
}

const postgresSchema = `
CREATE TABLE IF NOT EXISTS buckets (
	name       TEXT COLLATE "C" PRIMARY KEY,
	created_at TEXT NOT NULL,
	batch_id   TEXT NOT NULL DEFAULT '',
	sse        TEXT NOT NULL DEFAULT '',
	head_root  TEXT NOT NULL DEFAULT '',
	commit_seq BIGINT NOT NULL DEFAULT 0,
	cors       TEXT NOT NULL DEFAULT '',
	versioning TEXT NOT NULL DEFAULT '',
	tags       TEXT NOT NULL DEFAULT '',
	owner      TEXT NOT NULL DEFAULT '',
	act        BOOLEAN NOT NULL DEFAULT FALSE,
	act_history  TEXT NOT NULL DEFAULT '',
	act_grantees TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS snapshots (
	bucket     TEXT COLLATE "C" NOT NULL,
	label      TEXT COLLATE "C" NOT NULL,
	root       TEXT NOT NULL,
	seq        BIGINT NOT NULL,
	created_at TEXT NOT NULL,
	PRIMARY KEY (bucket, label)
);
CREATE TABLE IF NOT EXISTS objects (
	bucket        TEXT COLLATE "C" NOT NULL,
	key           TEXT COLLATE "C" NOT NULL,
	version_id    TEXT COLLATE "C" NOT NULL DEFAULT 'null',
	vseq          BIGINT NOT NULL DEFAULT 0,
	is_latest     BOOLEAN NOT NULL DEFAULT TRUE,
	delete_marker BOOLEAN NOT NULL DEFAULT FALSE,
	swarm_ref     TEXT NOT NULL,
	batch_id      TEXT NOT NULL DEFAULT '',
	size          BIGINT NOT NULL,
	etag          TEXT NOT NULL,
	content_type  TEXT NOT NULL DEFAULT '',
	storage_class TEXT NOT NULL DEFAULT '',
	user_meta     TEXT NOT NULL DEFAULT 'null',
	last_modified TEXT NOT NULL,
	parts         TEXT NOT NULL DEFAULT '',
	checksum_alg  TEXT NOT NULL DEFAULT '',
	checksum      TEXT NOT NULL DEFAULT '',
	content_enc   TEXT NOT NULL DEFAULT '',
	encrypted     BOOLEAN NOT NULL DEFAULT FALSE,
	tags          TEXT NOT NULL DEFAULT '',
	act_at        BIGINT NOT NULL DEFAULT 0,
	act_history   TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (bucket, key, version_id)
);
CREATE INDEX IF NOT EXISTS objects_latest ON objects (bucket, key) WHERE is_latest;
CREATE TABLE IF NOT EXISTS multipart_uploads (
	upload_id     TEXT COLLATE "C" PRIMARY KEY,
	bucket        TEXT COLLATE "C" NOT NULL,
	key           TEXT COLLATE "C" NOT NULL,
	initiated     TEXT NOT NULL,
	content_type  TEXT NOT NULL DEFAULT '',
	storage_class TEXT NOT NULL DEFAULT '',
	user_meta     TEXT NOT NULL DEFAULT 'null',
	batch_id      TEXT NOT NULL DEFAULT '',
	encrypted     BOOLEAN NOT NULL DEFAULT FALSE,
	tags          TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS multipart_parts (
	upload_id     TEXT COLLATE "C" NOT NULL,
	part_number   INTEGER NOT NULL,
	swarm_ref     TEXT NOT NULL,
	size          BIGINT NOT NULL,
	etag          TEXT NOT NULL,
	last_modified TEXT NOT NULL,
	act_at        BIGINT NOT NULL DEFAULT 0,
	act_history   TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (upload_id, part_number)
);
`

// OpenPostgres connects to the shared index database. dsn is a
// postgres:// / postgresql:// URL (or key=value DSN) as understood by pgx.
func OpenPostgres(dsn string) (*Postgres, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	// Serialize schema creation across gateways: CREATE TABLE IF NOT EXISTS
	// is not concurrency-safe in Postgres (two simultaneous cold starts race
	// the catalog and one dies with a duplicate-key error), and multiple
	// gateways starting together is exactly the HA deployment shape.
	if err := func() error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback() //nolint:errcheck // no-op after commit
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('s3warm/schema', 0))`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, postgresSchema); err != nil {
			return err
		}
		return tx.Commit()
	}(); err != nil {
		db.Close()
		return nil, fmt.Errorf("initializing schema: %w", err)
	}
	// Column migrations for databases created by older versions land here,
	// in the SQLite ALTER-loop pattern (none needed yet — the schema above
	// is the first Postgres shape).
	return &Postgres{db: db}, nil
}

func (s *Postgres) Close() error { return s.db.Close() }

// lockKey serializes writers of one object key across all gateways for the
// duration of the surrounding transaction.
func lockKey(ctx context.Context, tx *sql.Tx, bucket, key string) error {
	_, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, bucket+"/"+key)
	return err
}

func (s *Postgres) CreateBucket(ctx context.Context, b Bucket) error {
	if b.CreatedAt.IsZero() {
		b.CreatedAt = time.Now().UTC()
	}
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO buckets (name, created_at, batch_id, sse, head_root, commit_seq, cors, versioning, tags, owner, act, act_history, act_grantees)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		 ON CONFLICT (name) DO NOTHING`,
		b.Name, b.CreatedAt.UTC().Format(timeLayout), b.BatchID, b.Encryption, b.HeadRoot, b.CommitSeq, b.CORS, b.Versioning, b.Tags,
		b.Owner, b.ACT, b.ActHistory, b.ActGrantees)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrBucketExists
	}
	return nil
}

const bucketColumns = `name, created_at, batch_id, sse, head_root, commit_seq, cors, versioning, tags, owner, act, act_history, act_grantees`

func scanBucket(row interface{ Scan(...any) error }) (*Bucket, error) {
	var b Bucket
	var created string
	if err := row.Scan(&b.Name, &created, &b.BatchID, &b.Encryption, &b.HeadRoot, &b.CommitSeq, &b.CORS, &b.Versioning, &b.Tags,
		&b.Owner, &b.ACT, &b.ActHistory, &b.ActGrantees); err != nil {
		return nil, err
	}
	b.CreatedAt, _ = time.Parse(timeLayout, created)
	return &b, nil
}

func (s *Postgres) GetBucket(ctx context.Context, name string) (*Bucket, error) {
	b, err := scanBucket(s.db.QueryRowContext(ctx,
		`SELECT `+bucketColumns+` FROM buckets WHERE name = $1`, name))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrBucketNotFound
	}
	return b, err
}

func (s *Postgres) ListBuckets(ctx context.Context) ([]Bucket, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+bucketColumns+` FROM buckets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Bucket
	for rows.Next() {
		b, err := scanBucket(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *b)
	}
	return out, rows.Err()
}

func (s *Postgres) DeleteBucket(ctx context.Context, name string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	var one int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM buckets WHERE name = $1 FOR UPDATE`, name).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrBucketNotFound
	}
	if err != nil {
		return err
	}
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM objects WHERE bucket = $1 LIMIT 1`, name).Scan(&one)
	if err == nil {
		return ErrBucketNotEmpty
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM buckets WHERE name = $1`, name); err != nil {
		return err
	}
	return tx.Commit()
}

// setBucketColumn runs a single-column bucket UPDATE with not-found mapping.
func (s *Postgres) setBucketColumn(ctx context.Context, q string, args ...any) error {
	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrBucketNotFound
	}
	return nil
}

func (s *Postgres) SetBucketEncryption(ctx context.Context, bucket, algorithm string) error {
	return s.setBucketColumn(ctx, `UPDATE buckets SET sse = $1 WHERE name = $2`, algorithm, bucket)
}

func (s *Postgres) SetBucketVersioning(ctx context.Context, bucket, status string) error {
	return s.setBucketColumn(ctx, `UPDATE buckets SET versioning = $1 WHERE name = $2`, status, bucket)
}

func (s *Postgres) SetBucketCORS(ctx context.Context, bucket, corsJSON string) error {
	return s.setBucketColumn(ctx, `UPDATE buckets SET cors = $1 WHERE name = $2`, corsJSON, bucket)
}

func (s *Postgres) SetBucketTagging(ctx context.Context, bucket, tagsJSON string) error {
	return s.setBucketColumn(ctx, `UPDATE buckets SET tags = $1 WHERE name = $2`, tagsJSON, bucket)
}

func (s *Postgres) SetBucketACT(ctx context.Context, bucket, history, grantees string) error {
	return s.setBucketColumn(ctx,
		`UPDATE buckets SET act = TRUE, act_history = $1, act_grantees = $2 WHERE name = $3`,
		history, grantees, bucket)
}

func (s *Postgres) SetBucketHead(ctx context.Context, bucket, root string, seq int64) error {
	return s.setBucketColumn(ctx,
		`UPDATE buckets SET head_root = $1, commit_seq = $2 WHERE name = $3`, root, seq, bucket)
}

func (s *Postgres) SetObjectTags(ctx context.Context, bucket, key, versionID, tagsJSON string) error {
	q := `UPDATE objects SET tags = $1 WHERE bucket = $2 AND key = $3 AND is_latest`
	args := []any{tagsJSON, bucket, key}
	if versionID != "" {
		q = `UPDATE objects SET tags = $1 WHERE bucket = $2 AND key = $3 AND version_id = $4`
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

func (s *Postgres) PutSnapshot(ctx context.Context, snap Snapshot) error {
	if _, err := s.GetBucket(ctx, snap.Bucket); err != nil {
		return err
	}
	if snap.CreatedAt.IsZero() {
		snap.CreatedAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO snapshots (bucket, label, root, seq, created_at) VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (bucket, label) DO UPDATE SET root = EXCLUDED.root, seq = EXCLUDED.seq, created_at = EXCLUDED.created_at`,
		snap.Bucket, snap.Label, snap.Root, snap.Seq, snap.CreatedAt.UTC().Format(timeLayout))
	return err
}

func (s *Postgres) GetSnapshot(ctx context.Context, bucket, label string) (*Snapshot, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx,
		`SELECT bucket, label, root, seq, created_at FROM snapshots WHERE bucket = $1 AND label = $2`,
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

func (s *Postgres) ListSnapshots(ctx context.Context, bucket string) ([]Snapshot, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT bucket, label, root, seq, created_at FROM snapshots WHERE bucket = $1 ORDER BY label`, bucket)
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

func (s *Postgres) RestoreBucket(ctx context.Context, bucket string, objects []Object, root string, seq int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	res, err := tx.ExecContext(ctx,
		`UPDATE buckets SET head_root = $1, commit_seq = $2 WHERE name = $3`, root, seq, bucket)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrBucketNotFound
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE bucket = $1`, bucket); err != nil {
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
			 (bucket, key, version_id, vseq, is_latest, delete_marker, swarm_ref, batch_id, size, etag, content_type, storage_class, user_meta, last_modified, parts, checksum_alg, checksum, content_enc, encrypted, tags, act_at, act_history)
			 VALUES ($1, $2, $3, $4, TRUE, FALSE, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)`,
			o.Bucket, o.Key, o.VersionID, o.VSeq,
			o.SwarmRef, o.BatchID, o.Size, o.ETag, o.ContentType,
			o.StorageClass, string(meta), o.LastModified.UTC().Format(timeLayout), parts,
			o.ChecksumAlgorithm, o.Checksum, o.ContentEncoding, o.Encrypted, o.Tags, o.ActAt, o.ActHistory); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Postgres) PutObject(ctx context.Context, o Object, cond *PutCondition) error {
	meta, err := json.Marshal(o.UserMetadata)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	// Serialize writers of this key across gateways: the condition check,
	// latest demotion and insert below must be one atomic step (design §10).
	if err := lockKey(ctx, tx, o.Bucket, o.Key); err != nil {
		return err
	}
	var one int
	err = tx.QueryRowContext(ctx, `SELECT 1 FROM buckets WHERE name = $1`, o.Bucket).Scan(&one)
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
			`SELECT etag, delete_marker FROM objects WHERE bucket = $1 AND key = $2 AND is_latest`,
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
		`UPDATE objects SET is_latest = FALSE WHERE bucket = $1 AND key = $2 AND is_latest`,
		o.Bucket, o.Key); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO objects
		 (bucket, key, version_id, vseq, is_latest, delete_marker, swarm_ref, batch_id, size, etag, content_type, storage_class, user_meta, last_modified, parts, checksum_alg, checksum, content_enc, encrypted, tags, act_at, act_history)
		 VALUES ($1, $2, $3, $4, TRUE, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
		 ON CONFLICT (bucket, key, version_id) DO UPDATE SET
		   vseq = EXCLUDED.vseq, is_latest = TRUE, delete_marker = EXCLUDED.delete_marker,
		   swarm_ref = EXCLUDED.swarm_ref, batch_id = EXCLUDED.batch_id, size = EXCLUDED.size,
		   etag = EXCLUDED.etag, content_type = EXCLUDED.content_type, storage_class = EXCLUDED.storage_class,
		   user_meta = EXCLUDED.user_meta, last_modified = EXCLUDED.last_modified, parts = EXCLUDED.parts,
		   checksum_alg = EXCLUDED.checksum_alg, checksum = EXCLUDED.checksum, content_enc = EXCLUDED.content_enc,
		   encrypted = EXCLUDED.encrypted, tags = EXCLUDED.tags, act_at = EXCLUDED.act_at, act_history = EXCLUDED.act_history`,
		o.Bucket, o.Key, o.VersionID, o.VSeq, o.DeleteMarker,
		o.SwarmRef, o.BatchID, o.Size, o.ETag, o.ContentType,
		o.StorageClass, string(meta), o.LastModified.UTC().Format(timeLayout), parts,
		o.ChecksumAlgorithm, o.Checksum, o.ContentEncoding, o.Encrypted, o.Tags, o.ActAt, o.ActHistory); err != nil {
		return err
	}
	// A same-version replace (suspended "null" overwrites) may have replaced
	// the previous latest row entirely; promote the newest survivor.
	if _, err := tx.ExecContext(ctx,
		`UPDATE objects SET is_latest = TRUE WHERE bucket = $1 AND key = $2 AND vseq =
		   (SELECT MAX(vseq) FROM objects WHERE bucket = $1 AND key = $2)`,
		o.Bucket, o.Key); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Postgres) GetObject(ctx context.Context, bucket, key string) (*Object, error) {
	o, err := scanObject(s.db.QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE bucket = $1 AND key = $2 AND is_latest`, bucket, key))
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

func (s *Postgres) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (*Object, error) {
	o, err := scanObject(s.db.QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE bucket = $1 AND key = $2 AND version_id = $3`,
		bucket, key, versionID))
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

func (s *Postgres) DeleteObject(ctx context.Context, bucket, key string) error {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM objects WHERE bucket = $1 AND key = $2`, bucket, key)
	return err
}

func (s *Postgres) DeleteVersion(ctx context.Context, bucket, key, versionID string) (*Object, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit

	if err := lockKey(ctx, tx, bucket, key); err != nil {
		return nil, err
	}
	o, err := scanObject(tx.QueryRowContext(ctx,
		`SELECT `+objectColumns+` FROM objects WHERE bucket = $1 AND key = $2 AND version_id = $3`,
		bucket, key, versionID))
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
		`DELETE FROM objects WHERE bucket = $1 AND key = $2 AND version_id = $3`,
		bucket, key, versionID); err != nil {
		return nil, err
	}
	if o.IsLatest {
		if _, err := tx.ExecContext(ctx,
			`UPDATE objects SET is_latest = TRUE WHERE bucket = $1 AND key = $2 AND vseq =
			   (SELECT MAX(vseq) FROM objects WHERE bucket = $1 AND key = $2)`,
			bucket, key); err != nil {
			return nil, err
		}
	}
	return o, tx.Commit()
}

func (s *Postgres) ListObjects(ctx context.Context, bucket, prefix, after string, limit int) ([]Object, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	// Prefix filtering as a key range: [prefix, prefixEnd). Key columns are
	// COLLATE "C", so comparisons and ordering are bytewise like Go strings.
	end := prefixEnd(prefix)
	q := `SELECT ` + objectColumns + ` FROM objects
	      WHERE bucket = $1 AND is_latest AND NOT delete_marker AND key > $2 AND key >= $3`
	args := []any{bucket, after, prefix}
	if end != "" {
		q += fmt.Sprintf(` AND key < $%d`, len(args)+1)
		args = append(args, end)
	}
	q += ` ORDER BY key`
	if limit >= 0 {
		q += fmt.Sprintf(` LIMIT $%d`, len(args)+1)
		args = append(args, limit)
	}
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

func (s *Postgres) ListVersions(ctx context.Context, bucket, prefix, keyMarker, versionMarker string, limit int) ([]Object, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	end := prefixEnd(prefix)
	q := `SELECT ` + objectColumns + ` FROM objects WHERE bucket = $1 AND key >= $2`
	args := []any{bucket, prefix}
	if end != "" {
		q += fmt.Sprintf(` AND key < $%d`, len(args)+1)
		args = append(args, end)
	}
	if keyMarker != "" {
		markerVSeq := int64(-1)
		if versionMarker != "" {
			// Position strictly after that version of keyMarker.
			err := s.db.QueryRowContext(ctx,
				`SELECT vseq FROM objects WHERE bucket = $1 AND key = $2 AND version_id = $3`,
				bucket, keyMarker, versionMarker).Scan(&markerVSeq)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
		}
		if versionMarker != "" && markerVSeq >= 0 {
			q += fmt.Sprintf(` AND (key > $%d OR (key = $%d AND vseq < $%d))`, len(args)+1, len(args)+1, len(args)+2)
			args = append(args, keyMarker, markerVSeq)
		} else {
			q += fmt.Sprintf(` AND key > $%d`, len(args)+1)
			args = append(args, keyMarker)
		}
	}
	q += ` ORDER BY key ASC, vseq DESC`
	if limit >= 0 {
		q += fmt.Sprintf(` LIMIT $%d`, len(args)+1)
		args = append(args, limit)
	}
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

func (s *Postgres) CreateMultipartUpload(ctx context.Context, u MultipartUpload) error {
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
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		u.UploadID, u.Bucket, u.Key, u.Initiated.UTC().Format(timeLayout),
		u.ContentType, u.StorageClass, string(meta), u.BatchID, u.Encrypted, u.Tags)
	return err
}

func (s *Postgres) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*MultipartUpload, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT upload_id, bucket, key, initiated, content_type, storage_class, user_meta, batch_id, encrypted, tags
		 FROM multipart_uploads WHERE upload_id = $1 AND bucket = $2 AND key = $3`,
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

func (s *Postgres) PutPart(ctx context.Context, uploadID string, p Part) error {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO multipart_parts
		 (upload_id, part_number, swarm_ref, size, etag, last_modified, act_at, act_history)
		 SELECT $1, $2, $3, $4, $5, $6, $7, $8
		 WHERE EXISTS (SELECT 1 FROM multipart_uploads WHERE upload_id = $1)
		 ON CONFLICT (upload_id, part_number) DO UPDATE SET
		   swarm_ref = EXCLUDED.swarm_ref, size = EXCLUDED.size, etag = EXCLUDED.etag,
		   last_modified = EXCLUDED.last_modified, act_at = EXCLUDED.act_at, act_history = EXCLUDED.act_history`,
		uploadID, p.PartNumber, p.SwarmRef, p.Size, p.ETag,
		p.LastModified.UTC().Format(timeLayout), p.ActAt, p.ActHistory)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrUploadNotFound
	}
	return nil
}

func (s *Postgres) ListParts(ctx context.Context, uploadID string, afterPart, limit int) ([]Part, error) {
	var one int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM multipart_uploads WHERE upload_id = $1`, uploadID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrUploadNotFound
	}
	if err != nil {
		return nil, err
	}
	q := `SELECT part_number, swarm_ref, size, etag, last_modified, act_at, act_history
	      FROM multipart_parts WHERE upload_id = $1 AND part_number > $2
	      ORDER BY part_number`
	args := []any{uploadID, afterPart}
	if limit >= 0 {
		q += ` LIMIT $3`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Part
	for rows.Next() {
		var p Part
		var modified string
		if err := rows.Scan(&p.PartNumber, &p.SwarmRef, &p.Size, &p.ETag, &modified, &p.ActAt, &p.ActHistory); err != nil {
			return nil, err
		}
		p.LastModified, _ = time.Parse(timeLayout, modified)
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Postgres) ListMultipartUploads(ctx context.Context, bucket, prefix string) ([]MultipartUpload, error) {
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}
	end := prefixEnd(prefix)
	q := `SELECT upload_id, bucket, key, initiated, content_type, storage_class, user_meta, batch_id, encrypted, tags
	      FROM multipart_uploads WHERE bucket = $1 AND key >= $2`
	args := []any{bucket, prefix}
	if end != "" {
		q += ` AND key < $3`
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

func (s *Postgres) DeleteMultipartUpload(ctx context.Context, uploadID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op after commit
	res, err := tx.ExecContext(ctx, `DELETE FROM multipart_uploads WHERE upload_id = $1`, uploadID)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		return ErrUploadNotFound
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM multipart_parts WHERE upload_id = $1`, uploadID); err != nil {
		return err
	}
	return tx.Commit()
}

var _ Store = (*Postgres)(nil)
