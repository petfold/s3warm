package manifest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/store"
)

// ErrACTBucket marks a commit skipped because the bucket is ACT-protected:
// the commit chain is public by design and would leak the bucket's keys.
var ErrACTBucket = errors.New("commit chain is disabled for ACT-protected buckets")

// DefaultDebounce batches a burst of mutations into one commit (design §5:
// a commit costs only the changed path's node chain, but building it still
// isn't free — debounce keeps write amplification down).
const DefaultDebounce = 3 * time.Second

// Committer builds bucket commits asynchronously: mutations mark a bucket
// dirty, and after the debounce window a new manifest root is written and
// recorded as the bucket head. CommitNow forces one synchronously (used by
// snapshots).
type Committer struct {
	store    store.Store
	bee      *bee.Client
	batch    string // gateway default batch (bucket batch wins)
	deferred bool
	feed     *FeedPublisher // nil = no checkpoint publishing
	log      *slog.Logger
	debounce time.Duration

	mu     sync.Mutex
	dirty  map[string]time.Time
	commit sync.Mutex // serializes commit builds
}

func NewCommitter(st store.Store, beeClient *bee.Client, batch string, deferred bool, feed *FeedPublisher, logger *slog.Logger) *Committer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Committer{
		store:    st,
		bee:      beeClient,
		batch:    batch,
		deferred: deferred,
		feed:     feed,
		log:      logger,
		debounce: DefaultDebounce,
		dirty:    make(map[string]time.Time),
	}
}

// Notify marks a bucket dirty; a commit follows after the debounce window.
// Safe on a nil Committer (commits disabled).
func (c *Committer) Notify(bucket string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.dirty[bucket] = time.Now()
	c.mu.Unlock()
}

// Run drives the debounced commit loop until ctx is cancelled.
func (c *Committer) Run(ctx context.Context) {
	if c == nil {
		return
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.mu.Lock()
			var due []string
			for b, at := range c.dirty {
				if time.Since(at) >= c.debounce {
					due = append(due, b)
				}
			}
			c.mu.Unlock()
			for _, b := range due {
				if _, _, err := c.CommitNow(ctx, b); err != nil && !errors.Is(err, ErrACTBucket) {
					c.log.Warn("commit failed", "bucket", b, "err", err)
				}
			}
		}
	}
}

// CommitNow builds and records a commit for the bucket immediately,
// returning the new root and sequence.
func (c *Committer) CommitNow(ctx context.Context, bucket string) (string, int64, error) {
	if c == nil {
		return "", 0, fmt.Errorf("commits are disabled")
	}
	c.commit.Lock()
	defer c.commit.Unlock()
	c.mu.Lock()
	delete(c.dirty, bucket)
	c.mu.Unlock()

	b, err := c.store.GetBucket(ctx, bucket)
	if err != nil {
		return "", 0, err
	}
	if b.ACT {
		// The commit chain is a *public* on-Swarm representation — for an
		// ACT-protected bucket it would leak key names and structure, so it
		// stays off (design §8).
		return "", 0, ErrACTBucket
	}
	batch := b.BatchID
	if batch == "" {
		batch = c.batch
	}
	if batch == "" {
		return "", 0, fmt.Errorf("no postage batch available for commit")
	}

	var objects []store.Object
	after := ""
	for {
		page, err := c.store.ListObjects(ctx, bucket, "", after, 1000)
		if err != nil {
			return "", 0, err
		}
		objects = append(objects, page...)
		if len(page) < 1000 {
			break
		}
		after = page[len(page)-1].Key
	}

	commit := &Commit{
		Version:   1,
		Bucket:    bucket,
		Seq:       b.CommitSeq + 1,
		Parent:    b.HeadRoot,
		Timestamp: time.Now().UTC(),
		Objects:   objects,
	}
	ls := NewLoadSaver(c.bee, batch, c.deferred)
	root, err := Build(ctx, ls, commit)
	if err != nil {
		return "", 0, err
	}
	if err := c.store.SetBucketHead(ctx, bucket, root, commit.Seq); err != nil {
		return "", 0, err
	}
	c.log.Info("bucket commit", "bucket", bucket, "root", root, "seq", commit.Seq, "objects", len(objects))

	if c.feed != nil {
		// Checkpoint anchor (design §5): failure is availability-only — the
		// commit stands, the feed catches up on the next commit.
		if err := c.feed.Publish(ctx, bucket, root, commit.Seq, batch); err != nil {
			c.log.Warn("feed checkpoint failed", "bucket", bucket, "err", err)
		}
	}
	return root, commit.Seq, nil
}
