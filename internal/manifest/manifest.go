// Package manifest implements the commit chain (design §5): every bucket
// mutation batch produces a new mantaray manifest on Swarm whose forks make
// the bucket bzz-browsable, plus a reserved commit document linking to the
// parent root — a git-like chain in which one 32-byte reference captures the
// entire bucket at a point in time.
package manifest

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/ethersphere/bee/v2/pkg/manifest/mantaray"

	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/store"
)

// CommitPath is the reserved manifest fork holding the commit document.
const CommitPath = ".s3warm/commit"

// Commit is the chain document: parent link, sequence, timestamp and the
// full object index — the exact-restore source of truth. The manifest forks
// beside it are the browsable view.
type Commit struct {
	Version   int            `json:"version"`
	Bucket    string         `json:"bucket"`
	Seq       int64          `json:"seq"`
	Parent    string         `json:"parent,omitempty"` // hex root of the previous commit
	Timestamp time.Time      `json:"timestamp"`
	Objects   []store.Object `json:"objects"`
}

// LoadSaver persists mantaray nodes and commit documents through the Bee
// HTTP API: node bytes saved via /bytes are exactly what Bee's own manifest
// loader reads back, which is what keeps roots bzz-compatible.
type LoadSaver struct {
	bee      *bee.Client
	batch    string
	deferred bool
}

func NewLoadSaver(beeClient *bee.Client, batch string, deferred bool) *LoadSaver {
	return &LoadSaver{bee: beeClient, batch: batch, deferred: deferred}
}

func (l *LoadSaver) Save(ctx context.Context, data []byte) ([]byte, error) {
	ref, _, err := l.bee.UploadBytes(ctx, bytes.NewReader(data), bee.UploadOptions{
		BatchID:       l.batch,
		Deferred:      l.deferred,
		ContentLength: int64(len(data)),
	})
	if err != nil {
		return nil, err
	}
	return hex.DecodeString(ref)
}

func (l *LoadSaver) Load(ctx context.Context, ref []byte) ([]byte, error) {
	resp, err := l.bee.DownloadBytes(ctx, hex.EncodeToString(ref), bee.DownloadOptions{})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// Build writes the bucket manifest for a commit and returns its root: one
// fork per object (entry = the object's Swarm reference with Content-Type
// metadata, so `GET /bzz/{root}/{key}` on any Bee serves it), composite
// objects as JSON descriptors (design §7), and the commit document under
// CommitPath. Zero-byte objects live only in the commit document.
func Build(ctx context.Context, ls mantaray.LoadSaver, c *Commit) (string, error) {
	root := mantaray.New()
	root.SetObfuscationKey(mantaray.ZeroObfuscationKey)

	for _, o := range c.Objects {
		entry, meta, err := entryFor(ctx, ls, o)
		if err != nil {
			return "", fmt.Errorf("entry for %q: %w", o.Key, err)
		}
		if entry == nil {
			continue
		}
		if err := root.Add(ctx, []byte(o.Key), entry, meta, ls); err != nil {
			return "", fmt.Errorf("adding %q: %w", o.Key, err)
		}
	}

	doc, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	docRef, err := ls.Save(ctx, doc)
	if err != nil {
		return "", fmt.Errorf("saving commit document: %w", err)
	}
	err = root.Add(ctx, []byte(CommitPath), docRef,
		map[string]string{"Content-Type": "application/json", "s3warm": "commit/1"}, ls)
	if err != nil {
		return "", err
	}

	if err := root.Save(ctx, ls); err != nil {
		return "", fmt.Errorf("saving manifest: %w", err)
	}
	return hex.EncodeToString(root.Reference()), nil
}

func entryFor(ctx context.Context, ls mantaray.LoadSaver, o store.Object) ([]byte, map[string]string, error) {
	meta := map[string]string{}
	if o.ContentType != "" {
		meta["Content-Type"] = o.ContentType
	}
	switch {
	case len(o.Parts) > 0:
		// Composite descriptor chunk: direct bzz fetchers see the
		// descriptor; s3warm and the future consolidation job see through it.
		desc, err := json.Marshal(map[string]any{"s3warm": "composite/1", "parts": o.Parts})
		if err != nil {
			return nil, nil, err
		}
		ref, err := ls.Save(ctx, desc)
		if err != nil {
			return nil, nil, err
		}
		meta["Content-Type"] = "application/json"
		meta["s3warm-composite"] = "1"
		return ref, meta, nil
	case o.SwarmRef == "":
		return nil, nil, nil
	default:
		ref, err := hex.DecodeString(o.SwarmRef)
		if err != nil {
			return nil, nil, err
		}
		return ref, meta, nil
	}
}

// GetCommit loads the commit document from a manifest root.
func GetCommit(ctx context.Context, ls mantaray.LoadSaver, rootHex string) (*Commit, error) {
	ref, err := hex.DecodeString(rootHex)
	if err != nil {
		return nil, fmt.Errorf("malformed root %q: %w", rootHex, err)
	}
	node := mantaray.NewNodeRef(ref)
	entry, err := node.Lookup(ctx, []byte(CommitPath), ls)
	if err != nil {
		return nil, fmt.Errorf("looking up commit document: %w", err)
	}
	data, err := ls.Load(ctx, entry)
	if err != nil {
		return nil, err
	}
	var c Commit
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("decoding commit document: %w", err)
	}
	return &c, nil
}
