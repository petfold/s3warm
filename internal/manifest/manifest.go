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
	"errors"
	"fmt"
	"io"
	"time"

	"filippo.io/age"

	"github.com/ethersphere/bee/v2/pkg/manifest/mantaray"

	"github.com/petfold/s3warm/internal/bee"
	"github.com/petfold/s3warm/internal/recovery"
	"github.com/petfold/s3warm/internal/store"
)

// CommitPath is the reserved manifest fork holding the commit document.
const CommitPath = ".s3warm/commit"

// Commit is the chain document: parent link, sequence, timestamp and the
// full object index — the exact-restore source of truth. The manifest forks
// beside it are the browsable view.
//
// Objects are serialised through wireObject rather than store.Object. The
// commit document is published on Swarm in the clear, so what it carries is a
// deliberate choice and not whatever fields the index struct happens to have:
// an exported field added to store.Object must not silently join the public
// chain. SSE objects carry SealedRef instead of SwarmRef (see wireObject).
type Commit struct {
	Version   int            `json:"version"`
	Bucket    string         `json:"bucket"`
	Seq       int64          `json:"seq"`
	Parent    string         `json:"parent,omitempty"` // hex root of the previous commit
	Timestamp time.Time      `json:"timestamp"`
	Objects   []store.Object `json:"objects"`

	// sealed counts objects whose references were still sealed after load.
	sealed int
}

// ErrCommitVersion marks a commit document this gateway will not interpret.
// Refusing beats guessing: a reader that ignored SealedRef would build an
// index of empty references and call it a restore.
var ErrCommitVersion = errors.New("unsupported commit document version")

// CommitVersion is the commit-document format version. Version 2 added
// SealedRef: an object's reference may be sealed to the bucket's recovery
// recipient, so a reader that ignores it would silently produce an index of
// unreadable rows. Readers must refuse documents newer than they understand.
const CommitVersion = 2

// SSEDescriptor is the manifest fork entry for an object with sealed
// references: a 32-byte reference to this document, mirroring the composite/1
// indirection. Mantaray fixes a manifest's entry width from its first entry,
// so a 64-byte SSE reference cannot be a fork entry directly — and publishing
// it there would hand out the decryption key anyway.
//
// A composite object seals each part, so SealedParts carries them in order and
// SealedRef is empty; a single-part object is the reverse.
type SSEDescriptor struct {
	Kind        string       `json:"s3warm"`                // "sse/1"
	SealedRef   string       `json:"sealedRef,omitempty"`   // age-encrypted 64-byte reference
	SealedParts []SealedPart `json:"sealedParts,omitempty"` // composite objects
}

// SealedPart is one part of a composite object with its reference sealed.
type SealedPart struct {
	PartNumber int    `json:"partNumber"`
	SwarmRef   string `json:"swarmRef,omitempty"`  // plaintext parts of a mixed composite
	SealedRef  string `json:"sealedRef,omitempty"` // key-bearing parts
	Size       int64  `json:"size"`
}

// encRefHexLen is the hex length of an encrypted Swarm reference: 64 bytes,
// address plus embedded decryption key.
const encRefHexLen = 128

// keyBearing reports whether a reference carries its own decryption key, and
// therefore must never be published in the clear.
//
// This is decided per reference, not per object. An object's Encrypted flag
// describes how it was written; a reference can arrive from somewhere else —
// a whole-object UploadPartCopy reuses the source's reference, so a part of an
// unencrypted object can hold an encrypted one. Trusting the object flag there
// published exactly the capability this package exists to protect.
func keyBearing(ref string) bool {
	if len(ref) != encRefHexLen {
		return false
	}
	_, err := hex.DecodeString(ref)
	return err == nil
}

// HasKeyBearingRefs reports whether any reference in the object must be sealed
// before the object can enter the commit chain.
func HasKeyBearingRefs(o store.Object) bool {
	if keyBearing(o.SwarmRef) {
		return true
	}
	for _, p := range o.Parts {
		if keyBearing(p.SwarmRef) {
			return true
		}
	}
	return false
}

// wireObject is the commit document's on-Swarm shape for one object. It
// mirrors store.Object except that an encrypted object's reference is sealed:
// SwarmRef is empty and SealedRef carries the ciphertext. Restoring an index
// from a commit therefore needs the recovery identity for SSE objects, and
// nothing at all for plaintext ones.
type wireObject struct {
	Bucket            string            `json:"Bucket"`
	Key               string            `json:"Key"`
	SwarmRef          string            `json:"SwarmRef"`
	SealedRef         string            `json:"SealedRef,omitempty"`
	BatchID           string            `json:"BatchID"`
	Size              int64             `json:"Size"`
	ETag              string            `json:"ETag"`
	ContentType       string            `json:"ContentType"`
	ContentEncoding   string            `json:"ContentEncoding"`
	StorageClass      string            `json:"StorageClass"`
	UserMetadata      map[string]string `json:"UserMetadata"`
	LastModified      time.Time         `json:"LastModified"`
	ChecksumAlgorithm string            `json:"ChecksumAlgorithm"`
	Checksum          string            `json:"Checksum"`
	Encrypted         bool              `json:"Encrypted"`
	Tags              string            `json:"Tags"`
	ActAt             int64             `json:"ActAt"`
	ActHistory        string            `json:"ActHistory"`
	VersionID         string            `json:"VersionID"`
	VSeq              int64             `json:"VSeq"`
	IsLatest          bool              `json:"IsLatest"`
	DeleteMarker      bool              `json:"DeleteMarker"`
	Parts             []wirePart        `json:"Parts"`
}

// wirePart is one part of a composite object. An SSE upload's parts are
// individually key-bearing, so each is sealed the same way.
type wirePart struct {
	PartNumber   int       `json:"PartNumber"`
	SwarmRef     string    `json:"SwarmRef"`
	SealedRef    string    `json:"SealedRef,omitempty"`
	Size         int64     `json:"Size"`
	ETag         string    `json:"ETag"`
	LastModified time.Time `json:"LastModified"`
	ActAt        int64     `json:"ActAt,omitempty"`
	ActHistory   string    `json:"ActHistory,omitempty"`
}

// wireCommit is Commit as it is serialised.
type wireCommit struct {
	Version   int          `json:"version"`
	Bucket    string       `json:"bucket"`
	Seq       int64        `json:"seq"`
	Parent    string       `json:"parent,omitempty"`
	Timestamp time.Time    `json:"timestamp"`
	Objects   []wireObject `json:"objects"`
}

// seal converts an index row to its wire form, encrypting the reference of an
// encrypted object to the bucket's recovery recipient. A nil sealer with an
// encrypted object is a programming error: the committer refuses such buckets
// before reaching here.
func seal(o store.Object, rec age.Recipient) (wireObject, error) {
	w := wireObject{
		Bucket: o.Bucket, Key: o.Key, SwarmRef: o.SwarmRef, BatchID: o.BatchID,
		Size: o.Size, ETag: o.ETag, ContentType: o.ContentType,
		ContentEncoding: o.ContentEncoding, StorageClass: o.StorageClass,
		UserMetadata: o.UserMetadata, LastModified: o.LastModified,
		ChecksumAlgorithm: o.ChecksumAlgorithm, Checksum: o.Checksum,
		Encrypted: o.Encrypted, Tags: o.Tags, ActAt: o.ActAt,
		ActHistory: o.ActHistory, VersionID: o.VersionID, VSeq: o.VSeq,
		IsLatest: o.IsLatest, DeleteMarker: o.DeleteMarker,
	}
	for _, p := range o.Parts {
		wp := wirePart{
			PartNumber: p.PartNumber, SwarmRef: p.SwarmRef, Size: p.Size,
			ETag: p.ETag, LastModified: p.LastModified,
			ActAt: p.ActAt, ActHistory: p.ActHistory,
		}
		if keyBearing(p.SwarmRef) {
			sealed, err := recovery.Encrypt(rec, p.SwarmRef)
			if err != nil {
				return wireObject{}, err
			}
			wp.SwarmRef, wp.SealedRef = "", sealed
		}
		w.Parts = append(w.Parts, wp)
	}
	if keyBearing(o.SwarmRef) {
		sealed, err := recovery.Encrypt(rec, o.SwarmRef)
		if err != nil {
			return wireObject{}, err
		}
		w.SwarmRef, w.SealedRef = "", sealed
	}
	return w, nil
}

// open converts a wire row back to an index row, decrypting sealed references
// when an identity is supplied. Without one, a sealed row comes back with an
// empty SwarmRef and the caller must treat it as unrestorable.
func open(w wireObject, id age.Identity) (store.Object, bool, error) {
	sealed := false
	o := store.Object{
		Bucket: w.Bucket, Key: w.Key, SwarmRef: w.SwarmRef, BatchID: w.BatchID,
		Size: w.Size, ETag: w.ETag, ContentType: w.ContentType,
		ContentEncoding: w.ContentEncoding, StorageClass: w.StorageClass,
		UserMetadata: w.UserMetadata, LastModified: w.LastModified,
		ChecksumAlgorithm: w.ChecksumAlgorithm, Checksum: w.Checksum,
		Encrypted: w.Encrypted, Tags: w.Tags, ActAt: w.ActAt,
		ActHistory: w.ActHistory, VersionID: w.VersionID, VSeq: w.VSeq,
		IsLatest: w.IsLatest, DeleteMarker: w.DeleteMarker,
	}
	for _, wp := range w.Parts {
		p := store.Part{
			PartNumber: wp.PartNumber, SwarmRef: wp.SwarmRef, Size: wp.Size,
			ETag: wp.ETag, LastModified: wp.LastModified,
			ActAt: wp.ActAt, ActHistory: wp.ActHistory,
		}
		if wp.SealedRef != "" {
			if id == nil {
				sealed = true
			} else {
				ref, err := recovery.Decrypt(id, wp.SealedRef)
				if err != nil {
					return store.Object{}, false, fmt.Errorf("part %d of %q: %w", wp.PartNumber, w.Key, err)
				}
				p.SwarmRef = ref
			}
		}
		o.Parts = append(o.Parts, p)
	}
	if w.SealedRef != "" {
		if id == nil {
			sealed = true
		} else {
			ref, err := recovery.Decrypt(id, w.SealedRef)
			if err != nil {
				return store.Object{}, false, fmt.Errorf("object %q: %w", w.Key, err)
			}
			o.SwarmRef = ref
		}
	}
	return o, sealed, nil
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
func Build(ctx context.Context, ls mantaray.LoadSaver, c *Commit, rec age.Recipient) (string, error) {
	root := mantaray.New()
	root.SetObfuscationKey(mantaray.ZeroObfuscationKey)

	for _, o := range c.Objects {
		if HasKeyBearingRefs(o) && rec == nil {
			return "", ErrNoRecoveryRecipient
		}
		entry, meta, err := entryFor(ctx, ls, o, rec)
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

	wire := wireCommit{
		Version: c.Version, Bucket: c.Bucket, Seq: c.Seq,
		Parent: c.Parent, Timestamp: c.Timestamp,
		Objects: make([]wireObject, 0, len(c.Objects)),
	}
	for _, o := range c.Objects {
		w, err := seal(o, rec)
		if err != nil {
			return "", fmt.Errorf("sealing %q: %w", o.Key, err)
		}
		wire.Objects = append(wire.Objects, w)
	}
	doc, err := json.Marshal(wire)
	if err != nil {
		return "", err
	}
	docRef, err := ls.Save(ctx, doc)
	if err != nil {
		return "", fmt.Errorf("saving commit document: %w", err)
	}
	err = root.Add(ctx, []byte(CommitPath), docRef,
		map[string]string{"Content-Type": "application/json", "s3warm": "commit/2"}, ls)
	if err != nil {
		return "", err
	}

	if err := root.Save(ctx, ls); err != nil {
		return "", fmt.Errorf("saving manifest: %w", err)
	}
	return hex.EncodeToString(root.Reference()), nil
}

func entryFor(ctx context.Context, ls mantaray.LoadSaver, o store.Object, rec age.Recipient) ([]byte, map[string]string, error) {
	meta := map[string]string{}
	if o.ContentType != "" {
		meta["Content-Type"] = o.ContentType
	}
	// An SSE object's reference is 64 bytes and key-bearing, so it can be
	// neither a fork entry (mantaray entries are single-width, and the commit
	// document's own entry is 32) nor public. Seal it into a descriptor and
	// reference that instead. `bzz://{root}/{key}` consequently serves the
	// descriptor, not the object: an encrypted object is not browsable, which
	// is the point.
	if HasKeyBearingRefs(o) {
		d := SSEDescriptor{Kind: "sse/1"}
		if keyBearing(o.SwarmRef) {
			sealed, err := recovery.Encrypt(rec, o.SwarmRef)
			if err != nil {
				return nil, nil, err
			}
			d.SealedRef = sealed
		}
		for _, p := range o.Parts {
			sp := SealedPart{PartNumber: p.PartNumber, Size: p.Size}
			if keyBearing(p.SwarmRef) {
				sealed, err := recovery.Encrypt(rec, p.SwarmRef)
				if err != nil {
					return nil, nil, err
				}
				sp.SealedRef = sealed
			} else {
				// A mixed composite keeps its plaintext parts in the clear;
				// dropping them would leave the descriptor describing a
				// shorter object than exists.
				sp.SwarmRef = p.SwarmRef
			}
			d.SealedParts = append(d.SealedParts, sp)
		}
		desc, err := json.Marshal(d)
		if err != nil {
			return nil, nil, err
		}
		ref, err := ls.Save(ctx, desc)
		if err != nil {
			return nil, nil, err
		}
		meta["Content-Type"] = "application/json"
		meta["s3warm-sse"] = "1"
		return ref, meta, nil
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

// GetCommit loads the commit document from a manifest root, decrypting sealed
// references when an identity is supplied. Pass a nil identity to read a
// chain's structure without its encrypted references; SSE objects then come
// back with an empty SwarmRef.
func GetCommit(ctx context.Context, ls mantaray.LoadSaver, rootHex string, id age.Identity) (*Commit, error) {
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
	var w wireCommit
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("decoding commit document: %w", err)
	}
	c := Commit{
		Version: w.Version, Bucket: w.Bucket, Seq: w.Seq,
		Parent: w.Parent, Timestamp: w.Timestamp,
		Objects: make([]store.Object, 0, len(w.Objects)),
	}
	if w.Version < 1 || w.Version > CommitVersion {
		return nil, fmt.Errorf("%w: document is version %d, this gateway understands 1..%d",
			ErrCommitVersion, w.Version, CommitVersion)
	}
	for _, wo := range w.Objects {
		o, sealed, err := open(wo, id)
		if err != nil {
			return nil, err
		}
		if sealed {
			c.sealed++
		}
		c.Objects = append(c.Objects, o)
	}
	return &c, nil
}

// SealedCount reports how many of a commit's objects still carry sealed
// references after loading — i.e. how much of the bucket cannot be restored
// with the identity supplied (none, if it was the right one).
//
// This counts what the commit document actually said, rather than inferring
// it from an empty SwarmRef: a zero-byte object legitimately has no reference
// and nothing sealed, and inferring would make such a bucket permanently
// unrestorable.
func (c *Commit) SealedCount() int { return c.sealed }
