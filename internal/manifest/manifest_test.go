package manifest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/petfold/s3warm/internal/store"
)

// memLS is an in-memory mantaray LoadSaver that keeps every chunk it is given,
// so a test can inspect everything a commit publishes.
type memLS struct{ saved map[string][]byte }

func newMemLS() *memLS { return &memLS{saved: map[string][]byte{}} }

func (m *memLS) Save(_ context.Context, data []byte) ([]byte, error) {
	sum := sha256.Sum256(data)
	ref := sum[:32]
	m.saved[hex.EncodeToString(ref)] = append([]byte(nil), data...)
	return ref, nil
}

func (m *memLS) Load(_ context.Context, ref []byte) ([]byte, error) {
	data, ok := m.saved[hex.EncodeToString(ref)]
	if !ok {
		return nil, errors.New("not found")
	}
	return data, nil
}

// everythingPublished concatenates every chunk the commit wrote, which is
// exactly what a holder of the root can read off Swarm.
func (m *memLS) everythingPublished() []byte {
	var all bytes.Buffer
	for _, v := range m.saved {
		all.Write(v)
	}
	return all.Bytes()
}

const (
	// A 64-byte (128 hex) reference, the shape Bee returns for an encrypted
	// upload: address half plus embedded decryption key.
	sseRef = "b0a38309d027baebbaaed2527613e74919b291f35e2e06a1a32ba5acf025c419" +
		"1b879172164c82a23c1378403e599a67d371e0299abcc34bcf2ba2592415f6b8"
	sseRef2 = "cc11223344556677889900aabbccddeeff00112233445566778899aabbccddee" +
		"ff00112233445566778899aabbccddeeff00112233445566778899aabbccddee"
	plainRef = "859c2a441bc33697c274981461b8d173d2d312a62d97ac121da4183e07f93fd4"
)

func testIdentity(t *testing.T) (age.Identity, age.Recipient, string) {
	t.Helper()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	return id, id.Recipient(), id.Recipient().String()
}

func commitWith(objs ...store.Object) *Commit {
	return &Commit{Version: 1, Bucket: "b", Seq: 1, Timestamp: time.Unix(0, 0).UTC(), Objects: objs}
}

func sseObject() store.Object {
	return store.Object{Bucket: "b", Key: "secret.tar.gz", SwarmRef: sseRef,
		Size: 10, ETag: "e", Encrypted: true, VersionID: "null", IsLatest: true}
}

func sseMultipart() store.Object {
	return store.Object{Bucket: "b", Key: "big", Size: 20, ETag: "e-2", Encrypted: true,
		VersionID: "null", IsLatest: true,
		Parts: []store.Part{{PartNumber: 1, SwarmRef: sseRef2, Size: 20, ETag: "p"}}}
}

// A bucket holding SSE objects and no recovery recipient must not commit: the
// alternative is publishing decryption keys.
func TestBuildRefusesSSEWithoutRecipient(t *testing.T) {
	for name, o := range map[string]store.Object{
		"single-part": sseObject(),
		"multipart":   sseMultipart(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Build(context.Background(), newMemLS(), commitWith(o), nil); !errors.Is(err, ErrNoRecoveryRecipient) {
				t.Fatalf("want ErrNoRecoveryRecipient, got %v", err)
			}
		})
	}
}

// The regression test for the disclosed leak: no plaintext key-bearing
// reference may appear anywhere a root can reach. This is the in-process form
// of fetching every reference in a commit document from a public gateway.
func TestCommitPublishesNoPlaintextSSERef(t *testing.T) {
	_, rec, _ := testIdentity(t)
	ls := newMemLS()
	c := commitWith(sseObject(), sseMultipart(),
		store.Object{Bucket: "b", Key: "public.txt", SwarmRef: plainRef, Size: 3, ETag: "p", VersionID: "null", IsLatest: true})

	if _, err := Build(context.Background(), ls, c, rec); err != nil {
		t.Fatal(err)
	}
	published := ls.everythingPublished()
	for _, secret := range []string{sseRef, sseRef2} {
		if bytes.Contains(published, []byte(secret)) {
			t.Fatalf("key-bearing reference %s... published in the commit chain", secret[:16])
		}
		// Also reject the address half on its own: half a reference is still
		// more than a public chain should say about an encrypted object.
		if bytes.Contains(published, []byte(secret[:64])) {
			t.Fatalf("address half of %s... published in the commit chain", secret[:16])
		}
	}
	// The plaintext object is unaffected: it stays browsable.
	if !bytes.Contains(published, []byte(plainRef)) {
		t.Fatal("plaintext object reference should still be published")
	}
}

// Round trip: with the identity the bucket restores exactly; without it, the
// sealed rows are visibly unrestorable rather than silently empty.
func TestGetCommitRoundTrip(t *testing.T) {
	id, rec, _ := testIdentity(t)
	ls := newMemLS()
	c := commitWith(sseObject(), sseMultipart())
	root, err := Build(context.Background(), ls, c, rec)
	if err != nil {
		t.Fatal(err)
	}

	got, err := GetCommit(context.Background(), ls, root, id)
	if err != nil {
		t.Fatal(err)
	}
	if n := got.SealedCount(); n != 0 {
		t.Fatalf("with the identity, want 0 sealed, got %d", n)
	}
	if got.Objects[0].SwarmRef != sseRef {
		t.Fatalf("single-part reference not recovered: %q", got.Objects[0].SwarmRef)
	}
	if got.Objects[1].Parts[0].SwarmRef != sseRef2 {
		t.Fatalf("part reference not recovered: %q", got.Objects[1].Parts[0].SwarmRef)
	}

	blind, err := GetCommit(context.Background(), ls, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n := blind.SealedCount(); n != 2 {
		t.Fatalf("without the identity, want 2 sealed, got %d", n)
	}
	if blind.Objects[0].SwarmRef != "" {
		t.Fatal("reference readable without the identity")
	}
}

// The wrong identity must fail loudly, not yield garbage.
func TestGetCommitWrongIdentity(t *testing.T) {
	_, rec, _ := testIdentity(t)
	other, _, _ := testIdentity(t)
	ls := newMemLS()
	root, err := Build(context.Background(), ls, commitWith(sseObject()), rec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := GetCommit(context.Background(), ls, root, other); err == nil ||
		!strings.Contains(err.Error(), "decrypting reference") {
		t.Fatalf("want a decryption error, got %v", err)
	}
}

// A plaintext bucket must be unaffected by any of this, including needing no
// recipient at all.
func TestPlaintextBucketUnchanged(t *testing.T) {
	ls := newMemLS()
	c := commitWith(store.Object{Bucket: "b", Key: "a.txt", SwarmRef: plainRef,
		Size: 3, ETag: "p", VersionID: "null", IsLatest: true})
	root, err := Build(context.Background(), ls, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := GetCommit(context.Background(), ls, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Objects[0].SwarmRef != plainRef || got.SealedCount() != 0 {
		t.Fatalf("plaintext round trip broken: %+v", got.Objects[0])
	}
}
