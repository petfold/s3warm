package recovery

import (
	"strings"
	"testing"

	"filippo.io/age"
)

func newPair(t *testing.T) (age.Identity, age.Recipient) {
	t.Helper()
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	return id, id.Recipient()
}

const ref = "b0a38309d027baebbaaed2527613e74919b291f35e2e06a1a32ba5acf025c419" +
	"1b879172164c82a23c1378403e599a67d371e0299abcc34bcf2ba2592415f6b8"

func TestRoundTrip(t *testing.T) {
	id, rec := newPair(t)
	sealed, err := Encrypt(rec, ref)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sealed, ref) || strings.Contains(sealed, ref[:64]) {
		t.Fatal("ciphertext contains the plaintext reference")
	}
	got, err := Decrypt(id, sealed)
	if err != nil {
		t.Fatal(err)
	}
	if got != ref {
		t.Fatalf("round trip = %q", got)
	}
}

func TestWrongIdentityFails(t *testing.T) {
	_, rec := newPair(t)
	other, _ := newPair(t)
	sealed, err := Encrypt(rec, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decrypt(other, sealed); err == nil {
		t.Fatal("wrong identity decrypted the reference")
	}
}

// Two seals of the same reference must differ: age is randomised, so a chain
// cannot be probed by comparing ciphertexts across commits.
func TestSealsAreNotDeterministic(t *testing.T) {
	_, rec := newPair(t)
	a, err := Encrypt(rec, ref)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Encrypt(rec, ref)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("identical ciphertexts leak that two objects share a reference")
	}
}

func TestEmptyRefStaysEmpty(t *testing.T) {
	_, rec := newPair(t)
	if s, err := Encrypt(rec, ""); err != nil || s != "" {
		t.Fatalf("empty reference: %q %v", s, err)
	}
}

func TestRejectsBadKeys(t *testing.T) {
	if _, err := ParseRecipient("not-a-key"); err == nil {
		t.Fatal("accepted a bad recipient")
	}
	if _, err := ParseIdentity("not-a-key"); err == nil {
		t.Fatal("accepted a bad identity")
	}
}
