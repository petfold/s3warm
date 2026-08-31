// Package recovery encrypts the Swarm references of SSE objects so they can
// travel in the public commit chain without becoming read capabilities.
//
// An SSE object's 64-byte reference embeds its decryption key (design §12).
// The commit chain is public by construction — that is the point of it — so a
// reference written there in cleartext lets anyone holding a commit root read
// the object from any Bee node or public gateway. Encrypting the reference to
// a recipient the operator holds keeps the chain public and browsable while
// leaving the objects readable only by the keyholder.
//
// The gateway never needs the identity to serve a bucket: it reads references
// from its index. The identity is needed only to rebuild an index from a
// commit root, and is supplied per restore request rather than stored.
package recovery

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"

	"filippo.io/age"
)

// ErrDecrypt marks a sealed reference that could not be opened — nearly always
// the wrong identity. Callers distinguish it from upstream failures so an
// operator mistake is not reported as a node outage.
var ErrDecrypt = errors.New("cannot decrypt sealed reference")

// ParseRecipient validates an age recipient (the public half, "age1...").
func ParseRecipient(s string) (age.Recipient, error) {
	r, err := age.ParseX25519Recipient(s)
	if err != nil {
		return nil, fmt.Errorf("recovery recipient must be an age X25519 recipient (age1...): %w", err)
	}
	return r, nil
}

// ParseIdentity validates an age identity (the private half, "AGE-SECRET-KEY-1...").
func ParseIdentity(s string) (age.Identity, error) {
	i, err := age.ParseX25519Identity(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("recovery identity must be an age X25519 identity (AGE-SECRET-KEY-1...): %w", err)
	}
	return i, nil
}

// Encrypt seals a reference to the recipient, returning base64 (std, padded).
// The result is opaque to anyone without the identity.
func Encrypt(recipient age.Recipient, ref string) (string, error) {
	if ref == "" {
		return "", nil
	}
	var buf bytes.Buffer
	w, err := age.Encrypt(&buf, recipient)
	if err != nil {
		return "", err
	}
	if _, err := io.WriteString(w, ref); err != nil {
		return "", err
	}
	if err := w.Close(); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

// Decrypt opens a reference sealed by Encrypt.
func Decrypt(identity age.Identity, sealed string) (string, error) {
	if sealed == "" {
		return "", nil
	}
	raw, err := base64.StdEncoding.DecodeString(sealed)
	if err != nil {
		return "", fmt.Errorf("%w: not base64: %v", ErrDecrypt, err)
	}
	r, err := age.Decrypt(bytes.NewReader(raw), identity)
	if err != nil {
		return "", fmt.Errorf("%w (wrong identity?): %v", ErrDecrypt, err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	return string(out), nil
}
