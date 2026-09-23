package quacker

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	json "github.com/goccy/go-json"

	"github.com/kartikbazzad/quacker/internal/engine"
)

// Codec (de)serializes user payloads: task inputs and outputs, dependency
// outputs, event payloads, and durable journal values. Schema structures
// (steps.depends_on and steps.labels) are always JSON, because the SQL gates
// read them, so a custom codec does not affect them.
//
// The same codec must be used across restarts for a given database, since
// journal values and dependency outputs are decoded by the engine that
// encoded them.
type Codec = engine.Codec

// JSONCodec is the default codec (goccy/go-json).
type JSONCodec = engine.JSONCodec

// WithCodec sets the payload codec (default JSONCodec). It applies engine-wide
// and is used for task input/output encoding and decoding, DepOutput, event
// payloads, and durable RunOnce/WaitFor values.
func WithCodec(c Codec) Option {
	return func(cfg *config) { cfg.codec = c }
}

// WithPayloadKey encrypts user payloads at rest with AES-256-GCM. key must be
// 32 bytes; Open rejects any other length, and rejects combining it with
// WithCodec.
//
// Every user payload goes through the codec — task input/output, DepOutput,
// event payloads, and durable RunOnce/WaitFor values — so all of it is
// encrypted. Schema structures the SQL gates read (steps.depends_on,
// steps.labels, runs.unique_key, sequence keys) and task logs stay plaintext,
// and introspection (Execution, Events) returns ciphertext. Use the same key
// across restarts.
func WithPayloadKey(key []byte) Option {
	return func(cfg *config) { cfg.payloadKey = append([]byte(nil), key...) }
}

// codecEnvelopeV1 prefixes every encrypted payload: a version byte, then a
// 12-byte nonce, then the AES-GCM seal of the JSON encoding.
const codecEnvelopeV1 = 1

// encryptedCodec is a Codec that AES-256-GCM-encrypts the JSON encoding of a
// value. A fresh random nonce is used per Marshal.
type encryptedCodec struct {
	aead cipher.AEAD
}

// NewEncryptedJSONCodec returns a Codec that encrypts payloads with AES-256-GCM
// under a 32-byte key; it errors on any other key length.
func NewEncryptedJSONCodec(key []byte) (Codec, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("quacker: payload key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("quacker: payload key: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("quacker: payload cipher: %w", err)
	}
	return encryptedCodec{aead: aead}, nil
}

func (encryptedCodec) Name() string { return "json+aesgcm" }

func (c encryptedCodec) Marshal(v any) ([]byte, error) {
	plain, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := make([]byte, 0, 1+len(nonce)+len(plain)+c.aead.Overhead())
	out = append(out, codecEnvelopeV1)
	out = append(out, nonce...)
	return c.aead.Seal(out, nonce, plain, nil), nil
}

func (c encryptedCodec) Unmarshal(data []byte, v any) error {
	if len(data) == 0 {
		// Match JSONCodec's behavior for an absent payload.
		return json.Unmarshal(data, v)
	}
	ns := c.aead.NonceSize()
	if len(data) < 1+ns {
		return fmt.Errorf("quacker: encrypted payload is too short (%d bytes)", len(data))
	}
	if data[0] != codecEnvelopeV1 {
		return fmt.Errorf("quacker: unknown payload envelope version %d", data[0])
	}
	nonce := data[1 : 1+ns]
	plain, err := c.aead.Open(nil, nonce, data[1+ns:], nil)
	if err != nil {
		return fmt.Errorf("quacker: decrypt payload (wrong key or corrupt data): %w", err)
	}
	return json.Unmarshal(plain, v)
}
