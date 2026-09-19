package quacker

import "github.com/kartikbazzad/quacker/internal/engine"

// Codec (de)serializes user payloads: task inputs and outputs, dependency
// outputs, event payloads, and durable journal values. Schema structures
// (steps.depends_on and steps.labels) are always JSON, because the SQL gates
// read them, so a custom codec does not affect them.
//
// The same codec must be used across restarts for a given database, since
// journal values and dependency outputs are decoded by the engine that
// encoded them.
type Codec = engine.Codec

// JSONCodec is the default codec (encoding/json).
type JSONCodec = engine.JSONCodec

// WithCodec sets the payload codec (default JSONCodec). It applies engine-wide
// and is used for task input/output encoding and decoding, DepOutput, event
// payloads, and durable RunOnce/WaitFor values.
func WithCodec(c Codec) Option {
	return func(cfg *config) { cfg.codec = c }
}
