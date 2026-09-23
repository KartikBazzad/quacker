package engine

import (
	"context"
	json "github.com/goccy/go-json"
)

// Codec (de)serializes user payloads: task inputs and outputs, dependency
// outputs, event payloads, and durable journal values. Schema structures —
// steps.depends_on and steps.labels — are always JSON regardless of the codec,
// because the SQL gates read them.
type Codec interface {
	Name() string
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONCodec is the default codec; it wraps github.com/goccy/go-json.
type JSONCodec struct{}

func (JSONCodec) Name() string                       { return "json" }
func (JSONCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (JSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// CodecFromContext returns the codec of the engine executing the task, or the
// default JSON codec outside a task (used by task bodies and DepOutput).
func CodecFromContext(ctx context.Context) Codec {
	if ss, ok := ctx.Value(ctxKey{}).(*stepState); ok && ss != nil && ss.eng != nil {
		return ss.eng.codec
	}
	return JSONCodec{}
}

// codec returns the codec for this step's engine.
func (ss *stepState) codec() Codec {
	if ss.eng != nil {
		return ss.eng.codec
	}
	return JSONCodec{}
}

// Codec returns the engine's payload codec.
func (e *Engine) Codec() Codec { return e.codec }
