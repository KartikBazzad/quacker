package quacker

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func testKey() []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = byte(i + 1)
	}
	return b
}

func TestPayloadKeyValidation(t *testing.T) {
	if _, err := Open(WithStorage(Memory()), WithPayloadKey([]byte("tooshort"))); err == nil {
		t.Fatal("short key accepted")
	}
	if _, err := Open(WithStorage(Memory()), WithPayloadKey(testKey()), WithCodec(taggedCodec{})); err == nil {
		t.Fatal("WithPayloadKey + WithCodec accepted")
	}
}

func TestEncryptedRoundTripAndAtRest(t *testing.T) {
	key := testKey()
	q := newTestQ(t, WithPayloadKey(key))
	ctx := context.Background()

	charge := NewTask("enc.charge", func(ctx context.Context, in greetIn) (greetOut, error) {
		return greetOut{Greeting: "hi " + in.Name}, nil
	})
	ship := NewTask("enc.ship", func(ctx context.Context, in greetIn) (greetOut, error) {
		rec, err := DepOutput[greetOut](ctx, "charge")
		if err != nil {
			return greetOut{}, err
		}
		return greetOut{Greeting: "ship " + rec.Greeting}, nil
	})
	wf := NewWorkflow[greetIn]("enc.flow", Step("charge", charge), Step("ship", ship, "charge"))
	h, err := EnqueueWorkflow[greetOut](ctx, q, wf, greetIn{Name: "ada"})
	if err != nil {
		t.Fatal(err)
	}
	out, err := h.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "ship hi ada" {
		t.Fatalf("output = %q", out.Greeting)
	}

	// The stored input/output must not contain the plaintext.
	var rawIn, rawOut []byte
	if err := q.st.Read().QueryRowContext(ctx,
		`SELECT input, output FROM runs WHERE id=?`, h.RunID()).Scan(&rawIn, &rawOut); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(rawIn, []byte("ada")) || bytes.Contains(rawOut, []byte("ada")) {
		t.Fatalf("plaintext present at rest: in=%q out=%q", rawIn, rawOut)
	}
	var dec greetIn
	if err := q.eng.Codec().Unmarshal(rawIn, &dec); err != nil || dec.Name != "ada" {
		t.Fatalf("decrypt input = %+v err=%v", dec, err)
	}
}

func TestEncryptedDurableAndEvents(t *testing.T) {
	q := newTestQ(t, WithPayloadKey(testKey()))
	ctx := context.Background()
	task := NewTask("enc.durable", func(ctx context.Context, in greetIn) (greetOut, error) {
		res, err := RunOnce(ctx, "once", func() (string, error) { return "res", nil })
		if err != nil {
			return greetOut{}, err
		}
		p, err := WaitFor[greetIn](ctx, "enc.go", 5*time.Second)
		if err != nil {
			return greetOut{}, err
		}
		return greetOut{Greeting: res + " " + p.Name}, nil
	})
	h, err := Enqueue(ctx, q, task, greetIn{})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		s, err := q.Execution(ctx, h.RunID())
		return err == nil && len(s.Steps) == 1 && s.Steps[0].WaitEvent == "enc.go"
	})
	if _, err := q.Emit(ctx, "enc.go", greetIn{Name: "e"}); err != nil {
		t.Fatal(err)
	}
	out, err := h.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out.Greeting != "res e" {
		t.Fatalf("output = %q, want %q", out.Greeting, "res e")
	}
	// The durable journal's stored payload is ciphertext too.
	var payload []byte
	if err := q.st.Read().QueryRowContext(ctx,
		`SELECT payload FROM step_journal WHERE step_id=? ORDER BY idx LIMIT 1`,
		h.RunID()+"/enc.durable").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte(`"e"`)) {
		t.Fatalf("event payload plaintext at rest: %q", payload)
	}
}

func TestEncryptedWrongKey(t *testing.T) {
	good, err := NewEncryptedJSONCodec(testKey())
	if err != nil {
		t.Fatal(err)
	}
	bad, err := NewEncryptedJSONCodec(bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	blob, err := good.Marshal(greetIn{Name: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(blob), "secret") {
		t.Fatal("ciphertext contains plaintext")
	}
	var v greetIn
	if err := good.Unmarshal(blob, &v); err != nil || v.Name != "secret" {
		t.Fatalf("round trip = %+v err=%v", v, err)
	}
	if err := bad.Unmarshal(blob, &v); err == nil {
		t.Fatal("wrong key decrypted")
	}
}
