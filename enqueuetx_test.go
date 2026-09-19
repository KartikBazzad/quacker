package quacker

import (
	"context"
	"errors"
	"testing"
)

func TestEnqueueTxSQLiteUnsupported(t *testing.T) {
	q := newTestQ(t)
	task := NewTask("tx.sqlite", func(ctx context.Context, in string) (string, error) { return in, nil })
	// The tx is nil on purpose: SQLite is rejected before it is used.
	if _, err := EnqueueTx(context.Background(), nil, q, task, "x"); !errors.Is(err, ErrExternalTxUnsupported) {
		t.Fatalf("want ErrExternalTxUnsupported, got %v", err)
	}
	if _, err := EnqueueBatchTx(context.Background(), nil, q, task, []string{"x"}); !errors.Is(err, ErrExternalTxUnsupported) {
		t.Fatalf("batch: want ErrExternalTxUnsupported, got %v", err)
	}
}
