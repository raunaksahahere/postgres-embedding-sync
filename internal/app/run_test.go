package app

import (
	"context"
	"errors"
	"testing"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/pipeline"
)

func TestPickRunErrorRecordsSchemaPauseOnlyFromCommitter(t *testing.T) {
	pe := &pipeline.PauseError{Reason: "schema_incompatible", InStream: true, Pipeline: "docs"}
	// The committer reached the paused unit: every earlier unit is durable.
	got := pickRunError([]error{tagErr("planner", pe), tagErr("committer", pe), tagErr("reader", context.Canceled)})
	if p, ok := pipeline.IsPause(got); !ok || !p.InStream {
		t.Fatalf("committer pause must keep InStream: %v", got)
	}
	// Only the planner saw it (committer cancelled by the shutdown timeout):
	// still a pause, but not recorded as the stream position.
	got = pickRunError([]error{tagErr("planner", pe), tagErr("committer", context.Canceled)})
	p, ok := pipeline.IsPause(got)
	if !ok || p.InStream || p.Reason != "schema_incompatible" {
		t.Fatalf("planner-only pause must drop InStream: %+v", got)
	}
	if !pe.InStream {
		t.Fatal("the shared pause value must not be modified")
	}
	// No pause: the first real error wins over cancellations.
	boom := errors.New("boom")
	got = pickRunError([]error{tagErr("reader", context.Canceled), tagErr("embed", boom)})
	if !errors.Is(got, boom) {
		t.Fatalf("got %v", got)
	}
}
