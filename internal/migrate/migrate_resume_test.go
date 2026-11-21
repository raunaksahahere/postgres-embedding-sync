package migrate

import (
	"testing"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/store"
)

func TestResumeStep(t *testing.T) {
	cases := []struct {
		step, state, want string
	}{
		{"catchup", store.StateReady, "catchup"},
		{"catchup", store.StateBuilding, "backfill"},
		{"gate", store.StateBuilding, "backfill"},
		{"gate", store.StateReady, "gate"},
		{"promote", store.StateReady, "promote"},
		{"promote", store.StateBuilding, "promote"},
		{"backfill", store.StateReady, "backfill"},
		{"backfill", store.StateBuilding, "backfill"},
		{"backfill", store.StatePending, "backfill"},
	}
	for _, c := range cases {
		if got := resumeStep(c.step, c.state); got != c.want {
			t.Errorf("resumeStep(%q, %q) = %q, want %q", c.step, c.state, got, c.want)
		}
	}
}
