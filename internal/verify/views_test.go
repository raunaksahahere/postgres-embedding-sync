package verify

import (
	"testing"

	"github.com/raunaksahahere/postgres-embedding-sync/internal/config"
)

func TestOrderByClause(t *testing.T) {
	text := map[string]bool{"name": true, "title": true}
	cases := []struct {
		name      string
		cols      []string
		alias     string
		textCols  map[string]bool
		nullsLast bool
		want      string
	}{
		{"plain", []string{"tenant_id", "document_id"}, "", nil, false, `"tenant_id", "document_id"`},
		{"text collate", []string{"position", "title"}, "", text, false, `"position", "title" COLLATE "C"`},
		{"nulls last", []string{"position", "title"}, "", text, true, `"position" NULLS LAST, "title" COLLATE "C" NULLS LAST`},
		{"alias", []string{"name", "id"}, "t", text, true, `"t"."name" COLLATE "C" NULLS LAST, "t"."id" NULLS LAST`},
		{"alias key", []string{"tenant_id", "id"}, "k", nil, false, `"k"."tenant_id", "k"."id"`},
		{"quoting", []string{`we"ird`}, "t", map[string]bool{`we"ird`: true}, false, `"t"."we""ird" COLLATE "C"`},
		{"empty", nil, "", nil, true, ``},
	}
	for _, c := range cases {
		if got := orderByClause(c.cols, c.alias, c.textCols, c.nullsLast); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
}

func TestSelectListAndTableIdent(t *testing.T) {
	if got, want := selectList("t", []string{"tenant_id", "id"}, []string{"name"}), `"t"."tenant_id"::text, "t"."id"::text, "t"."name"::text`; got != want {
		t.Errorf("selectList: got %s, want %s", got, want)
	}
	if got, want := selectList("", []string{"id"}), `"id"::text`; got != want {
		t.Errorf("selectList: got %s, want %s", got, want)
	}
	if got, want := tableIdent("public.document_tags"), `"public"."document_tags"`; got != want {
		t.Errorf("tableIdent: got %s, want %s", got, want)
	}
}

// Regression: the child query sorted integer keys as text ("803" after
// "3802") because ORDER BY bound to the unqualified ::text output columns.
func TestChildQueryQualifiesOrderColumns(t *testing.T) {
	ch := &config.ChildDef{Name: "sections", Table: "public.kb_sections", Key: config.KeyColumns{"tenant_id", "id"},
		Parent: config.KeyColumns{"tenant_id", "document_id"}, OrderBy: []string{"position"}}
	q := childQuery(ch, []string{"body"}, map[string]bool{"body": true})
	want := `SELECT "c"."tenant_id"::text, "c"."id"::text, "c"."tenant_id"::text, "c"."document_id"::text, "c"."body"::text FROM "public"."kb_sections" AS "c" ORDER BY "c"."tenant_id", "c"."document_id", "c"."position" NULLS LAST, "c"."tenant_id", "c"."id"`
	if q != want {
		t.Fatalf("child query:\n got %s\nwant %s", q, want)
	}
}
