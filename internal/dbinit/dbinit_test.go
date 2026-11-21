package dbinit

import (
	"reflect"
	"testing"
)

func TestPlanPublication(t *testing.T) {
	const markers = "embedsync.markers"
	tests := []struct {
		name                 string
		want, have           map[string]string
		adds, relist, extras []string
	}{
		{
			name: "identical",
			want: map[string]string{markers: anyColumns, "public.documents": "body,id,title"},
			have: map[string]string{markers: "id,pipeline", "public.documents": "body,id,title"},
		},
		{
			name: "missing table is added",
			want: map[string]string{markers: anyColumns, "public.documents": "body,id,title", "support.tickets": "id,subject"},
			have: map[string]string{markers: "id", "public.documents": "body,id,title"},
			adds: []string{"support.tickets"},
		},
		{
			name: "missing markers is added",
			want: map[string]string{markers: anyColumns, "public.documents": "body,id,title"},
			have: map[string]string{"public.documents": "body,id,title"},
			adds: []string{markers},
		},
		{
			name: "empty publication gets every table",
			want: map[string]string{markers: anyColumns, "public.documents": "body,id,title", "support.tickets": "id,subject"},
			have: map[string]string{},
			adds: []string{markers, "public.documents", "support.tickets"},
		},
		{
			name:   "different columns are relisted",
			want:   map[string]string{markers: anyColumns, "public.documents": "body,id,title"},
			have:   map[string]string{markers: "id", "public.documents": "body,id"},
			relist: []string{"public.documents"},
		},
		{
			name:   "extra table is reported, never removed",
			want:   map[string]string{markers: anyColumns, "public.documents": "body,id,title"},
			have:   map[string]string{markers: "id", "public.documents": "body,id,title", "public.notes": "id,text"},
			extras: []string{"public.notes"},
		},
		{
			name: "markers with any columns need no change",
			want: map[string]string{markers: anyColumns},
			have: map[string]string{markers: "at,id,lsn,pipeline"},
		},
		{
			name:   "all at once",
			want:   map[string]string{markers: anyColumns, "public.documents": "body,id,title", "support.tickets": "id,subject"},
			have:   map[string]string{markers: "id", "public.documents": "id,title", "public.notes": "id", "archive.old": "id"},
			adds:   []string{"support.tickets"},
			relist: []string{"public.documents"},
			extras: []string{"archive.old", "public.notes"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			adds, relist, extras := planPublication(tc.want, tc.have)
			if !reflect.DeepEqual(adds, tc.adds) {
				t.Errorf("adds = %q, want %q", adds, tc.adds)
			}
			if !reflect.DeepEqual(relist, tc.relist) {
				t.Errorf("relist = %q, want %q", relist, tc.relist)
			}
			if !reflect.DeepEqual(extras, tc.extras) {
				t.Errorf("extra = %q, want %q", extras, tc.extras)
			}
		})
	}
}
