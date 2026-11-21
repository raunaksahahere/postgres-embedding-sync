package main

import (
	"strings"
	"testing"
)

func TestResolveAdminDSN(t *testing.T) {
	const side = "EMBEDSYNC_SOURCE_ADMIN_DSN"
	cases := []struct {
		name    string
		env     map[string]string
		envName string
		flag    string
		want    string
		wantErr bool
	}{
		{name: "side env wins over everything", env: map[string]string{side: "side", adminURLEnv: "envurl"}, envName: side, flag: "flag", want: "side"},
		{name: "flag when side env unset", env: map[string]string{adminURLEnv: "envurl"}, envName: side, flag: "flag", want: "flag"},
		{name: "flag when side env empty", env: map[string]string{side: ""}, envName: side, flag: "flag", want: "flag"},
		{name: "EMBEDSYNC_ADMIN_URL last", env: map[string]string{adminURLEnv: "envurl"}, envName: side, want: "envurl"},
		{name: "empty env var name skips the side env", env: map[string]string{"": "bogus"}, envName: "", flag: "flag", want: "flag"},
		{name: "nothing set", env: map[string]string{}, envName: side, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveAdminDSN(tc.envName, tc.flag, func(k string) string { return tc.env[k] })
			if tc.wantErr {
				if err == nil {
					t.Fatalf("want error, got %q", got)
				}
				for _, s := range []string{side, "--admin-url", adminURLEnv} {
					if !strings.Contains(err.Error(), s) {
						t.Errorf("error %q does not mention %s", err, s)
					}
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("got %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}
