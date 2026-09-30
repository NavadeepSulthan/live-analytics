package platform

import (
	"net/http/httptest"
	"testing"
)

func TestParseKeysValidation(t *testing.T) {
	bad := [][]string{
		{},
		{"acme:ingest"},
		{"Acme:ingest:0123456789abcdef"},
		{"acme:admin:0123456789abcdef"},
		{"acme:ingest:short"},
	}
	for _, entries := range bad {
		if _, err := ParseKeys(entries); err == nil {
			t.Errorf("ParseKeys(%q) should fail", entries)
		}
	}
}

func TestAuthenticateScopesByRoleAndTenant(t *testing.T) {
	store, err := ParseKeys([]string{
		"acme:ingest:acme-ingest-secret-1",
		"acme:read:acme-read-secret-00001",
		"globex:read:globex-read-secret-01",
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, header, value string
		role                Role
		tenant              string
		ok                  bool
	}{
		{"ingest key via X-API-Key", "X-API-Key", "acme-ingest-secret-1", RoleIngest, "acme", true},
		{"read key via Bearer", "Authorization", "Bearer acme-read-secret-00001", RoleRead, "acme", true},
		{"other tenant", "Authorization", "Bearer globex-read-secret-01", RoleRead, "globex", true},
		{"ingest key cannot read", "X-API-Key", "acme-ingest-secret-1", RoleRead, "", false},
		{"read key cannot ingest", "X-API-Key", "acme-read-secret-00001", RoleIngest, "", false},
		{"unknown key", "X-API-Key", "nope-nope-nope-nope", RoleIngest, "", false},
		{"missing key", "", "", RoleIngest, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/", nil)
			if tc.header != "" {
				r.Header.Set(tc.header, tc.value)
			}
			tenant, err := store.Authenticate(r, tc.role)
			if tc.ok != (err == nil) || tenant != tc.tenant {
				t.Fatalf("got (%q, %v), want tenant %q ok=%v", tenant, err, tc.tenant, tc.ok)
			}
		})
	}
}
