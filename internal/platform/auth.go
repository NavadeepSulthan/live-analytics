package platform

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

// Role is what an API key may do. Keys are single-purpose so a leaked ingest key (embedded in
// servers) can't read analytics, and a dashboard read key can't write.
type Role string

const (
	RoleIngest Role = "ingest"
	RoleRead   Role = "read"
)

var ErrUnauthorized = errors.New("unauthorized")

var tenantPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

type apiKey struct {
	hash   [sha256.Size]byte
	tenant string
	role   Role
}

// KeyStore authenticates API keys. Only SHA-256 digests are kept in memory and comparison
// is constant-time.
type KeyStore struct {
	keys []apiKey
}

// ParseKeys parses "tenant:role:secret" entries (e.g. from the API_KEYS env var).
func ParseKeys(entries []string) (*KeyStore, error) {
	store := &KeyStore{}
	for _, entry := range entries {
		parts := strings.SplitN(entry, ":", 3)
		if len(parts) != 3 {
			return nil, fmt.Errorf("api key entry must be tenant:role:secret")
		}
		tenant, role, secret := parts[0], Role(parts[1]), parts[2]
		if !tenantPattern.MatchString(tenant) {
			return nil, fmt.Errorf("api key tenant %q is not a slug", tenant)
		}
		if role != RoleIngest && role != RoleRead {
			return nil, fmt.Errorf("api key role %q must be ingest or read", role)
		}
		if len(secret) < 16 {
			return nil, fmt.Errorf("api key for %s/%s must be at least 16 characters", tenant, role)
		}
		store.keys = append(store.keys, apiKey{hash: sha256.Sum256([]byte(secret)), tenant: tenant, role: role})
	}
	if len(store.keys) == 0 {
		return nil, errors.New("no api keys configured")
	}
	return store, nil
}

// Authenticate returns the tenant for a request carrying a key with the given role, taken
// from "X-API-Key" or "Authorization: Bearer".
func (s *KeyStore) Authenticate(r *http.Request, role Role) (string, error) {
	secret := r.Header.Get("X-API-Key")
	if secret == "" {
		secret, _ = strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	}
	if secret == "" {
		return "", ErrUnauthorized
	}
	digest := sha256.Sum256([]byte(secret))
	tenant := ""
	for _, k := range s.keys {
		// Check every key so timing doesn't reveal which (if any) matched.
		if subtle.ConstantTimeCompare(digest[:], k.hash[:]) == 1 && k.role == role {
			tenant = k.tenant
		}
	}
	if tenant == "" {
		return "", ErrUnauthorized
	}
	return tenant, nil
}
