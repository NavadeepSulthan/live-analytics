// Package platform holds cross-cutting infrastructure shared by all services:
// configuration, logging, tracing, metrics, auth and HTTP plumbing.
package platform

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env reads typed configuration from environment variables, collecting every problem so a
// misconfigured service reports all errors at once and refuses to start.
type Env struct {
	errs []string
}

func (e *Env) String(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func (e *Env) Required(key string) string {
	v := os.Getenv(key)
	if v == "" {
		e.errs = append(e.errs, key+": required")
	}
	return v
}

func (e *Env) Int(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Sprintf("%s: not an integer: %q", key, v))
	}
	return n
}

func (e *Env) Duration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Sprintf("%s: not a duration: %q", key, v))
	}
	return d
}

func (e *Env) List(key, fallback string) []string {
	var out []string
	for _, item := range strings.Split(e.String(key, fallback), ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// Err returns all accumulated configuration errors.
func (e *Env) Err() error {
	if len(e.errs) == 0 {
		return nil
	}
	return fmt.Errorf("invalid configuration: %s", strings.Join(e.errs, "; "))
}
