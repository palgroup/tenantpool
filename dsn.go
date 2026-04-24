package tenantpool

import (
	"errors"
	"fmt"
	"net/url"
)

// DirectOpts configures the DirectDSN builder — a plain
// user@host/tenant_db connection string with no pooler in front.
type DirectOpts struct {
	// Host is the Postgres host:port.
	Host string
	// User is the database role used for every tenant.
	User string
	// Password is the role's password.
	Password string
	// SSLMode is appended to the DSN. Default "require".
	SSLMode string
	// ExtraParams are appended after sslmode. Keys and values are URL
	// escaped.
	ExtraParams map[string]string
}

// DirectDSN returns a DSNBuilder that connects each tenant to a
// database whose name equals its tenant ID. Suitable for PgBouncer or
// direct-PG deployments that use DB-per-tenant isolation without a
// tenant-aware pooler in front.
func DirectDSN(opts DirectOpts) func(string) (string, error) {
	if opts.SSLMode == "" {
		opts.SSLMode = "require"
	}
	if err := requireFields("DirectDSN", map[string]string{
		"Host":     opts.Host,
		"User":     opts.User,
		"Password": opts.Password,
	}); err != nil {
		return errorBuilder(err)
	}
	return func(tenantID string) (string, error) {
		if tenantID == "" {
			return "", errors.New("tenantpool: DirectDSN tenantID empty")
		}
		return fmt.Sprintf(
			"postgres://%s:%s@%s/%s?sslmode=%s%s",
			url.UserPassword(opts.User, opts.Password).Username(),
			url.QueryEscape(opts.Password),
			opts.Host,
			url.PathEscape(tenantID),
			opts.SSLMode,
			extraParams(opts.ExtraParams),
		), nil
	}
}

// SupavisorOpts configures the SupavisorDSN builder — Supabase's
// tenant-aware pooler. Supavisor parses the dotted username
// ("{schema}.{tenantID}") to route the connection to the tenant's
// backend Postgres.
type SupavisorOpts struct {
	// Host is the Supavisor endpoint (host:port).
	Host string
	// UserPrefix is prepended before the dot in the wire username. In
	// Supabase Cloud this is "postgres"; in self-hosted deployments it
	// is typically your module schema name (e.g. "auth"). Example:
	// UserPrefix="auth" → username "auth.tenant_abc".
	UserPrefix string
	// Password is the role password.
	Password string
	// DBName overrides the Postgres dbname in the DSN. When empty, the
	// tenantID is used — Supavisor will pass this through to the
	// backend. Most deployments leave this empty.
	DBName string
	// SSLMode is appended to the DSN. Default "require".
	SSLMode string
	// ExtraParams are appended after sslmode. Keys and values are URL
	// escaped. Use this to set application_name,
	// default_query_exec_mode, etc.
	ExtraParams map[string]string
}

// SupavisorDSN returns a DSNBuilder that produces Supavisor-style
// connection strings. The tenantID is embedded in the wire username so
// Supavisor can route server-side; no custom SupavisorOpts field
// references the tenantID itself.
func SupavisorDSN(opts SupavisorOpts) func(string) (string, error) {
	if opts.SSLMode == "" {
		opts.SSLMode = "require"
	}
	if err := requireFields("SupavisorDSN", map[string]string{
		"Host":       opts.Host,
		"UserPrefix": opts.UserPrefix,
		"Password":   opts.Password,
	}); err != nil {
		return errorBuilder(err)
	}
	return func(tenantID string) (string, error) {
		if tenantID == "" {
			return "", errors.New("tenantpool: SupavisorDSN tenantID empty")
		}
		dbname := opts.DBName
		if dbname == "" {
			dbname = tenantID
		}
		user := opts.UserPrefix + "." + tenantID
		return fmt.Sprintf(
			"postgres://%s:%s@%s/%s?sslmode=%s%s",
			url.PathEscape(user),
			url.QueryEscape(opts.Password),
			opts.Host,
			url.PathEscape(dbname),
			opts.SSLMode,
			extraParams(opts.ExtraParams),
		), nil
	}
}

// PgBouncerOpts is an alias for DirectOpts kept for clarity at call
// sites. PgBouncer is transparent at the wire level (same user/DSN
// shape as direct PG), so the two builders produce identical strings.
type PgBouncerOpts = DirectOpts

// PgBouncerDSN is an alias for DirectDSN. PgBouncer deployments use
// DB-per-tenant with a shared pooler role; the DSN shape is identical
// to direct PG.
func PgBouncerDSN(opts PgBouncerOpts) func(string) (string, error) {
	return DirectDSN(opts)
}

// extraParams renders a map as "&k1=v1&k2=v2" in stable order by key.
// Empty map → "".
func extraParams(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	// Small and predictable: sort keys for determinism.
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// tiny allocation-free sort
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	out := ""
	for _, k := range keys {
		out += "&" + url.QueryEscape(k) + "=" + url.QueryEscape(m[k])
	}
	return out
}

func requireFields(name string, fields map[string]string) error {
	for k, v := range fields {
		if v == "" {
			return fmt.Errorf("%w: %s.%s is required", ErrInvalidConfig, name, k)
		}
	}
	return nil
}

// errorBuilder returns a DSNBuilder that always returns the given
// configuration error. Used when *DSN helpers detect missing required
// fields at construction time; New will surface the error on first
// Get.
func errorBuilder(err error) func(string) (string, error) {
	return func(_ string) (string, error) { return "", err }
}
