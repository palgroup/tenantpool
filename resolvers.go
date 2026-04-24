package tenantpool

import (
	"errors"
	"fmt"
	"net/http"
)

// HeaderResolver returns a Resolver that reads tenantID from the given
// HTTP header. An empty header value produces ErrTenantNotFound so the
// default error handler maps it to 404 rather than leaking a 500.
//
// Typical deployment: a gateway (Kong, nginx) injects the header after
// authenticating an API key or iJWT. Modules never touch the raw token.
func HeaderResolver(name string) Resolver {
	if name == "" {
		name = "X-Tenant-Ref"
	}
	return func(r *http.Request) (string, error) {
		v := r.Header.Get(name)
		if v == "" {
			return "", fmt.Errorf("%w: header %q missing", ErrTenantNotFound, name)
		}
		return v, nil
	}
}

// StaticResolver returns a Resolver that always yields the given
// tenantID. Intended for single-tenant deployments that still want to
// exercise the multi-tenant code path (e.g. integration tests), or for
// workers whose tenancy is fixed at startup.
func StaticResolver(tenantID string) Resolver {
	return func(_ *http.Request) (string, error) {
		if tenantID == "" {
			return "", errors.New("tenantpool: StaticResolver configured with empty tenantID")
		}
		return tenantID, nil
	}
}
