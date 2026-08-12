//go:build exitgate

// This file is separated from patterns_test.go by a build constraint and
// nothing else — a build tag is file-scoped in Go, so a guard that must not run
// in the default suite cannot share a file with one that must. Its helpers
// (palbaseRoot, scanGo, report) live in patterns_test.go, which is built under
// every tag.
//
// WHY THE TAG. DEL-016 is a repo-wide removal that no single task performs. The
// occurrences it forbids are spread across modules owned by T007 (messaging),
// T008 (notifications, user-flags), T010 (backend/database) and T015
// (backend/management), plus three in database/proxy. T005 removes the last
// PoolLess line and writes this guard, but running it here would demand that
// one task finish four others' work — and the only ways to make it pass early
// are to widen the task past its file boundary or to weaken the assertion.
// Neither is acceptable, and a guard that is skipped at runtime (t.Skip) still
// reads as a passing test in a suite summary, which is its own quiet lie.
//
// So it is written now, compiled now, and run at T024 as the run's exit gate:
//
//	go test -tags exitgate -run TestDEL016 ./...
//
// by which point the owning tasks have swept their modules.

package tenantpool_test

import (
	"regexp"
	"testing"
)

// orchestratorExcluded is pruned from the DEL-016 census, with the reason
// stated here rather than applied silently: modules/orchestrator is a CLOUD
// component and is out of scope for the self-host run entirely — it is not one
// of the four containers (envoy · palsvc · runtime · postgres), it is not built
// into the self-host image, and no task in this run touches it. Its 14
// search_path lines belong to the multi-tenant provisioning path that continues
// to exist in the hosted product. Excluding them keeps this gate honest about
// what the run actually promised; hiding them behind an unexplained filter
// would make a green here mean less than it appears to.
//
// The exclusion is one named directory, not a pattern. Anything else that grows
// a search_path must fail this gate.
var orchestratorExcluded = []string{"modules/orchestrator"}

// scanGoRe is scanGo with a regexp instead of a substring. It lives here rather
// than beside its sibling because this file is its only caller, and a helper
// compiled under every tag with a caller under one is dead code to the default
// build — which is exactly what CI reported.
//
// Some patterns are only a violation in one shape: `search_path` names both the
// multi-tenant schema switch and an injection-hardening line that must survive,
// so a substring census cannot tell a door back to shared schemas from a
// security control. Where the distinction matters, match the statement, not the
// word.
func scanGoRe(t *testing.T, root string, re *regexp.Regexp, excludeRel []string) (hits []hit, filesRead int) {
	t.Helper()
	return scanGoMatch(t, root, excludeRel, func(line string) bool { return re.MatchString(line) })
}

// tenantSearchPath matches the two shapes that SELECT a schema, and only those.
//
// `SET search_path TO env_<id>` (and its LOCAL form) is the multi-tenant switch:
// the target is dynamic, so isolation becomes a property of a session variable.
// `RuntimeParams["search_path"]` is the same choice made at dial time — a
// connection pinned to one module's schema is a connection the other eight
// cannot use.
//
// What it deliberately does NOT match: `SET LOCAL search_path = public`. That is
// injection hardening, not tenancy — transaction-scoped, pinned to a constant,
// and issued in the same statement as `SET LOCAL ROLE` so a hostile schema
// cannot shadow a function the query resolves (see
// modules/database/proxy/internal/handler/handler.go:89). Banning the bare word
// would leave exactly two ways to green this gate: weaken the assertion, or
// delete a security control. Both are forbidden, so the gate matches the
// statement instead of the word — the same regex three module-level guards in
// this run already use.
var tenantSearchPath = regexp.MustCompile(`(?i)(SET\s+(LOCAL\s+)?search_path\s+TO\b)|(RuntimeParams\s*\[\s*"search_path"\s*\])`)

// TestDEL016_NoSearchPathStatements locks the removal of schema-switching.
//
// v1 gave every tenant a schema in one shared database and reached it with
// `SET search_path TO env_<id>`, which made isolation a property of a session
// variable: any code path that opened a connection without setting it, or set
// it from an unvalidated request value, crossed tenants. A single-tenant stack
// owns its whole database, so the statement has nothing left to select and its
// continued presence is a door back to the shared-schema model.
//
// Mutation: re-add a `SET search_path TO <anything>` under modules/ or platform/
// outside the exclusion above, and this test goes red naming the file and line.
func TestDEL016_NoSearchPathStatements(t *testing.T) {
	root, ok := palbaseRoot()
	if !ok {
		t.Skip("not inside the palbase parent checkout: no modules/ + platform/ tree to take the census over")
	}
	hits, files := scanGoRe(t, root, tenantSearchPath, orchestratorExcluded)
	t.Logf("census over %d production .go files under %s (excluding %v)", files, root, orchestratorExcluded)
	if len(hits) > 0 {
		report(t, "search_path", hits)
	}
}
