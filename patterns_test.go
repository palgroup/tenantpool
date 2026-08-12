package tenantpool_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Pattern census guards. Unlike deleted_paths_test.go — which asks whether one
// file still exists — these ask whether a PATTERN still occurs anywhere in the
// tree, because the thing being removed was never confined to a file.
//
// The census is the canonical count from
// docs/paltimate/2026-08-12-v2-faz0-selfhost/deletion-inventory.md §10:
//
//	grep -rn --include="*.go" -F "<pattern>" modules platform \
//	  | grep -v "_test\.go\|/vendor/" | wc -l
//
// scanGo below reproduces it exactly — Go sources only, under modules/ and
// platform/, no _test.go, no vendored copies — so a guard here and a census on
// the command line can never disagree about scope. Test files are excluded for
// the same reason the census excludes them: a test that names the removed
// pattern in order to forbid it must not count as an occurrence of it.

// palbaseRoot returns the parent-checkout root: the directory holding both
// modules/ and platform/.
//
// This package is a git submodule (modules/common/tenantpool), so it has two
// lives. Inside the palbase checkout the tree above it is the census scope.
// Cloned on its own — which is how its own CI runs it — there is no tree above
// it and no census to take; the second return value is false and the caller
// skips, because reporting "0 occurrences" for a tree that was never read would
// be a green that means nothing.
func palbaseRoot() (string, bool) {
	dir, err := os.Getwd()
	if err != nil {
		return "", false
	}
	for {
		modules := filepath.Join(dir, "modules")
		platform := filepath.Join(dir, "platform")
		if isDir(modules) && isDir(platform) && isDir(filepath.Join(modules, "palsvc")) {
			return dir, true
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

func isDir(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.IsDir()
}

// hit is one census line.
type hit struct {
	path string // repo-relative
	line int    // 1-based
	text string
}

func (h hit) String() string { return h.path + ":" + itoa(h.line) + ": " + strings.TrimSpace(h.text) }

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// prunedDirs are skipped whole. vendor/ is pruned rather than filtered because
// a vendored copy of a package still carries the symbols removed from the
// original, and counting those would report a finished teardown as undone.
var prunedDirs = map[string]bool{".git": true, "node_modules": true, ".next": true, "vendor": true}

// scanGo takes the canonical census of `pattern` under root/modules and
// root/platform. excludeRel prunes whole repo-relative subtrees (see the
// orchestrator carve-out in the DEL-016 guard, where the reason is written
// out); pass nil for no exclusions.
//
// It returns the hits and the number of production .go files actually read. The
// caller asserts that count is plausible: a walk that silently visited nothing
// — wrong root, moved directory, a prune rule that swallowed the tree — would
// otherwise return zero hits and read as a pass. That is the vacuous-guard
// failure this suite exists to prevent, so it is checked rather than assumed.
func scanGo(t *testing.T, root, pattern string, excludeRel []string) (hits []hit, filesRead int) {
	t.Helper()
	for _, top := range []string{"modules", "platform"} {
		base := filepath.Join(root, top)
		err := filepath.WalkDir(base, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			rel = filepath.ToSlash(rel)
			if d.IsDir() {
				if prunedDirs[d.Name()] {
					return filepath.SkipDir
				}
				for _, ex := range excludeRel {
					if rel == ex || strings.HasPrefix(rel, ex+"/") {
						return filepath.SkipDir
					}
				}
				return nil
			}
			name := d.Name()
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				return nil
			}
			body, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			filesRead++
			for i, line := range strings.Split(string(body), "\n") {
				if strings.Contains(line, pattern) {
					hits = append(hits, hit{path: rel, line: i + 1, text: line})
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", base, err)
		}
	}
	// The tree held ~1300 production .go files when this floor was written; a
	// walk that reads fewer than 500 is not a small tree, it is a broken walk.
	if filesRead < 500 {
		t.Fatalf("census read only %d production .go files under %s — the walk is broken, so a zero count here would prove nothing", filesRead, root)
	}
	return hits, filesRead
}

func report(t *testing.T, pattern string, hits []hit) {
	t.Helper()
	var b strings.Builder
	b.WriteString(pattern + " still occurs " + itoa(len(hits)) + " time(s) under the canonical census:\n")
	for _, h := range hits {
		b.WriteString("  " + h.String() + "\n")
	}
	t.Fatal(b.String())
}

// TestDEL017_NoPoolLessMarker locks the removal of the PoolLess marker
// interface and every implementation of it.
//
// The marker existed to split palsvc's authed mount in two: modules that read a
// per-request tenant pool went behind the tenantpool middleware, modules that
// did not were spared the resolve. A single-tenant stack opens one pool at boot
// and hands it to everything, so the split has no second side — every module is
// what the marker used to call pool-less, and a marker that describes all
// modules distinguishes none.
//
// Mutation: re-add the interface to palsvc's platform package, or one module's
// marker method, and this test goes red.
func TestDEL017_NoPoolLessMarker(t *testing.T) {
	root, ok := palbaseRoot()
	if !ok {
		t.Skip("not inside the palbase parent checkout: no modules/ + platform/ tree to take the census over")
	}
	hits, files := scanGo(t, root, "PoolLess", nil)
	t.Logf("census over %d production .go files under %s", files, root)
	if len(hits) > 0 {
		report(t, "PoolLess", hits)
	}
}
