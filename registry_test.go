package tenantpool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// fakeDSN returns a parseable-but-unreachable DSN so Registry can build
// pgxpool objects without dialling a real Postgres. pgxpool.NewWithConfig
// is lazy; actual dial happens on Acquire, which our tests never call.
func fakeDSN(tenantID string) (string, error) {
	return fmt.Sprintf(
		"postgres://u:p@127.0.0.1:1/%s?sslmode=disable",
		tenantID,
	), nil
}

func newMultiRegistry(t *testing.T, maxPools int) *Registry {
	t.Helper()
	r, err := New(Config{
		DSNBuilder:     fakeDSN,
		Resolver:       StaticResolver("t1"),
		MaxPoolsCached: maxPools,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(r.Close)
	return r
}

func TestNew_RequiresEitherDatabaseURLOrDSNBuilder(t *testing.T) {
	_, err := New(Config{})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestNew_RejectsBothModes(t *testing.T) {
	_, err := New(Config{
		DatabaseURL: "postgres://u:p@127.0.0.1:1/db?sslmode=disable",
		DSNBuilder:  fakeDSN,
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestSingleMode_GetReturnsSharedPool(t *testing.T) {
	r, err := New(Config{
		DatabaseURL: "postgres://u:p@127.0.0.1:1/db?sslmode=disable",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer r.Close()

	p1, err := r.Get(context.Background(), "ignored-a")
	if err != nil {
		t.Fatalf("Get1: %v", err)
	}
	p2, err := r.Get(context.Background(), "ignored-b")
	if err != nil {
		t.Fatalf("Get2: %v", err)
	}
	if p1 != p2 {
		t.Error("single mode should return the same pool for different tenant IDs")
	}
	if s := r.Stats(); !s.SingleMode {
		t.Error("Stats.SingleMode should be true")
	}
}

func TestMultiMode_CachesByTenant(t *testing.T) {
	r := newMultiRegistry(t, 10)
	ctx := context.Background()

	p1, err := r.Get(ctx, "a")
	if err != nil {
		t.Fatalf("Get a: %v", err)
	}
	p2, err := r.Get(ctx, "a")
	if err != nil {
		t.Fatalf("Get a again: %v", err)
	}
	p3, err := r.Get(ctx, "b")
	if err != nil {
		t.Fatalf("Get b: %v", err)
	}
	if p1 != p2 {
		t.Error("same tenant should yield the same pool")
	}
	if p1 == p3 {
		t.Error("different tenants should yield different pools")
	}
	if s := r.Stats(); s.ActivePools != 2 || s.Created != 2 || s.Hits != 1 || s.Misses != 2 {
		t.Errorf("unexpected stats: %+v", s)
	}
}

func TestMultiMode_EmptyTenantIDRejected(t *testing.T) {
	r := newMultiRegistry(t, 10)
	_, err := r.Get(context.Background(), "")
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestLRUEviction_ClosesOldestAboveCap(t *testing.T) {
	r := newMultiRegistry(t, 2)
	ctx := context.Background()

	_, _ = r.Get(ctx, "t1")
	time.Sleep(2 * time.Millisecond)
	_, _ = r.Get(ctx, "t2")
	time.Sleep(2 * time.Millisecond)
	_, _ = r.Get(ctx, "t3") // evicts t1

	if s := r.Stats(); s.ActivePools != 2 || s.Evicted != 1 {
		t.Errorf("ActivePools=%d Evicted=%d, want 2 + 1", s.ActivePools, s.Evicted)
	}

	// Re-getting t1 is a miss (new pool), not a hit.
	_, _ = r.Get(ctx, "t1")
	if s := r.Stats(); s.Misses != 4 {
		t.Errorf("Misses=%d, want 4 (t1 re-miss after eviction)", s.Misses)
	}
}

func TestLRUEviction_KeepsRecentlyAccessed(t *testing.T) {
	r := newMultiRegistry(t, 2)
	ctx := context.Background()

	_, _ = r.Get(ctx, "t1")
	time.Sleep(2 * time.Millisecond)
	_, _ = r.Get(ctx, "t2")
	time.Sleep(2 * time.Millisecond)
	_, _ = r.Get(ctx, "t1") // refresh t1 → t2 is now oldest
	time.Sleep(2 * time.Millisecond)
	_, _ = r.Get(ctx, "t3") // should evict t2, not t1

	hitsBefore := r.Stats().Hits
	_, _ = r.Get(ctx, "t1")
	hitsAfter := r.Stats().Hits
	if hitsAfter != hitsBefore+1 {
		t.Errorf("t1 should still be cached; hits before=%d after=%d", hitsBefore, hitsAfter)
	}
}

func TestEvictIdle_ClosesIdlePools(t *testing.T) {
	r, err := New(Config{
		DSNBuilder:     fakeDSN,
		MaxPoolsCached: 100,
		IdleTimeout:    10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	ctx := context.Background()
	_, _ = r.Get(ctx, "t1")
	_, _ = r.Get(ctx, "t2")
	if s := r.Stats(); s.ActivePools != 2 {
		t.Fatalf("setup: %+v", s)
	}

	time.Sleep(20 * time.Millisecond)
	if got := r.EvictIdle(); got != 2 {
		t.Errorf("EvictIdle=%d, want 2", got)
	}
	if s := r.Stats(); s.ActivePools != 0 {
		t.Errorf("ActivePools after EvictIdle=%d, want 0", s.ActivePools)
	}
}

func TestInvalidate_RemovesTenant(t *testing.T) {
	r := newMultiRegistry(t, 10)
	ctx := context.Background()
	_, _ = r.Get(ctx, "t1")
	r.Invalidate("t1")
	if s := r.Stats(); s.ActivePools != 0 {
		t.Errorf("ActivePools after Invalidate=%d, want 0", s.ActivePools)
	}
	_, _ = r.Get(ctx, "t1")
	if s := r.Stats(); s.Misses != 2 {
		t.Errorf("Misses=%d, want 2 (re-miss after invalidate)", s.Misses)
	}
}

func TestConcurrentGet_SingleflightDeduplicates(t *testing.T) {
	r := newMultiRegistry(t, 100)
	ctx := context.Background()

	const goroutines = 50
	const itersPerG = 20
	const tenantCount = 5

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < itersPerG; i++ {
				tenant := fmt.Sprintf("t%d", (gid+i)%tenantCount)
				if _, err := r.Get(ctx, tenant); err != nil {
					t.Errorf("Get: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	s := r.Stats()
	if s.ActivePools != tenantCount {
		t.Errorf("ActivePools=%d, want %d", s.ActivePools, tenantCount)
	}
	if s.Created != uint64(tenantCount) {
		t.Errorf("Created=%d, want %d (race-created duplicates?)", s.Created, tenantCount)
	}
}

func TestDSNBuilder_ErrorMapsToTenantNotFound(t *testing.T) {
	r, err := New(Config{
		DSNBuilder: func(string) (string, error) {
			return "", errors.New("no such tenant")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, err = r.Get(context.Background(), "ghost")
	if !errors.Is(err, ErrTenantNotFound) {
		t.Errorf("expected ErrTenantNotFound, got %v", err)
	}
}

func TestConfigurePool_RunsOnEachBuild(t *testing.T) {
	var calls int
	r, err := New(Config{
		DSNBuilder: fakeDSN,
		ConfigurePool: func(c *pgxpool.Config) error {
			calls++
			c.MaxConns = 3
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, _ = r.Get(context.Background(), "a")
	_, _ = r.Get(context.Background(), "b")
	if calls != 2 {
		t.Errorf("ConfigurePool calls=%d, want 2", calls)
	}
}

func TestClose_DrainsAllPools(t *testing.T) {
	r := newMultiRegistry(t, 10)
	_, _ = r.Get(context.Background(), "t1")
	_, _ = r.Get(context.Background(), "t2")
	r.Close()
	if s := r.Stats(); s.ActivePools != 0 {
		t.Errorf("ActivePools after Close=%d, want 0", s.ActivePools)
	}
}

func TestClose_DrainsSingleMode(t *testing.T) {
	r, err := New(Config{DatabaseURL: "postgres://u:p@127.0.0.1:1/db?sslmode=disable"})
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	// Subsequent Get returns the single pool which is now closed;
	// we just verify SingleMode reports false after Close.
	if s := r.Stats(); s.SingleMode {
		t.Error("SingleMode should flip off after Close")
	}
}

func TestSupavisorDSN_EmbedsTenantInUsername(t *testing.T) {
	build := SupavisorDSN(SupavisorOpts{
		Host: "supa:5432", UserPrefix: "auth", Password: "pw",
	})
	dsn, err := build("abc12345m")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "auth.abc12345m") {
		t.Errorf("expected username auth.abc12345m in DSN, got %q", dsn)
	}
	if !strings.Contains(dsn, "@supa:5432/") {
		t.Errorf("host missing: %q", dsn)
	}
}

func TestSupavisorDSN_RejectsEmptyRequiredFields(t *testing.T) {
	build := SupavisorDSN(SupavisorOpts{Host: "", UserPrefix: "auth", Password: "pw"})
	_, err := build("t1")
	if !errors.Is(err, ErrInvalidConfig) {
		t.Errorf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestDirectDSN_UsesTenantAsDBName(t *testing.T) {
	build := DirectDSN(DirectOpts{Host: "pg:5432", User: "u", Password: "p"})
	dsn, err := build("tenant_abc")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dsn, "/tenant_abc?") {
		t.Errorf("dbname missing: %q", dsn)
	}
}
