package tenantpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestStaticPasswordResolver(t *testing.T) {
	t.Parallel()

	t.Run("returns password", func(t *testing.T) {
		t.Parallel()
		r := StaticPasswordResolver("hunter2")
		got, err := r.Resolve(context.Background(), "any-tenant")
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if got != "hunter2" {
			t.Fatalf("got %q, want %q", got, "hunter2")
		}
	})

	t.Run("rejects empty password", func(t *testing.T) {
		t.Parallel()
		r := StaticPasswordResolver("")
		if _, err := r.Resolve(context.Background(), "t"); err == nil {
			t.Fatal("expected error for empty password")
		}
	})
}

func TestCachingPasswordResolver(t *testing.T) {
	t.Parallel()

	t.Run("caches within ttl", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int64
		inner := PasswordResolverFunc(func(_ context.Context, tenant string) (string, error) {
			calls.Add(1)
			return "pw-" + tenant, nil
		})
		r := NewCachingPasswordResolver(inner, time.Hour)

		for i := 0; i < 10; i++ {
			got, err := r.Resolve(context.Background(), "acme")
			if err != nil {
				t.Fatalf("Resolve: %v", err)
			}
			if got != "pw-acme" {
				t.Fatalf("call %d: got %q", i, got)
			}
		}
		if c := calls.Load(); c != 1 {
			t.Fatalf("inner called %d times, want 1", c)
		}
	})

	t.Run("expires after ttl", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int64
		inner := PasswordResolverFunc(func(_ context.Context, _ string) (string, error) {
			calls.Add(1)
			return "pw", nil
		})
		r := NewCachingPasswordResolver(inner, 10*time.Millisecond)

		_, _ = r.Resolve(context.Background(), "t")
		time.Sleep(20 * time.Millisecond)
		_, _ = r.Resolve(context.Background(), "t")

		if c := calls.Load(); c != 2 {
			t.Fatalf("inner called %d times, want 2 after expiry", c)
		}
	})

	t.Run("ttl zero disables caching", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int64
		inner := PasswordResolverFunc(func(_ context.Context, _ string) (string, error) {
			calls.Add(1)
			return "pw", nil
		})
		r := NewCachingPasswordResolver(inner, 0)
		_, _ = r.Resolve(context.Background(), "t")
		_, _ = r.Resolve(context.Background(), "t")
		_, _ = r.Resolve(context.Background(), "t")
		if c := calls.Load(); c != 3 {
			t.Fatalf("inner called %d times, want 3 with ttl=0", c)
		}
	})

	t.Run("invalidate forces refetch", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int64
		inner := PasswordResolverFunc(func(_ context.Context, _ string) (string, error) {
			calls.Add(1)
			return "pw", nil
		})
		r := NewCachingPasswordResolver(inner, time.Hour)
		_, _ = r.Resolve(context.Background(), "t")
		r.Invalidate("t")
		_, _ = r.Resolve(context.Background(), "t")
		if c := calls.Load(); c != 2 {
			t.Fatalf("inner called %d times, want 2 after invalidate", c)
		}
	})

	t.Run("propagates inner error", func(t *testing.T) {
		t.Parallel()
		want := errors.New("boom")
		inner := PasswordResolverFunc(func(_ context.Context, _ string) (string, error) {
			return "", want
		})
		r := NewCachingPasswordResolver(inner, time.Hour)
		if _, err := r.Resolve(context.Background(), "t"); !errors.Is(err, want) {
			t.Fatalf("got %v, want %v", err, want)
		}
	})

	t.Run("nil inner panics", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic for nil inner")
			}
		}()
		NewCachingPasswordResolver(nil, time.Second)
	})
}

func TestRegistry_DSNTemplateRequiresResolver(t *testing.T) {
	t.Parallel()
	_, err := New(Config{
		DSNTemplate: "postgres://u:{{password}}@h/{{tenant}}",
	})
	if err == nil {
		t.Fatal("expected error: template w/ {{password}} but no resolver")
	}
}

func TestRegistry_DSNTemplateAndBuilderMutuallyExclusive(t *testing.T) {
	t.Parallel()
	_, err := New(Config{
		DSNTemplate:      "postgres://u:p@h/{{tenant}}",
		PasswordResolver: StaticPasswordResolver("p"),
		DSNBuilder:       func(string) (string, error) { return "", nil },
	})
	if err == nil {
		t.Fatal("expected error: both template and builder set")
	}
}
