package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolRunsEverySubmittedTask(t *testing.T) {
	pool := NewPool(PoolConfig{Size: 4, QueueSize: 16})
	pool.Start(context.Background())

	var executed atomic.Int64

	for range 128 {
		if err := pool.Submit(context.Background(), func(context.Context) {
			executed.Add(1)
		}); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}

	pool.Stop()

	if executed.Load() != 128 {
		t.Fatalf("expected 128 executed tasks, got %d", executed.Load())
	}
}

func TestPoolLimitsConcurrency(t *testing.T) {
	pool := NewPool(PoolConfig{Size: 3, QueueSize: 32})
	pool.Start(context.Background())

	var (
		mu      sync.Mutex
		current int
		peak    int
	)

	release := make(chan struct{})

	for range 24 {
		if err := pool.Submit(context.Background(), func(context.Context) {
			mu.Lock()
			current++
			if current > peak {
				peak = current
			}
			mu.Unlock()

			<-release

			mu.Lock()
			current--
			mu.Unlock()
		}); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}

	time.Sleep(20 * time.Millisecond)
	close(release)
	pool.Stop()

	if peak > 3 {
		t.Fatalf("expected at most 3 concurrent tasks, observed %d", peak)
	}
}

func TestTrySubmitReportsSaturation(t *testing.T) {
	pool := NewPool(PoolConfig{Size: 1, QueueSize: 1})
	pool.Start(context.Background())

	block := make(chan struct{})

	if err := pool.Submit(context.Background(), func(context.Context) { <-block }); err != nil {
		t.Fatalf("submit: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	if err := pool.TrySubmit(func(context.Context) {}); err != nil {
		t.Fatalf("expected buffered slot to accept task, got %v", err)
	}

	saturated := false
	for range 4 {
		if errors.Is(pool.TrySubmit(func(context.Context) {}), ErrPoolSaturated) {
			saturated = true
			break
		}
	}

	if !saturated {
		t.Fatal("expected pool to report saturation")
	}

	close(block)
	pool.Stop()
}

func TestSubmitAfterStopIsRejected(t *testing.T) {
	pool := NewPool(PoolConfig{Size: 2, QueueSize: 2})
	pool.Start(context.Background())
	pool.Stop()

	if err := pool.Submit(context.Background(), func(context.Context) {}); !errors.Is(err, ErrPoolStopped) {
		t.Fatalf("expected ErrPoolStopped, got %v", err)
	}
}

func TestSubmitHonoursContextCancellation(t *testing.T) {
	pool := NewPool(PoolConfig{Size: 1, QueueSize: 0})
	pool.Start(context.Background())

	block := make(chan struct{})

	if err := pool.Submit(context.Background(), func(context.Context) { <-block }); err != nil {
		t.Fatalf("submit: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := pool.Submit(ctx, func(context.Context) {}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	close(block)
	pool.Stop()
}

func TestStopIsIdempotent(t *testing.T) {
	pool := NewPool(PoolConfig{Size: 2, QueueSize: 2})
	pool.Start(context.Background())

	pool.Stop()
	pool.Stop()
}
