package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrPoolStopped   = errors.New("worker: pool is stopped")
	ErrPoolSaturated = errors.New("worker: pool is saturated")
)

type Task func(ctx context.Context)

type PoolConfig struct {
	Size      int
	QueueSize int
}

type Pool struct {
	config   PoolConfig
	tasks    chan Task
	wg       sync.WaitGroup
	stopOnce sync.Once
	started  atomic.Bool
	stopped  atomic.Bool
	inFlight atomic.Int64
	queued   atomic.Int64
}

func (c PoolConfig) withDefaults() PoolConfig {
	if c.Size < 1 {
		c.Size = 1
	}

	if c.QueueSize < 0 {
		c.QueueSize = 0
	}

	return c
}

func NewPool(config PoolConfig) *Pool {
	config = config.withDefaults()

	return &Pool{
		config: config,
		tasks:  make(chan Task, config.QueueSize),
	}
}

func (p *Pool) Start(ctx context.Context) {
	if !p.started.CompareAndSwap(false, true) {
		return
	}

	for range p.config.Size {
		p.wg.Add(1)

		go func() {
			defer p.wg.Done()
			p.run(ctx)
		}()
	}
}

func (p *Pool) run(ctx context.Context) {
	for task := range p.tasks {
		p.queued.Add(-1)
		p.inFlight.Add(1)
		task(ctx)
		p.inFlight.Add(-1)
	}
}

func (p *Pool) Submit(ctx context.Context, task Task) error {
	if p.stopped.Load() {
		return ErrPoolStopped
	}

	p.queued.Add(1)

	select {
	case p.tasks <- task:
		return nil
	case <-ctx.Done():
		p.queued.Add(-1)
		return ctx.Err()
	}
}

func (p *Pool) TrySubmit(task Task) error {
	if p.stopped.Load() {
		return ErrPoolStopped
	}

	p.queued.Add(1)

	select {
	case p.tasks <- task:
		return nil
	default:
		p.queued.Add(-1)
		return ErrPoolSaturated
	}
}

func (p *Pool) Stop() {
	p.stopOnce.Do(func() {
		p.stopped.Store(true)
		close(p.tasks)
	})

	p.wg.Wait()
}

func (p *Pool) Size() int {
	return p.config.Size
}

func (p *Pool) Capacity() int {
	return p.config.QueueSize
}

func (p *Pool) InFlight() int {
	return int(p.inFlight.Load())
}

func (p *Pool) Queued() int {
	return int(p.queued.Load())
}
