package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

type AdjustablePool struct {
	factory WorkerFactory
	running int32
	exiting int32
	wg      *sync.WaitGroup
	lock    sync.Mutex
	workers []*ContextWithWorker
	done    chan struct{}
}

type Task struct {
	Num int // Num is positive number or negative number
}

type WorkerFactory func(interface{}) (Worker, error)

type Worker func(ctx context.Context)

type ContextWithWorker struct {
	Ctx      context.Context
	Cancel   context.CancelFunc
	Worker   Worker
	Callback func()
}

func NewAdjustablePool(f WorkerFactory) (*AdjustablePool, error) {
	if f == nil {
		return nil, errors.New("must provide function for pool")
	}

	p := &AdjustablePool{
		factory: f,
		wg:      &sync.WaitGroup{},
		done:    make(chan struct{}),
	}

	return p, nil
}

func (p *AdjustablePool) Add(i int, v interface{}) error {
	if i <= 0 {
		return nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	for ; i > 0; i-- {
		worker, err := p.factory(v)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithCancel(context.Background())
		cw := &ContextWithWorker{
			Ctx:    ctx,
			Cancel: cancel,
			Worker: worker,
		}

		p.workers = append(p.workers, cw)

		p.wg.Add(1)
		go func() {
			defer func() {
				atomic.AddInt32(&p.running, -1)
				if cw.Callback != nil {
					cw.Callback()
				}
				p.wg.Done()
			}()

			atomic.AddInt32(&p.running, 1)

			worker(ctx)
		}()
	}
	return nil
}

func (p *AdjustablePool) Reduce(i int) error {
	if i <= 0 {
		return nil
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	for ; i > 0 && len(p.workers) > 0; i-- {
		k := len(p.workers) - 1
		worker := p.workers[k]
		worker.Callback = func() {
			atomic.AddInt32(&p.exiting, -1)
		}
		worker.Cancel()
		atomic.AddInt32(&p.exiting, 1)
		p.workers = append(p.workers[:k], p.workers[k+1:]...)
	}
	return nil
}

func (p *AdjustablePool) Running() int {
	return int(atomic.LoadInt32(&p.running))
}

func (p *AdjustablePool) Exiting() int {
	return int(atomic.LoadInt32(&p.exiting))
}

func (p *AdjustablePool) isStopped() bool {
	select {
	case <-p.done:
		return true
	default:
	}
	return false
}

func (p *AdjustablePool) Stop() {
	if p.isStopped() {
		return
	}
	close(p.done)

	_ = p.Reduce(len(p.workers))

	p.wg.Wait()
}
