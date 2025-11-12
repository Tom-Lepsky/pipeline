package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
)

// Runnable — интерфейс для объектов, которые могут быть запущены в пайплайне.
type Runnable interface {
	Run(ctx context.Context, wg *sync.WaitGroup, errChan chan<- error, commonErrChan bool)
}

// Pipeline представляет собой оркестратор для выполнения узлов в пайплайне. Поддерживает добавление нод, запуск с
// контекстом, ожидание завершения и остановку. Все ноды запускаются параллельно
type Pipeline struct {
	cancelFunc    context.CancelFunc
	wg            *sync.WaitGroup
	errChan       chan error
	errChanClosed atomic.Bool
	run           atomic.Bool
	nodes         []Runnable
}

// New создаёт новый пайплайн
func New() Pipeline {
	return Pipeline{
		wg:      &sync.WaitGroup{},
		errChan: make(chan error),
	}
}

// ErrChan получить канал для чтения ошибок
func (p *Pipeline) ErrChan() <-chan error {
	return p.errChan
}

// AddNode добавляет ноды в пайплайн. Если пайплайн уже запущен (run=true),
// добавление игнорируется
func (p *Pipeline) AddNode(n ...Runnable) {
	if p.run.Load() {
		return
	}
	p.nodes = append(p.nodes, n...)
}

// Run запускает все ноды пайплайна параллельно в контексте, производном от parentCtx
func (p *Pipeline) Run(parentCtx context.Context, commonErrors bool) {
	if !p.run.CompareAndSwap(false, true) {
		return
	}
	ctx, cancel := context.WithCancel(parentCtx)
	p.cancelFunc = cancel

	for i := 0; i < len(p.nodes); i++ {
		p.nodes[i].Run(ctx, p.wg, p.errChan, commonErrors)
	}
}

// Wait ожидает завершения всех нод.
// Блокирует вызывающую горутину до полного завершения пайплайна.
func (p *Pipeline) Wait() {
	if !p.run.Load() {
		return
	}
	p.wg.Wait()
	if p.errChanClosed.CompareAndSwap(false, true) {
		close(p.errChan)
	}
	p.run.Store(false)
}

// Stop останавливает пайплайн.
func (p *Pipeline) Stop() {
	if p.run.CompareAndSwap(true, false) {
		if p.cancelFunc != nil {
			p.cancelFunc()
		}

		p.wg.Wait()
		if p.errChanClosed.CompareAndSwap(false, true) {
			close(p.errChan)
		}
	}
}
