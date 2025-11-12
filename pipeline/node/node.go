package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"github.com/tom-lepsky/pipeline/pipeline/util"
)

const maxIO = 64

var (
	ErrInputIdxOutOfRange  = errors.New("input index out of range")
	ErrOutputIdxOutOfRange = errors.New("output index out of range")
	ErrInputsWired         = errors.New("all inputs are wire")
	ErrOutputsWired        = errors.New("all outputs are wire")
)

// Handler представляет собой функцию-обработчик, которая принимает контекст, канал входных данных,
// канал выходных данных и канал для ошибок. Обработчик должен читать из input, писать в output
// и отправлять ошибки в errChan при необходимости. Закрытие каналов output и errChan ответственность клиента
type Handler[I, O any] func(ctx context.Context, input <-chan I, output chan<- O, errChan chan<- error)

// Node представляет собой базовый узел в пайплайне обработки данных. Поддерживает множественные
// входы и выходы на основе каналов.
type Node[I, O any] struct {
	name        string
	inputsMask  uint64
	outputsMask uint64
	inputs      []<-chan I
	outputs     []chan<- O
	handler     Handler[I, O]
	started     sync.Once
}

// New создаёт новый узел с заданным именем, количеством входов, выходов, опциональными буферами
// для выходных каналов и обработчиком. Паникует, если handler nil, размеры буферов не совпадают
// с количеством выходов.
func New[I, O any](name string, inputNum int, outputNum int, outputBuffSize []int, handler Handler[I, O]) Node[I, O] {
	if handler == nil {
		panic("nil handler")
	}

	if outputBuffSize != nil && len(outputBuffSize) != outputNum {
		panic("mismatch output buff size")
	}

	if inputNum > maxIO || outputNum > maxIO {
		panic("I/O out of range")
	}

	outputs := make([]chan<- O, outputNum)
	for i := 0; i < outputNum; i++ {
		bufferSize := 0
		if outputBuffSize != nil {
			bufferSize = outputBuffSize[i]
		}
		outputs[i] = make(chan O, bufferSize)
	}

	return Node[I, O]{
		name:    name,
		inputs:  make([]<-chan I, inputNum),
		outputs: outputs,
		handler: handler,
	}
}

// SetInput устанавливает канал входа по указанному индексу. Возвращает ошибку, если индекс
// выходит за пределы количества входов.
func (n *Node[I, O]) SetInput(idx int, input <-chan I) error {
	if idx < 0 || idx >= len(n.inputs) {
		return n.wrapError(ErrInputIdxOutOfRange)
	}

	n.inputs[idx] = input
	n.occupyInput(idx)

	return nil
}

// SetOutput устанавливает канал выхода по указанному индексу и помечает его как занятый.
// Возвращает ошибку, если индекс выходит за пределы количества выходов.
func (n *Node[I, O]) SetOutput(idx int, output chan<- O) error {
	if idx < 0 || idx >= len(n.outputs) {
		return n.wrapError(ErrOutputIdxOutOfRange)
	}

	n.outputs[idx] = output
	n.occupyOutput(idx)

	return nil
}

// occupyOutput помечает вход по индексу как занятый в маске
func (n *Node[I, O]) occupyInput(idx int) {
	n.inputsMask = setBit(n.inputsMask, idx)
}

// occupyOutput помечает выход по индексу как занятый в маске
func (n *Node[I, O]) occupyOutput(idx int) {
	n.outputsMask = setBit(n.outputsMask, idx)
}

// setBit устанавливает бит
func setBit(mask uint64, idx int) uint64 {
	mask |= 1 << uint(idx)
	return mask
}

// vacantInput возвращает индекс первого свободного входа или -1, если все заняты
func (n *Node[I, O]) vacantInput() int {
	for i := 0; i < len(n.inputs); i++ {
		if (n.inputsMask & (1 << uint(i))) == 0 {
			return i
		}
	}
	return -1
}

// AutowireInput подключает предоставленные каналы входа к первым свободным слотам.
// Возвращает ошибку, если все входы уже подключены или произошла ошибка установки.
func (n *Node[I, O]) AutowireInput(input ...chan I) error {
	for i := 0; i < len(input); i++ {
		inIdx := n.vacantInput()
		if inIdx == -1 {
			return n.wrapError(ErrInputsWired)
		}

		err := n.SetInput(inIdx, input[i])
		if err != nil {
			return n.wrapError(err)
		}
	}
	return nil
}

// vacantOutput возвращает индекс первого свободного выхода или -1, если все заняты
func (n *Node[I, O]) vacantOutput() int {
	for i := 0; i < len(n.outputs); i++ {
		if (n.outputsMask & (1 << uint(i))) == 0 {
			return i
		}
	}
	return -1
}

// AutowireOutput подключает предоставленные каналы выхода к первым свободным слотам.
// Возвращает ошибку, если все выходы уже подключены или произошла ошибка установки.
func (n *Node[I, O]) AutowireOutput(output ...chan O) error {
	for i := 0; i < len(output); i++ {
		outIdx := n.vacantOutput()
		if outIdx == -1 {
			return n.wrapError(ErrOutputsWired)
		}

		err := n.SetOutput(outIdx, output[i])
		if err != nil {
			return n.wrapError(err)
		}
	}
	return nil
}

// Run запускает обработчик узла в горутине.
// Паникует, если какой-то вход не подключен. Запуск происходит только один раз (sync.Once).
// ВАЖНО: Закрытие каналов output лежит на ответственности реализатора handler
func (n *Node[I, O]) Run(ctx context.Context, wg *sync.WaitGroup, errChan chan<- error) {
	for i, ch := range n.inputs {
		if ch == nil {
			panic(n.wrapError(fmt.Errorf("input %d: unused", i)))
		}
	}

	n.started.Do(func() {
		wg.Add(1)
		go func() {
			defer wg.Done()

			var input <-chan I
			if len(n.inputs) == 1 {
				input = n.inputs[0]
			} else {
				input = util.FanIn(ctx, n.inputs...)
			}

			var output chan<- O
			if len(n.outputs) == 1 {
				output = n.outputs[0]
			} else {
				output = util.FanOut(ctx, n.outputs...)
			}

			proxyErr := n.proxyErrChan(wg, errChan)
			defer close(proxyErr)
			n.handler(ctx, input, output, proxyErr)
		}()
	})
}

// Connect подключает выход from[outIdx] к входу to[inIdx]
// Помечает выход как занятый. Возвращает ошибку, если индексы неверны.
func Connect[I, O, T any](from *Node[I, O], outIdx int, to *Node[O, T], inIdx int) error {
	if outIdx < 0 || outIdx >= len(from.outputs) {
		return from.wrapError(ErrOutputIdxOutOfRange)
	}

	if inIdx < 0 || inIdx >= len(to.inputs) {
		return to.wrapError(ErrInputIdxOutOfRange)
	}

	to.inputs[inIdx] = toBidirectional(from.outputs[outIdx])
	to.occupyInput(inIdx)
	from.occupyOutput(outIdx)

	return nil
}

// toBidirectional выполняет каст chan<- T в chan T для bidirectional использования
func toBidirectional[T any](ch chan<- T) chan T {
	return *(*chan T)(unsafe.Pointer(&ch))
}

// Autowire автоматически подключает свободные выходы from к свободным входам to-узлов.
// Возвращает ошибку, если выходы/входы исчерпаны или произошла ошибка подключения.
func Autowire[I, O, T any](from *Node[I, O], to ...*Node[O, T]) error {
	for i := 0; i < len(to); i++ {
		outIdx := from.vacantOutput()
		if outIdx == -1 {
			return from.wrapError(ErrOutputsWired)
		}

		inIdx := to[i].vacantInput()
		if inIdx == -1 {
			return to[i].wrapError(ErrInputsWired)
		}

		err := Connect(from, outIdx, to[i], inIdx)
		if err != nil {
			return from.wrapError(err)
		}
	}
	return nil
}

// wrapError оборачивает ошибку в префикс с именем узла для удобства отладки
func (n *Node[I, O]) wrapError(err error) error {
	return fmt.Errorf("[%s] %w", n.name, err)
}

// proxyErrChan декоратор для ошибок
func (n *Node[I, O]) proxyErrChan(wg *sync.WaitGroup, errChan chan<- error) chan<- error {
	proxy := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range proxy {
			errChan <- n.wrapError(err)
		}
	}()

	return proxy
}
