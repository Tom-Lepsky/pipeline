package util

import (
	"context"
	"sync"
)

// FanIn объединяет несколько каналов входа в один выходной канал. Выходной
// канал закрывается автоматически после того, как все входные каналы закрыты.
// Если входных каналов 0, возвращает nil. Буфер выходного канала равен количеству входов.
func FanIn[T any](ctx context.Context, inputs ...<-chan T) <-chan T {
	l := len(inputs)
	if l == 0 {
		return nil
	}

	out := make(chan T, l)
	var wg sync.WaitGroup
	for _, input := range inputs {
		wg.Add(1)
		go func(ch <-chan T) {
			defer wg.Done()

			for {
				select {
				case val, ok := <-ch:
					if !ok {
						return
					}
					select {
					case out <- val:
					case <-ctx.Done():
						return
					}

				case <-ctx.Done():
					return
				}
			}
		}(input)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// FanOut распределяет значения из входного канала по нескольким выходным каналам в
// round-robin режиме (поочерёдно). Если канал блокируется, переходит к следующему.
// Если контекст отменён, распределение прекращается. Выходные каналы закрываются
// автоматически после закрытия входного канала. Если выходных каналов 0, возвращает nil.
// Буфер входного канала равен количеству выходов.
func FanOut[T any](ctx context.Context, outputs ...chan<- T) chan<- T {
	l := len(outputs)
	if l == 0 {
		return nil
	}

	out := make(chan T, l)
	go func() {
		defer func() {
			for _, output := range outputs {
				close(output)
			}
		}()

		currChanIdx := 0
		for {
			select {
			case val, ok := <-out:
				if !ok {
					return
				}

				send := false
				for i := 0; i < l; i++ {
					select {
					case outputs[currChanIdx] <- val:
						send = true
					default:
					}
					currChanIdx = (currChanIdx + 1) % l
					if send {
						break
					}
				}

				if !send {
					select {
					case outputs[currChanIdx] <- val:
						currChanIdx = (currChanIdx + 1) % l
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}
