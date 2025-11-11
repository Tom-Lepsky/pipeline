package example

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"

	"github.com/tom-lepsky/pipeline/pipeline"
	"github.com/tom-lepsky/pipeline/pipeline/node"
)

// HashFilePipeline пайплайн для обхода заданных директорий и подсчета md5 хешей
func HashFilePipeline(parallelHash int, paths []chan string, result []chan string, errChan chan error) (*pipeline.Pipeline, error) {
	// создаём узел для обхода директорий и привязываем к нему входы с потоком директорий
	buffSize := make([]int, parallelHash)
	for i := range buffSize {
		buffSize[i] = 1
	}
	pathWalkerNode := node.New[string, string]("Path walker", 2, parallelHash, buffSize, PathReceiver)
	err := pathWalkerNode.AutowireInput(paths...)
	if err != nil {
		return nil, err
	}

	// создаем узел для объединения результатов параллельного подсчета хешей в 1 канал
	// и привязываем результирующий канал(клиентский) к выходу ноды
	demuxNode := node.New[string, string]("Demux", parallelHash, 1, []int{1}, Demux)
	err = demuxNode.AutowireOutput(result...)
	if err != nil {
		return nil, err
	}

	// создаём узлы параллельно подсчитывающие хеши файлов и привязываем их выходы к демультиплексору
	hasherNodes := make([]*node.Node[string, string], 0, parallelHash)
	for i := 0; i < parallelHash; i++ {
		h := node.New[string, string](fmt.Sprintf("Hasher %d", i), 1, 1, []int{1}, Hasher)
		err := node.Autowire(&h, &demuxNode)
		if err != nil {
			return nil, err
		}
		hasherNodes = append(hasherNodes, &h)
	}

	// Привязываем ноды вычисляющие хеши к узлу, обходящему папки
	err = node.Autowire(&pathWalkerNode, hasherNodes...)
	if err != nil {
		return nil, err
	}

	runnable := make([]pipeline.Runnable, 0, parallelHash)
	for i := 0; i < parallelHash; i++ {
		runnable = append(runnable, hasherNodes[i])
	}

	// Создаем пайплайн и добавляем в него все узлы
	pipe := pipeline.New(errChan)
	pipe.AddNode(&pathWalkerNode)
	pipe.AddNode(runnable...)
	pipe.AddNode(&demuxNode)

	return &pipe, nil
}

func PathReceiver(ctx context.Context, input <-chan string, output chan<- string, errChan chan<- error) {
	defer close(output)
	for path := range input {
		select {
		case <-ctx.Done():
			errChan <- ctx.Err()
			return
		default:
			dirWalk(ctx, path, output, errChan)
		}
	}
}

func dirWalk(ctx context.Context, path string, output chan<- string, errChan chan<- error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		errChan <- err
		return
	}

	if !fileInfo.IsDir() {
		errChan <- fmt.Errorf("%s is not a directory", path)
		return
	}

	queue := make([]string, 0, 20)
	queue = append(queue, path)
	for len(queue) > 0 {
		path = queue[0]
		queue = queue[1:]

		directory, err := os.ReadDir(path)
		if err != nil {
			errChan <- err
			continue
		}

		for _, file := range directory {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				fullPath := filepath.Join(path, file.Name())
				if file.IsDir() {
					queue = append(queue, fullPath)
				} else {
					output <- fullPath
				}
			}
		}
	}
}

func Hasher(ctx context.Context, input <-chan string, output chan<- string, errChan chan<- error) {
	defer close(output)
	for path := range input {
		select {
		case <-ctx.Done():
			errChan <- ctx.Err()
			return
		default:
			md5Hash(ctx, path, output, errChan)
		}

	}
}

func md5Hash(ctx context.Context, path string, output chan<- string, errChan chan<- error) {
	var (
		file []byte
		err  error
	)
	select {
	case <-ctx.Done():
		errChan <- ctx.Err()
		return
	default:
		file, err = os.ReadFile(path)
		if err != nil {
			errChan <- err
			return
		}
	}

	select {
	case <-ctx.Done():
		errChan <- ctx.Err()
		return
	default:
		hash := md5.Sum(file)
		output <- fmt.Sprintf("%s: %x", path, hash)
	}
}

func Demux(ctx context.Context, input <-chan string, output chan<- string, errChan chan<- error) {
	defer close(output)
	for in := range input {
		select {
		case <-ctx.Done():
			errChan <- ctx.Err()
			break
		default:
			output <- in
		}

	}
}
