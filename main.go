package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/tom-lepsky/pipeline/example"
)

func main() {

	var wg sync.WaitGroup
	paths := ProducePaths(&wg)
	result := ConsumeResult(&wg)
	errChan := make(chan error)

	parallelHash := 10

	pipe, err := example.HashFilePipeline(parallelHash, paths, result, errChan)
	if err != nil {
		fmt.Println(err)
		return
	}
	HandleError(&wg, errChan)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	pipe.Run(ctx)
	pipe.Wait()
	close(errChan)
	wg.Wait()
}

func ProducePaths(wg *sync.WaitGroup) []chan string {
	dataPath := path.Join(FindRoot(), "testdata")
	data := make([]chan string, 2)
	for i := 0; i < len(data); i++ {
		data[i] = make(chan string, 10)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, val := range []string{filepath.Join(dataPath, "a"), filepath.Join(dataPath, "b")} {
			data[0] <- val
		}
		close(data[0])
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, val := range []string{filepath.Join(dataPath, "undefined"), filepath.Join(dataPath, "c")} {
			data[1] <- val
		}
		close(data[1])
	}()

	return data
}

func ConsumeResult(wg *sync.WaitGroup) []chan string {
	result := make([]chan string, 1)
	for i := 0; i < len(result); i++ {
		result[i] = make(chan string, 1)
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for r := range result[idx] {
				fmt.Println(r)
			}
		}(i)
	}

	return result
}

func HandleError(wg *sync.WaitGroup, errChan <-chan error) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errChan {
			fmt.Println(err)
		}
	}()
}

func FindRoot() string {
	root, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	for {
		if _, err := os.Stat(path.Join(root, "go.mod")); err == nil {
			return root
		}
		parent := filepath.Dir(root)
		if parent == root {
			panic("go.mod not found")
		}
		root = parent
	}
}
