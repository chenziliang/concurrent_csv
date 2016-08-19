package main

import (
	"fmt"
	"github.com/chenziliang/concurrent_csv"
	"io/ioutil"
)

func main() {
	data, err := ioutil.ReadFile("test.csv2")
	if err != nil {
		panic(err)
	}

	chunker := concurrent_csv.NewTextChunker(data, int64(len(data)), 0, 8, true)
	err = chunker.Process()
	if err != nil {
		panic(err)
	}

	numChunks := chunker.NumChunks()
	for i, j := 0, 0; i < numChunks; i++ {
		start, end := chunker.Chunk(i)
		if start == end {
			continue
		}
		j++
		filename := fmt.Sprintf("chunk%d", j)
		ioutil.WriteFile(filename, data[start:end], 0x600)
	}
}
