package main

import (
	"fmt"
	"github.com/chenziliang/concurrent_csv"
	"io/ioutil"
	"time"
)

func main() {
	data, err := ioutil.ReadFile("test.csv")
	if err != nil {
		panic(err)
	}

	r := concurrent_csv.NewConcurrentParser(data)
	start := time.Now().UnixNano()
	rowLists, err := r.ReadAll()
	if err != nil {
		panic(err)
	}

	cost := time.Now().UnixNano() - start
	total := 0
	for _, rows := range rowLists {
		total += len(rows)
	}
	fmt.Printf("bytes.Reader, rows=%d, cost=%.5g seconds\n", total, float64(cost)/1000000000.0)
}
