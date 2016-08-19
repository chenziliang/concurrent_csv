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

	r := concurrent_csv.NewConcurrentReader(data)
	start := time.Now().UnixNano()
	rows, err := r.ReadAll()
	if err != nil {
		fmt.Println(err)
	}
	cost := time.Now().UnixNano() - start
	/*for _, row := range rows {
		fmt.Printf("%s\n", row)
	}*/
	fmt.Printf("bytes.Reader, rows=%d, cost=%.5g seconds\n", len(rows), float64(cost)/1000000000.0)
}
