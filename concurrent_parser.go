package concurrent_csv

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"
)

type ConcurrentParser struct {
	// If CSV has quote, set to true. If CSV has not quote, then set it to false
	// can improve perf. By default, it is true
	HasQuote bool

	// How many CPUs to use, by default runtime.NumCPU()
	NumCPU int

	// The following exported fields are borrowed from golang csv.Reader
	// Comma is the field delimiter.
	// It is set to comma (',') by NewReader.
	Comma rune
	// Comment, if not 0, is the comment character. Lines beginning with the
	// Comment character without preceding whitespace are ignored.
	// With leading whitespace the Comment character becomes part of the
	// field, even if TrimLeadingSpace is true.
	Comment rune
	// FieldsPerRecord is the number of expected fields per record.
	// If FieldsPerRecord is positive, Read requires each record to
	// have the given number of fields. If FieldsPerRecord is 0, Read sets it to
	// the number of fields in the first record, so that future records must
	// have the same field count. If FieldsPerRecord is negative, no check is
	// made and records may have a variable number of fields.
	FieldsPerRecord int

	TrailingComma bool // ignored; here for backwards compatibility
	// If TrimLeadingSpace is true, leading white space in a field is ignored.
	// This is done even if the field delimiter, Comma, is white space.
	TrimLeadingSpace bool
	// contains filtered or unexported fields

	data []byte
}

func NewConcurrentParser(data []byte) *ConcurrentParser {
	return &ConcurrentParser{
		NumCPU:   runtime.NumCPU(),
		HasQuote: true,
		Comma:    ',',
		data:     data,
	}
}

type chunkResult struct {
	sequence int
	records  [][]string
}

type bySequence []*chunkResult

func (res bySequence) Swap(i, j int) {
	res[i], res[j] = res[j], res[i]
}

func (res bySequence) Len() int {
	return len(res)
}

func (res bySequence) Less(i, j int) bool {
	return res[i].sequence < res[j].sequence
}

// ReadAll returns a list of CSV records set ([][]string) which preserves the same sequence
// as in origin data. The reason this function doesn't merge all records into one CSV record
// set is to avoid unnecessary memory reallocation
func (p *ConcurrentParser) ReadAll() ([][][]string, error) {
	chunker := NewTextChunker(p.data, int64(len(p.data)), 0, p.NumCPU, p.HasQuote)
	err := chunker.Process()
	if err != nil {
		return nil, err
	}

	recordsChan := make(chan *chunkResult)
	errsChan := make(chan error)
	done := make(chan bool)

	var results []*chunkResult
	var allErrs []error
	go func() {
		for {
			select {
			case rs := <-recordsChan:
				results = append(results, rs)
			case err = <-errsChan:
				if err != nil {
					allErrs = append(allErrs, err)
				}
			case <-done:
				return
			}
		}
	}()

	numChunk := chunker.NumChunks()
	var wg sync.WaitGroup
	for i := 0; i < numChunk; i++ {
		start, end := chunker.Chunk(i)
		if start == end {
			continue
		}

		wg.Add(1)
		go func(idx int, start, end int64) {
			defer wg.Done()
			reader := p.getCsvReader(bytes.NewReader(p.data[start:end]))
			results, err := reader.ReadAll()
			if err != nil {
				errsChan <- err
				return
			}
			res := &chunkResult{
				sequence: idx,
				records:  results,
			}
			recordsChan <- res
		}(i, start, end)
	}

	wg.Wait()
	done <- true

	if allErrs != nil && len(allErrs) > 0 {
		return nil, errors.New(fmt.Sprintf("%s", allErrs))
	}

	sort.Sort(bySequence(results))
	var records [][][]string
	for _, result := range results {
		records = append(records, result.records)
	}
	return records, nil
}

func (p *ConcurrentParser) getCsvReader(r io.Reader) *csv.Reader {
	reader := csv.NewReader(r)
	reader.Comma = p.Comma
	reader.Comment = p.Comment
	reader.FieldsPerRecord = p.FieldsPerRecord
	reader.TrailingComma = p.TrailingComma
	reader.TrimLeadingSpace = p.TrimLeadingSpace
	return reader
}
