package concurrent_csv

import (
	"bytes"
	"errors"
	"encoding/csv"
	"fmt"
	"runtime"
	"sync"
)

type ConcurrentReader struct {
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
    // If LazyQuotes is true, a quote may appear in an unquoted field and a
    // non-doubled quote may appear in a quoted field.
    LazyQuotes    bool
    TrailingComma bool // ignored; here for backwards compatibility
    // If TrimLeadingSpace is true, leading white space in a field is ignored.
    // This is done even if the field delimiter, Comma, is white space.
    TrimLeadingSpace bool
    // contains filtered or unexported fields

	data []byte
}

type params struct {
	cpu int
	minBatchSize int
	buffer []byte
	bufferCap int
	bufferLen int
	recordsChan chan [][]string
	errs chan error
}

func NewConcurrentReader(data []byte) *ConcurrentReader {
	return &ConcurrentReader{
		data: data,
	}
}

func findRecordBoundary(startPos, endPos int, buffer []byte) int {
	for endPos--; endPos > startPos; endPos-- {
		if buffer[endPos] == '\n' {
			// FIXME me, '\n' in quote
			return endPos
		}
	}
	return -1
}

func divideInBatches(p *params) ([][]byte, int) {
	var batches [][]byte
	if p.bufferLen <= p.minBatchSize {
		batches = append(batches, p.buffer[0:p.bufferLen])
		return batches, p.bufferLen
	}

	batchCount := p.bufferLen / p.minBatchSize
	if batchCount > p.cpu {
		batchCount = p.cpu
	}
	batchSize := p.bufferLen / batchCount

	startPos := 0
	endPos := startPos + batchSize
	for ; endPos <= p.bufferLen; {
		endPos = findRecordBoundary(startPos, endPos, p.buffer)
		if endPos < 0 {
			// the record has been broken, acculumate to next round
			break
		}
		startPos = endPos + 2
		endPos = startPos + batchSize
		if endPos > p.bufferLen {
			endPos = p.bufferLen
			batches = append(batches, p.buffer[startPos: endPos])
		} else {
			batches = append(batches, p.buffer[startPos: endPos + 1])
		}
	}

	if endPos < 0 {
		return batches, startPos
	} else {
		return batches, endPos
	}
}

func readRecords(batch []byte, wg *sync.WaitGroup, p *params) {
	defer wg.Done()

	reader := csv.NewReader(bytes.NewReader(batch))
	records, err := reader.ReadAll()
	if err != nil {
		p.errs <- err
		return
	}

	if records != nil && len(records) > 0 {
		p.recordsChan <- records
	}
}

func concurrentReadRecords(p *params) int {
	var wg sync.WaitGroup
	batches, offset := divideInBatches(p)
	for _, batch := range batches {
		wg.Add(1)
		go readRecords(batch, &wg, p)
	}

	if len(batches) > 0 {
		wg.Wait()
	}

	// Move the broken record to at the begining of the buffer
	for i := 0; i < p.bufferLen - offset; i++ {
		p.buffer[i] = p.buffer[offset + i]
	}
	return p.bufferLen - offset
}

func (r *ConcurrentReader) ReadAll() ([][]string, error) {
	chunker := NewTextChunker(r.data, int64(len(r.data)), 0, runtime.NumCPU(), true)
	err := chunker.Process()
	if err != nil {
		return nil, err
	}

	recordsChan := make(chan [][]string)
	errsChan := make(chan error)
	done := make(chan bool)

	var records [][]string
	var allErrs []error
	go func() {
		for {
			select {
			case rs := <-recordsChan:
				records = append(records, rs...)
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
		fmt.Printf("%d: [%d, %d]\n", i, start, end)

		wg.Add(1)
		go func(idx int, start, end int64) {
			defer wg.Done()
			reader := csv.NewReader(bytes.NewReader(r.data[start:end]))
			results, err := reader.ReadAll()
			if err != nil {
				fmt.Printf("%d, error=%s", idx, err)
				errsChan <- err
				return
			}
			recordsChan <- results
		}(i, start, end)
	}

	wg.Wait()
	done <- true

	if allErrs != nil && len(allErrs) > 0 {
		return nil, errors.New(fmt.Sprintf("%s", allErrs))
	}
	return records, nil
}
