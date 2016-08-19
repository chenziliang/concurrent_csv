package concurrent_csv

import (
	"errors"
	"fmt"
	"sync"
)

type TextChunker struct {
	data                []byte
	length              int64 // length in data
	maximumChunks       int
	allowQuotedNewlines bool
	startingOffset      int64
	startOfChunk        []int64
	endOfChunk          []int64
}

func NewTextChunker(
	data []byte, length int64, startingOffset int64,
	maximumChunks int, allowQuotedNewlines bool) *TextChunker {
	return &TextChunker{
		data:                data,
		length:              length,
		maximumChunks:       maximumChunks,
		allowQuotedNewlines: allowQuotedNewlines,
	}
}

// Process computes the boundaries of the text chunks.
// param filename          The text filename to open to computer offsets.
// param starting_offset   The starting offset of the first chunk.
// param maximum_chunks    The maximum number of chunks. The number of chunks
// will be as close to this number as possible.

func (chunker *TextChunker) Process() error {
	return chunker.computeOffsets(chunker.allowQuotedNewlines)
}

// NumChunks Returns the number of chunks determined by this chunker.

func (chunker *TextChunker) NumChunks() int {
	return len(chunker.startOfChunk)
}

// Chunk Returns the (start, end) boundaries of a specific chunk. The ending
// index is always inclusive.

func (chunker *TextChunker) Chunk(index int) (int64, int64) {
	return chunker.startOfChunk[index], chunker.endOfChunk[index]
}

func minInt64(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (chunker *TextChunker) computeOffsets(allowQuotedNewlines bool) error {
	chunkSize := (chunker.length - chunker.startingOffset) / int64(chunker.maximumChunks)
	startOfChunk := chunker.startingOffset

	for workerId := 0; workerId < chunker.maximumChunks; workerId++ {
		endOfChunk := minInt64(chunker.length, startOfChunk+chunkSize)
		if workerId == chunker.maximumChunks-1 {
			endOfChunk = chunker.length
		}

		chunker.startOfChunk = append(chunker.startOfChunk, startOfChunk)
		chunker.endOfChunk = append(chunker.endOfChunk, endOfChunk)

		startOfChunk = minInt64(chunker.length, endOfChunk)
	}

	if chunker.allowQuotedNewlines {
		return chunker.adjustOffsetsAccordingToQuotedNewlines()
	} else {
		return chunker.adjustOffsetsAccordingToUnquotedNewlines()
	}
}

func (chunker *TextChunker) adjustOffsetsAccordingToUnquotedNewlines() error {
	for workerId, _ := range chunker.startOfChunk {
		if chunker.startOfChunk[workerId] == chunker.endOfChunk[workerId] {
			continue
		}

		newEnd := chunker.endOfChunk[workerId]
		for ; newEnd < chunker.length; newEnd++ {
			if chunker.data[newEnd] == '\n' {
				break
			}
		}

		chunker.endOfChunk[workerId] = newEnd

		// Adjust the start/end offset according to the new end
		for otherWorkerId := workerId + 1; otherWorkerId < len(chunker.startOfChunk); otherWorkerId++ {
			if chunker.startOfChunk[otherWorkerId] < newEnd && chunker.endOfChunk[otherWorkerId] < newEnd {
				// "\n" pass over a full chunk, shrink the passed over chunk to 0
				chunker.startOfChunk[otherWorkerId] = 0
				chunker.endOfChunk[otherWorkerId] = 0
			} else if chunker.startOfChunk[otherWorkerId] < newEnd {
				chunker.startOfChunk[otherWorkerId] = newEnd
				chunker.endOfChunk[otherWorkerId] = maxInt64(chunker.endOfChunk[otherWorkerId], newEnd)
			}
		}
	}
	return nil
}

func (chunker *TextChunker) doAdjustOffsetsAccordingToQuotedNewlines() []*QuoteNewlineAdjustmentWorker {
	var sg sync.WaitGroup
	var workers []*QuoteNewlineAdjustmentWorker
	for workerId := 0; workerId < chunker.maximumChunks; workerId++ {
		sg.Add(1)
		worker := NewQuoteNewlineAdjustmentWorker(
			chunker.data, chunker.startOfChunk[workerId], chunker.endOfChunk[workerId])
		workers = append(workers, worker)

		go func(w *QuoteNewlineAdjustmentWorker) {
			defer sg.Done()
			w.Parse()
		}(worker)
	}
	sg.Wait()

	return workers
}

func (chunker *TextChunker) adjustOffsetsAccordingToQuotedNewlines() error {
	workers := chunker.doAdjustOffsetsAccordingToQuotedNewlines()

	currWorkerId := 0
	nextWorkerId := 1
	quotesSoFar := workers[currWorkerId].NumQuotes()

	for currWorkerId < len(workers) {
		if chunker.endOfChunk[currWorkerId] == chunker.startOfChunk[currWorkerId] {
			// Ignore empty range chunker
			chunker.startOfChunk[currWorkerId] = 0
			chunker.endOfChunk[currWorkerId] = 0
			currWorkerId++
			nextWorkerId = currWorkerId + 1
			continue
		}

		if quotesSoFar%2 == 0 {
			if nextWorkerId < len(workers) {
				quotesSoFar += workers[nextWorkerId].NumQuotes()
				if workers[nextWorkerId].FirstUnquotedNewline() >= 0 {
					chunker.endOfChunk[currWorkerId] = workers[nextWorkerId].FirstUnquotedNewline()
					chunker.startOfChunk[nextWorkerId] = minInt64(
						chunker.endOfChunk[nextWorkerId], chunker.endOfChunk[currWorkerId]+1)
					currWorkerId = nextWorkerId
				} else {
					chunker.endOfChunk[currWorkerId] = chunker.endOfChunk[nextWorkerId]
					chunker.startOfChunk[nextWorkerId] = 0
					chunker.endOfChunk[nextWorkerId] = 0
				}
				nextWorkerId++
			} else {
				chunker.endOfChunk[currWorkerId] = chunker.length - 1
				break
			}
		} else {
			if nextWorkerId < len(workers) {
				quotesSoFar += workers[nextWorkerId].NumQuotes()
				if workers[nextWorkerId].FirstQuotedNewline() >= 0 {
					chunker.endOfChunk[currWorkerId] = workers[nextWorkerId].FirstQuotedNewline()
					chunker.startOfChunk[nextWorkerId] = minInt64(
						chunker.endOfChunk[nextWorkerId], chunker.endOfChunk[currWorkerId]+1)
					currWorkerId = nextWorkerId
				} else {
					chunker.endOfChunk[currWorkerId] = chunker.endOfChunk[nextWorkerId]
					chunker.startOfChunk[nextWorkerId] = 0
					chunker.endOfChunk[nextWorkerId] = 0
				}
				nextWorkerId++
			} else {
				return errors.New(fmt.Sprintf("The file ends with an open quote (%d)", quotesSoFar))
			}
		}
	}

	return nil
}
