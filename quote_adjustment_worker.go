package concurrent_csv

type QuoteNewlineAdjustmentWorker struct {
	data                 []byte
	chunkStart           int64
	chunkEnd             int64
	numQuotes            int64
	firstUnquotedNewline int64
	firstQuotedNewline   int64
}

func NewQuoteNewlineAdjustmentWorker(data []byte, chunkStart, chunkEnd int64) *QuoteNewlineAdjustmentWorker {
	return &QuoteNewlineAdjustmentWorker{
		data:                 data,
		chunkStart:           chunkStart,
		chunkEnd:             chunkEnd,
		numQuotes:            0,
		firstUnquotedNewline: -1,
		firstQuotedNewline:   -1,
	}
}

func (w *QuoteNewlineAdjustmentWorker) Parse() {
	current := w.chunkStart
	inQuote := false

	var i int64 = 0
	nread := w.chunkEnd - w.chunkStart
	for current <= w.chunkEnd {
		for i < nread && w.firstUnquotedNewline < 0 && w.firstQuotedNewline < 0 {
			if inQuote {
				for ; i < nread; i++ {
					if w.data[i] == '"' {
						w.numQuotes++
						inQuote = false
						i++
						break
					} else if w.data[i] == '\n' {
						w.firstQuotedNewline = current + i
						i++
						break
					}
				}
			} else {
				for ; i < nread; i++ {
					if w.data[i] == '"' {
						w.numQuotes++
						inQuote = true
						i++
						break
					} else if w.data[i] == '\n' {
						w.firstUnquotedNewline = current + i
						i++
						break
					}
				}
			}
		}

		for i < nread && w.firstUnquotedNewline < 0 {
			if inQuote {
				for ; i < nread; i++ {
					if w.data[i] == '"' {
						w.numQuotes++
						inQuote = false
						i++
						break
					}
				}
			} else {
				for ; i < nread; i++ {
					if w.data[i] == '"' {
						w.numQuotes++
						inQuote = true
						i++
						break
					} else if w.data[i] == '\n' {
						w.firstUnquotedNewline = current + i
						i++
						break
					}
				}
			}
		}

		for i < nread && w.firstQuotedNewline < 0 {
			if inQuote {
				for ; i < nread; i++ {
					if w.data[i] == '"' {
						w.numQuotes++
						inQuote = false
						i++
						break
					} else if w.data[i] == '\n' {
						w.firstQuotedNewline = current + i
						i++
						break
					}
				}
			} else {
				for ; i < nread; i++ {
					if w.data[i] == '"' {
						w.numQuotes++
						inQuote = true
						i++
						break
					}
				}
			}
		}

		// If we got here, then either we've found both the first quoted newline and
		// unquoted newline, or we've processed all the data in the buffer.
		for i < nread {
			if inQuote {
				for ; i < nread; i++ {
					if w.data[i] == '"' {
						w.numQuotes++
						inQuote = false
						i++
						break
					}
				}
			} else {
				for ; i < nread; i++ {
					if w.data[i] == '"' {
						w.numQuotes++
						inQuote = true
						i++
						break
					}
				}
			}
		}
		current += nread
	}
}

func (w *QuoteNewlineAdjustmentWorker) ChunkStart() int64 {
	return w.chunkStart
}

func (w *QuoteNewlineAdjustmentWorker) ChunkEnd() int64 {
	return w.chunkEnd
}

func (w *QuoteNewlineAdjustmentWorker) NumQuotes() int64 {
	return w.numQuotes
}

func (w *QuoteNewlineAdjustmentWorker) FirstQuotedNewline() int64 {
	return w.firstQuotedNewline
}

func (w *QuoteNewlineAdjustmentWorker) FirstUnquotedNewline() int64 {
	return w.firstUnquotedNewline
}

func (w *QuoteNewlineAdjustmentWorker) Clear() {
	w.chunkStart = 0
	w.chunkEnd = 0
	w.numQuotes = 0
	w.firstUnquotedNewline = 0
	w.firstQuotedNewline = 0
}
