package csv

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/gabriel-vasile/mimetype/internal/util"
	"io"
)

const (
	svLineLimit = 10
	quote       = '"'
	comment     = '#'
)

type detectState struct {
	// source
	delimiter byte
	lineLimit int

	// read state
	prev                                                                                                                     *byte
	cur                                                                                                                      byte
	next                                                                                                                     *byte
	lineSize, csvLineIdx, quoteCount                                                                                         int
	sawCsvDataOnCurrentLine, isWithinInferredQuote, isWithinExplicitQuote, isWithinComment, nextIsFieldTerminator, isNewline bool
	recordFields                                                                                                             map[int]int

	// conclusion
	complete bool
	invalid  bool
}

type slidingWindow struct {
	reader     io.Reader
	bufSize    int
	lookAhead  int
	lookBehind int
	buf        []byte
	window     []byte
	firstIter  bool
	start      int
	end        int
}

func newSlidingWindow(reader io.Reader, bufSize, lookAhead, lookBehind int) *slidingWindow {
	if lookAhead <= 0 {
		lookAhead = 3
	}
	if lookBehind <= 0 {
		lookBehind = 1
	}

	return &slidingWindow{
		reader:     reader,
		bufSize:    bufSize,
		lookAhead:  lookAhead,
		lookBehind: lookBehind,
		buf:        make([]byte, bufSize),
		window:     make([]byte, bufSize+lookAhead+lookBehind),
		firstIter:  true,
		start:      0,
		end:        0,
	}
}

func (sw *slidingWindow) Process(processFunc func(buf []byte, i, length int) int) error {
	var offset int
	for {
		// Read into buffer
		n, err := sw.reader.Read(sw.buf)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		if n == 0 {
			break
		}

		// Move the valid range to the start of the window if necessary
		if sw.start > 0 {
			copy(sw.window, sw.window[sw.start:sw.end])
			sw.end -= sw.start
			sw.start = 0
		}

		// Append the new read bytes to the sliding window
		copy(sw.window[sw.end:], sw.buf[:n])
		sw.end += n

		// Process the combined buffer
		i := sw.start
		if sw.firstIter {
			i = 0
			sw.firstIter = false
		} else {
			i = sw.lookBehind
		}

		//fmt.Printf("start...\n")
		for ; i < sw.end-sw.lookAhead; i++ {
			//fmt.Printf("  ")
			offset = processFunc(sw.window, i, sw.end)
			//fmt.Printf("offset=%d start=%d end=%d\n", offset, sw.start, sw.end)
			i += offset
		}

		sw.start = i - 1

		// Check if we are done reading
		if errors.Is(err, io.EOF) {
			break
		}
	}

	//fmt.Printf("offset=%d\n", offset)

	sw.start += 1

	// Process any remaining bytes in the sliding window
	//if sw.end > sw.start {
	for i := sw.start; i < sw.end; i++ {
		//fmt.Printf("* ")
		offset = processFunc(sw.window, i, sw.end)
		//fmt.Printf("offset=%d start=%d end=%d\n", offset, sw.start, sw.end)
		i += offset
	}
	//}

	return nil
}

// Detect takes raw bytes and indicates if it is a CSV file (or other given value-delimited file). This reads up
// to the given limit of bytes to make a determination, validating no further than the first 10 lines of the file.
func Detect(raw []byte, delimiter byte, limit uint32) bool {
	//return svStdlib(raw, rune(delimiter), limit)
	lineLimit := svLineLimit
	if limit > 0 {
		lineLimit = -1
	}
	reader := prepSvReader(raw, limit)
	state := newDetectState(delimiter, lineLimit)
	window := newSlidingWindow(reader, 1024, 3, 1)

	if err := window.Process(state.read); err != nil {
		panic("errg")
		return false
	}

	// treat rows with missing newline as valid lines
	state.resetLine()

	return state.isValidCSV()
}

func newDetectState(delimiter byte, lineLimit int) *detectState {
	return &detectState{
		delimiter:    delimiter,
		lineLimit:    lineLimit,
		recordFields: make(map[int]int, lineLimit),
	}
}

func byteStr(b *byte) string {
	if b == nil {
		return " nil"
	}

	if *b == '"' {
		return `  " `
	}

	return fmt.Sprintf("%4s", fmt.Sprintf("%q", *b))
}

func (d *detectState) read(buf []byte, i, n int) int {
	if d.complete {
		return 0
	}

	if i < 0 {
		return i * -1
	}

	d.cur = buf[i]

	if i > 0 {
		d.prev = &buf[i-1]
	} else {
		d.prev = nil
	}

	if i < n-1 {
		d.next = &buf[i+1]
	} else {
		d.next = nil
	}

	isNoNext := d.next == nil

	{
		var nextNext *byte
		if i < n-2 {
			nextNext = &buf[i+2]
		} else {
			nextNext = nil
		}

		//fmt.Printf("%d/%d   d.prev: %s  d.cur: %s  d.next: %s  nextNext: %s  ...  ", i, n, byteStr(d.prev), byteStr(&d.cur), byteStr(d.next), byteStr(nextNext))

		isNextLinuxNewline := d.cur != '\r' && isByte(d.next, '\n')
		isNextWindowsNewline := isByte(d.next, '\r') && isByte(nextNext, '\n')
		isNextDelimiter := isByte(d.next, d.delimiter)
		d.nextIsFieldTerminator = isNextLinuxNewline || isNextWindowsNewline || isNextDelimiter || isNoNext
	}

	isLinuxNewline := d.cur == '\n' && !isByte(d.prev, '\r')
	isWindowsNewline := d.cur == '\r' && isByte(d.next, '\n')
	d.isNewline = isLinuxNewline || isWindowsNewline

	// edge case from stdlib csv reader: drop trailing carriage returns
	if d.cur == '\r' && isByte(d.prev, '\n') && isNoNext {
		// skip processing the trailing carriage return
		return 0
	}

	if !d.isNewline {
		d.lineSize++
	} else {
		d.handleNewline()
		if isWindowsNewline {
			return 1 // don't process \n if we're on the \r
		}
		return 0
	}

	return d.processLineChar(i)
}

func isByte(b *byte, c byte) bool {
	if b == nil {
		return false
	}
	return *b == c
}

func (d *detectState) handleNewline() {
	if d.isWithinExplicitQuote {
		// newlines within quotes are valid
		return
	}
	if d.lineLimit > 0 && d.csvLineIdx >= d.lineLimit {
		// we've processed as much data as we're allowed to consider
		d.markComplete()
		return
	}

	if !d.isWithinComment && d.lineSize > 0 && !d.lineHasData() {
		// this should have been a csv line, but we saw content without a delimiter that was not in a comment
		d.markInvalid()
		return
	}

	// iterate to next line...
	d.resetLine()
	return
}

func (d *detectState) processLineChar(i int) int {
	switch {
	case d.cur == quote:
		return d.handleQuote(i)

	case !d.isWithinComment:
		d.handleDataCharacter()
	}
	return 0
}

func (d *detectState) handleQuote(i int) int {
	if d.isWithinComment {
		return 0
	}

	d.startDataLine()

	if d.isWithinExplicitQuote {
		// we MIGHT be ending a quote...
		switch {
		case isByte(d.next, quote):
			// ... NOPE, this is an escape for the next quote
			// skip processing the next quote character altogether
			return 1
		default:
			if d.nextIsFieldTerminator {
				// we're ending the quote
				d.isWithinExplicitQuote = false
				d.quoteCount++ // count the discovered quote
			} else {
				// this doesn't appear to be the end of a field... so we'll treat it as if this current
				// quote was escaped
				return 0
			}

		}
	} else {
		// we're within an inferred quote
		if d.isWithinInferredQuote {
			if isByte(d.next, d.delimiter) {
				// we're ending the inferred quote
				d.isWithinInferredQuote = false
				d.quoteCount++ // count the inferred quote
				d.quoteCount++ // count the discovered quote
			}
			// we're escaping this quote (don't count it)
		} else {
			// we're starting a quote
			d.isWithinExplicitQuote = true
			d.isWithinInferredQuote = false
			d.quoteCount++ // count the discovered quote
		}
	}

	if d.isWithinExplicitQuote || d.isWithinInferredQuote {
		return 0
	}

	// quotes should either encapsulate a field entirely or there be only a single quote within the field
	switch {
	case d.nextIsFieldTerminator:
	default:
		// we found a field that the quote encapsulation is not correct (e.g. ...,"something"else,... )
		d.markInvalid()
		return 0
	}
	return 0
}

func (d *detectState) markInvalid() {
	d.complete = true
	d.invalid = true
}

func (d *detectState) markComplete() {
	d.complete = true
}

func (d detectState) lineHasData() bool {
	_, ok := d.recordFields[d.csvLineIdx]
	return ok
}

func (d *detectState) resetLine() {
	d.isWithinInferredQuote = false
	d.quoteCount = 0

	if d.sawCsvDataOnCurrentLine {
		d.csvLineIdx++
	}

	d.sawCsvDataOnCurrentLine = false
	d.lineSize = 0
	d.isWithinComment = false
}

func (d *detectState) handleDataCharacter() {
	switch {
	case d.cur == comment && !d.isWithinExplicitQuote && !d.isWithinInferredQuote:
		d.isWithinComment = true

	case d.cur == d.delimiter:
		if !d.isWithinExplicitQuote {
			d.newField()
		}
	default:
		// we've seen a non-delimiter, so we know this is a data row... but we can't count this as a field until we see the first delimiter
		if d.startDataLine() {
			if d.cur != quote {
				d.isWithinInferredQuote = true
			}
		}
	}
}

func (d *detectState) newField() {
	d.isWithinInferredQuote = false

	// 0: no quotes
	// 1: imbalanced quotes (this condition)
	// 2: balanced quotes
	if d.quoteCount == 1 {
		if !d.isWithinInferredQuote {
			d.markInvalid()
		}
	}

	d.quoteCount = 0

	d.incrementFields()
	if !d.isWithinInferredQuote {
		if !isByte(d.next, quote) && !d.nextIsFieldTerminator {
			// infer that we're starting with data and it's implicitly quoted (lazy quote)
			d.isWithinInferredQuote = true
		}
	}
}

func (d *detectState) incrementFields() {
	if d.recordFields[d.csvLineIdx] == 0 {
		// by definition, we've seen a delimiter, so we can count the previous field and the new field to the right
		// of the delimiter
		d.recordFields[d.csvLineIdx] = 1
	}
	d.recordFields[d.csvLineIdx]++
	if !d.sawCsvDataOnCurrentLine {
		d.sawCsvDataOnCurrentLine = true
	}
}

func (d *detectState) startDataLine() bool {
	var isNew bool
	if _, ok := d.recordFields[d.csvLineIdx]; !ok {
		// we've seen at least a single character that is not in a comment, this is a CSV row candidate
		d.recordFields[d.csvLineIdx] = 0
		isNew = true
	}
	if !d.sawCsvDataOnCurrentLine {
		d.sawCsvDataOnCurrentLine = true
	}
	return isNew
}

func (d detectState) isValidCSV() bool {
	if d.invalid {
		return false
	}

	var fieldCount int
	for _, fields := range d.recordFields {
		if fields > 0 {
			fieldCount = fields
			break
		}
	}

	var badFieldCount bool
	for _, fields := range d.recordFields {
		if fields != fieldCount {
			badFieldCount = true
			break
		}
	}

	return !badFieldCount && fieldCount > 1 && d.csvLineIdx > 1
}

func prepSvReader(in []byte, limit uint32) io.Reader {
	var reader io.Reader = bytes.NewReader(util.DropLastLine(in, limit))
	if limit > 0 {
		reader = io.LimitReader(reader, int64(limit))
	}

	return reader
}
