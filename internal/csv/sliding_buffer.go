package csv

import (
	"errors"
	"io"
)

type slidingBuffer struct {
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

func newSlidingBuffer(reader io.Reader, bufSize, lookAhead, lookBehind int) *slidingBuffer {
	if lookAhead <= 0 {
		lookAhead = 3
	}
	if lookBehind <= 0 {
		lookBehind = 1
	}
	if bufSize <= 0 {
		bufSize = 1024
	}

	return &slidingBuffer{
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

func (sw *slidingBuffer) iterate(processFunc func(buf []byte, i, length int) (int, error)) error {
	var offset, n int
	var procErr, readErr error
	for {
		// read into buffer
		n, readErr = sw.reader.Read(sw.buf)
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return readErr
		}

		if n == 0 {
			break
		}

		// move the valid range to the start of the window if necessary
		if sw.start > 0 {
			copy(sw.window, sw.window[sw.start:sw.end])
			sw.end -= sw.start
			sw.start = 0
		}

		// append the new read bytes to the sliding window
		copy(sw.window[sw.end:], sw.buf[:n])
		sw.end += n

		// process the combined buffer
		i := sw.start
		if sw.firstIter {
			i = 0
			sw.firstIter = false
		} else {
			i = sw.lookBehind
		}

		for ; i < sw.end-sw.lookAhead; i++ {
			offset, procErr = processFunc(sw.window, i, sw.end)
			if procErr != nil {
				if errors.Is(procErr, io.EOF) {
					// bail early
					return nil
				}
				// processing error
				return procErr
			}

			i += offset
		}

		sw.start = i - 1

		// check if we are done reading
		if errors.Is(readErr, io.EOF) {
			break
		}
	}

	sw.start += 1

	// process any remaining bytes in the sliding window
	for i := sw.start; i < sw.end; i++ {

		offset, procErr = processFunc(sw.window, i, sw.end)
		if procErr != nil {
			if errors.Is(procErr, io.EOF) {
				// bail early
				return nil
			}
			// processing error
			return procErr
		}
		i += offset
	}

	return nil
}
