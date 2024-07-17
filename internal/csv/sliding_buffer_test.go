package csv

import (
	"errors"
	"fmt"
	"io"
	"testing"
)

// Mock reader to simulate different io.Reader behaviors
type mockReader struct {
	data   []byte
	offset int
	err    error
}

func (mr *mockReader) Read(p []byte) (int, error) {
	if mr.err != nil {
		return 0, mr.err
	}
	if mr.offset >= len(mr.data) {
		return 0, io.EOF
	}
	n := copy(p, mr.data[mr.offset:])
	mr.offset += n
	return n, nil
}

func TestNewSlidingBuffer(t *testing.T) {
	reader := &mockReader{data: []byte("test data")}

	// default values
	sb := newSlidingBuffer(reader, 0, 0, 0)
	if sb == nil {
		t.Error("Expected non-nil slidingBuffer")
	}
	if sb.lookAhead != 3 {
		t.Errorf("Expected default lookAhead 3, got %d", sb.lookAhead)
	}
	if sb.lookBehind != 1 {
		t.Errorf("Expected default lookBehind 1, got %d", sb.lookBehind)
	}
	if sb.bufSize != 1024 {
		t.Errorf("Expected default bufSize 10, got %d", sb.bufSize)
	}

	// user values
	sb = newSlidingBuffer(reader, 10, 5, 2)
	if sb == nil {
		t.Error("Expected non-nil slidingBuffer")
	}
	if sb.lookAhead != 5 {
		t.Errorf("Expected user lookAhead 5, got %d", sb.lookAhead)
	}
	if sb.lookBehind != 2 {
		t.Errorf("Expected user lookBehind 2, got %d", sb.lookBehind)
	}
	if sb.bufSize != 10 {
		t.Errorf("Expected user bufSize 10, got %d", sb.bufSize)
	}
}

func TestSlidingBufferIterate(t *testing.T) {
	readErr := errors.New("read error")
	processErr := errors.New("processing error")
	tests := []struct {
		name          string
		bufferSize    int
		readerData    []byte
		readerErr     error
		processFunc   func(buf []byte, i, length int) (int, error)
		expectedErr   error
		expectedCalls int
	}{
		{
			name:       "normal processing",
			readerData: []byte("test data"),
			processFunc: func(buf []byte, i, length int) (int, error) {
				return 0, nil
			},
			expectedErr:   nil,
			expectedCalls: 9,
		},
		{
			name:       "empty input",
			readerData: []byte(""),
			processFunc: func(buf []byte, i, length int) (int, error) {
				return 0, nil
			},
			expectedErr:   nil,
			expectedCalls: 0,
		},
		{
			name:       "process func error",
			readerData: []byte("test data"),
			processFunc: func(buf []byte, i, length int) (int, error) {
				return 0, processErr
			},
			expectedErr:   processErr,
			expectedCalls: 1,
		},
		{
			name:       "reader error",
			readerData: []byte("test data"),
			readerErr:  readErr,
			processFunc: func(buf []byte, i, length int) (int, error) {
				return 0, nil
			},
			expectedErr:   readErr,
			expectedCalls: 0,
		},
		{
			name:       "early return from processor",
			readerData: []byte("test data"),
			processFunc: func(buf []byte, i, length int) (int, error) {
				if i == 4 {
					return 0, io.EOF
				}
				return 0, nil
			},
			expectedErr:   nil,
			expectedCalls: 5, // 4 bytes + 1 iteration for the processor to realize it at the top of the call
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			reader := &mockReader{data: test.readerData, err: test.readerErr}
			sb := newSlidingBuffer(reader, test.bufferSize, 0, 0) // use the default valies (zero values from test) or non-zero test values
			callCount := 0
			err := sb.iterate(func(buf []byte, i, length int) (int, error) {
				callCount++
				return test.processFunc(buf, i, length)
			})
			if !errors.Is(err, test.expectedErr) {
				t.Errorf("Expected error %q, got %q", test.expectedErr, err)
			}
			if callCount != test.expectedCalls {
				t.Errorf("Expected %d calls to processFunc, got %d", test.expectedCalls, callCount)
			}
		})
	}
}

type iteration struct {
	i    int
	n    int
	buff string
	cur  string
}

func newIteration(buf []byte, i, n int) iteration {
	var b string
	if i >= 0 && i < n {
		b = string(buf[i])
	}
	return iteration{
		i:    i,
		n:    n,
		buff: string(buf),
		cur:  b,
	}
}

func diffIterations(t testing.TB, expected, got []iteration) {
	t.Helper()

	if len(expected) != len(got) {
		for _, e := range got {
			fmt.Printf("%#v\n", e)
		}
		t.Fatalf("different iteration counts: got %d, want %d", len(got), len(expected))
	}

	for i, e := range expected {
		g := got[i]
		if e.i != g.i {
			t.Errorf("iteration %d: different i: got %d, want %d", i, g.i, e.i)
		}
		if e.n != g.n {
			t.Errorf("iteration %d: different n: got %d, want %d", i, g.n, e.n)
		}
		if e.buff != g.buff {
			t.Errorf("iteration %d: different buff: got %q, want %q", i, g.buff, e.buff)
		}
		if e.cur != g.cur {
			t.Errorf("iteration %d: different cur: got %q, want %q", i, g.cur, e.cur)
		}
	}
}

func TestSlidingBuffer_honorOffsets(t *testing.T) {
	readerData := []byte("test data is awesome")
	reader := &mockReader{data: readerData}

	expected := []iteration{
		// first read iteration...
		{i: 0, n: 5, buff: "test \x00\x00\x00\x00", cur: "t"},
		{i: 1, n: 5, buff: "test \x00\x00\x00\x00", cur: "e"},
		// second read iteration...
		{i: 1, n: 9, buff: "est data ", cur: "s"},
		{i: 2, n: 9, buff: "est data ", cur: "t"},
		{i: 3, n: 9, buff: "est data ", cur: " "},
		{i: 4, n: 9, buff: "est data ", cur: "d"},
		{i: 5, n: 9, buff: "est data ", cur: "a"},
		// third read iteration...
		{i: 1, n: 9, buff: "ata is aw", cur: "t"},
		{i: 2, n: 9, buff: "ata is aw", cur: "a"},
		{i: 3, n: 9, buff: "ata is aw", cur: " "},
		{i: 4, n: 9, buff: "ata is aw", cur: "i"},
		{i: 5, n: 9, buff: "ata is aw", cur: "s"},
		// fourth read iteration...
		{i: 1, n: 9, buff: "s awesome", cur: " "},
		{i: 2, n: 9, buff: "s awesome", cur: "a"},
		{i: 3, n: 9, buff: "s awesome", cur: "w"},
		//{i: 4, n: 9, buff: "s awesome", cur: "e"}, // SKIP THIS ONE
		{i: 5, n: 9, buff: "s awesome", cur: "s"},
		// post main loop read...
		{i: 6, n: 9, buff: "s awesome", cur: "o"},
		{i: 7, n: 9, buff: "s awesome", cur: "m"},
		{i: 8, n: 9, buff: "s awesome", cur: "e"},
	}

	sb := newSlidingBuffer(reader, 5, 3, 1)
	var got []iteration
	err := sb.iterate(func(buf []byte, i, n int) (int, error) {
		got = append(got, newIteration(buf, i, n))

		// special case: skip w+1 indexes
		// this proves we honor offsets...
		if buf[i] == 'w' {
			return 1, nil
		}

		return 0, nil
	})

	if err != nil {
		t.Fatalf("got an error while iterating: %v", err)
	}

	diffIterations(t, expected, got)

}

func TestSlidingBuffer_crossMainLoopReadBufferOffsetHonored(t *testing.T) {
	readerData := []byte("test data is awesome")
	reader := &mockReader{data: readerData}

	expected := []iteration{
		// first read iteration...
		{i: 0, n: 5, buff: "test \x00\x00\x00\x00", cur: "t"},
		{i: 1, n: 5, buff: "test \x00\x00\x00\x00", cur: "e"},
		// second read iteration...
		{i: 1, n: 9, buff: "est data ", cur: "s"},
		{i: 2, n: 9, buff: "est data ", cur: "t"},
		{i: 3, n: 9, buff: "est data ", cur: " "},
		{i: 4, n: 9, buff: "est data ", cur: "d"},
		{i: 5, n: 9, buff: "est data ", cur: "a"},
		// third read iteration...
		{i: 1, n: 9, buff: "ata is aw", cur: "t"},
		{i: 2, n: 9, buff: "ata is aw", cur: "a"},
		{i: 3, n: 9, buff: "ata is aw", cur: " "},
		{i: 4, n: 9, buff: "ata is aw", cur: "i"},
		{i: 5, n: 9, buff: "ata is aw", cur: "s"},
		// fourth read iteration...
		{i: 1, n: 9, buff: "s awesome", cur: " "},
		{i: 2, n: 9, buff: "s awesome", cur: "a"},
		{i: 3, n: 9, buff: "s awesome", cur: "w"},
		{i: 4, n: 9, buff: "s awesome", cur: "e"},
		{i: 5, n: 9, buff: "s awesome", cur: "s"},
		// post main loop read...
		//{i: 6, n: 9, buff: "s awesome", cur: "o"}, // SKIP THIS ONE
		{i: 7, n: 9, buff: "s awesome", cur: "m"},
		{i: 8, n: 9, buff: "s awesome", cur: "e"},
	}

	sb := newSlidingBuffer(reader, 5, 3, 1)
	var got []iteration
	var row int
	err := sb.iterate(func(buf []byte, i, n int) (int, error) {
		got = append(got, newIteration(buf, i, n))
		fmt.Printf("%#v\n", got[len(got)-1])

		row++

		// this proves we honor offsets across the main loop read and post main loop
		if row == 17 {
			return 1, nil
		}

		return 0, nil
	})

	if err != nil {
		t.Fatalf("got an error while iterating: %v", err)
	}

	diffIterations(t, expected, got)

}

func TestSlidingBuffer_crossReadBufferOffsetHonored(t *testing.T) {
	readerData := []byte("test data is awesome")
	reader := &mockReader{data: readerData}

	expected := []iteration{
		// first read iteration...
		{i: 0, n: 5, buff: "test \x00\x00\x00\x00", cur: "t"},
		{i: 1, n: 5, buff: "test \x00\x00\x00\x00", cur: "e"},
		// second read iteration...
		{i: 1, n: 9, buff: "est data ", cur: "s"},
		{i: 2, n: 9, buff: "est data ", cur: "t"},
		{i: 3, n: 9, buff: "est data ", cur: " "},
		{i: 4, n: 9, buff: "est data ", cur: "d"},
		{i: 5, n: 9, buff: "est data ", cur: "a"},
		// third read iteration...
		//{i: 1, n: 9, buff: "ata is aw", cur: "t"}, // SKIP THIS ONE
		{i: 1, n: 8, buff: "ta is aw ", cur: "a"}, // note the offset has been incorporated into i and n
		{i: 2, n: 8, buff: "ta is aw ", cur: " "},
		{i: 3, n: 8, buff: "ta is aw ", cur: "i"},
		{i: 4, n: 8, buff: "ta is aw ", cur: "s"},
		// fourth read iteration...
		{i: 1, n: 9, buff: "s awesome", cur: " "},
		{i: 2, n: 9, buff: "s awesome", cur: "a"},
		{i: 3, n: 9, buff: "s awesome", cur: "w"},
		{i: 4, n: 9, buff: "s awesome", cur: "e"},
		{i: 5, n: 9, buff: "s awesome", cur: "s"},
		// post main loop read...
		{i: 6, n: 9, buff: "s awesome", cur: "o"},
		{i: 7, n: 9, buff: "s awesome", cur: "m"},
		{i: 8, n: 9, buff: "s awesome", cur: "e"},
	}

	sb := newSlidingBuffer(reader, 5, 3, 1)
	var got []iteration
	var row int
	err := sb.iterate(func(buf []byte, i, n int) (int, error) {
		got = append(got, newIteration(buf, i, n))
		fmt.Printf("%#v\n", got[len(got)-1])

		row++

		// this proves we honor offsets across read loop iterations
		if row == 7 {
			return 1, nil
		}

		return 0, nil
	})

	if err != nil {
		t.Fatalf("got an error while iterating: %v", err)
	}

	diffIterations(t, expected, got)

}
