package util

import "testing"

func TestDropLastLine(t *testing.T) {
	dropTests := []struct {
		raw   string
		cutAt uint32
		res   string
	}{
		{"", 0, ""},
		{"", 1, ""},
		{"å", 2, "å"},
		{"\n", 0, "\n"},
		{"\n", 1, "\n"},
		{"\n\n", 1, "\n"},
		{"\n\n", 3, "\n\n"},
		{"a\n\n", 3, "a\n"},
		{"\na\n", 3, "\na"},
		{"å\n\n", 5, "å\n\n"},
		{"\nå\n", 5, "\nå\n"},
	}
	for i, tt := range dropTests {
		got := DropLastLine([]byte(tt.raw), tt.cutAt)
		if got := string(got); got != tt.res {
			t.Errorf("dropLastLine %d error: expected %q; got %q", i, tt.res, got)
		}
	}
}
