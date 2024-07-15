package magic

import (
	"testing"
)

func TestCsv(t *testing.T) {
	tests := []struct {
		name  string
		input string
		limit uint32
		want  bool
	}{

		{
			name:  "csv multiple lines",
			input: "a,b,c\n1,2,3",
			want:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Csv([]byte(tt.input), tt.limit); got != tt.want {
				t.Errorf("Csv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTsv(t *testing.T) {
	tests := []struct {
		name  string
		input string
		limit uint32
		want  bool
	}{

		{
			name:  "tsv multiple lines",
			input: "a\tb\tc\n1\t2\t3",
			want:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Tsv([]byte(tt.input), tt.limit); got != tt.want {
				t.Errorf("Csv() = %v, want %v", got, tt.want)
			}
		})
	}
}
