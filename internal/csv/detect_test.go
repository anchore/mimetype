package csv

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/gabriel-vasile/mimetype/internal/util"
	"io"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// svStdlib was the previous function used for CSV/TSV detection. It is currently
// used to test the correctness of CSV detection.
func svStdlib(in []byte, comma rune, limit uint32) bool {
	r := svStdlibReader(in, comma, limit)

	lines := 0
	for {
		_, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return false
		}
		lines++
	}

	return r.FieldsPerRecord > 1 && lines > 1
}

func svStdlibReader(in []byte, comma rune, limit uint32) *csv.Reader {
	r := csv.NewReader(bytes.NewReader(util.DropLastLine(in, limit)))
	r.Comma = comma
	r.ReuseRecord = true
	r.LazyQuotes = true
	r.Comment = '#'
	return r
}

func TestDetect(t *testing.T) {
	tests := []struct {
		name              string
		delimiter         byte
		input             string
		limit             uint32
		want              bool
		skipStdlibCompare bool
	}{
		{
			name:      "empty",
			delimiter: ',',
			input:     "",
			want:      false,
		},
		{
			name:      "csv single line",
			delimiter: ',',
			input:     "a,b,c",
			want:      false,
		},
		{
			name:      "csv single line with newlines",
			delimiter: ',',
			input:     "a,b,c\n\n",
			want:      false,
		},
		{
			name:      "csv single line with (windows line ending + comment)",
			input:     ",\r\n#,",
			delimiter: ',',
			want:      false,
		},
		{
			name:      "csv functionally single line (has empty comment as top line)",
			delimiter: ',',
			input:     "#\na,b,c",
			want:      false,
		},
		{
			name:      "single line csv with following comment line",
			delimiter: ',',
			input:     "1,2\n#something\n\n",
			want:      false,
		},
		{
			name:      "csv multiple lines",
			delimiter: ',',
			input:     "a,b,c,d,e,f,g,h\n1,2,3,4,5,6,7,8",
			want:      true,
		},
		{
			name:      "csv multiple lines (windows line endings)",
			input:     "a,b,c\r\n1,2,3",
			delimiter: ',',
			want:      true,
		},
		{
			name:      "multi line csv with last line as comment",
			delimiter: ',',
			input:     "1,2\n1,2\n#something",
			want:      true,
		},
		{
			name:      "csv with spaces",
			delimiter: ',',
			input:     "  a ,\t\tb,   c\n1, 2 , 3  ",
			want:      true,
		},
		{
			name:      "csv multiple lines under limit",
			delimiter: ',',
			input:     "a,b,c\n1,2,3\n4,5,6",
			limit:     10,
			want:      true,
		},
		{
			name:      "csv multiple lines over limit",
			delimiter: ',',
			input:     "a,b,c\n1,2,3\n4,5,6",
			limit:     1,
			want:      false, // we're only allowed to read a single byte, this could never be a CSV
			// the previous limit value was not being honored (all bytes were read from the reader instead of the limit)
			skipStdlibCompare: true,
		},
		{
			name:      "csv 2 line with incomplete last line",
			delimiter: ',',
			input:     "a,b,c\n1,2",
			want:      false,
		},
		{
			name:      "csv 3 line with incomplete last line",
			delimiter: ',',
			input:     "a,b,c\na,b,c\n1,2",
			limit:     10,
			want:      true,
		},
		{
			name:      "within quotes",
			delimiter: ',',
			input: `"a,b,c
1,2,3
4,5,6"`,
			want: false,
		},
		{
			name:      "partial quotes",
			delimiter: ',',
			input: `"a,b,c
1,2,3
4,5,6`,
			want: false,
		},
		{
			name:      "has quotes",
			delimiter: ',',
			input: `"a","b","c"
1,",2",3
"4",5,6`,
			want: true,
		},
		{
			name: "all quotes missing newline",
			input: `"a","b","c"
"1","2","3"`,
			delimiter: ',',
			want:      true,
		},
		{
			name:      "has improper quote encapsulation",
			delimiter: ',',
			input: `1,2,3
1,","2,3`,
			want: false,
		},
		{
			name:      "comma within quotes",
			delimiter: ',',
			input:     "\"a,b\",\"c\"\n1,2,3\n\"4\",5,6",
			want:      false,
		},
		{
			name:      "ignore comments",
			delimiter: ',',
			input:     "#a,b,c\n#1,2,3",
			want:      false,
		},
		{
			name:      "multiple comments at the end of line",
			delimiter: ',',
			input:     "a,b#,c\n1,2#,3",
			want:      true,
		},
		{
			name:      "a non csv line within a csv file",
			delimiter: ',',
			input:     "#comment\nsomething else\na,b,c\n1,2,3",
			want:      false,
		},
		{
			name:      "mixing comments and csv lines",
			delimiter: ',',
			input:     "#comment\na,b,c\n#something else\n1,2,3",
			want:      true,
		},
		{
			name:      "ignore empty lines",
			delimiter: ',',
			input:     "#comment\na,b,c\n\n\n#something else\n1,2,3",
			want:      true,
		},
		{
			name:      "resilient to empty lines",
			delimiter: ',',
			input:     "\n0,\n\n0,\n\n",
			want:      true,
		},
		{
			name:      "single row with one field",
			delimiter: ',',
			input:     "0,",
			want:      false,
		},
		{
			name:      "space prefix invalidates format",
			delimiter: ',',
			input:     " #\na,b,c\na,b,c",
			want:      false,
		},
		{
			name:      "comment with quotes",
			delimiter: ',',
			input: `#comment with "quotes"
a,b,c
#some"thing" else "oops this is imbalanced
1,2,3`,
			want: true,
		},
		{
			name:      "a quote counts as content",
			delimiter: ',',
			input: `,
"`,
			want: false,
		},
		{
			name: "unbalanced quotes within field",
			input: `,,"""
#0`,
			delimiter: ',',
			want:      false,
		},
		{
			name: "last non-empty field in first row has single unbalanced quote with prefix",
			input: `,,aa00110"
,,`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "last empty field with balanced quotes",
			input: `,,"",
,,,`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "last field with single unbalanced quote",
			input: `,,,"
,,,
,,,`,
			delimiter: ',',
			want:      false,
		},
		{
			name: "first field in first row has unbalanced quote",
			input: `",
,`,
			delimiter: ',',
			want:      false,
		},
		{
			name: "first field in first row has unbalanced quote with field prefix",
			input: `0",
,`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "middle field in first row has unbalanced quote",
			input: `,",
,,`,
			delimiter: ',',
			want:      false,
		},
		{
			name: "middle field in first row has unbalanced quote with prefix",
			input: `#something
#else
,0",0
,,`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "last field with single unbalanced quote on last row",
			input: `,
,
,"`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "last field with single unbalanced quote on last row with prefix",
			input: `,
,
,0a"`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "last field with single unbalanced quote on middle row",
			input: `,
,"
,`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "last field with single unbalanced quote on middle row with field prefix",
			input: `,
,0a"
,`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "multiple empty rows",
			input: `,
,`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "middle field with single unbalanced quote",
			input: `,",
,,`,
			delimiter: ',',
			want:      false,
		},
		{
			name: "imbalanced quotes across lines",
			input: `,
",","
""something"","
,`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "ignore escaped quotes",
			input: `0,0,0
0,"something ""else"" is happening",0
0,0,0`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "3 quotes is not an escape",
			input: `0,0,0
0,""",0
0,0,0`,
			delimiter: ',',
			want:      false,
		},
		{
			name: "infer starting quote",
			input: `0,0,0
0,0""",0
0,0,0`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "literal starting quote",
			input: `0,0,0
0,"0""",0
0,0,0`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "infer starting quote with whitespace",
			input: `0  , 0, 0  
0,   0""",0
0,0,0`,
			delimiter: ',',
			want:      true,
		},
		{
			name:      "handle null characters as content",
			input:     ",,\n\x00,,",
			delimiter: ',',
			want:      true,
		},
		{
			name:      "comment character within inferred quote",
			input:     ",\n0#,",
			delimiter: ',',
			want:      true,
		},
		{
			name:      "allow carriage return as content",
			input:     ",\n,\r0",
			delimiter: ',',
			want:      true,
		},
		{
			name:      "stdlib edge case: drop trailing carriage return",
			input:     ",\n,\n\r",
			delimiter: ',',
			want:      true,
		},
		{
			name:      "dont drop multiple trailing carriage returns (they should be treated as a bad data row)",
			input:     ",\n,\n\r\r",
			delimiter: ',',
			want:      false,
		},
		{
			name: "inferred escaped quote within the middle of a field",
			input: `,
0"0,`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "detect explicit quotes and escapes",
			input: `""0","",",,,"
0,",,,","`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "too many escapes",
			input: `"""0","",",,,"
0,",,,","`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "explicit quote start without explicit end (odd)",
			input: `""""0,0,0
0,0,0`,
			delimiter: ',',
			want:      false,
		},
		{
			name: "explicit quote start without explicit end (even)",
			input: `"""0,0,0
0,0,0`,
			delimiter: ',',
			want:      false,
		},
		{
			name: "several valid escapes",
			// interpreted as:
			// 0: []string{
			//			"\"\"0",   // explicit quoted string with 2 escaped quotes (4 literal)... 5 quotes total
			//			"\"\"",    // explicit quoted string with 2 escaped quotes (4 literal)... 6 quotes total
			//			",,,",     // delimiter embedded within quotes in a single field
			//		}
			// 1: []string{
			//			"0",      // implicit quote start and end
			//			",\",,",  // explicit quoted string with embedded delimiter and one escaped quotes (2 literal)... 4 quotes total
			//			"",       // implicit quote end
			//			}
			input: `""""0","""""",",,,"
0,","",,","`,

			delimiter: ',',
			want:      true,
		},
		{
			name: "explicit quote starts detect escapes and explicit end quotes",
			input: `,,"0"0"
,,`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "explicit quote starts detect escapes and explicit end quotes (multi field)",
			input: `,"0""0","0"0"
,,`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "inferred quote starts detect escapes and explicit end quotes",
			input: `,0""0","0"0"
,,`,
			delimiter: ',',
			want:      true,
		},
		{
			name: "if a field starts a quoted it must explicitly be end quoted",
			input: `,"0""0,
,,`,
			delimiter: ',',
			want:      false,
		},
		{
			name: "first line triple quotes are not",
			input: `""",0
0,0`,
			delimiter: ',',
			want:      false,
		},
		{
			name: "last line with triple quotes and line ending are not ok",
			input: `1,2
""",0
`,
			delimiter: ',',
			want:      false,
		},
		{
			name: "last line with triple quotes and no line ending are not ok",
			input: `1,2
""",0`,
			delimiter: ',',
			want:      false,
		},
		{
			name: "last line with triple quotes, last field empty, and no line ending are not ok",
			input: `,
""",`,
			delimiter: ',',
			want:      false,
		},
		{
			name: "erg",
			input: `1,
2,"""",3`,
			delimiter: ',',
			want:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Detect([]byte(tt.input), tt.delimiter, tt.limit)

			if got != tt.want {
				t.Errorf("Detect(): got %v, want %v", got, tt.want)
			}

			if tt.skipStdlibCompare {
				return
			}

			stdlib := svStdlib([]byte(tt.input), rune(tt.delimiter), tt.limit)

			if got != stdlib {
				t.Errorf("Detect(): got %v, stdlib %v", got, stdlib)
				if stdlib {
					reader := svStdlibReader([]byte(tt.input), rune(tt.delimiter), tt.limit)
					records, err := reader.ReadAll()
					if err != nil {
						t.Fatalf("unable to get std records: %v", err)
					}
					for i, record := range records {
						t.Logf("record %d: %#v", i+1, record)
					}
				}
			}

		})
	}
}

func FuzzDetect(f *testing.F) {
	samples := []string{
		"a,b,c\n1,2,3",     // simple csv
		"a,b,c\r\n1,2,3",   // with \r\n line ending
		"a,b,c\n#c\n1,2,3", // with comment
		"æ,ø,å\n1,2,3",     // utf-8

		`"a","b","c"
"1","2","3"`, // quotes

		`a,b,c
#"c"
1,2,3`, // quoted comment
	}

	for _, s := range samples {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, d []byte) {
		//if strings.Contains(string(d), "\"\"") {
		//	return
		//}

		if strings.Count(string(d), "\n") >= svLineLimit {
			// we no longer check the entire CSV
			return
		}

		prev := svStdlib(d, ',', 0)
		curr := Detect(d, ',', 0)
		if prev != curr {
			t.Errorf("curr detector does not match prev:\ncurr: %t, stdlib: %t, input: %s",
				curr, prev, string(d))
		}
	})
}

func BenchmarkDetectVsSv(b *testing.B) {
	contents := generateCSV()

	for _, limit := range []uint32{0, 100, 1000} {
		b.Run(fmt.Sprintf("svStdlib(limit=%d)", limit), func(b *testing.B) {
			svStdlib([]byte(contents), ',', limit)
		})

		b.Run(fmt.Sprintf("Detect(limit=%d)", limit), func(b *testing.B) {
			Detect([]byte(contents), ',', limit)
		})
	}
}

func generateCSV() string {
	const (
		numRows = 1500
		numCols = 40
	)

	var sb strings.Builder

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < numRows; i++ {
		row := make([]string, numCols)
		for j := 0; j < numCols; j++ {
			// randomly decide the content of the cell
			switch r.Intn(4) {
			case 0:
				// a plain number
				row[j] = strconv.Itoa(r.Intn(1000000000) - 5000000)
			case 1:
				// a plain string
				row[j] = generateRandomString(r, false)
			case 2:
				// a string surrounded by quotes
				row[j] = "\"" + generateRandomString(r, false) + "\""
			case 3:
				// a string with extra quotes
				row[j] = generateRandomString(r, true)
			}
		}
		sb.WriteString(strings.Join(row, ","))
		sb.WriteString("\n")
	}

	return sb.String()
}

func generateRandomString(r *rand.Rand, extraQuotes bool) string {
	n := r.Intn(10) + 5 // Random string length between 5 and 15
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, n)
	for i := range result {
		result[i] = chars[r.Intn(len(chars))]
	}

	if extraQuotes {
		insertQuotes(r, result)
	}

	return string(result)
}

func insertQuotes(r *rand.Rand, s []byte) {
	pos1 := r.Intn(len(s) - 1)
	pos2 := r.Intn(len(s) - 1)
	if pos1 > pos2 {
		pos1, pos2 = pos2, pos1
	}
	// escaped quote
	s[pos1] = '"'
	s[pos1+1] = '"'

	// just a random quote
	s[pos2] = '"'
}

func Test_prepSvReader(t *testing.T) {

	tests := []struct {
		name  string
		input string
		limit uint32
		want  string
	}{
		{
			name:  "multiple lines",
			input: "a,b,c\n1,2,3",
			limit: 0,
			want:  "a,b,c\n1,2,3",
		},
		{
			name:  "limit",
			input: "a,b,c\n1,2,3",
			limit: 5,
			want:  "a,b,c",
		},
		{
			name:  "drop last line",
			input: "a,b,c\na,b,c\na,b,c\n1,2",
			limit: 20,
			want:  "a,b,c\na,b,c\na,b,c",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := prepSvReader([]byte(tt.input), tt.limit)
			by, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("prepSvReader() error = %v", err)
			}
			if !reflect.DeepEqual(string(by), tt.want) {
				t.Errorf("prepSvReader() = '%v', want '%v'", string(by), tt.want)
			}
		})
	}
}
