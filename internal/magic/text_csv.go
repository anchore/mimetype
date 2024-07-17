package magic

import "github.com/gabriel-vasile/mimetype/internal/csv"

// Csv matches a comma-separated values file.
func Csv(raw []byte, limit uint32) bool {
	return csv.Detect(raw, ',', limit)
}

// Tsv matches a tab-separated values file.
func Tsv(raw []byte, limit uint32) bool {
	return csv.Detect(raw, '\t', limit)
}
