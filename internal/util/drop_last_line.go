package util

// DropLastLine drops the last incomplete line from b.
//
// mimetype limits itself to ReadLimit bytes when performing a detection.
// This means, for file formats like CSV for NDJSON, the last line of the input
// can be an incomplete line.
func DropLastLine(b []byte, readLimit uint32) []byte {
	if readLimit == 0 || uint32(len(b)) < readLimit {
		return b
	}
	for i := len(b) - 1; i > 0; i-- {
		if b[i] == '\n' {
			return b[:i]
		}
	}
	return b
}
