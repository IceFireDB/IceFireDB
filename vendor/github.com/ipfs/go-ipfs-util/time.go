package util

import "time"

// TimeFormatIpfs is the format ipfs uses to represent time in string form.
//
// Deprecated: use github.com/ipfs/boxo/util.TimeFormatIpfs
var TimeFormatIpfs = time.RFC3339Nano

// ParseRFC3339 parses an RFC3339Nano-formatted time stamp and
// returns the UTC time.
//
// Deprecated: use github.com/ipfs/boxo/util.ParseRFC3339
func ParseRFC3339(s string) (time.Time, error) {
	t, err := time.Parse(TimeFormatIpfs, s)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

// FormatRFC3339 returns the string representation of the
// UTC value of the given time in RFC3339Nano format.
//
// Deprecated: use github.com/ipfs/boxo/util.FormatRFC3339
func FormatRFC3339(t time.Time) string {
	return t.UTC().Format(TimeFormatIpfs)
}
