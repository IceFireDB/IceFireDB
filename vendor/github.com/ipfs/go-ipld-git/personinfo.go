package ipldgit

import (
	"bytes"
	"fmt"
	"strings"
)

func parsePersonInfo(line []byte) (PersonInfo, error) {
	parts := bytes.Split(line, []byte{' '})
	if len(parts) < 3 {
		return nil, fmt.Errorf("incorrectly formatted person info line: %q", line)
	}

	//TODO: just use regex?
	//skip prefix
	at := 1

	var pi _PersonInfo
	var name string

	for {
		if at == len(parts) {
			return nil, fmt.Errorf("invalid personInfo: %q", line)
		}
		part := parts[at]
		if len(part) != 0 {
			if part[0] == '<' {
				break
			}
			name += string(part) + " "
		} else if len(name) > 0 {
			name += " "
		}
		at++
	}
	if len(name) != 0 {
		pi.name = _String{name[:len(name)-1]}
	}

	var email strings.Builder
	for {
		if at == len(parts) {
			return nil, fmt.Errorf("invalid personInfo: %q", line)
		}
		part := parts[at]
		// A part can be empty when the email contains repeated spaces, which
		// git itself produces, so skip rather than index into it. The name loop
		// above already treats that case the same way.
		if len(part) > 0 && part[0] == '<' {
			part = part[1:]
		}

		at++
		if len(part) > 0 && part[len(part)-1] == '>' {
			email.WriteString(string(part[:len(part)-1]))
			break
		}
		email.WriteString(string(part) + " ")
	}
	pi.email = _String{email.String()}

	if at == len(parts) {
		return &pi, nil
	}
	pi.date = _String{string(parts[at])}

	at++
	if at == len(parts) {
		return &pi, nil
	}
	pi.timezone = _String{string(parts[at])}
	return &pi, nil
}

func (p _PersonInfo) GitString() string {
	f := "%s <%s>"
	arg := []any{p.name.x, p.email.x}
	if p.date.x != "" {
		f = f + " %s"
		arg = append(arg, p.date.x)
	}

	if p.timezone.x != "" {
		f = f + " %s"
		arg = append(arg, p.timezone.x)
	}
	return fmt.Sprintf(f, arg...)
}
