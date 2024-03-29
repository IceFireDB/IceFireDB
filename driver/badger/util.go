package badger

import (
	"fmt"
	"log"
	"math"
	"strings"
	"time"
)

func printf(tpl string, args ...interface{}) {
	newArgs := make([]interface{}, 0, len(args))
	for _, arg := range args {
		var newArg interface{}
		switch v := arg.(type) {
		case string:
			newArg = strings.TrimSpace(v)
		case []byte:
			newArg = strings.TrimSpace(string(v))
		default:
			newArg = arg
		}
		newArgs = append(newArgs, newArg)
	}
	log.Println(fmt.Sprintf(tpl, newArgs...))
}

func timeTs() uint64 {
	return uint64(time.Now().UnixNano())
}

func maxTs() uint64 {
	return math.MaxUint64
}
