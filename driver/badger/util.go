package badger

import (
	"fmt"
	"log"
	"strings"
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