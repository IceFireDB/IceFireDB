package argshandler

import (
	"strconv"
	"time"
)

//rewrite some redis instructions

func RedisCmdRewrite(args [][]string, v ...interface{}) {
	var nowUnixTime int64

	for i, arg := range args {
		switch arg[0] {
		case "setex":
			if nowUnixTime == 0 {
				for _, vv := range v {
					switch vtype := vv.(type) {
					case time.Time:
						nowUnixTime = vtype.Unix()
					}
				}
			}
			if len(arg) == 4 && nowUnixTime != 0 {
				exDuration, err := strconv.ParseInt(arg[2], 10, 64)
				if err == nil {
					args[i] = []string{"setexat", arg[1], strconv.FormatInt(nowUnixTime+exDuration, 10), arg[3]}
				}
			}
		default:
		}
	}
}
