package argshandler

import (
	"strconv"
	"strings"
	"time"
)

//rewrite some redis instructions

func RedisCmdRewrite(args [][]string, v ...interface{}) {
	var nowUnixTime int64

	for i, arg := range args {
		switch strings.ToLower(arg[0]) {
		case "setex":
			if nowUnixTime == 0 {
				nowUnixTime = getUnixTime(v...)
			}
			if len(arg) == 4 && nowUnixTime != 0 {
				exDuration, err := strconv.ParseInt(arg[2], 10, 64)
				if err == nil {
					args[i] = []string{"setexat", arg[1], strconv.FormatInt(nowUnixTime+exDuration, 10), arg[3]}
				}
			}
		case "expire":
			if nowUnixTime == 0 {
				nowUnixTime = getUnixTime(v...)
			}
			if len(arg) == 3 && nowUnixTime != 0 {
				exDuration, err := strconv.ParseInt(arg[2], 10, 64)
				if err == nil {
					args[i] = []string{"expireat", arg[1], strconv.FormatInt(nowUnixTime+exDuration, 10), arg[2]}
				}
			}
		case "lexpire":
			if nowUnixTime == 0 {
				nowUnixTime = getUnixTime(v...)
			}
			if len(arg) == 3 && nowUnixTime != 0 {
				exDuration, err := strconv.ParseInt(arg[2], 10, 64)
				if err == nil {
					args[i] = []string{"lexpireat", arg[1], strconv.FormatInt(nowUnixTime+exDuration, 10), arg[2]}
				}
			}
		case "hexpire":
			if nowUnixTime == 0 {
				nowUnixTime = getUnixTime(v...)
			}
			if len(arg) == 3 && nowUnixTime != 0 {
				exDuration, err := strconv.ParseInt(arg[2], 10, 64)
				if err == nil {
					args[i] = []string{"lexpireat", arg[1], strconv.FormatInt(nowUnixTime+exDuration, 10), arg[2]}
				}
			}
		default:
		}
	}
}

func getUnixTime(v ...interface{}) int64 {
	var nowUnixTime int64
	for _, vv := range v {
		switch vtype := vv.(type) {
		case time.Time:
			nowUnixTime = vtype.Unix()
		}
	}
	return nowUnixTime
}
