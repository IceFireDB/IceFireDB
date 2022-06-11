package utils

import (
	"fmt"
	"github.com/libp2p/go-libp2p-core/host"
	ma "github.com/multiformats/go-multiaddr"
	"os"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/siddontang/go/hack"
	"github.com/sirupsen/logrus"
)

func InArray(in string, array []string) bool {
	for k := range array {
		if in == array[k] {
			return true
		}
	}
	return false
}

func GoWithRecover(handler func(), recoverHandler func(r interface{})) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				logrus.Errorf("%s goroutine panic: %v\n%s\n", time.Now().Format("2006-01-02 15:04:05"), r, string(debug.Stack()))
				if recoverHandler != nil {
					go func() {
						defer func() {
							if p := recover(); p != nil {
								logrus.Errorf("recover goroutine panic:%v\n%s\n", p, string(debug.Stack()))
							}
						}()
						recoverHandler(r)
					}()
				}
			}
		}()
		handler()
	}()
}

func GetString(d interface{}) (string, error) {
	switch v := d.(type) {
	case string:
		return v, nil
	case []byte:
		return hack.String(v), nil
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 64), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case nil:
		return "", nil
	case time.Time:
		return v.String(), nil
	default:
		return "", fmt.Errorf("data type is %T", v)
	}
}

func IsFileExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func GetHostAddress(ha host.Host) string {
	// Build host multiaddress
	hostAddr, _ := ma.NewMultiaddr(fmt.Sprintf("/ipfs/%s", ha.ID().Pretty()))

	// Now we can build a full multiaddress to reach this host
	// by encapsulating both addresses:
	addr := ha.Addrs()[0]
	return addr.Encapsulate(hostAddr).String()
}
