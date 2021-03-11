/*
 * @Author: gitsrc
 * @Date: 2020-12-23 14:26:19
 * @LastEditors: gitsrc
 * @LastEditTime: 2021-03-11 18:37:16
 * @FilePath: /IceFireDB/rafthub/redisService.go
 */

package rafthub

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/tidwall/redcon"
)

// redisService provides a service that is compatible with the Redis protocol.
func redisService() (func(io.Reader) bool, func(Service, net.Listener)) {
	return nil, redisServiceHandler
}

type redisClient struct {
	authorized bool
	opts       SendOptions
}

func redisCommandToArgs(cmd redcon.Command) []string {
	args := make([]string, len(cmd.Args))
	args[0] = strings.ToLower(string(cmd.Args[0]))
	for i := 1; i < len(cmd.Args); i++ {
		args[i] = string(cmd.Args[i])
	}
	return args
}

type redisQuitClose struct{}

func redisServiceExecArgs(s Service, client *redisClient, conn redcon.Conn,
	args [][]string,
) {
	recvs := make([]Receiver, len(args))
	var close bool
	for i, args := range args {
		var r Receiver
		switch args[0] {
		case "quit":
			r = Response(redisQuitClose{}, 0, nil)
			close = true
		case "auth":
			if len(args) != 2 {
				r = Response(nil, 0, ErrWrongNumArgs)
			} else if err := s.Auth(args[1]); err != nil {
				client.authorized = false
				r = Response(nil, 0, err)
			} else {
				client.authorized = true
				r = Response(redcon.SimpleString("OK"), 0, nil)
			}
		default:
			if !client.authorized {
				if err := s.Auth(""); err != nil {
					client.authorized = false
					r = Response(nil, 0, err)
				} else {
					client.authorized = true
				}
			}
			if client.authorized {
				switch args[0] {
				case "ping":
					if len(args) == 1 {
						r = Response(redcon.SimpleString("PONG"), 0, nil)
					} else if len(args) == 2 {
						r = Response(args[1], 0, nil)
					} else {
						r = Response(nil, 0, ErrWrongNumArgs)
					}
				case "echo":
					if len(args) != 2 {
						r = Response(nil, 0, ErrWrongNumArgs)
					} else {
						r = Response(args[1], 0, nil)
					}
				default:
					r = s.Send(args, &client.opts)
				}
			}
		}
		recvs[i] = r
		if close {
			break
		}
	}
	// receive responses
	var filteredArgs [][]string
	for i, r := range recvs {
		resp, elapsed, err := r.Recv()
		if err != nil {
			if err == ErrUnknownCommand {
				err = fmt.Errorf("%s '%s'", err, args[i][0])
			}
			conn.WriteAny(err)
		} else {
			switch v := resp.(type) {
			case FilterArgs:
				filteredArgs = append(filteredArgs, v)
			case Hijack:
				conn := newRedisHijackedConn(conn.Detach())
				go v(s, conn)
			case redisQuitClose:
				conn.WriteString("OK")
				conn.Close()
			default:
				conn.WriteAny(v)
			}
		}
		// broadcast the request and response to all observers
		s.Monitor().Send(Message{
			Addr:    conn.RemoteAddr(),
			Args:    args[i],
			Resp:    resp,
			Err:     err,
			Elapsed: elapsed,
		})
	}
	if len(filteredArgs) > 0 {
		redisServiceExecArgs(s, client, conn, filteredArgs)
	}
}

func redisServiceHandler(s Service, ln net.Listener) {

	s.Log().Fatal(redcon.Serve(ln,
		// handle commands
		func(conn redcon.Conn, cmd redcon.Command) {
			client := conn.Context().(*redisClient)
			var args [][]string
			args = append(args, redisCommandToArgs(cmd))
			for _, cmd := range conn.ReadPipeline() {
				args = append(args, redisCommandToArgs(cmd))
			}

			/*
				这边针对raft分布式，进行指令重写，SETEX => SETEXAT
			*/
			for _, cmdArgs := range args {
				if len(cmdArgs) == 0 {
					continue
				}

				switch strings.ToUpper(cmdArgs[0]) {
				case "SETEX": //重写SETEX指令到 SETEXAT指令
					if len(cmdArgs) < 4 {
						continue
					}
					ttl, err := strconv.ParseInt(cmdArgs[2], 10, 64)
					if err != nil {
						continue
					}
					timestamp := time.Now().Unix() + ttl
					cmdArgs[2] = strconv.FormatInt(timestamp, 10)
					cmdArgs[0] = "SETEXAT"

				case "EXPIRE": //重写EXPIRE指令到 EXPIREAT指令
					if len(cmdArgs) < 3 {
						continue
					}

					ttl, err := strconv.ParseInt(cmdArgs[2], 10, 64)
					if err != nil {
						continue
					}
					timestamp := time.Now().Unix() + ttl
					cmdArgs[2] = strconv.FormatInt(timestamp, 10)
					cmdArgs[0] = "EXPIREAT"
				case "HEXPIRE": //重写HEXPIRE指令到 HEXPIREAT指令
					if len(cmdArgs) < 3 {
						continue
					}

					ttl, err := strconv.ParseInt(cmdArgs[2], 10, 64)
					if err != nil {
						continue
					}

					timestamp := time.Now().Unix() + ttl
					cmdArgs[2] = strconv.FormatInt(timestamp, 10)
					cmdArgs[0] = "HEXPIREAT"

				}

				//log.Println(args[index])
			}

			redisServiceExecArgs(s, client, conn, args)
		},
		// handle opened connection
		func(conn redcon.Conn) bool {
			context, accept := s.Opened(conn.RemoteAddr())
			if !accept {
				return false
			}
			client := new(redisClient)
			client.opts.From = client
			client.opts.Context = context
			conn.SetContext(client)
			return true
		},
		// handle closed connection
		func(conn redcon.Conn, err error) {
			if conn.Context() == nil {
				return
			}
			client := conn.Context().(*redisClient)
			s.Closed(client.opts.Context, conn.RemoteAddr())
		}),
	)
}

// RedisDial is a helper function that dials out to another rafthub server with
// redis protocol and using the provded TLS config and Auth token. The TLS/Auth
// must be correct in order to establish a connection.
func RedisDial(addr, auth string, tlscfg *tls.Config) (redis.Conn, error) {
	var conn redis.Conn
	var err error
	if tlscfg != nil {
		conn, err = redis.Dial("tcp", addr,
			redis.DialUseTLS(true), redis.DialTLSConfig(tlscfg))
	} else {
		conn, err = redis.Dial("tcp", addr)
	}
	if err != nil {
		return nil, err
	}
	if auth != "" {
		res, err := redis.String(conn.Do("auth", auth))
		if err != nil {
			conn.Close()
			return nil, err
		}
		if res != "OK" {
			conn.Close()
			return nil, fmt.Errorf("'OK', got '%s'", res)
		}
	}
	return conn, nil
}
