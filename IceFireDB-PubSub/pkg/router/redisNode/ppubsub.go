package redisNode

import (
	"errors"
	"github.com/IceFireDB/IceFireDB-PubSub/pkg/ppubsub"
	"github.com/IceFireDB/IceFireDB-PubSub/pkg/router"
)

func (r *Router) cmdPpub(s *router.Context) error {
	args := s.Args
	if len(args) != 3 {
		return errors.New("ERR wrong number of arguments for 'ppub' command")
	}

	var topicName, message string

	// p2p 过来的数据，这边类型不是 []byte，而是 string，所以要转换一下
	if args1Byte, ok := args[1].([]byte); ok {
		topicName = string(args1Byte)
	} else {
		topicName = args[1].(string)
	}
	if args2Byte, ok := args[2].([]byte); ok {
		message = string(args2Byte)
	} else {
		message = args[2].(string)
	}
	err := ppubsub.Pub(topicName, message)
	if err != nil {
		return errors.New("ERR pub:" + err.Error())
	}
	return router.WriteSimpleString(s.Writer, "OK")
}

func (r *Router) cmdPsub(s *router.Context) error {
	args := s.Args
	if len(args) != 2 {
		return errors.New("ERR wrong number of arguments for 'psub' command")
	}
	topicName := string(args[1].([]byte))
	_, err := ppubsub.Sub(s.Writer, topicName)
	if err != nil {
		return errors.New("ERR sub:" + err.Error())
	}
	return nil

}
