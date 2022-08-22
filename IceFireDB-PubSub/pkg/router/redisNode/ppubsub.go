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

	topicName := string(args[1].([]byte))
	message := string(args[2].([]byte))
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
