package models

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/juju/errors"
	"github.com/ledisdb/xcodis/utils"
	"github.com/ngaut/zkhelper"
)

type ServerType string

const (
	ServerTypeLeader    ServerType = "leader"
	ServerTypeFollower  ServerType = "follower"
	ServerTypeCandidate ServerType = "candidate"
	ServerTypeOffline   ServerType = "offline"
)

type ServerInfo struct {
	Addr string     `json:"addr"`
	Type ServerType `json:"type"`
}

type Server struct {
	ID      int        `json:"id"`
	GroupId int        `json:"group_id"`
	Addr    string     `json:"addr"`
	Type    ServerType `json:"type"` // todo remove
}

type ServerGroup struct {
	Id          int      `json:"id"`
	ProductName string   `json:"product_name"`
	Servers     []Server `json:"servers"`
}

func (g *ServerGroup) Encode() []byte {
	return jsonEncode(g)
}

func (s *Server) Encode() []byte {
	return jsonEncode(s)
}

func (s Server) String() string {
	b, _ := json.MarshalIndent(s, "", "  ")
	return string(b)
}

func (self ServerGroup) String() string {
	b, _ := json.MarshalIndent(self, "", "  ")
	return string(b) + "\n"
}

func GetServer(zkConn zkhelper.Conn, zkPath string) (*Server, error) {
	data, _, err := zkConn.Get(zkPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	srv := Server{}
	if err := json.Unmarshal(data, &srv); err != nil {
		return nil, errors.Trace(err)
	}
	return &srv, nil
}

func NewServer(serverType ServerType, addr string) *Server {
	return &Server{
		Type:    serverType,
		GroupId: INVALID_ID,
		Addr:    addr,
	}
}

func NewServerGroup(productName string, id int) *ServerGroup {
	return &ServerGroup{
		Id:          id,
		ProductName: productName,
	}
}

func GroupExists(zkConn zkhelper.Conn, productName string, groupId int) (bool, error) {
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", productName, groupId)
	exists, _, err := zkConn.Exists(zkPath)
	if err != nil {
		return false, errors.Trace(err)
	}
	return exists, nil
}

func GetGroup(zkConn zkhelper.Conn, productName string, groupId int) (*ServerGroup, error) {
	exists, err := GroupExists(zkConn, productName, groupId)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exists {
		return nil, errors.NotFoundf("group %d", groupId)
	}

	group := &ServerGroup{
		ProductName: productName,
		Id:          groupId,
	}

	group.Servers, err = group.GetServers(zkConn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return group, nil
}

func ServerGroups(zkConn zkhelper.Conn, productName string) ([]ServerGroup, error) {
	var ret []ServerGroup
	root := fmt.Sprintf("/zk/codis/db_%s/servers", productName)
	groups, _, err := zkConn.Children(root)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Buggy :X
	// zkhelper.ChildrenRecursive(*zkConn, root)

	for _, group := range groups {
		// parse group_1 => 1
		groupId, err := strconv.Atoi(strings.Split(group, "_")[1])
		if err != nil {
			return nil, errors.Trace(err)
		}
		g, err := GetGroup(zkConn, productName, groupId)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, *g)
	}
	return ret, nil
}

func (self *ServerGroup) Master(zkConn zkhelper.Conn) (*Server, error) {
	servers, err := self.GetServers(zkConn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, s := range servers {
		// TODO check if there are two masters
		if s.Type == ServerTypeLeader {
			return &s, nil
		}
	}
	return nil, nil
}

func (self *ServerGroup) Remove(zkConn zkhelper.Conn) error {
	// check if this group is not used by any slot
	slots, err := Slots(zkConn, self.ProductName)
	if err != nil {
		return errors.Trace(err)
	}

	for _, slot := range slots {
		if slot.GroupId == self.Id {
			return errors.AlreadyExistsf("group %d is using by slot %d", slot.GroupId, slot.Id)
		}
	}

	// do delte
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", self.ProductName, self.Id)
	err = zkhelper.DeleteRecursive(zkConn, zkPath, -1)

	err = NewAction(zkConn, self.ProductName, ACTION_TYPE_SERVER_GROUP_REMOVE, self, "", false)
	return errors.Trace(err)
}

func (self *ServerGroup) RemoveServer(zkConn zkhelper.Conn, s Server) error {
	if s.Type == ServerTypeLeader {
		return errors.New("cannot remove master, use promote first")
	}

	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d/%s", self.ProductName, self.Id, s.Addr)
	err := zkConn.Delete(zkPath, -1)
	if err != nil {
		return errors.Trace(err)
	}

	for i := 0; i < len(self.Servers); i++ {
		if self.Servers[i].Addr == s.Addr {
			self.Servers = append(self.Servers[:i], self.Servers[i+1:]...)
			break
		}
	}

	err = NewAction(zkConn, self.ProductName, ACTION_TYPE_SERVER_GROUP_CHANGED, self, "", false)
	return errors.Trace(err)
}

func (self *ServerGroup) Promote(conn zkhelper.Conn, addr string) error {
	var s Server
	exists := false
	for i := 0; i < len(self.Servers); i++ {
		if self.Servers[i].Addr == addr {
			s = self.Servers[i]
			exists = true
			break
		}
	}

	if !exists {
		return errors.NotFoundf("no such addr %s", addr)
	}

	err := utils.SlaveNoOne(s.Addr)
	if err != nil {
		return errors.Trace(err)
	}

	// set origin master offline
	master, err := self.Master(conn)
	if err != nil {
		return errors.Trace(err)
	}

	// old master may be nil
	if master != nil {
		master.Type = ServerTypeOffline
		err = self.AddServer(conn, master)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// promote new server to master
	s.Type = ServerTypeLeader
	err = self.AddServer(conn, &s)
	return errors.Trace(err)
}

func (self *ServerGroup) Create(zkConn zkhelper.Conn) error {
	if self.Id < 0 {
		return errors.NotSupportedf("invalid server group id %d", self.Id)
	}
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", self.ProductName, self.Id)
	_, err := zkhelper.CreateOrUpdate(zkConn, zkPath, "", 0, zkhelper.DefaultDirACLs(), true)
	if err != nil {
		return errors.Trace(err)
	}
	err = NewAction(zkConn, self.ProductName, ACTION_TYPE_SERVER_GROUP_CHANGED, self, "", false)
	if err != nil {
		return errors.Trace(err)
	}

	// set no server slots' group id to this server group, no need to return error
	slots, err := NoGroupSlots(zkConn, self.ProductName)
	if err == nil && len(slots) > 0 {
		SetSlots(zkConn, self.ProductName, slots, self.Id, SLOT_STATUS_ONLINE)
	}

	return nil
}

func (self *ServerGroup) Exists(zkConn zkhelper.Conn) (bool, error) {
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", self.ProductName, self.Id)
	b, err := zkhelper.NodeExists(zkConn, zkPath)
	if err != nil {
		return false, errors.Trace(err)
	}
	return b, nil
}

var ErrNodeExists = errors.New("node already exists")

func (self *ServerGroup) AddServer(zkConn zkhelper.Conn, s *Server) error {
	s.GroupId = self.Id
	val, err := json.Marshal(s)
	if err != nil {
		return errors.Trace(err)
	}

	if s.Type == ServerTypeLeader {
		// make sure there is only one master
		servers, err := self.GetServers(zkConn)
		if err != nil {
			return errors.Trace(err)
		}
		for _, server := range servers {
			if server.Type == ServerTypeLeader {
				return errors.Trace(ErrNodeExists)
			}
		}
	}

	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d/%s", self.ProductName, self.Id, s.Addr)
	_, err = zkhelper.CreateOrUpdate(zkConn, zkPath, string(val), 0, zkhelper.DefaultFileACLs(), true)

	// update servers
	servers, err := self.GetServers(zkConn)
	if err != nil {
		return errors.Trace(err)
	}
	self.Servers = servers

	if s.Type == ServerTypeLeader {
		err = NewAction(zkConn, self.ProductName, ACTION_TYPE_SERVER_GROUP_CHANGED, self, "", true)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (self *ServerGroup) GetServers(zkConn zkhelper.Conn) ([]Server, error) {
	var ret []Server
	root := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", self.ProductName, self.Id)
	nodes, _, err := zkConn.Children(root)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, node := range nodes {
		nodePath := root + "/" + node
		s, err := GetServer(zkConn, nodePath)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, *s)
	}
	return ret, nil
}
