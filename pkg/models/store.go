// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"regexp"

	"github.com/pkg/errors"
)

func init() {
	if filepath.Separator != '/' {
		log.Panicf("bad Separator = '%c', must be '/'", filepath.Separator)
	}
}

const BaseDir = "/icefire"

func ProductDir(product string) string {
	return filepath.Join(BaseDir, product)
}

func LockPath(product string) string {
	return filepath.Join(BaseDir, product, "topom")
}

func SlotPath(product string, sid int) string {
	return filepath.Join(BaseDir, product, "slots", fmt.Sprintf("slot-%04d", sid))
}

func GroupDir(product string) string {
	return filepath.Join(BaseDir, product, "group")
}

func ServerDir(product string) string {
	return filepath.Join(BaseDir, product, "server")
}

func GroupPath(product string, gid int) string {
	return filepath.Join(BaseDir, product, "group", fmt.Sprintf("group-%04d", gid))
}

func ServerPath(product string, sid int) string {
	return filepath.Join(BaseDir, product, "server", fmt.Sprintf("server-%04d", sid))
}

func LoadTopom(client Client, product string, must bool) (*Topom, error) {
	b, err := client.Read(LockPath(product), must)
	if err != nil || b == nil {
		return nil, err
	}
	t := &Topom{}
	if err := jsonDecode(t, b); err != nil {
		return nil, err
	}
	return t, nil
}

type Store struct {
	client  Client
	product string
}

func NewStore(client Client, product string) *Store {
	return &Store{client, product}
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Client() Client {
	return s.client
}

func (s *Store) LockPath() string {
	return LockPath(s.product)
}

func (s *Store) SlotPath(sid int) string {
	return SlotPath(s.product, sid)
}

func (s *Store) GroupDir() string {
	return GroupDir(s.product)
}

func (s *Store) GroupPath(gid int) string {
	return GroupPath(s.product, gid)
}

func (s *Store) ServerDir() string {
	return ServerDir(s.product)
}

func (s *Store) ServerPath(sid int) string {
	return ServerPath(s.product, sid)
}

func (s *Store) Acquire(topom *Topom) error {
	return s.client.Create(s.LockPath(), topom.Encode())
}

func (s *Store) Release() error {
	return s.client.Delete(s.LockPath())
}

func (s *Store) LoadTopom(must bool) (*Topom, error) {
	return LoadTopom(s.client, s.product, must)
}

func (s *Store) GetSlot(sid int, must bool) (*Slot, error) {
	data, err := s.client.Read(s.SlotPath(sid), must)
	if err != nil || data == nil {
		return nil, err
	}
	var slot Slot
	if err := json.Unmarshal(data, &slot); err != nil {
		return nil, err
	}

	return &slot, nil
}

func (s *Store) GetMigratingSlots(MaxSlotNum int) ([]*Slot, error) {
	migrateSlots := make([]*Slot, 0)
	slots, err := s.Slots(MaxSlotNum)
	if err != nil {
		return nil, err
	}

	for _, slot := range slots {
		if slot.State.Status == SLOT_STATUS_MIGRATE {
			migrateSlots = append(migrateSlots, slot)
		}
	}

	return migrateSlots, nil
}

func (s *Store) Slots(MaxSlotNum int) ([]*Slot, error) {
	slots := make([]*Slot, MaxSlotNum)
	for i := range slots {
		m, err := s.GetSlot(i, false)
		if err != nil {
			return nil, err
		}
		if m != nil {
			slots[i] = m
		} else {
			slots[i] = &Slot{Id: i}
		}
	}
	return slots, nil
}

func (s *Store) UpdateSlot(m *Slot) error {
	return s.client.Update(s.SlotPath(m.Id), m.Encode())
}

func (s *Store) ListGroup() (map[int]*ServerGroup, error) {
	paths, err := s.client.List(s.GroupDir(), false)
	if err != nil {
		return nil, err
	}
	group := make(map[int]*ServerGroup)
	for _, path := range paths {
		b, err := s.client.Read(path, true)
		if err != nil {
			return nil, err
		}
		g := &ServerGroup{}
		if err := jsonDecode(g, b); err != nil {
			return nil, err
		}
		group[g.Id] = g
	}
	return group, nil
}

func (s *Store) LoadGroup(gid int, must bool) (*ServerGroup, error) {
	b, err := s.client.Read(s.GroupPath(gid), must)
	if err != nil || b == nil {
		return nil, err
	}
	g := &ServerGroup{}
	if err := jsonDecode(g, b); err != nil {
		return nil, err
	}
	return g, nil
}

func (s *Store) UpdateGroup(g *ServerGroup) error {
	return s.client.Update(s.GroupPath(g.Id), g.Encode())
}

func (s *Store) DeleteGroup(gid int) error {
	return s.client.Delete(s.GroupPath(gid))
}

func (s *Store) UpdateServer(server *Server) error {
	return s.client.Update(s.ServerPath(server.ID), server.Encode())
}

func (s *Store) DeleteServer(sid int) error {
	return s.client.Delete(s.ServerPath(sid))
}

func ValidateProduct(name string) error {
	if regexp.MustCompile(`^\w[\w\.\-]*$`).MatchString(name) {
		return nil
	}
	return errors.Errorf("bad product name = %s", name)
}
