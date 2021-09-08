package models

import (
	"encoding/json"
	"fmt"
	"path"

	"github.com/ngaut/zkhelper"

	"github.com/juju/errors"
)

func GetSlotPath(productName string, slotId int) string {
	return fmt.Sprintf("/zk/codis/db_%s/slots/slot_%d", productName, slotId)
}

func GetSlotBasePath(productName string) string {
	return fmt.Sprintf("/zk/codis/db_%s/slots", productName)
}

func GetSlot(zkConn zkhelper.Conn, productName string, id int) (*Slot, error) {
	zkPath := GetSlotPath(productName, id)
	data, _, err := zkConn.Get(zkPath)
	if err != nil {
		return nil, err
	}

	var slot Slot
	if err := json.Unmarshal(data, &slot); err != nil {
		return nil, err
	}

	return &slot, nil
}

func GetMigratingSlots(conn zkhelper.Conn, productName string) ([]Slot, error) {
	migrateSlots := make([]Slot, 0)
	slots, err := Slots(conn, productName)
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

func Slots(zkConn zkhelper.Conn, productName string) ([]Slot, error) {
	zkPath := GetSlotBasePath(productName)
	children, _, err := zkConn.Children(zkPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var slots []Slot
	for _, p := range children {
		data, _, err := zkConn.Get(path.Join(zkPath, p))
		if err != nil {
			return nil, errors.Trace(err)
		}
		slot := Slot{}
		if err := json.Unmarshal(data, &slot); err != nil {
			return nil, errors.Trace(err)
		}
		slots = append(slots, slot)
	}

	return slots, nil
}

func NoGroupSlots(zkConn zkhelper.Conn, productName string) ([]Slot, error) {
	slots, err := Slots(zkConn, productName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var ret []Slot
	for _, slot := range slots {
		if slot.GroupId == INVALID_ID {
			ret = append(ret, slot)
		}
	}
	return ret, nil
}

func SetSlots(zkConn zkhelper.Conn, productName string, slots []Slot, groupId int, status SlotStatus) error {
	if status != SLOT_STATUS_OFFLINE && status != SLOT_STATUS_ONLINE {
		return errors.New("invalid status")
	}

	ok, err := GroupExists(zkConn, productName, groupId)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		return errors.NotFoundf("group %d", groupId)
	}

	for _, s := range slots {
		s.GroupId = groupId
		s.State.Status = status
		data, err := json.Marshal(s)
		if err != nil {
			return errors.Trace(err)
		}

		zkPath := GetSlotPath(productName, s.Id)
		_, err = zkhelper.CreateOrUpdate(zkConn, zkPath, string(data), 0, zkhelper.DefaultFileACLs(), true)
		if err != nil {
			return errors.Trace(err)
		}
	}

	param := SlotMultiSetParam{
		From:    -1,
		To:      -1,
		GroupId: groupId,
		Status:  status,
	}

	err = NewAction(zkConn, productName, ACTION_TYPE_MULTI_SLOT_CHANGED, param, "", true)
	return errors.Trace(err)
}

func SetSlotRange(zkConn zkhelper.Conn, productName string, fromSlot, toSlot, groupId int, status SlotStatus) error {
	if status != SLOT_STATUS_OFFLINE && status != SLOT_STATUS_ONLINE {
		return errors.New("invalid status")
	}

	ok, err := GroupExists(zkConn, productName, groupId)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		return errors.NotFoundf("group %d", groupId)
	}

	for i := fromSlot; i <= toSlot; i++ {
		s, err := GetSlot(zkConn, productName, i)
		if err != nil {
			return errors.Trace(err)
		}
		s.GroupId = groupId
		s.State.Status = status
		data, err := json.Marshal(s)
		if err != nil {
			return errors.Trace(err)
		}

		zkPath := GetSlotPath(productName, i)
		_, err = zkhelper.CreateOrUpdate(zkConn, zkPath, string(data), 0, zkhelper.DefaultFileACLs(), true)
		if err != nil {
			return errors.Trace(err)
		}
	}

	param := SlotMultiSetParam{
		From:    fromSlot,
		To:      toSlot,
		GroupId: groupId,
		Status:  status,
	}
	err = NewAction(zkConn, productName, ACTION_TYPE_MULTI_SLOT_CHANGED, param, "", true)
	return errors.Trace(err)
}

// danger operation !
func InitSlotSet(zkConn zkhelper.Conn, productName string, totalSlotNum int) error {
	for i := 0; i < totalSlotNum; i++ {
		slot := NewSlot(productName, i)
		if err := slot.Update(zkConn); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *Slot) SetMigrateStatus(zkConn zkhelper.Conn, fromGroup, toGroup int) error {
	if fromGroup < 0 || toGroup < 0 {
		return errors.Errorf("invalid group id, from %d, to %d", fromGroup, toGroup)
	}
	// wait until all proxy confirmed
	err := NewAction(zkConn, s.ProductName, ACTION_TYPE_SLOT_PREMIGRATE, s, "", true)
	if err != nil {
		return errors.Trace(err)
	}

	s.State.Status = SLOT_STATUS_MIGRATE
	s.State.MigrateStatus.From = fromGroup
	s.State.MigrateStatus.To = toGroup

	s.GroupId = toGroup

	return s.Update(zkConn)
}

func (s *Slot) Update(zkConn zkhelper.Conn) error {
	// status validation
	switch s.State.Status {
	case SLOT_STATUS_MIGRATE, SLOT_STATUS_OFFLINE,
		SLOT_STATUS_ONLINE, SLOT_STATUS_PRE_MIGRATE:
		{
			// valid status, OK
		}
	default:
		{
			return errors.Trace(ErrUnknownSlotStatus)
		}
	}

	data, err := json.Marshal(s)
	if err != nil {
		return errors.Trace(err)
	}
	zkPath := GetSlotPath(s.ProductName, s.Id)
	_, err = zkhelper.CreateOrUpdate(zkConn, zkPath, string(data), 0, zkhelper.DefaultFileACLs(), true)
	if err != nil {
		return errors.Trace(err)
	}

	if s.State.Status == SLOT_STATUS_MIGRATE {
		err = NewAction(zkConn, s.ProductName, ACTION_TYPE_SLOT_MIGRATE, s, "", true)
	} else {
		err = NewAction(zkConn, s.ProductName, ACTION_TYPE_SLOT_CHANGED, s, "", true)
	}

	return errors.Trace(err)
}
