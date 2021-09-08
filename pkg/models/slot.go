// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"encoding/json"

	"github.com/CodisLabs/codis/pkg/utils/errors"
)

type SlotStatus string

const (
	SLOT_STATUS_ONLINE      SlotStatus = "online"
	SLOT_STATUS_OFFLINE     SlotStatus = "offline"
	SLOT_STATUS_MIGRATE     SlotStatus = "migrate"
	SLOT_STATUS_PRE_MIGRATE SlotStatus = "pre_migrate"
)

var (
	ErrSlotAlreadyExists = errors.New("slots already exists")
	ErrUnknownSlotStatus = errors.New("unknown slot status, slot status should be (online, offline, migrate, pre_migrate)")
)

type SlotMigrateStatus struct {
	From int `json:"from"`
	To   int `json:"to"`
}

type SlotMultiSetParam struct {
	From    int        `json:"from"`
	To      int        `json:"to"`
	Status  SlotStatus `json:"status"`
	GroupId int        `json:"group_id"`
}

type SlotState struct {
	Status        SlotStatus        `json:"status"`
	MigrateStatus SlotMigrateStatus `json:"migrate_status"`
	LastOpTs      string            `json:"last_op_ts"` // operation timestamp
}

type Slot struct {
	ProductName string    `json:"product_name"`
	Id          int       `json:"id"`
	GroupId     int       `json:"group_id"`
	State       SlotState `json:"state"`
}

func (s *Slot) String() string {
	b, _ := json.MarshalIndent(s, "", "  ")
	return string(b)
}

func NewSlot(productName string, id int) *Slot {
	return &Slot{
		ProductName: productName,
		Id:          id,
		GroupId:     INVALID_ID,
		State: SlotState{
			Status:   SLOT_STATUS_OFFLINE,
			LastOpTs: "0",
			MigrateStatus: SlotMigrateStatus{
				From: INVALID_ID,
				To:   INVALID_ID,
			},
		},
	}
}

func (m *Slot) Encode() []byte {
	return jsonEncode(m)
}
