package domain

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rs/xid"
)

type Namespace struct {
	Id   UUID
	Name string
}

type Message struct {
	Id           UUID
	Topic        string
	Priority     uint32
	Namespace    *Namespace
	Payload      []byte
	Metadata     []byte
	DeliverAfter time.Duration
	TTL          time.Duration
}

type UUID [16]byte

func (u *UUID) XID() xid.ID {
	v, err := xid.FromBytes(u[4:])
	if err != nil {
		panic(err)
	}
	return v
}

func (u *UUID) ShardId() uint32 {
	return binary.BigEndian.Uint32(u[:4])
}

func (u *UUID) String() string {
	return fmt.Sprintf("%d-%s", u.ShardId(), u.XID().String())
}

func (u *UUID) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		if len(src) != 16 {
			return fmt.Errorf("invalid key length: %d", len(src))
		}
		copy(u[0:], src)
		return nil
	}
	return nil
}

func (u *UUID) Bytes() []byte {
	return u[0:]
}

func NewUUID(shardId uint32) UUID {
	var uid UUID
	binary.BigEndian.PutUint32(uid[:4], shardId)
	copy(uid[4:], xid.New().Bytes())

	return uid
}

func ParseUUID(v string) (*UUID, error) {
	tokens := strings.Split(v, "-")
	if len(tokens) != 2 {
		return nil, ErrInvalidUUIDFormat
	}

	sid, err := strconv.Atoi(tokens[0])
	if err != nil {
		return nil, ErrInvalidUUIDFormat
	}
	xuid, err := xid.FromString(tokens[1])
	if err != nil {
		return nil, ErrInvalidUUIDFormat
	}

	var uid UUID
	binary.BigEndian.PutUint32(uid[:4], uint32(sid))
	copy(uid[4:], xuid.Bytes())
	return &uid, nil
}

var (
	ErrInvalidUUIDFormat = errors.New("invalid UUID format")
)
