package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rs/xid"
)

type Queue struct {
	Id   UUID
	Name string
}

func (q *Queue) Get(shard *ShardMeta) error {
	statement := "SELECT Id, Name FROM queues WHERE Id = $1"
	return shard.Conn().QueryRow(statement, q.Id.Bytes()).Scan(&q.Id, &q.Name)
}

func (q *Queue) Create(shard *ShardMeta) error {
	statement := "INSERT INTO queues (Id, Name) VALUES ($1, $2) RETURNING Id"

	newUid := NewUUID(shard.Id)
	return shard.Conn().QueryRow(statement, newUid.Bytes(), q.Name).Scan(&q.Id)
}

func SearchQueues(shard *ShardMeta) ([]Queue, error) {
	statement := "SELECT Id, Name FROM queues"

	rows, err := shard.Conn().Query(statement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vals []Queue
	for rows.Next() {
		var v Queue
		if err := rows.Scan(&v.Id, &v.Name); err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}

	return vals, nil
}

type Message struct {
	Id           UUID
	Topic        string
	Priority     uint32
	Queue        *Queue
	Payload      []byte
	Metadata     []byte
	DeliverAfter time.Duration
	TTL          time.Duration
}

func (msg *Message) Create(shard *ShardMeta) error {
	statement := `INSERT INTO messages (
		Id, Topic, Priority, QueueId,
		Payload, Metadata, DeliverAfter, TTL
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	RETURNING Id`

	newUid := NewUUID(shard.Id)

	return shard.Conn().QueryRow(statement,
		newUid.Bytes(),
		msg.Topic,
		msg.Priority,
		msg.Queue.Id.Bytes(),
		msg.Payload,
		msg.Metadata,
		msg.DeliverAfter,
		msg.TTL,
	).Scan(&msg.Id)
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
