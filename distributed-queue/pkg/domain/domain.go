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

// Namespace for queue messages.
type Namespace struct {
	Id   UUID
	Name string
}

// Message represents a single message that can be sent to the queue
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

// UUID type is a custom-built identifier for sharded records.
// It combines a shard identifier (4 bytes) with a generated XID (12 bytes).
// This way, every record identifier can be immediately matched to its sharded
// location by extracting the first 4 bytes of UUID.
//
// With this application design I decided to employ XIDs instead of UUIDs because
// XIDs have a time-based component and are inherently sortable. This feature is
// essential in database optimization like when creating indexes.
type UUID [16]byte

// XID returns the XID component of the UUID (bytes 4-16)
func (u *UUID) XID() xid.ID {
	v, err := xid.FromBytes(u[4:])
	if err != nil {
		panic(err)
	}
	return v
}

// ShardId returns the shard id component of the UUID (bytes 0-4)
func (u *UUID) ShardId() uint32 {
	return binary.BigEndian.Uint32(u[:4])
}

// String representation of the custom UUID
func (u *UUID) String() string {
	return fmt.Sprintf("%d-%s", u.ShardId(), u.XID().String())
}

// Scan implementation of the sql.Scanner interface to read UUID from databases.
// Because this is a custom type, the only way to store this into a database is
// using a []byte representation like PostgreSQL's BYTEA. Implementing the Scanner
// interface will enalbe the standard database/sql package to load these values
// directly into a UUID type.
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

// Bytes representation of the UUID
func (u *UUID) Bytes() []byte {
	return u[0:]
}

// NewUUID generates a new UUID based on the given shardId and a generate
// XID component.
func NewUUID(shardId uint32) UUID {
	var uid UUID
	binary.BigEndian.PutUint32(uid[:4], shardId)
	copy(uid[4:], xid.New().Bytes())

	return uid
}

// ParseUUID from its string representation
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
