package main

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/rs/xid"
)

type Namespace struct {
	Id   UUID
	Name string
}

func (q *Namespace) Get(shard *ShardMeta) error {
	statement := "SELECT id, name FROM namespaces WHERE id = $1"
	return shard.Conn().QueryRow(statement, q.Id.Bytes()).Scan(&q.Id, &q.Name)
}

func (q *Namespace) Create(shard *ShardMeta) error {
	statement := "INSERT INTO namespaces (id, name) VALUES ($1, $2) RETURNING id"

	newUid := NewUUID(shard.Id)
	return shard.Conn().QueryRow(statement, newUid.Bytes(), q.Name).Scan(&q.Id)
}

func SearchNamespaces(shard *ShardMeta) ([]Namespace, error) {
	statement := "SELECT id, name FROM namespaces"

	rows, err := shard.Conn().Query(statement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vals []Namespace
	for rows.Next() {
		var v Namespace
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
	Namespace    *Namespace
	Payload      []byte
	Metadata     []byte
	DeliverAfter time.Duration
	TTL          time.Duration
}

func (msg *Message) Create(shard *ShardMeta) error {
	statement := `INSERT INTO messages (
		id, topic, priority, namespace,
		payload, metadata, deliverafter, ttl,
		readyat, expiresat
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	RETURNING id`

	newUid := NewUUID(shard.Id)

	return shard.Conn().QueryRow(statement,
		newUid.Bytes(),
		msg.Topic,
		msg.Priority,
		msg.Namespace.Id.Bytes(),
		msg.Payload,
		msg.Metadata,
		msg.DeliverAfter,
		msg.TTL,
		time.Now().Add(msg.DeliverAfter),
		time.Now().Add(msg.TTL),
	).Scan(&msg.Id)
}

func SearchMessages(shard *ShardMeta, prefetched bool, excludedTopics []string,
	maxRowsByTopic int, optsFn ...OptsFn) ([]Message, error) {

	statement := `WITH ranked AS(
		SELECT id, topic, priority, payload, metadata,
		ROW_NUMBER() OVER (PARTITION BY topic ORDER BY id) AS rn
		FROM messages
		WHERE readyat <= $1 AND expiresat > $1 AND prefetched = $2 AND topic NOT IN ($3)
		ORDER BY priority
	)
	SELECT id, topic, priority, payload, metadata FROM ranked
	WHERE rn <= $4 LIMIT $5`

	// TODO:
	// Store lease duration and lease identifier when prefetching
	// Include in pre-fetch rows with expired leases
	// Sort returned rows by ascending priority

	opts := &sqlOpts{}
	opts.withDefaults(optsFn)

	rows, err := shard.Conn().Query(statement,
		time.Now(), prefetched, pq.Array(excludedTopics), maxRowsByTopic, opts.rows)
	if err != nil {
		return nil, err
	}

	results := []Message{}
	for rows.Next() {
		item := Message{}
		rows.Scan(&item.Id, &item.Topic, &item.Priority, &item.Payload, &item.Metadata)
		results = append(results, item)
	}
	return results, nil
}

func UpdatePrefetchedBatch(shard *ShardMeta, ids []UUID, v bool) (*sql.Tx, error) {
	tx, err := shard.Conn().Begin()
	if err != nil {
		return nil, err
	}

	statement := `UPDATE messages SET prefetched = $1 WHERE id=ANY($2)`
	_, err = tx.Exec(statement, v, byteArray(ids))
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	return tx, nil
}

func byteArray(items []UUID) interface{} {
	arr := make([][]byte, len(items))
	for idx, item := range items {
		arr[idx] = item.Bytes()
	}
	return pq.Array(arr)
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

const (
	defaultLimitRows = 100
)

type OptsFn func(*sqlOpts)

type sqlOpts struct {
	rows   int
	offset int
}

func (opts *sqlOpts) withDefaults(fns []OptsFn) {
	opts.offset = 0
	opts.rows = defaultLimitRows

	for _, f := range fns {
		f(opts)
	}
}

func WithLimit(rows int) OptsFn {
	return func(opts *sqlOpts) {
		opts.rows = rows
	}
}
func WithOffset(offset int) OptsFn {
	return func(opts *sqlOpts) {
		opts.offset = offset
	}
}
