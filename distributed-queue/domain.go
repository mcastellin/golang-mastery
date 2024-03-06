package main

import (
	"time"

	"github.com/google/uuid"
)

type ShardConnector interface {
	Connect(ShardHasher) (*ShardMeta, error)
}

type Queue struct {
	ID   uuid.UUID
	Name string
}

func (q *Queue) CreateTable(c ShardConnector) error {
	statement := `CREATE TABLE IF NOT EXISTS queues (
		Id UUID PRIMARY KEY,
		Name VARCHAR(50)
	)`

	shard, err := c.Connect(q)
	if err != nil {
		return err
	}

	_, err = shard.Conn().Exec(statement)
	return err
}

func (q *Queue) Get(c ShardConnector) error {
	statement := "SELECT Name FROM queues WHERE Id = $1;"
	shard, err := c.Connect(q)
	if err != nil {
		return err
	}
	return shard.Conn().QueryRow(statement, q.ID).Scan(&q.Name)
}

func (q *Queue) Create(c ShardConnector) error {
	statement := "INSERT INTO queues (Id, Name) VALUES ($1, $2) RETURNING Id;"

	shard, err := c.Connect(q)
	if err != nil {
		return err
	}
	return shard.Conn().QueryRow(statement, genUUID(), q.Name).Scan(&q.ID)
}

func SearchQueues(c ShardConnector) ([]Queue, error) {
	statement := "SELECT Id, Name FROM queues;"
	shard, err := c.Connect(nil)
	if err != nil {
		return nil, err
	}

	rows, err := shard.Conn().Query(statement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vals []Queue
	for rows.Next() {
		var v Queue
		if err := rows.Scan(&v.ID, &v.Name); err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}

	return vals, nil
}

func (q *Queue) Hashed() int {
	return 0 // all in one shard for now
}

type Message struct {
	Id           uuid.UUID
	Topic        string
	Priority     uint32
	Queue        *Queue
	Payload      []byte
	Metadata     []byte
	DeliverAfter time.Duration
	TTL          time.Duration
}

func (msg *Message) CreateTable(c ShardConnector) error {
	statement := `CREATE TABLE IF NOT EXISTS messages (
		Id UUID PRIMARY KEY,
		Topic VARCHAR(50),
		Priority INTEGER,
		QueueId UUID,
		Payload BYTEA,
		Metadata BYTEA,
		DeliverAfter INTERVAL,
		TTL INTERVAL
	)`

	shard, err := c.Connect(msg)
	if err != nil {
		return err
	}
	_, err = shard.Conn().Exec(statement)
	return err
}

func (msg *Message) Create(shard *ShardMeta) error {
	statement := `INSERT INTO messages (
		Id, Topic, Priority, QueueId,
		Payload, Metadata, DeliverAfter, TTL
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	RETURNING Id;`

	return shard.Conn().QueryRow(statement,
		genUUID(),
		msg.Topic,
		msg.Priority,
		msg.Queue.ID,
		msg.Payload,
		msg.Metadata,
		msg.DeliverAfter,
		msg.TTL,
	).Scan(&msg.Id)
}

func (msg *Message) Hashed() int {
	return 0 // all in one shard for now
}

func genUUID() uuid.UUID {
	return uuid.New()
}
