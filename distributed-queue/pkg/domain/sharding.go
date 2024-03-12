package domain

import (
	"database/sql"
	"fmt"
)

type ShardMeta struct {
	Id         uint32
	ConnString string

	conn   *sql.DB
	master bool
}

func (meta *ShardMeta) Conn() *sql.DB {
	return meta.conn
}

func (m *ShardMeta) initialize() error {

	// TODO store shard information in a table
	return nil
}

type ShardManager struct {
	shards []*ShardMeta
	index  map[uint32]*ShardMeta
}

func (m *ShardManager) Add(shardId uint32, master bool, connString string) (*ShardMeta, error) {
	dbConn, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}

	meta := &ShardMeta{
		Id:         shardId,
		ConnString: connString,
		conn:       dbConn,
		master:     master,
	}
	if err := meta.initialize(); err != nil {
		meta.conn.Close() // bad shard initialization: closing
		return nil, err
	}
	m.shards = append(m.shards, meta)

	if m.index == nil {
		m.index = map[uint32]*ShardMeta{}
	}
	m.index[meta.Id] = meta

	return meta, nil
}

func (m *ShardManager) Shards() []*ShardMeta {
	return m.shards
}

func (m *ShardManager) Get(id uint32) *ShardMeta {
	return m.index[id]
}

func (m *ShardManager) Master() *ShardMeta {
	for _, m := range m.shards {
		if m.master {
			return m
		}
	}
	return nil
}

func (m *ShardManager) Close() {
	for _, meta := range m.shards {
		if err := meta.Conn().Close(); err != nil {
			fmt.Printf("%v\n", err)
		}
	}
}
