package main

import (
	"database/sql"
	"fmt"
)

type shardBins map[int]int

type ShardHasher interface {
	Hashed() int
}

type ShardMeta struct {
	Name       string
	ConnString string

	conn *sql.DB
}

func (meta *ShardMeta) Conn() *sql.DB {
	return meta.conn
}

type ShardManager struct {
	shards []*ShardMeta
}

func (m *ShardManager) Add(shardName string, connString string) (*ShardMeta, error) {
	dbConn, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}

	meta := &ShardMeta{
		Name:       shardName,
		ConnString: connString,
		conn:       dbConn,
	}
	m.shards = append(m.shards, meta)

	return meta, nil
}

func (m *ShardManager) Connect(v ShardHasher) (*ShardMeta, error) {
	idx := m.calculateBin(v)
	return m.shards[idx], nil
}

func (m *ShardManager) calculateBin(v ShardHasher) int {
	hash := 0
	if v != nil {
		hash = v.Hashed()
	}
	return int(hash % len(m.shards))
}

func (m *ShardManager) Close() {
	for _, meta := range m.shards {
		if err := meta.Conn().Close(); err != nil {
			fmt.Printf("%v\n", err)
		}
	}
}
