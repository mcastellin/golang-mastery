package db

import (
	"database/sql"

	"go.uber.org/zap"
)

// ShardMeta represents a connected database shard
type ShardMeta struct {
	Id         uint32
	ConnString string

	conn *sql.DB
	main bool
}

// Conn returns an active sql.DB connection that can be used to
// communicate with the shard
func (meta *ShardMeta) Conn() *sql.DB {
	return meta.conn
}

func (m *ShardMeta) initialize() error {

	// TODO read shard information from database
	// at the moment shards are added using fixed configuration
	// but this is not scalabe. We need to allow adding and removing
	// shards dynamically for horizontal scaling the service.
	// This means that information about the shardId and its content
	// should live inside the database and loaded using this function
	// after the initial connection.
	return nil
}

// ShardManager maintains the state of active database shards
type ShardManager struct {
	Logger *zap.Logger
	shards []*ShardMeta
	index  map[uint32]*ShardMeta
}

// Add a connection to an existing database shard
func (m *ShardManager) Add(shardId uint32, main bool, connString string) (*ShardMeta, error) {
	dbConn, err := sql.Open("postgres", connString)
	if err != nil {
		return nil, err
	}

	meta := &ShardMeta{
		Id:         shardId,
		ConnString: connString,
		conn:       dbConn,
		main:       main,
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

// Shards returns the list of active ShardMeta
func (m *ShardManager) Shards() []*ShardMeta {
	return m.shards
}

// Get an active shard by its ID
func (m *ShardManager) Get(id uint32) *ShardMeta {
	return m.index[id]
}

// MainShard returns the shard that acts as a "main" to store common
// non-sharded information
func (m *ShardManager) MainShard() *ShardMeta {
	for _, m := range m.shards {
		if m.main {
			return m
		}
	}
	return nil
}

// Close all active connections to shards
func (m *ShardManager) Close() {
	for _, meta := range m.shards {
		if err := meta.Conn().Close(); err != nil {
			m.Logger.Error("error closing connection to shard",
				zap.Uint32("shardId", meta.Id),
				zap.Error(err))
		}
	}
}
