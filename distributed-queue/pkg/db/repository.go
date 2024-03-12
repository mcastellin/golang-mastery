package db

import (
	"database/sql"
	"time"

	"github.com/lib/pq"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/domain"
)

type NamespaceRepository struct{}

func (r *NamespaceRepository) Save(shard *ShardMeta, item *domain.Namespace) error {
	statement := "INSERT INTO namespaces (id, name) VALUES ($1, $2) RETURNING id"

	newUid := domain.NewUUID(shard.Id)
	return shard.Conn().QueryRow(statement, newUid.Bytes(), item.Name).Scan(&item.Id)
}

func (r *NamespaceRepository) FindByStringId(shard *ShardMeta, id string) (*domain.Namespace, error) {
	uid, err := domain.ParseUUID(id)
	if err != nil {
		return nil, err
	}
	statement := "SELECT id, name FROM namespaces WHERE id = $1"
	var item domain.Namespace
	err = shard.Conn().QueryRow(statement, uid.Bytes()).Scan(&item.Id, &item.Name)
	return &item, err
}

func (r *NamespaceRepository) FindAll(shard *ShardMeta, fns ...OptsFn) ([]domain.Namespace, error) {
	statement := "SELECT id, name FROM namespaces LIMIT $1"

	opts := &sqlOpts{}
	opts.withDefaults(fns)

	rows, err := shard.Conn().Query(statement, opts.rows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vals []domain.Namespace
	for rows.Next() {
		var v domain.Namespace
		if err := rows.Scan(&v.Id, &v.Name); err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}

	return vals, nil
}

type MessageRepository struct{}

func (r *MessageRepository) Save(shard *ShardMeta, item *domain.Message) error {
	statement := `INSERT INTO messages (
		id, topic, priority, namespace,
		payload, metadata, deliverafter, ttl,
		readyat, expiresat
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	RETURNING id`

	newUid := domain.NewUUID(shard.Id)

	return shard.Conn().QueryRow(statement,
		newUid.Bytes(),
		item.Topic,
		item.Priority,
		item.Namespace.Id.Bytes(),
		item.Payload,
		item.Metadata,
		item.DeliverAfter,
		item.TTL,
		time.Now().Add(item.DeliverAfter),
		time.Now().Add(item.TTL),
	).Scan(&item.Id)
}

func (r *MessageRepository) Ack(shard *ShardMeta, uid domain.UUID, ack bool) error {
	var statement string
	if ack {
		statement = `DELETE FROM messages WHERE id = $1`
	} else {
		statement = `UPDATE messages SET prefetched = false WHERE id = $1`
	}
	_, err := shard.Conn().Exec(statement, uid.Bytes())
	return err
}

func (r *MessageRepository) FindMessagesReadyForDelivery(shard *ShardMeta, prefetched bool,
	excludedTopics []string, maxRowsByTopic int, fns ...OptsFn) ([]domain.Message, error) {

	statement := `WITH ranked AS(
		SELECT id, topic, priority, payload, metadata,
		ROW_NUMBER() OVER (PARTITION BY topic ORDER BY id) AS rn
		FROM messages
		WHERE readyat <= $1 AND expiresat > $1 AND prefetched = $2 AND NOT topic = ANY($3)
		ORDER BY priority
	)
	SELECT id, topic, priority, payload, metadata FROM ranked
	WHERE rn <= $4 LIMIT $5`

	// TODO:
	// Store lease duration and lease identifier when prefetching
	// Include in pre-fetch rows with expired leases
	// Sort returned rows by ascending priority

	opts := &sqlOpts{}
	opts.withDefaults(fns)

	rows, err := shard.Conn().Query(statement,
		time.Now(), prefetched, pq.Array(excludedTopics), maxRowsByTopic, opts.rows)
	if err != nil {
		return nil, err
	}

	results := []domain.Message{}
	for rows.Next() {
		item := domain.Message{}
		rows.Scan(&item.Id, &item.Topic, &item.Priority, &item.Payload, &item.Metadata)
		results = append(results, item)
	}
	return results, nil
}

func (r *MessageRepository) UpdatePrefetchedBatch(shard *ShardMeta, ids []domain.UUID, v bool) (*sql.Tx, error) {
	tx, err := shard.Conn().Begin()
	if err != nil {
		return nil, err
	}

	statement := `UPDATE messages SET prefetched = $1 WHERE id=ANY($2)`
	_, err = tx.Exec(statement, v, uuidToByteArray(ids))
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	return tx, nil
}

func uuidToByteArray(items []domain.UUID) interface{} {
	arr := make([][]byte, len(items))
	for idx, item := range items {
		arr[idx] = item.Bytes()
	}
	return pq.Array(arr)
}

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
