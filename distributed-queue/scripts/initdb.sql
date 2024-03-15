CREATE TABLE IF NOT EXISTS namespaces (
    id BYTEA PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS messages (
    id BYTEA PRIMARY KEY,
    topic VARCHAR(50) NOT NULL,
    priority INTEGER NOT NULL,
    namespace BYTEA NOT NULL,
    payload BYTEA NOT NULL,
    metadata BYTEA NOT NULL,
    deliverafter INTERVAL NOT NULL,
    ttl INTERVAL NOT NULL,
    readyat TIMESTAMP NOT NULL,
    expiresat TIMESTAMP NOT NULL,
    prefetched BOOLEAN DEFAULT false
);

CREATE INDEX IF NOT EXISTS topic_id_idx ON messages (topic, id);

CREATE INDEX IF NOT EXISTS messages_filter_idx ON messages (prefetched, readyat, expiresat)
WHERE prefetched = false; -- partial index assuming prefetched = false most of the time
