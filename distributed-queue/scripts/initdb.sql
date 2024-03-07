CREATE TABLE IF NOT EXISTS queues (
    Id BYTEA PRIMARY KEY,
    Name VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS messages (
    Id BYTEA PRIMARY KEY,
    Topic VARCHAR(50) NOT NULL,
    Priority INTEGER NOT NULL,
    QueueId BYTEA NOT NULL,
    Payload BYTEA NOT NULL,
    Metadata BYTEA NOT NULL,
    DeliverAfter INTERVAL NOT NULL,
    TTL INTERVAL NOT NULL
);
