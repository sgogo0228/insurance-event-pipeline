create database if not exists raw;

-- Bronze layer: every Kafka message from every topic, stored as-is (append-only, no parsing).
create table if not exists raw.kafka_messages
(
    topic           LowCardinality(String),
    kafka_partition UInt32,
    kafka_offset    UInt64,
    message_key     Nullable(String),
    message_value   String,
    kafka_timestamp DateTime64(3, 'UTC'),
    ingested_at     DateTime64(3, 'UTC') default now64(3)
)
engine = MergeTree
partition by toYYYYMM(ingested_at)
order by (topic, ingested_at);
