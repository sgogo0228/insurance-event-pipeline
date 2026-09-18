-- Application user also acts as the CDC user, so it needs the REPLICATION attribute.
create user app with password 'app' replication;
create database insurance_app owner app;

create user airflow with password 'airflow';
create database airflow owner airflow;

\connect insurance_app app

create sequence policy_seq;

-- OLTP table of the policy admin system (source of CDC).
create table policies (
    policy_id      text        primary key default 'P' || lpad(nextval('policy_seq')::text, 6, '0'),
    customer_id    text        not null,
    product_code   text        not null,
    sum_insured    bigint      not null,
    annual_premium bigint      not null,
    status         text        not null default 'active' check (status in ('active', 'lapsed', 'surrendered')),
    issued_at      timestamptz not null default now(),
    updated_at     timestamptz not null default now()
);

-- Emit full "before" row images on UPDATE/DELETE so CDC events show what changed.
alter table policies replica identity full;

-- OLTP tables of the billing & claims system (append-only, published by the polling producer).
-- event_id is fixed per row, so a re-published row keeps the same event_id and can be deduplicated.
create table payments (
    id             bigserial   primary key,
    event_id       uuid        not null default gen_random_uuid(),
    payment_id     text        not null unique,
    policy_id      text        not null,
    amount         bigint      not null,
    payment_method text        not null,
    paid_at        timestamptz not null default now()
);

create table claims (
    id           bigserial   primary key,
    event_id     uuid        not null default gen_random_uuid(),
    claim_id     text        not null unique,
    policy_id    text        not null,
    claim_amount bigint      not null,
    claim_detail jsonb       not null,
    filed_at     timestamptz not null default now()
);

-- Polling producer state: the last row id already published per source table.
create table publisher_checkpoints (
    source_table text        primary key,
    last_id      bigint      not null,
    updated_at   timestamptz not null default now()
);
insert into publisher_checkpoints (source_table, last_id) values ('payments', 0), ('claims', 0);

-- OLTP table of the notification service: the unique key guarantees one notification per claim.
create table notifications (
    claim_id     text        primary key,
    policy_id    text        not null,
    claim_amount bigint      not null,
    event_id     text        not null,
    notified_at  timestamptz not null default now()
);
