create database if not exists test_task;
create table if not exists fact_login
(
    ts      DateTime default now(),
    user_id UInt32
)
    engine = MergeTree() order by (ts,user_id);

create table if not exists fact_payment
(
    ts      DateTime default now(),
    user_id UInt32,
    usd     Nullable(UInt32)
)
    engine = MergeTree() order by (ts,user_id);

create table if not exists fact_reg
(
    ts      DateTime default now(),
    user_id UInt32,
    ref     String
)
    engine = MergeTree() order by (ts,user_id);

