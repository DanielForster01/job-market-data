create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists audit;

create schema if not exists staging;
create schema if not exists intermediate;
create schema if not exists marts;

create table if not exists audit.pipeline_runs (
    run_id bigserial primary key,
    pipeline_name text not null,
    status text not null,
    started_at timestamptz default now(),
    finished_at timestamptz,
    message text
);