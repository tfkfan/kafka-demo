create table public.weekly_summary
(
    user_id varchar(255) not null
        constraint weekly_summary_pk
            primary key,
    amount  numeric
);

alter table public.weekly_summary
    owner to postgres;
