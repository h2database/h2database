create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select hour(time '23:10:59') d23 from test;
> D23
> ---
> 23
> rows: 1
