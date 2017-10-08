create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select second(timestamp '2005-01-01 23:10:59') d59 from test;
> D59
> ---
> 59
> rows: 1
