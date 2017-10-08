create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select minute(timestamp '2005-01-01 23:10:59') d10 from test;
> D10
> ---
> 10
> rows: 1
