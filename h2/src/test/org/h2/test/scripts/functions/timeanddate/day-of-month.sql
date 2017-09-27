create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select dayofmonth(date '2005-09-12') d12 from test;
> D12
> ---
> 12
> rows: 1
