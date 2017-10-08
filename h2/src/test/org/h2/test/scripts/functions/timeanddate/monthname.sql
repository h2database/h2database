create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select monthname(date '2005-09-12') d_sept from test;
> D_SEPT
> ---------
> September
> rows: 1
