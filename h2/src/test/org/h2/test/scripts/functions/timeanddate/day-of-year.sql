create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select dayofyear(date '2005-01-01') d1 from test;
> D1
> --
> 1
> rows: 1
