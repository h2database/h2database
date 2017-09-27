create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select year(date '2005-01-01') d2005 from test;
> D2005
> -----
> 2005
> rows: 1
