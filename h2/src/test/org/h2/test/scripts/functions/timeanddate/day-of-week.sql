create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select dayofweek(date '2005-09-12') d2 from test;
> D2
> --
> 2
> rows: 1
