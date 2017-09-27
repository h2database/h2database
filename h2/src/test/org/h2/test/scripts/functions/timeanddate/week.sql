create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select week(date '2003-01-09') d1 from test;
> D1
> --
> 2
> rows: 1
