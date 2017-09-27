create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select dateadd('month', 1, timestamp '2003-01-31 10:20:30.012345678') d1 from test;
> D1
> -----------------------------
> 2003-02-28 10:20:30.012345678
> rows: 1

select dateadd('year', -1, timestamp '2000-02-29 10:20:30.012345678') d1 from test;
> D1
> -----------------------------
> 1999-02-28 10:20:30.012345678
> rows: 1
