create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select convert(null, varchar(255)) xn, convert(' 10', int) x10, convert(' 20 ', int) x20 from test;
> XN   X10 X20
> ---- --- ---
> null 10  20
> rows: 1
