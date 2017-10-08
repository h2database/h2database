create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1


select cast(null as varchar(255)) xn, cast(' 10' as int) x10, cast(' 20 ' as int) x20 from test;
> XN   X10 X20
> ---- --- ---
> null 10  20
> rows: 1
