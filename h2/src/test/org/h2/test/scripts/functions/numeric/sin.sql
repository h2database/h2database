create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select sin(null) vn, sin(-1) r1 from test;
> VN   R1
> ---- -------------------
> null -0.8414709848078965
> rows: 1


