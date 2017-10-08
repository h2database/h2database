create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select acos(null) vn, acos(-1) r1 from test;
> VN   R1
> ---- -----------------
> null 3.141592653589793
> rows: 1

