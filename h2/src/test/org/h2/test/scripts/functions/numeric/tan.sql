create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select tan(null) vn, tan(-1) r1 from test;
> VN   R1
> ---- -------------------
> null -1.5574077246549023
> rows: 1



