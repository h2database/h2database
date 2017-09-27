create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select cot(null) vn, cot(-1) r1 from test;
> VN   R1
> ---- -------------------
> null -0.6420926159343306
> rows: 1


