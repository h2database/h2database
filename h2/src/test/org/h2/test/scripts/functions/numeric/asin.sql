create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select asin(null) vn, asin(-1) r1  from test;
> VN   R1
> ---- -------------------
> null -1.5707963267948966
> rows: 1

