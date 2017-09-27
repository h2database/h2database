create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select floor(null) vn, floor(1) v1, floor(1.1) v2, floor(-1.1) v3, floor(1.9) v4, floor(-1.9) v5 from test;
> VN   V1  V2  V3   V4  V5
> ---- --- --- ---- --- ----
> null 1.0 1.0 -2.0 1.0 -2.0
> rows: 1






