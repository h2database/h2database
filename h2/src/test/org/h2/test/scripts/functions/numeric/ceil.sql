create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select ceil(null) vn, ceil(1) v1, ceiling(1.1) v2, ceil(-1.1) v3, ceiling(1.9) v4, ceiling(-1.9) v5 from test;
> VN   V1  V2  V3   V4  V5
> ---- --- --- ---- --- ----
> null 1.0 2.0 -1.0 2.0 -1.0
> rows: 1





