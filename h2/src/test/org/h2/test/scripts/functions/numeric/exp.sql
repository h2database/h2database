create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select exp(null) vn, left(exp(1), 4) v1, left(exp(1.1), 4) v2, left(exp(-1.1), 4) v3, left(exp(1.9), 4) v4, left(exp(-1.9), 4) v5 from test;
> VN   V1   V2   V3   V4   V5
> ---- ---- ---- ---- ---- ----
> null 2.71 3.00 0.33 6.68 0.14
> rows: 1



