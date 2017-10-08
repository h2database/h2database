create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select abs(-1) r1, abs(id) r1b from test;
> R1 R1B
> -- ---
> 1  1
> rows: 1

select abs(sum(id)) r1 from test;
> R1
> --
> 1
> rows: 1

select abs(null) vn, abs(-1) r1, abs(1) r2, abs(0) r3, abs(-0.1) r4, abs(0.1) r5 from test;
> VN   R1 R2 R3 R4  R5
> ---- -- -- -- --- ---
> null 1  1  0  0.1 0.1
> rows: 1

