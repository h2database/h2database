create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select sign(null) en, sign(10) e1, sign(0) e0, sign(-0.1) em1 from test;
> EN   E1 E0 EM1
> ---- -- -- ---
> null 1  0  -1
> rows: 1




