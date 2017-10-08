create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select TRIM(BOTH '_' FROM '__A__') A, TRIM(LEADING FROM '    B    ') BS, TRIM(TRAILING 'x' FROM 'xAx') XA from test;
> A BS XA
> - -- --
> A B  xA
> rows: 1
