create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select nullif(null, null) xn, nullif('a', 'a') xn, nullif('1', '2') x1 from test;
> XN   XN   X1
> ---- ---- --
> null null 1
> rows: 1
