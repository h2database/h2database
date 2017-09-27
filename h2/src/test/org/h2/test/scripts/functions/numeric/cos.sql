create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select cos(null) vn, cos(-1) r1 from test;
> VN   R1
> ---- ------------------
> null 0.5403023058681398
> rows: 1


