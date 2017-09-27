create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select bitand(null, 1) vn, bitand(1, null) vn1, bitand(null, null) vn2, bitand(3, 6) e2 from test;
> VN   VN1  VN2  E2
> ---- ---- ---- --
> null null null 2
> rows: 1




