create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select bitor(null, 1) vn, bitor(1, null) vn1, bitor(null, null) vn2, bitor(3, 6) e7 from test;
> VN   VN1  VN2  E7
> ---- ---- ---- --
> null null null 7
> rows: 1




