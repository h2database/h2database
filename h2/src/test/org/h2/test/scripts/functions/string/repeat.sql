create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select repeat(null, null) en, repeat('Ho', 2) abcehoho , repeat('abc', 0) ee from test;
> EN   ABCEHOHO EE
> ---- -------- --
> null HoHo
> rows: 1
