create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');

select difference(null, null) en, difference('a', null) en1, difference(null, 'a') en2 from test;
> EN   EN1  EN2
> ---- ---- ----
> null null null
> rows: 1

select difference('abc', 'abc') e0, difference('Thomas', 'Tom') e1 from test;
> E0 E1
> -- --
> 4  3
> rows: 1


