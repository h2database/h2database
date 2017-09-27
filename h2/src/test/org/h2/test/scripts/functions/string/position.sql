create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select position(null, null) en, position(null, 'abc') en1, position('World', 'Hello World') e7, position('hi', 'abchihihi') e1 from test;
> EN   EN1  E7 E1
> ---- ---- -- --
> null null 7  4
> rows: 1
