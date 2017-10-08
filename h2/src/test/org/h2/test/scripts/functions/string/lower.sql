create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select lower(null) en, lower('Hello') hello, lower('ABC') abc from test;
> EN   HELLO ABC
> ---- ----- ---
> null hello abc
> rows: 1

select lcase(null) en, lcase('Hello') hello, lcase('ABC') abc from test;
> EN   HELLO ABC
> ---- ----- ---
> null hello abc
> rows: 1
