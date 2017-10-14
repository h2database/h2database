create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select replace(null, null) en, replace(null, null, null) en1 from test;
> EN   EN1
> ---- ----
> null null
> rows: 1

select replace('abchihihi', 'i', 'o') abcehohoho, replace('that is tom', 'i') abcethstom from test;
> ABCEHOHOHO ABCETHSTOM
> ---------- ----------
> abchohoho  that s tom
> rows: 1

set mode oracle;
> ok

select replace('white space', ' ', '') x, replace('white space', ' ', null) y from dual;
> X          Y
> ---------- ----------
> whitespace whitespace
> rows: 1
