create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select hextoraw(null) en, rawtohex(null) en1, hextoraw(rawtohex('abc')) abc from test;
> EN   EN1  ABC
> ---- ---- ---
> null null abc
> rows: 1

