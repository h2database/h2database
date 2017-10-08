create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select ascii(null) en, ascii('') en, ascii('Abc') e65 from test;
> EN   EN   E65
> ---- ---- ---
> null null 65
> rows: 1




