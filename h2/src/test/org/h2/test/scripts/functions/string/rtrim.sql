create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select rtrim(null) en, '>' || rtrim('a') || '<' ea, '>' || rtrim(' a ') || '<' es from test;
> EN   EA  ES
> ---- --- ----
> null >a< > a<
> rows: 1
