create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select space(null) en, '>' || space(1) || '<' es, '>' || space(3) || '<' e2 from test;
> EN   ES  E2
> ---- --- ---
> null > < > <
> rows: 1
