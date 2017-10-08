create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select ltrim(null) en, '>' || ltrim('a') || '<' ea, '>' || ltrim(' a ') || '<' e_as from test;
> EN   EA  E_AS
> ---- --- ----
> null >a< >a <
> rows: 1
