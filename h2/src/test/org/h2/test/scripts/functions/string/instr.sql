create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select instr('Hello World', 'World') e7, instr('abchihihi', 'hi', 2) e3, instr('abcooo', 'o') e2 from test;
> E7 E3 E2
> -- -- --
> 7  4  4
> rows: 1
