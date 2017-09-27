create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select length(curdate()) c1, length(current_date()) c2, substring(curdate(), 5, 1) c3 from test;
> C1 C2 C3
> -- -- --
> 10 10 -
> rows: 1
