create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select readonly() x_false from test;
> X_FALSE
> -------
> FALSE
> rows: 1
