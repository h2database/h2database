create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select autocommit() x_true from test;
> X_TRUE
> ------
> TRUE
> rows: 1
