create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select right(database(), 6) x_script from test;
> X_SCRIPT
> --------
> SCRIPT
> rows: 1
