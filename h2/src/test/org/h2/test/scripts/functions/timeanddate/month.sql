create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select month(date '2005-09-25') d9 from test;
> D9
> --
> 9
> rows: 1
