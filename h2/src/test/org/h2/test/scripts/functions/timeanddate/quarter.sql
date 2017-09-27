create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select quarter(date '2005-09-01') d3 from test;
> D3
> --
> 3
> rows: 1
