create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select pi() pi from test;
> PI
> -----------------
> 3.141592653589793
> rows: 1



