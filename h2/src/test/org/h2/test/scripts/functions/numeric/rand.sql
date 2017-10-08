create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select rand(1) e, random() f from test;
> E                  F
> ------------------ -------------------
> 0.7308781907032909 0.41008081149220166
> rows: 1

select rand() e from test;
> E
> -------------------
> 0.20771484130971707
> rows: 1




