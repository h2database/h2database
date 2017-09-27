create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select dayname(date '2005-09-12') d_monday from test;
> D_MONDAY
> --------
> Monday
> rows: 1
