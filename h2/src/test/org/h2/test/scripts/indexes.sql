-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create table test(a int, b int);
> ok

insert into test values(1, 1);
> update count: 1

create index on test(a, b desc);
> ok

select * from test where a = 1;
> A B
> - -
> 1 1
> rows: 1

drop table test;
> ok

create table test(x int);
> ok

create hash index on test(x);
> ok

select 1 from test group by x;
> 1
> -
> rows: 0

drop table test;
> ok
