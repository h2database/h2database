-- Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create memory table test(id int primary key, name varchar(255));
> ok

insert into test values(1, 'Hello');
> update count: 1

select length(curtime())>=8 c1, length(current_time())>=8 c2, substring(curtime(), 3, 1) c3 from test;
> C1   C2   C3
> ---- ---- --
> TRUE TRUE :
> rows: 1


select length(now())>20 c1, length(current_timestamp())>20 c2, length(now(0))>20 c3, length(now(2))>20 c4, substring(now(5), 20, 1) c5 from test;
> C1   C2   C3   C4   C5
> ---- ---- ---- ---- --
> TRUE TRUE TRUE TRUE .
> rows: 1
