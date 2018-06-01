-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE test (id INT NOT NULL, name VARCHAR);
> ok

select * from test where id = (1, 2);
> exception COMPARING_ARRAY_TO_SCALAR

drop table test;
> ok