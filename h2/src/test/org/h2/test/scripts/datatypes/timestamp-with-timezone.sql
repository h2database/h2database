-- Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--


CREATE TABLE tab_with_timezone(x TIMESTAMP WITH TIME ZONE);
> ok

INSERT INTO tab_with_timezone(x) VALUES ('2017-01-01');
> update count: 1

SELECT "Query".* FROM (select * from tab_with_timezone where x > '2016-01-01') AS "Query";
> X
> ------------------------
> 2017-01-01 00:00:00.0+00
