-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
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

DELETE FROM tab_with_timezone;
> update count: 1

INSERT INTO tab_with_timezone VALUES ('2018-03-25 01:59:00 Europe/Berlin'), ('2018-03-25 03:00:00 Europe/Berlin');
> update count: 2

SELECT * FROM tab_with_timezone ORDER BY X;
> X
> ------------------------
> 2018-03-25 01:59:00.0+01
> 2018-03-25 03:00:00.0+02
> rows (ordered): 2
