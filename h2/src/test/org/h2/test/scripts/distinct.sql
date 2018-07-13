-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(ID BIGINT, NAME VARCHAR);
> ok

INSERT INTO TEST VALUES (1, 'a'), (2, 'B'), (3, 'c'), (1, 'a');
> update count: 4

CREATE TABLE TEST2(ID2 BIGINT);
> ok

INSERT INTO TEST2 VALUES (1), (2);
> update count: 2

SET MODE MySQL;
> ok

SELECT DISTINCT ID FROM TEST ORDER BY NAME;
> ID
> --
> 2
> 1
> 3
> rows (ordered): 3

SELECT DISTINCT ID FROM TEST ORDER BY LOWER(NAME);
> ID
> --
> 1
> 2
> 3
> rows (ordered): 3

SELECT DISTINCT ID FROM TEST JOIN TEST2 ON ID = ID2 ORDER BY LOWER(NAME);
> ID
> --
> 1
> 2
> rows (ordered): 2

SET MODE Regular;
> ok

DROP TABLE TEST;
> ok

DROP TABLE TEST2;
> ok
