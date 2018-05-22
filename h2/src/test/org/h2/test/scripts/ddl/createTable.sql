-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(A INT CONSTRAINT PK_1 PRIMARY KEY);
> ok

SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS;
> CONSTRAINT_NAME CONSTRAINT_TYPE
> --------------- ---------------
> PK_1            PRIMARY KEY
> rows: 1

DROP TABLE TEST;
> ok

CREATE TABLE T1(ID INT PRIMARY KEY, COL2 INT);
> ok

INSERT INTO T1 VALUES (1, 2), (11, 22);
> update count: 2

CREATE TABLE T2 AS SELECT * FROM T1;
> ok

SELECT * FROM T2 ORDER BY ID;
> ID COL2
> -- ----
> 1  2
> 11 22
> rows (ordered): 2

DROP TABLE T2;
> ok

CREATE TABLE T2 AS SELECT * FROM T1 WITH DATA;
> ok

SELECT * FROM T2 ORDER BY ID;
> ID COL2
> -- ----
> 1  2
> 11 22
> rows (ordered): 2

DROP TABLE T2;
> ok

CREATE TABLE T2 AS SELECT * FROM T1 WITH NO DATA;
> ok

SELECT * FROM T2 ORDER BY ID;
> ID COL2
> -- ----
> rows (ordered): 0

DROP TABLE T2;
> ok

DROP TABLE T1;
> ok
