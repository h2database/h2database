-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE VIEW TEST_VIEW(A) AS SELECT 'a';
> ok

CREATE OR REPLACE VIEW TEST_VIEW(B, C) AS SELECT 'b', 'c';
> ok

SELECT * FROM TEST_VIEW;
> B C
> - -
> b c
> rows: 1

DROP VIEW TEST_VIEW;
> ok

CREATE TABLE TEST(C1 INT) AS (VALUES 1, 2);
> ok

CREATE OR REPLACE VIEW TEST_VIEW AS (SELECT C1 AS A FROM TEST);
> ok

ALTER TABLE TEST ADD COLUMN C2 INT;
> ok

UPDATE TEST SET C2 = C1 + 1;
> update count: 2

CREATE OR REPLACE VIEW TEST_VIEW AS (SELECT C1 AS A, C2 AS B FROM TEST);
> ok

CREATE OR REPLACE VIEW TEST_VIEW AS (SELECT C2 AS B, C1 AS A FROM TEST);
> ok

SELECT * FROM TEST_VIEW;
> B A
> - -
> 2 1
> 3 2
> rows: 2

DROP TABLE TEST CASCADE;
> ok
