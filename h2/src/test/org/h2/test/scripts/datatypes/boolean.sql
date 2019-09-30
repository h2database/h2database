-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(B BOOLEAN) AS (VALUES TRUE, FALSE, UNKNOWN);
> ok

SELECT * FROM TEST ORDER BY B;
> B
> -----
> null
> FALSE
> TRUE
> rows (ordered): 3

DROP TABLE TEST;
> ok

CREATE TABLE TEST AS (SELECT UNKNOWN B);
> ok

SELECT TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST';
>> BOOLEAN

EXPLAIN SELECT CAST(NULL AS BOOLEAN);
>> SELECT UNKNOWN

SELECT NOT TRUE A, NOT FALSE B, NOT NULL C, NOT UNKNOWN D;
> A     B    C    D
> ----- ---- ---- ----
> FALSE TRUE null null
> rows: 1

DROP TABLE TEST;
> ok
