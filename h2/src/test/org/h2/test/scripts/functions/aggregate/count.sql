-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- with filter condition

create table test(v int);
> ok

insert into test values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (null);
> update count: 13

select count(v), count(v) filter (where v >= 4) from test where v <= 10;
> COUNT(V) COUNT(V) FILTER (WHERE V >= 4)
> -------- ------------------------------
> 10       7
> rows: 1

select count(*), count(*) filter (where v >= 4) from test;
> COUNT(*) COUNT(*) FILTER (WHERE V >= 4)
> -------- ------------------------------
> 13       9
> rows: 1

select count(*), count(*) filter (where v >= 4) from test where v <= 10;
> COUNT(*) COUNT(*) FILTER (WHERE V >= 4)
> -------- ------------------------------
> 10       7
> rows: 1

create index test_idx on test(v);
> ok

select count(v), count(v) filter (where v >= 4) from test where v <= 10;
> COUNT(V) COUNT(V) FILTER (WHERE V >= 4)
> -------- ------------------------------
> 10       7
> rows: 1

select count(v), count(v) filter (where v >= 4) from test;
> COUNT(V) COUNT(V) FILTER (WHERE V >= 4)
> -------- ------------------------------
> 12       9
> rows: 1

drop table test;
> ok

CREATE TABLE TEST (ID INT PRIMARY KEY, NAME VARCHAR);
> ok

INSERT INTO TEST VALUES (1, 'b'), (3, 'a');
> update count: 2

SELECT COUNT(ID) OVER (ORDER BY NAME) AS NR,
    A.ID AS ID FROM (SELECT ID, NAME FROM TEST ORDER BY NAME) AS A;
> NR ID
> -- --
> 1  3
> 2  1
> rows: 2

SELECT NR FROM (SELECT COUNT(ID) OVER (ORDER BY NAME) AS NR,
    A.ID AS ID FROM (SELECT ID, NAME FROM TEST ORDER BY NAME) AS A)
    AS B WHERE B.ID = 1;
>> 2

DROP TABLE TEST;
> ok

SELECT I, V, COUNT(V) OVER W C, COUNT(DISTINCT V) OVER W D FROM
    VALUES (1, 1), (2, 1), (3, 1), (4, 1), (5, 2), (6, 2), (7, 3) T(I, V)
    WINDOW W AS (ORDER BY I);
> I V C D
> - - - -
> 1 1 1 1
> 2 1 2 1
> 3 1 3 1
> 4 1 4 1
> 5 2 5 2
> 6 2 6 2
> 7 3 7 3
> rows: 7

SELECT I, C, COUNT(I) OVER (PARTITION BY C) CNT FROM
    VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 2) T(I, C);
> I C CNT
> - - ---
> 1 1 2
> 2 1 2
> 3 2 3
> 4 2 3
> 5 2 3
> rows: 5

SELECT X, COUNT(*) OVER (ORDER BY X) C FROM VALUES (1), (1), (2), (2), (3) V(X);
> X C
> - -
> 1 2
> 1 2
> 2 4
> 2 4
> 3 5
> rows: 5

CREATE TABLE TEST (N NUMERIC) AS VALUES (0), (0.0), (NULL);
> ok

SELECT COUNT(*) FROM TEST;
>> 3

SELECT COUNT(N) FROM TEST;
>> 2

SELECT COUNT(DISTINCT N) FROM TEST;
>> 1

SELECT COUNT(*) FROM TEST GROUP BY N;
> COUNT(*)
> --------
> 1
> 2
> rows: 2

SELECT COUNT(N) OVER (PARTITION BY N) C FROM TEST;
> C
> -
> 0
> 2
> 2
> rows: 3

DROP TABLE TEST;
> ok

CREATE TABLE TEST(A INT, B INT) AS (VALUES (1, NULL), (1, NULL), (2, NULL));
> ok

SELECT COUNT((A, B)) C, COUNT(DISTINCT (A, B)) CD FROM TEST;
> C CD
> - --
> 3 2
> rows: 1

SELECT COUNT(*) OVER (PARTITION BY A, B) C1, COUNT(*) OVER (PARTITION BY (A, B)) C2 FROM TEST;
> C1 C2
> -- --
> 1  1
> 2  2
> 2  2
> rows: 3

DROP TABLE TEST;
> ok

CREATE TABLE TEST(X INT) AS (VALUES 1, 2, NULL);
> ok

SELECT COUNT(*) FROM TEST;
>> 3

SELECT COUNT(1) FROM TEST;
>> 3

SELECT COUNT(DISTINCT 1) FROM TEST;
>> 1

SELECT COUNT(1) FROM TEST FILTER WHERE X <> 1;
>> 1

SELECT COUNT(1) OVER(PARTITION BY X IS NULL) FROM TEST;
> COUNT(*) OVER (PARTITION BY X IS NULL)
> --------------------------------------
> 1
> 2
> 2
> rows: 3

SELECT COUNT(NULL) FROM TEST;
>> 0

SELECT COUNT(DISTINCT NULL) FROM TEST;
>> 0

EXPLAIN SELECT COUNT(*) FROM TEST;
>> SELECT COUNT(*) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ /* direct lookup */

EXPLAIN SELECT COUNT(*) FILTER (WHERE TRUE) FROM TEST;
>> SELECT COUNT(*) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ /* direct lookup */

EXPLAIN SELECT COUNT(1) FROM TEST;
>> SELECT COUNT(*) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ /* direct lookup */

EXPLAIN SELECT COUNT(DISTINCT 1) FROM TEST;
>> SELECT COUNT(DISTINCT 1) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT COUNT(1) FROM TEST FILTER WHERE X <> 1;
>> SELECT COUNT(*) FROM "PUBLIC"."TEST" "FILTER" /* PUBLIC.TEST.tableScan */ WHERE "X" <> 1

EXPLAIN SELECT COUNT(1) OVER(PARTITION BY X IS NULL) FROM TEST;
>> SELECT COUNT(*) OVER (PARTITION BY "X" IS NULL) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT COUNT(NULL) FROM TEST;
>> SELECT CAST(0 AS BIGINT) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ GROUP BY () /* direct lookup */

EXPLAIN SELECT COUNT(DISTINCT NULL) FROM TEST;
>> SELECT CAST(0 AS BIGINT) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ GROUP BY () /* direct lookup */

SELECT COUNT(X) FROM TEST;
>> 2

EXPLAIN SELECT COUNT(X) FROM TEST;
>> SELECT COUNT("X") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

DELETE FROM TEST WHERE X IS NULL;
> update count: 1

ALTER TABLE TEST ALTER COLUMN X SET NOT NULL;
> ok

SELECT COUNT(X) FROM TEST;
>> 2

EXPLAIN SELECT COUNT(X) FROM TEST;
>> SELECT COUNT("X") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ /* direct lookup */

SELECT COUNT(DISTINCT X) FROM TEST;
>> 2

EXPLAIN SELECT COUNT(DISTINCT X) FROM TEST;
>> SELECT COUNT(DISTINCT "X") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

DROP TABLE TEST;
> ok
