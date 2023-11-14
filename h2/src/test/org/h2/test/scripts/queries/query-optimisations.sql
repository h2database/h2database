-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create table person(firstname varchar, lastname varchar);
> ok

create index person_1 on person(firstname, lastname);
> ok

insert into person select convert(x,varchar) as firstname, (convert(x,varchar) || ' last') as lastname from system_range(1,100);
> update count: 100

-- Issue #643: verify that when using an index, we use the IN part of the query, if that part of the query
-- can directly use the index.
--
explain analyze SELECT * FROM person WHERE firstname IN ('FirstName1', 'FirstName2') AND lastname='LastName1';
>> SELECT "PUBLIC"."PERSON"."FIRSTNAME", "PUBLIC"."PERSON"."LASTNAME" FROM "PUBLIC"."PERSON" /* PUBLIC.PERSON_1: FIRSTNAME IN('FirstName1', 'FirstName2') AND LASTNAME = 'LastName1' */ /* scanCount: 1 */ WHERE ("FIRSTNAME" IN('FirstName1', 'FirstName2')) AND ("LASTNAME" = 'LastName1')

CREATE TABLE TEST(A SMALLINT PRIMARY KEY, B SMALLINT);
> ok

CREATE INDEX TEST_IDX_1 ON TEST(B);
> ok

CREATE INDEX TEST_IDX_2 ON TEST(B, A);
> ok

INSERT INTO TEST VALUES (1, 2), (3, 4);
> update count: 2

EXPLAIN SELECT _ROWID_ FROM TEST WHERE B = 4;
>> SELECT _ROWID_ FROM "PUBLIC"."TEST" /* PUBLIC.TEST_IDX_1: B = 4 */ WHERE "B" = 4

EXPLAIN SELECT _ROWID_, A FROM TEST WHERE B = 4;
>> SELECT _ROWID_, "A" FROM "PUBLIC"."TEST" /* PUBLIC.TEST_IDX_1: B = 4 */ WHERE "B" = 4

EXPLAIN SELECT A FROM TEST WHERE B = 4;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.TEST_IDX_1: B = 4 */ WHERE "B" = 4

SELECT _ROWID_, A FROM TEST WHERE B = 4;
> _ROWID_ A
> ------- -
> 3       3
> rows: 1

DROP TABLE TEST;
> ok

CREATE TABLE TEST(A TINYINT PRIMARY KEY, B TINYINT);
> ok

CREATE INDEX TEST_IDX_1 ON TEST(B);
> ok

CREATE INDEX TEST_IDX_2 ON TEST(B, A);
> ok

INSERT INTO TEST VALUES (1, 2), (3, 4);
> update count: 2

EXPLAIN SELECT _ROWID_ FROM TEST WHERE B = 4;
>> SELECT _ROWID_ FROM "PUBLIC"."TEST" /* PUBLIC.TEST_IDX_1: B = 4 */ WHERE "B" = 4

EXPLAIN SELECT _ROWID_, A FROM TEST WHERE B = 4;
>> SELECT _ROWID_, "A" FROM "PUBLIC"."TEST" /* PUBLIC.TEST_IDX_1: B = 4 */ WHERE "B" = 4

EXPLAIN SELECT A FROM TEST WHERE B = 4;
>> SELECT "A" FROM "PUBLIC"."TEST" /* PUBLIC.TEST_IDX_1: B = 4 */ WHERE "B" = 4

SELECT _ROWID_, A FROM TEST WHERE B = 4;
> _ROWID_ A
> ------- -
> 3       3
> rows: 1

DROP TABLE TEST;
> ok

CREATE TABLE TEST(V VARCHAR(2)) AS VALUES -1, -2;
> ok

CREATE INDEX TEST_INDEX ON TEST(V);
> ok

SELECT * FROM TEST WHERE V >= -1;
>> -1

-- H2 may use the index for a table scan, but may not create index conditions due to incompatible type
EXPLAIN SELECT * FROM TEST WHERE V >= -1;
>> SELECT "PUBLIC"."TEST"."V" FROM "PUBLIC"."TEST" /* PUBLIC.TEST_INDEX */ WHERE "V" >= -1

EXPLAIN SELECT * FROM TEST WHERE V IN (-1, -3);
>> SELECT "PUBLIC"."TEST"."V" FROM "PUBLIC"."TEST" /* PUBLIC.TEST_INDEX */ WHERE "V" IN(-1, -3)

SELECT * FROM TEST WHERE V < -1;
>> -2

DROP TABLE TEST;
> ok

CREATE TABLE T(ID INT, V INT) AS VALUES (1, 1), (1, 2), (2, 1), (2, 2);
> ok

SELECT T1.ID, T2.V AS LV FROM (SELECT ID, MAX(V) AS LV FROM T GROUP BY ID) AS T1
    INNER JOIN T AS T2 ON T2.ID = T1.ID AND T2.V = T1.LV
    WHERE T1.ID IN (1, 2) ORDER BY ID;
> ID LV
> -- --
> 1  2
> 2  2
> rows (ordered): 2
-- TODO:kiss034: This should be fixed later.

EXPLAIN SELECT T1.ID, T2.V AS LV FROM (SELECT ID, MAX(V) AS LV FROM T GROUP BY ID) AS T1
    INNER JOIN T AS T2 ON T2.ID = T1.ID AND T2.V = T1.LV
    WHERE T1.ID IN (1, 2) ORDER BY ID;
>> SELECT "T1"."ID", "T2"."V" AS "LV" FROM "PUBLIC"."T" "T2" /* PUBLIC.T.tableScan */ INNER JOIN ( SELECT "ID", MAX("V") AS "LV" FROM "PUBLIC"."T" GROUP BY "ID" ) "T1" /* SELECT ID, MAX(V) AS LV FROM PUBLIC.T /* PUBLIC.T.tableScan */ WHERE ID IS NOT DISTINCT FROM ?1 GROUP BY ID HAVING MAX(V) IS NOT DISTINCT FROM ?2: ID = T2.ID AND LV = T2.V */ ON 1=1 WHERE ("T1"."ID" IN(1, 2)) AND ("T2"."ID" = "T1"."ID") AND ("T2"."V" = "T1"."LV") ORDER BY 1
-- TODO:kiss034: This should be fixed later.

DROP TABLE T;
> ok

SELECT (SELECT ROWNUM) R FROM VALUES 1, 2, 3;
> R
> -
> 1
> 1
> 1
> rows: 3

CREATE TABLE TEST(A INT, B INT, C INT) AS VALUES (1, 1, 1);
> ok

SELECT T1.A FROM TEST T1 LEFT OUTER JOIN TEST T2 ON T1.B = T2.A WHERE (SELECT T2.C) IS NOT NULL ORDER BY T1.A;
>> 1

EXPLAIN SELECT T1.A FROM TEST T1 LEFT OUTER JOIN TEST T2 ON T1.B = T2.A WHERE (SELECT T2.C) IS NOT NULL ORDER BY T1.A;
>> SELECT "T1"."A" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.TEST.tableScan */ LEFT OUTER JOIN "PUBLIC"."TEST" "T2" /* PUBLIC.TEST.tableScan */ ON "T1"."B" = "T2"."A" WHERE "T2"."C" IS NOT NULL ORDER BY 1

SELECT X, (SELECT X IN (SELECT B FROM TEST)) FROM SYSTEM_RANGE(1, 2);
> X X IN( SELECT DISTINCT B FROM PUBLIC.TEST)
> - -----------------------------------------
> 1 TRUE
> 2 FALSE
> rows: 2

SELECT T1.A FROM TEST T1 LEFT OUTER JOIN TEST T2 ON T1.B = T2.A WHERE (SELECT T2.C + ROWNUM) IS NOT NULL ORDER BY T1.A;
>> 1

EXPLAIN SELECT T1.A FROM TEST T1 LEFT OUTER JOIN TEST T2 ON T1.B = T2.A WHERE (SELECT T2.C + ROWNUM) IS NOT NULL ORDER BY T1.A;
>> SELECT "T1"."A" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.TEST.tableScan */ LEFT OUTER JOIN "PUBLIC"."TEST" "T2" /* PUBLIC.TEST.tableScan */ ON "T1"."B" = "T2"."A" WHERE ("T2"."C" + CAST(1 AS BIGINT)) IS NOT NULL ORDER BY 1

DROP TABLE TEST;
> ok

CREATE TABLE A(T TIMESTAMP WITH TIME ZONE UNIQUE) AS VALUES
    TIMESTAMP WITH TIME ZONE '2020-01-01 00:01:02+02',
    TIMESTAMP WITH TIME ZONE '2020-01-01 00:01:02+01';
> ok

CREATE TABLE B(D DATE) AS VALUES DATE '2020-01-01';
> ok

SET TIME ZONE '01:00';
> ok

SELECT T FROM A JOIN B ON T >= D;
>> 2020-01-01 00:01:02+01

EXPLAIN SELECT T FROM A JOIN B ON T >= D;
>> SELECT "T" FROM "PUBLIC"."B" /* PUBLIC.B.tableScan */ INNER JOIN "PUBLIC"."A" /* PUBLIC.CONSTRAINT_INDEX_4: T >= D */ ON 1=1 WHERE "T" >= "D"

SET TIME ZONE LOCAL;
> ok

DROP TABLE A, B;
> ok

CREATE TABLE TEST(T TIMESTAMP WITH TIME ZONE) AS VALUES
    NULL,
    TIMESTAMP WITH TIME ZONE '2020-01-01 00:00:00+00',
    TIMESTAMP WITH TIME ZONE '2020-01-01 01:00:00+01',
    TIMESTAMP WITH TIME ZONE '2020-01-01 02:00:00+01',
    NULL;
> ok

SELECT T AT TIME ZONE 'UTC' FROM TEST GROUP BY T;
> T AT TIME ZONE 'UTC'
> ----------------------
> 2020-01-01 00:00:00+00
> 2020-01-01 01:00:00+00
> null
> rows: 3

CREATE INDEX TEST_T_IDX ON TEST(T);
> ok

SELECT T AT TIME ZONE 'UTC' FROM TEST GROUP BY T;
> T AT TIME ZONE 'UTC'
> ----------------------
> 2020-01-01 00:00:00+00
> 2020-01-01 01:00:00+00
> null
> rows: 3

EXPLAIN SELECT T AT TIME ZONE 'UTC' FROM TEST GROUP BY T;
>> SELECT "T" AT TIME ZONE 'UTC' FROM "PUBLIC"."TEST" /* PUBLIC.TEST_T_IDX */ GROUP BY "T" /* group sorted */

DROP TABLE TEST;
> ok

CREATE TABLE T1(A INT, B INT, C INT) AS VALUES (1, 1, 1), (1, 2, 2), (2, 1, 3), (2, 2, 4);
> ok

CREATE TABLE T2(D INT, E INT) AS VALUES (1, 1), (2, 2);
> ok

SET @V = 1;
> ok

CREATE VIEW V1 AS SELECT T2.D, T1.C FROM T2 LEFT JOIN T1 ON T2.E = T1.A AND T1.B = @V;
> ok

TABLE V1;
> D C
> - -
> 1 1
> 2 3
> rows: 2

SET @V = 2;
> ok

TABLE V1;
> D C
> - -
> 1 2
> 2 4
> rows: 2

DROP VIEW V1;
> ok

DROP TABLE T1, T2;
> ok

SET @V = NULL;
> ok

CREATE TABLE T1(A INT, B INT);
> ok

CREATE INDEX T1_A_IDX ON T1(A);
> ok

EXPLAIN SELECT * FROM T1 WHERE (A, B) = (1, 2);
>> SELECT "PUBLIC"."T1"."A", "PUBLIC"."T1"."B" FROM "PUBLIC"."T1" /* PUBLIC.T1_A_IDX: A = 1 */ WHERE ROW ("A", "B") = ROW (1, 2)

EXPLAIN SELECT * FROM T1 WHERE (A, B) > (1, 2);
>> SELECT "PUBLIC"."T1"."A", "PUBLIC"."T1"."B" FROM "PUBLIC"."T1" /* PUBLIC.T1_A_IDX: A >= 1 */ WHERE ROW ("A", "B") > ROW (1, 2)

EXPLAIN SELECT * FROM T1 WHERE (A, B) >= (1, 2);
>> SELECT "PUBLIC"."T1"."A", "PUBLIC"."T1"."B" FROM "PUBLIC"."T1" /* PUBLIC.T1_A_IDX: A >= 1 */ WHERE ROW ("A", "B") >= ROW (1, 2)

EXPLAIN SELECT * FROM T1 WHERE (A, B) < (1, 2);
>> SELECT "PUBLIC"."T1"."A", "PUBLIC"."T1"."B" FROM "PUBLIC"."T1" /* PUBLIC.T1_A_IDX: A <= 1 */ WHERE ROW ("A", "B") < ROW (1, 2)

EXPLAIN SELECT * FROM T1 WHERE (A, B) <= (1, 2);
>> SELECT "PUBLIC"."T1"."A", "PUBLIC"."T1"."B" FROM "PUBLIC"."T1" /* PUBLIC.T1_A_IDX: A <= 1 */ WHERE ROW ("A", "B") <= ROW (1, 2)

EXPLAIN SELECT * FROM T1 WHERE ROW (A) = 1;
>> SELECT "PUBLIC"."T1"."A", "PUBLIC"."T1"."B" FROM "PUBLIC"."T1" /* PUBLIC.T1_A_IDX: A = 1 */ WHERE ROW ("A") = 1

EXPLAIN SELECT * FROM T1 JOIN T1 T2 ON (T1.A, T1.B) IN ((1, T2.A), (2, T2.B));
>> SELECT "PUBLIC"."T1"."A", "PUBLIC"."T1"."B", "T2"."A", "T2"."B" FROM "PUBLIC"."T1" /* PUBLIC.T1_A_IDX: A IN(1, 2) */ INNER JOIN "PUBLIC"."T1" "T2" /* PUBLIC.T1.tableScan */ ON 1=1 WHERE ROW ("T1"."A", "T1"."B") IN(ROW (1, "T2"."A"), ROW (2, "T2"."B"))

EXPLAIN SELECT * FROM T1 WHERE (A, B) IN ((1, 2), (3, 4));
>> SELECT "PUBLIC"."T1"."A", "PUBLIC"."T1"."B" FROM "PUBLIC"."T1" /* PUBLIC.T1_A_IDX: A IN(1, 3) */ WHERE ROW ("A", "B") IN(ROW (1, 2), ROW (3, 4))

DROP INDEX T1_A_IDX;
> ok

CREATE INDEX T1_B_IDX ON T1(B);
> ok

EXPLAIN SELECT * FROM T1 WHERE (A, B) = (1, 2);
>> SELECT "PUBLIC"."T1"."A", "PUBLIC"."T1"."B" FROM "PUBLIC"."T1" /* PUBLIC.T1_B_IDX: B = 2 */ WHERE ROW ("A", "B") = ROW (1, 2)

EXPLAIN SELECT * FROM T1 WHERE (A, B) > (1, 2);
>> SELECT "PUBLIC"."T1"."A", "PUBLIC"."T1"."B" FROM "PUBLIC"."T1" /* PUBLIC.T1.tableScan */ WHERE ROW ("A", "B") > ROW (1, 2)

CREATE INDEX T1_A_B_IDX ON T1(A, B);
> ok

EXPLAIN SELECT * FROM T1 WHERE (A, B) = (1, 2);
>> SELECT "PUBLIC"."T1"."A", "PUBLIC"."T1"."B" FROM "PUBLIC"."T1" /* PUBLIC.T1_A_B_IDX: A = 1 AND B = 2 */ WHERE ROW ("A", "B") = ROW (1, 2)
-- TODO:kiss034: This should be fixed later.

EXPLAIN SELECT * FROM T1 WHERE (A, B) > (1, 2);
>> SELECT "PUBLIC"."T1"."A", "PUBLIC"."T1"."B" FROM "PUBLIC"."T1" /* PUBLIC.T1_A_B_IDX: A >= 1 */ WHERE ROW ("A", "B") > ROW (1, 2)

DROP TABLE T1;
> ok
