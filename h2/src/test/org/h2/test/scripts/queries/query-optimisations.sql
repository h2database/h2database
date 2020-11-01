-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
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
