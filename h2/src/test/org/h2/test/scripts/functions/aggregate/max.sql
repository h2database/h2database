-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- with filter condition

create table test(v int);
> ok

insert into test values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12);
> update count: 12

select max(v), max(v) filter (where v <= 8) from test where v <= 10;
> MAX(V) MAX(V) FILTER (WHERE V <= 8)
> ------ ----------------------------
> 10     8
> rows: 1

create index test_idx on test(v);
> ok

select max(v), max(v) filter (where v <= 8) from test where v <= 10;
> MAX(V) MAX(V) FILTER (WHERE V <= 8)
> ------ ----------------------------
> 10     8
> rows: 1

select max(v), max(v) filter (where v <= 8) from test;
> MAX(V) MAX(V) FILTER (WHERE V <= 8)
> ------ ----------------------------
> 12     8
> rows: 1

drop table test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, V INT) AS VALUES (1, 1), (2, NULL), (3, 5);
> ok

CREATE INDEX TEST_IDX ON TEST(V NULLS LAST);
> ok

EXPLAIN SELECT MAX(V) FROM TEST;
>> SELECT MAX("V") FROM "PUBLIC"."TEST" /* PUBLIC.TEST_IDX */ /* direct lookup */

SELECT MAX(V) FROM TEST;
>> 5

DROP TABLE TEST;
> ok

EXPLAIN SELECT MAX(X) FROM SYSTEM_RANGE(1, 2);
>> SELECT MAX("X") FROM SYSTEM_RANGE(1, 2) /* range index */ /* direct lookup */

SELECT MAX(X) FROM SYSTEM_RANGE(1, 2, 0);
> exception STEP_SIZE_MUST_NOT_BE_ZERO

SELECT MAX(X) FROM SYSTEM_RANGE(1, 2);
>> 2

SELECT MAX(X) FROM SYSTEM_RANGE(2, 1);
>> null

SELECT MAX(X) FROM SYSTEM_RANGE(1, 2, -1);
>> null

SELECT MAX(X) FROM SYSTEM_RANGE(2, 1, -1);
>> 2
