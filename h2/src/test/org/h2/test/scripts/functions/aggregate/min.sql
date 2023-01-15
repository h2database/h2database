-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- with filter condition

create table test(v int);
> ok

insert into test values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12);
> update count: 12

select min(v), min(v) filter (where v >= 4) from test where v >= 2;
> MIN(V) MIN(V) FILTER (WHERE V >= 4)
> ------ ----------------------------
> 2      4
> rows: 1

create index test_idx on test(v);
> ok

select min(v), min(v) filter (where v >= 4) from test where v >= 2;
> MIN(V) MIN(V) FILTER (WHERE V >= 4)
> ------ ----------------------------
> 2      4
> rows: 1

select min(v), min(v) filter (where v >= 4) from test;
> MIN(V) MIN(V) FILTER (WHERE V >= 4)
> ------ ----------------------------
> 1      4
> rows: 1

drop table test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, V INT);
> ok

CREATE INDEX TEST_IDX ON TEST(V NULLS FIRST);
> ok

EXPLAIN SELECT MIN(V) FROM TEST;
>> SELECT MIN("V") FROM "PUBLIC"."TEST" /* PUBLIC.TEST_IDX */ /* direct lookup */

SELECT MIN(V) FROM TEST;
>> null

INSERT INTO TEST VALUES (1, 1), (2, NULL), (3, 5);
> update count: 3

SELECT MIN(V) FROM TEST;
>> 1

DROP TABLE TEST;
> ok

EXPLAIN SELECT MIN(X) FROM SYSTEM_RANGE(1, 2);
>> SELECT MIN("X") FROM SYSTEM_RANGE(1, 2) /* range index */ /* direct lookup */

SELECT MIN(X) FROM SYSTEM_RANGE(1, 2, 0);
> exception STEP_SIZE_MUST_NOT_BE_ZERO

SELECT MIN(X) FROM SYSTEM_RANGE(1, 2);
>> 1

SELECT MIN(X) FROM SYSTEM_RANGE(2, 1);
>> null

SELECT MIN(X) FROM SYSTEM_RANGE(1, 2, -1);
>> null

SELECT MIN(X) FROM SYSTEM_RANGE(2, 1, -1);
>> 1
