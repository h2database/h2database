-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- with filter condition

create table test(v bigint);
> ok

insert into test values (1), (2), (4), (8), (16), (32), (64), (128), (256), (512), (1024), (2048);
> update count: 12

select BIT_OR_AGG(v), BIT_OR_AGG(v) filter (where v >= 8) from test where v <= 512;
> BIT_OR_AGG(V) BIT_OR_AGG(V) FILTER (WHERE V >= 8)
> ------------- -----------------------------------
> 1023          1016
> rows: 1

SELECT BIT_NOR_AGG(V), BIT_NOR_AGG(V) FILTER (WHERE V >= 8) FROM TEST WHERE V <= 512;
> BIT_NOR_AGG(V) BIT_NOR_AGG(V) FILTER (WHERE V >= 8)
> -------------- ------------------------------------
> -1024          -1017
> rows: 1

create index test_idx on test(v);
> ok

select BIT_OR_AGG(v), BIT_OR_AGG(v) filter (where v >= 8) from test where v <= 512;
> BIT_OR_AGG(V) BIT_OR_AGG(V) FILTER (WHERE V >= 8)
> ------------- -----------------------------------
> 1023          1016
> rows: 1

SELECT BIT_NOR_AGG(V), BIT_NOR_AGG(V) FILTER (WHERE V >= 8) FROM TEST WHERE V <= 512;
> BIT_NOR_AGG(V) BIT_NOR_AGG(V) FILTER (WHERE V >= 8)
> -------------- ------------------------------------
> -1024          -1017
> rows: 1

EXPLAIN SELECT BITNOT(BIT_OR_AGG(V)), BITNOT(BIT_NOR_AGG(V)) FROM TEST;
>> SELECT BIT_NOR_AGG("V"), BIT_OR_AGG("V") FROM "PUBLIC"."TEST" /* PUBLIC.TEST_IDX */

drop table test;
> ok
