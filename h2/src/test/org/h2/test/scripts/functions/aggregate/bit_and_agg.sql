-- Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- with filter condition

create table test(v bigint);
> ok

insert into test values
    (0xfffffffffff0), (0xffffffffff0f), (0xfffffffff0ff), (0xffffffff0fff),
    (0xfffffff0ffff), (0xffffff0fffff), (0xfffff0ffffff), (0xffff0fffffff),
    (0xfff0ffffffff), (0xff0fffffffff), (0xf0ffffffffff), (0x0fffffffffff);
> update count: 12

select BIT_AND_AGG(v), BIT_AND_AGG(v) filter (where v <= 0xffffffff0fff) from test where v >= 0xff0fffffffff;
> BIT_AND_AGG(V)  BIT_AND_AGG(V) FILTER (WHERE V <= 281474976649215)
> --------------- --------------------------------------------------
> 280375465082880 280375465086975
> rows: 1

SELECT BIT_NAND_AGG(V), BIT_NAND_AGG(V) FILTER (WHERE V <= 0xffffffff0fff) FROM TEST WHERE V >= 0xff0fffffffff;
> BIT_NAND_AGG(V)  BIT_NAND_AGG(V) FILTER (WHERE V <= 281474976649215)
> ---------------- ---------------------------------------------------
> -280375465082881 -280375465086976
> rows: 1

create index test_idx on test(v);
> ok

select BIT_AND_AGG(v), BIT_AND_AGG(v) filter (where v <= 0xffffffff0fff) from test where v >= 0xff0fffffffff;
> BIT_AND_AGG(V)  BIT_AND_AGG(V) FILTER (WHERE V <= 281474976649215)
> --------------- --------------------------------------------------
> 280375465082880 280375465086975
> rows: 1

SELECT BIT_NAND_AGG(V), BIT_NAND_AGG(V) FILTER (WHERE V <= 0xffffffff0fff) FROM TEST WHERE V >= 0xff0fffffffff;
> BIT_NAND_AGG(V)  BIT_NAND_AGG(V) FILTER (WHERE V <= 281474976649215)
> ---------------- ---------------------------------------------------
> -280375465082881 -280375465086976
> rows: 1

EXPLAIN SELECT BITNOT(BIT_AND_AGG(V)), BITNOT(BIT_NAND_AGG(V)) FROM TEST;
>> SELECT BIT_NAND_AGG("V"), BIT_AND_AGG("V") FROM "PUBLIC"."TEST" /* PUBLIC.TEST_IDX */

drop table test;
> ok
