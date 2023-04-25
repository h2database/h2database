-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
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

SELECT
    V,
    BITNOT(BIT_AND_AGG(V) FILTER (WHERE V > 0) OVER (PARTITION BY BITAND(V, 7) ORDER BY V)) G,
    BIT_NAND_AGG(V) FILTER (WHERE V > 0) OVER (PARTITION BY BITAND(V, 7) ORDER BY V) C FROM TEST;
> V               G                C
> --------------- ---------------- ----------------
> 17592186044415  -17592186044416  -17592186044416
> 264982302294015 -1099511627776   -1099511627776
> 280444184559615 -68719476736     -68719476736
> 281410552201215 -4294967296      -4294967296
> 281470950178815 -268435456       -268435456
> 281474725052415 -16777216        -16777216
> 281474960982015 -1048576         -1048576
> 281474975727615 -65536           -65536
> 281474976649215 -4096            -4096
> 281474976706815 -256             -256
> 281474976710415 -16              -16
> 281474976710640 -281474976710641 -281474976710641
> rows: 12

EXPLAIN SELECT BITNOT(BIT_AND_AGG(V) FILTER (WHERE V > 0) OVER (PARTITION BY BITAND(V, 7) ORDER BY V)) FROM TEST;
>> SELECT BIT_NAND_AGG("V") FILTER (WHERE "V" > CAST(0 AS BIGINT)) OVER (PARTITION BY BITAND("V", 7) ORDER BY "V") FROM "PUBLIC"."TEST" /* PUBLIC.TEST_IDX */

drop table test;
> ok
