-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: Joe Littlejohn
--

select bitnot(null) vn, bitnot(0) v1, bitnot(10) v2, bitnot(-10) v3;
> VN   V1 V2  V3
> ---- -- --- --
> null -1 -11 9
> rows: 1

CREATE TABLE TEST(A BIGINT);
> ok

EXPLAIN SELECT BITNOT(BITNOT(A)), BITNOT(LSHIFT(A, 1)) FROM TEST;
>> SELECT "A", BITNOT(LSHIFT("A", 1)) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

DROP TABLE TEST;
> ok

EXPLAIN SELECT
    BITNOT(CAST((0xC5 - 0x100) AS TINYINT)),
    BITNOT(CAST(0xC5 AS SMALLINT)),
    BITNOT(CAST(0xC5 AS INTEGER)),
    BITNOT(CAST(0xC5 AS BIGINT)),
    BITNOT(CAST(X'C5' AS VARBINARY)),
    BITNOT(CAST(X'C5' AS BINARY));
>> SELECT CAST(58 AS TINYINT), CAST(-198 AS SMALLINT), -198, CAST(-198 AS BIGINT), X'3a', CAST(X'3a' AS BINARY(1))

SELECT BITNOT('AA');
> exception INVALID_VALUE_2
