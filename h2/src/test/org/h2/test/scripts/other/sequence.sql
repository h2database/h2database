-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE SEQUENCE SEQ NO CACHE;
> ok

CREATE TABLE TEST(NEXT INT, CURRENT INT) AS (VALUES (10, 11), (20, 21));
> ok

SELECT NEXT VALUE, NEXT VALUE FOR SEQ, CURRENT VALUE, CURRENT VALUE FOR SEQ FROM TEST;
> VALUE NEXT VALUE FOR PUBLIC.SEQ VALUE CURRENT VALUE FOR PUBLIC.SEQ
> ----- ------------------------- ----- ----------------------------
> 10    1                         11    1
> 20    2                         21    2
> rows: 2

EXPLAIN SELECT NEXT VALUE, NEXT VALUE FOR SEQ, CURRENT VALUE, CURRENT VALUE FOR SEQ FROM TEST;
>> SELECT "NEXT" AS "VALUE", NEXT VALUE FOR "PUBLIC"."SEQ", "CURRENT" AS "VALUE", CURRENT VALUE FOR "PUBLIC"."SEQ" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

DROP TABLE TEST;
> ok

DROP SEQUENCE SEQ;
> ok
