-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT BIT_XOR_AGG(V), BIT_XOR_AGG(DISTINCT V), BIT_XOR_AGG(V) FILTER (WHERE V <> 1) FROM (VALUES 1, 1, 2, 3, 4) T(V);
> BIT_XOR_AGG(V) BIT_XOR_AGG(DISTINCT V) BIT_XOR_AGG(V) FILTER (WHERE V <> 1)
> -------------- ----------------------- ------------------------------------
> 5              4                       5
> rows: 1

SELECT BIT_XNOR_AGG(V), BIT_XNOR_AGG(DISTINCT V), BIT_XNOR_AGG(V) FILTER (WHERE V <> 1) FROM (VALUES 1, 1, 2, 3, 4) T(V);
> BIT_XNOR_AGG(V) BIT_XNOR_AGG(DISTINCT V) BIT_XNOR_AGG(V) FILTER (WHERE V <> 1)
> --------------- ------------------------ -------------------------------------
> -6              -5                       -6
> rows: 1

CREATE TABLE TEST(V BIGINT);
> ok

EXPLAIN SELECT BITNOT(BIT_XOR_AGG(V)), BITNOT(BIT_XNOR_AGG(V)) FROM TEST;
>> SELECT BIT_XNOR_AGG("V"), BIT_XOR_AGG("V") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

DROP TABLE TEST;
> ok
