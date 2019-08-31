-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE SEQUENCE SEQ NO CACHE;
> ok

CREATE TABLE TEST(NEXT INT) AS (VALUES 10, 20);
> ok

SELECT NEXT VALUE, NEXT VALUE FOR SEQ FROM TEST;
> VALUE NEXT VALUE FOR PUBLIC.SEQ
> ----- -------------------------
> 10    1
> 20    2
> rows: 2

DROP TABLE TEST;
> ok

DROP SEQUENCE SEQ;
> ok
