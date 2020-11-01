-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE MEMORY TABLE TEST(D1 REAL, D2 FLOAT4, D3 FLOAT(1), D4 FLOAT(24));
> ok

ALTER TABLE TEST ADD COLUMN D5 FLOAT(0);
> exception INVALID_VALUE_PRECISION

ALTER TABLE TEST ADD COLUMN D5 FLOAT(-1);
> exception INVALID_VALUE_2

SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE,
    DECLARED_DATA_TYPE, DECLARED_NUMERIC_PRECISION, DECLARED_NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE NUMERIC_PRECISION NUMERIC_PRECISION_RADIX NUMERIC_SCALE DECLARED_DATA_TYPE DECLARED_NUMERIC_PRECISION DECLARED_NUMERIC_SCALE
> ----------- --------- ----------------- ----------------------- ------------- ------------------ -------------------------- ----------------------
> D1          REAL      24                2                       null          REAL               null                       null
> D2          REAL      24                2                       null          REAL               null                       null
> D3          REAL      24                2                       null          FLOAT              1                          null
> D4          REAL      24                2                       null          FLOAT              24                         null
> rows (ordered): 4

SCRIPT NODATA NOPASSWORDS NOSETTINGS NOVERSION TABLE TEST;
> SCRIPT
> -------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "D1" REAL, "D2" REAL, "D3" FLOAT(1), "D4" FLOAT(24) );
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> rows (ordered): 3

DROP TABLE TEST;
> ok

EXPLAIN VALUES CAST(0 AS REAL);
>> VALUES (CAST(0.0 AS REAL))

CREATE TABLE TEST(F REAL, I INT) AS VALUES (2000000000, 2000000001);
> ok

SELECT F, I, F = I FROM TEST;
> F     I          F = I
> ----- ---------- -----
> 2.0E9 2000000001 FALSE
> rows: 1

DROP TABLE TEST;
> ok
