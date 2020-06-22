-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE MEMORY TABLE TEST(D1 DOUBLE, D2 DOUBLE PRECISION, D3 FLOAT, D4 FLOAT(25), D5 FLOAT(53));
> ok

ALTER TABLE TEST ADD COLUMN D6 FLOAT(54);
> exception INVALID_VALUE_PRECISION

SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE,
    DECLARED_DATA_TYPE, DECLARED_NUMERIC_PRECISION, DECLARED_NUMERIC_SCALE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE        NUMERIC_PRECISION NUMERIC_PRECISION_RADIX NUMERIC_SCALE DECLARED_DATA_TYPE DECLARED_NUMERIC_PRECISION DECLARED_NUMERIC_SCALE COLUMN_TYPE
> ----------- ---------------- ----------------- ----------------------- ------------- ------------------ -------------------------- ---------------------- ----------------
> D1          DOUBLE PRECISION 53                2                       null          DOUBLE PRECISION   null                       null                   DOUBLE
> D2          DOUBLE PRECISION 53                2                       null          DOUBLE PRECISION   null                       null                   DOUBLE PRECISION
> D3          DOUBLE PRECISION 53                2                       null          FLOAT              null                       null                   FLOAT
> D4          DOUBLE PRECISION 53                2                       null          FLOAT              25                         null                   FLOAT(25)
> D5          DOUBLE PRECISION 53                2                       null          FLOAT              53                         null                   FLOAT(53)
> rows (ordered): 5

SCRIPT NODATA NOPASSWORDS NOSETTINGS TABLE TEST;
> SCRIPT
> ----------------------------------------------------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "D1" DOUBLE, "D2" DOUBLE PRECISION, "D3" FLOAT, "D4" FLOAT(25), "D5" FLOAT(53) );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> rows: 3

DROP TABLE TEST;
> ok

EXPLAIN VALUES CAST(0 AS DOUBLE);
>> VALUES (CAST(0.0 AS DOUBLE PRECISION))
