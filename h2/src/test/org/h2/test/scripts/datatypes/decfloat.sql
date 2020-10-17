-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE MEMORY TABLE TEST(D1 DECFLOAT, D2 DECFLOAT(5), D3 DECFLOAT(10));
> ok

INSERT INTO TEST VALUES(1, 1, 9999999999);
> update count: 1

TABLE TEST;
> D1 D2 D3
> -- -- ----------
> 1  1  9999999999
> rows: 1

SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE,
    DECLARED_DATA_TYPE, DECLARED_NUMERIC_PRECISION, DECLARED_NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE NUMERIC_PRECISION NUMERIC_PRECISION_RADIX NUMERIC_SCALE DECLARED_DATA_TYPE DECLARED_NUMERIC_PRECISION DECLARED_NUMERIC_SCALE
> ----------- --------- ----------------- ----------------------- ------------- ------------------ -------------------------- ----------------------
> D1          DECFLOAT  100000            10                      null          DECFLOAT           null                       null
> D2          DECFLOAT  5                 10                      null          DECFLOAT           5                          null
> D3          DECFLOAT  10                10                      null          DECFLOAT           10                         null
> rows (ordered): 3

SELECT D2 + D3 A, D2 - D3 S, D2 * D3 M, D2 / D3 D FROM TEST;
> A     S           M          D
> ----- ----------- ---------- ----------------
> 1E+10 -9999999998 9999999999 1.0000000001E-10
> rows: 1

CREATE TABLE RESULT AS SELECT D2 + D3 A, D2 - D3 S, D2 * D3 M, D2 / D3 D FROM TEST;
> ok

TABLE RESULT;
> A     S           M          D
> ----- ----------- ---------- ----------------
> 1E+10 -9999999998 9999999999 1.0000000001E-10
> rows: 1

SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE,
    DECLARED_DATA_TYPE, DECLARED_NUMERIC_PRECISION, DECLARED_NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'RESULT' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE NUMERIC_PRECISION NUMERIC_PRECISION_RADIX NUMERIC_SCALE DECLARED_DATA_TYPE DECLARED_NUMERIC_PRECISION DECLARED_NUMERIC_SCALE
> ----------- --------- ----------------- ----------------------- ------------- ------------------ -------------------------- ----------------------
> A           DECFLOAT  11                10                      null          DECFLOAT           11                         null
> S           DECFLOAT  11                10                      null          DECFLOAT           11                         null
> M           DECFLOAT  15                10                      null          DECFLOAT           15                         null
> D           DECFLOAT  11                10                      null          DECFLOAT           11                         null
> rows (ordered): 4

DROP TABLE TEST, RESULT;
> ok

EXPLAIN VALUES (CAST(-9223372036854775808 AS DECFLOAT(19)), CAST(9223372036854775807 AS DECFLOAT(19)), 1.0, -9223372036854775809,
    9223372036854775808);
>> VALUES (CAST(-9223372036854775808 AS DECFLOAT), CAST(9223372036854775807 AS DECFLOAT), 1.0, -9223372036854775809, 9223372036854775808)

CREATE TABLE T(C DECFLOAT(0));
> exception INVALID_VALUE_2

SELECT CAST(11 AS DECFLOAT(1));
>> 1E+1

SELECT 1E1 IS OF(DECFLOAT);
>> TRUE

SELECT (CAST(1 AS REAL) + CAST(1 AS SMALLINT)) IS OF(REAL);
>> TRUE

SELECT (CAST(1 AS REAL) + CAST(1 AS BIGINT)) IS OF(DECFLOAT);
>> TRUE

SELECT (CAST(1 AS REAL) + CAST(1 AS NUMERIC)) IS OF(DECFLOAT);
>> TRUE

SELECT MOD(CAST(5 AS DECFLOAT), CAST(2 AS DECFLOAT));
>> 1

EXPLAIN SELECT 1.1E0, 1E1;
>> SELECT CAST(1.1 AS DECFLOAT), 1E+1
