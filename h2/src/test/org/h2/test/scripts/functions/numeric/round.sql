-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT ROUND(-1.2), ROUND(-1.5), ROUND(-1.6), ROUND(2), ROUND(1.5), ROUND(1.8), ROUND(1.1);
> -1 -2 -2 2 2 2 1
> -- -- -- - - - -
> -1 -2 -2 2 2 2 1
> rows: 1

select round(null, null) en, round(10.49, 0) e10, round(10.05, 1) e101;
> EN   E10 E101
> ---- --- ----
> null 10  10.1
> rows: 1

select round(null) en, round(0.6, null) en2, round(1.05) e1, round(-1.51) em2;
> EN   EN2  E1 EM2
> ---- ---- -- ---
> null null 1  -2
> rows: 1

CALL ROUND(998.5::DOUBLE);
>> 999.0

CALL ROUND(998.5::REAL);
>> 999.0

SELECT
    ROUND(4503599627370495.0::DOUBLE), ROUND(4503599627370495.5::DOUBLE),
    ROUND(4503599627370496.0::DOUBLE), ROUND(4503599627370497.0::DOUBLE);
> 4.503599627370495E15 4.503599627370496E15 4.503599627370496E15 4.503599627370497E15
> -------------------- -------------------- -------------------- --------------------
> 4.503599627370495E15 4.503599627370496E15 4.503599627370496E15 4.503599627370497E15
> rows: 1

SELECT
    ROUND(450359962737049.50::DOUBLE, 1), ROUND(450359962737049.55::DOUBLE, 1),
    ROUND(450359962737049.60::DOUBLE, 1), ROUND(450359962737049.70::DOUBLE, 1);
> 4.503599627370495E14 4.503599627370496E14 4.503599627370496E14 4.503599627370497E14
> -------------------- -------------------- -------------------- --------------------
> 4.503599627370495E14 4.503599627370496E14 4.503599627370496E14 4.503599627370497E14
> rows: 1

CALL ROUND(0.285, 2);
>> 0.29

CALL ROUND(0.285::DOUBLE, 2);
>> 0.29

CALL ROUND(0.285::REAL, 2);
>> 0.29

CALL ROUND(1.285, 2);
>> 1.29

CALL ROUND(1.285::DOUBLE, 2);
>> 1.29

CALL ROUND(1.285::REAL, 2);
>> 1.29

CALL ROUND(1, 1) IS OF (INTEGER);
>> TRUE

CALL ROUND(1::DOUBLE, 1) IS OF (DOUBLE);
>> TRUE

CALL ROUND(1::REAL, 1) IS OF (REAL);
>> TRUE

CREATE TABLE T1(N NUMERIC(10, 2), D DECFLOAT(10), I INTEGER) AS VALUES (99999999.99, 99999999.99, 10);
> ok

SELECT ROUND(N, -1) NN, ROUND(N) N0, ROUND(N, 1) N1, ROUND(N, 2) N2, ROUND(N, 3) N3, ROUND(N, 100000) NL,
    ROUND(D) D0, ROUND(D, 2) D2, ROUND(D, 3) D3,
    ROUND(I) I0, ROUND(I, 1) I1, ROUND(I, I) II FROM T1;
> NN        N0        N1          N2          N3          NL          D0   D2          D3          I0 I1 II
> --------- --------- ----------- ----------- ----------- ----------- ---- ----------- ----------- -- -- --
> 100000000 100000000 100000000.0 99999999.99 99999999.99 99999999.99 1E+8 99999999.99 99999999.99 10 10 10
> rows: 1

CREATE TABLE T2 AS SELECT ROUND(N, -1) NN, ROUND(N) N0, ROUND(N, 1) N1, ROUND(N, 2) N2, ROUND(N, 3) N3, ROUND(N, 100000) NL,
    ROUND(D) D0, ROUND(D, 2) D2, ROUND(D, 3) D3,
    ROUND(I) I0, ROUND(I, 1) I1, ROUND(I, I) II FROM T1;
> ok

SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'T2' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE NUMERIC_PRECISION NUMERIC_SCALE
> ----------- --------- ----------------- -------------
> NN          NUMERIC   9                 0
> N0          NUMERIC   9                 0
> N1          NUMERIC   10                1
> N2          NUMERIC   10                2
> N3          NUMERIC   10                2
> NL          NUMERIC   10                2
> D0          DECFLOAT  10                null
> D2          DECFLOAT  10                null
> D3          DECFLOAT  10                null
> I0          INTEGER   32                0
> I1          INTEGER   32                0
> II          INTEGER   32                0
> rows (ordered): 12

DROP TABLE T1;
> ok

SELECT ROUND(1, -100001);
> exception INVALID_VALUE_2

SELECT ROUND(1, 100001);
> exception INVALID_VALUE_2

SELECT ROUND(1, -100000);
>> 0

SELECT ROUND(9223372036854775807, -14);
> exception NUMERIC_VALUE_OUT_OF_RANGE_1

SELECT ROUND(9223372036854775807, -15);
>> 9223000000000000000

SELECT ROUND(2147483647, -7);
> exception NUMERIC_VALUE_OUT_OF_RANGE_1

SELECT ROUND(2147483647, -9);
>> 2000000000

SELECT ROUND(2147483647, -10);
>> 0
