-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT TRUNCATE(1.234, 2);
>> 1.23

SELECT TRUNCATE(DATE '2011-03-05');
>> 2011-03-05 00:00:00

SELECT TRUNCATE(TIMESTAMP '2011-03-05 02:03:04');
>> 2011-03-05 00:00:00

SELECT TRUNCATE(TIMESTAMP WITH TIME ZONE '2011-03-05 02:03:04+07');
>> 2011-03-05 00:00:00+07

SELECT TRUNCATE(CURRENT_DATE, 1);
> exception INVALID_PARAMETER_COUNT_2

SELECT TRUNCATE(LOCALTIMESTAMP, 1);
> exception INVALID_PARAMETER_COUNT_2

SELECT TRUNCATE(CURRENT_TIMESTAMP, 1);
> exception INVALID_PARAMETER_COUNT_2

SELECT TRUNCATE('2011-03-05 02:03:04', 1);
> exception INVALID_PARAMETER_COUNT_2

SELECT TRUNCATE('bad');
> exception INVALID_DATETIME_CONSTANT_2

SELECT TRUNCATE(1, 2, 3);
> exception SYNTAX_ERROR_2

select truncate(null, null) en, truncate(1.99, 0) e1, truncate(-10.9, 0) em10;
> EN   E1 EM10
> ---- -- ----
> null 1  -10
> rows: 1

select trunc(null, null) en, trunc(1.99, 0) e1, trunc(-10.9, 0) em10;
> EN   E1 EM10
> ---- -- ----
> null 1  -10
> rows: 1

select trunc(1.3);
>> 1

SELECT TRUNCATE(1.3) IS OF (NUMERIC);
>> TRUE

SELECT TRUNCATE(CAST(1.3 AS DOUBLE)) IS OF (DOUBLE);
>> TRUE

SELECT TRUNCATE(CAST(1.3 AS REAL)) IS OF (REAL);
>> TRUE

SELECT TRUNCATE(1.99, 0), TRUNCATE(1.99, 1), TRUNCATE(-1.99, 0), TRUNCATE(-1.99, 1);
> 1 1.9 -1 -1.9
> - --- -- ----
> 1 1.9 -1 -1.9
> rows: 1

SELECT TRUNCATE(1.99::DOUBLE, 0), TRUNCATE(1.99::DOUBLE, 1), TRUNCATE(-1.99::DOUBLE, 0), TRUNCATE(-1.99::DOUBLE, 1);
> 1.0 1.9 -1.0 -1.9
> --- --- ---- ----
> 1.0 1.9 -1.0 -1.9
> rows: 1

SELECT TRUNCATE(1.99::REAL, 0), TRUNCATE(1.99::REAL, 1), TRUNCATE(-1.99::REAL, 0), TRUNCATE(-1.99::REAL, 1);
> 1.0 1.9 -1.0 -1.9
> --- --- ---- ----
> 1.0 1.9 -1.0 -1.9
> rows: 1

SELECT TRUNCATE(V, S) FROM (VALUES (1.111, 1)) T(V, S);
>> 1.100

SELECT TRUNC(1, 10000000);
>> 1

CREATE TABLE T1(N NUMERIC(10, 2), D DECFLOAT(10), I INTEGER) AS VALUES (99999999.99, 99999999.99, 10);
> ok

SELECT TRUNC(N, -1) NN, TRUNC(N) N0, TRUNC(N, 1) N1, TRUNC(N, 2) N2, TRUNC(N, 3) N3, TRUNC(N, 10000000) NL,
    TRUNC(D) D0, TRUNC(D, 2) D2, TRUNC(D, 3) D3,
    TRUNC(I) I0, TRUNC(I, 1) I1, TRUNC(I, I) II FROM T1;
> NN       N0       N1         N2          N3          NL          D0       D2          D3          I0 I1 II
> -------- -------- ---------- ----------- ----------- ----------- -------- ----------- ----------- -- -- --
> 99999990 99999999 99999999.9 99999999.99 99999999.99 99999999.99 99999999 99999999.99 99999999.99 10 10 10
> rows: 1

CREATE TABLE T2 AS SELECT TRUNC(N, -1) NN, TRUNC(N) N0, TRUNC(N, 1) N1, TRUNC(N, 2) N2, TRUNC(N, 3) N3, TRUNC(N, 10000000) NL,
    TRUNC(D) D0, TRUNC(D, 2) D2, TRUNC(D, 3) D3,
    TRUNC(I) I0, TRUNC(I, 1) I1, TRUNC(I, I) II FROM T1;
> ok

SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'T2' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE NUMERIC_PRECISION NUMERIC_SCALE
> ----------- --------- ----------------- -------------
> NN          NUMERIC   8                 0
> N0          NUMERIC   8                 0
> N1          NUMERIC   9                 1
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

SELECT TRUNC(11, -1) I, TRUNC(CAST(11 AS NUMERIC(2)), -1) N;
> I  N
> -- --
> 10 10
> rows: 1

SELECT TRUNC(11, -2) I, TRUNC(CAST(11 AS NUMERIC(2)), -2) N;
> I N
> - -
> 0 0
> rows: 1
