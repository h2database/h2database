-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

select floor(null) vn, floor(1) v1, floor(1.1) v2, floor(-1.1) v3, floor(1.9) v4, floor(-1.9) v5;
> VN   V1 V2 V3 V4 V5
> ---- -- -- -- -- --
> null 1  1  -2 1  -2
> rows: 1

SELECT FLOOR(1.5), FLOOR(-1.5), FLOOR(1.5) IS OF (NUMERIC);
> 1 -2 TRUE
> - -- ----
> 1 -2 TRUE
> rows: 1

SELECT FLOOR(1.5::DOUBLE), FLOOR(-1.5::DOUBLE), FLOOR(1.5::DOUBLE) IS OF (DOUBLE);
> 1.0 -2.0 TRUE
> --- ---- ----
> 1.0 -2.0 TRUE
> rows: 1

SELECT FLOOR(1.5::REAL), FLOOR(-1.5::REAL), FLOOR(1.5::REAL) IS OF (REAL);
> 1.0 -2.0 TRUE
> --- ---- ----
> 1.0 -2.0 TRUE
> rows: 1

CREATE TABLE S(N NUMERIC(5, 2));
> ok

CREATE TABLE T AS SELECT FLOOR(N) F FROM S;
> ok

SELECT DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'T';
> DATA_TYPE NUMERIC_PRECISION NUMERIC_SCALE
> --------- ----------------- -------------
> NUMERIC   4                 0
> rows: 1

DROP TABLE S, T;
> ok
