-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE MEMORY TABLE TEST(
    N1 NUMERIC, N2 NUMERIC(10), N3 NUMERIC(10, 0), N4 NUMERIC(10, 2),
    D1 DECIMAL, D2 DECIMAL(10), D3 DECIMAL(10, 0), D4 DECIMAL(10, 2), D5 DEC,
    X1 NUMBER(10), X2 NUMBER(10, 2));
> ok

SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE,
    DECLARED_DATA_TYPE, DECLARED_NUMERIC_PRECISION, DECLARED_NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE NUMERIC_PRECISION NUMERIC_PRECISION_RADIX NUMERIC_SCALE DECLARED_DATA_TYPE DECLARED_NUMERIC_PRECISION DECLARED_NUMERIC_SCALE
> ----------- --------- ----------------- ----------------------- ------------- ------------------ -------------------------- ----------------------
> N1          NUMERIC   100000            10                      0             NUMERIC            null                       null
> N2          NUMERIC   10                10                      0             NUMERIC            10                         null
> N3          NUMERIC   10                10                      0             NUMERIC            10                         0
> N4          NUMERIC   10                10                      2             NUMERIC            10                         2
> D1          NUMERIC   100000            10                      0             DECIMAL            null                       null
> D2          NUMERIC   10                10                      0             DECIMAL            10                         null
> D3          NUMERIC   10                10                      0             DECIMAL            10                         0
> D4          NUMERIC   10                10                      2             DECIMAL            10                         2
> D5          NUMERIC   100000            10                      0             DECIMAL            null                       null
> X1          NUMERIC   10                10                      0             NUMERIC            10                         null
> X2          NUMERIC   10                10                      2             NUMERIC            10                         2
> rows (ordered): 11

DROP TABLE TEST;
> ok

CREATE TABLE TEST(N NUMERIC(2, -1));
> exception INVALID_VALUE_SCALE

CREATE TABLE TEST(ID INT, X1 BIT, XT TINYINT, X_SM SMALLINT, XB BIGINT, XD DECIMAL(10,2), XD2 DOUBLE PRECISION, XR REAL);
> ok

INSERT INTO TEST VALUES(?, ?, ?, ?, ?, ?, ?, ?);
{
0,FALSE,0,0,0,0.0,0.0,0.0
1,TRUE,1,1,1,1.0,1.0,1.0
4,TRUE,4,4,4,4.0,4.0,4.0
-1,FALSE,-1,-1,-1,-1.0,-1.0,-1.0
NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
};
> update count: 5

SELECT ID, CAST(XT AS NUMBER(10,1)),
CAST(X_SM AS NUMBER(10,1)), CAST(XB AS NUMBER(10,1)), CAST(XD AS NUMBER(10,1)),
CAST(XD2 AS NUMBER(10,1)), CAST(XR AS NUMBER(10,1)) FROM TEST;
> ID   CAST(XT AS NUMERIC(10, 1)) CAST(X_SM AS NUMERIC(10, 1)) CAST(XB AS NUMERIC(10, 1)) CAST(XD AS NUMERIC(10, 1)) CAST(XD2 AS NUMERIC(10, 1)) CAST(XR AS NUMERIC(10, 1))
> ---- -------------------------- ---------------------------- -------------------------- -------------------------- --------------------------- --------------------------
> -1   -1.0                       -1.0                         -1.0                       -1.0                       -1.0                        -1.0
> 0    0.0                        0.0                          0.0                        0.0                        0.0                         0.0
> 1    1.0                        1.0                          1.0                        1.0                        1.0                         1.0
> 4    4.0                        4.0                          4.0                        4.0                        4.0                         4.0
> null null                       null                         null                       null                       null                        null
> rows: 5

DROP TABLE TEST;
> ok

CREATE TABLE TEST(I NUMERIC(-1));
> exception INVALID_VALUE_2

CREATE TABLE TEST(I NUMERIC(-1, -1));
> exception INVALID_VALUE_2

CREATE TABLE TEST (N NUMERIC(3, 1)) AS VALUES (0), (0.0), (NULL);
> ok

SELECT * FROM TEST;
> N
> ----
> 0.0
> 0.0
> null
> rows: 3

DROP TABLE TEST;
> ok

SELECT CAST(10000 AS NUMERIC(5));
>> 10000

CREATE DOMAIN N AS NUMERIC(10, 1);
> ok

CREATE TABLE TEST(V N);
> ok

SELECT NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'V';
>> 1

DROP TABLE TEST;
> ok

DROP DOMAIN N;
> ok

CREATE TABLE TEST(I INT PRIMARY KEY, V NUMERIC(1, 3));
> ok

INSERT INTO TEST VALUES (1, 1e-3), (2, 1.1e-3), (3, 1e-4);
> update count: 3

INSERT INTO TEST VALUES (4, 1e-2);
> exception VALUE_TOO_LONG_2

TABLE TEST;
> I V
> - -----
> 1 0.001
> 2 0.001
> 3 0.000
> rows: 3

DROP TABLE TEST;
> ok

CREATE TABLE TEST(I INT PRIMARY KEY, V NUMERIC(2));
> ok

INSERT INTO TEST VALUES (1, 1e-1), (2, 2e0), (3, 3e1);
> update count: 3

TABLE TEST;
> I V
> - --
> 1 0
> 2 2
> 3 30
> rows: 3

DROP TABLE TEST;
> ok

EXPLAIN VALUES (CAST(-9223372036854775808 AS NUMERIC(19)), CAST(9223372036854775807 AS NUMERIC(19)), 1.0, -9223372036854775809,
    9223372036854775808);
>> VALUES (CAST(-9223372036854775808 AS NUMERIC(19)), CAST(9223372036854775807 AS NUMERIC(19)), 1.0, -9223372036854775809, 9223372036854775808)

CREATE TABLE T(C NUMERIC(0));
> exception INVALID_VALUE_2

CREATE TABLE T1(A NUMERIC(100000));
> ok

CREATE TABLE T2(A NUMERIC(100001));
> exception INVALID_VALUE_PRECISION

SET TRUNCATE_LARGE_LENGTH TRUE;
> ok

CREATE TABLE T2(A NUMERIC(100001));
> ok

SELECT TABLE_NAME, NUMERIC_PRECISION, DECLARED_NUMERIC_PRECISION FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'PUBLIC';
> TABLE_NAME NUMERIC_PRECISION DECLARED_NUMERIC_PRECISION
> ---------- ----------------- --------------------------
> T1         100000            100000
> T2         100000            100000
> rows: 2

SET TRUNCATE_LARGE_LENGTH FALSE;
> ok

DROP TABLE T1, T2;
> ok

SET MODE Oracle;
> ok

CREATE TABLE TEST(N NUMERIC(2, 1));
> ok

INSERT INTO TEST VALUES 20;
> exception VALUE_TOO_LONG_2

INSERT INTO TEST VALUES CAST(20 AS NUMERIC(2));
> exception VALUE_TOO_LONG_2

DROP TABLE TEST;
> ok

SET MODE PostgreSQL;
> ok

CREATE TABLE TEST(A NUMERIC, B DECIMAL, C DEC, D NUMERIC(1));
> ok

SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE,
    DECLARED_DATA_TYPE, DECLARED_NUMERIC_PRECISION, DECLARED_NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE NUMERIC_PRECISION NUMERIC_PRECISION_RADIX NUMERIC_SCALE DECLARED_DATA_TYPE DECLARED_NUMERIC_PRECISION DECLARED_NUMERIC_SCALE
> ----------- --------- ----------------- ----------------------- ------------- ------------------ -------------------------- ----------------------
> A           DECFLOAT  100000            10                      null          DECFLOAT           null                       null
> B           DECFLOAT  100000            10                      null          DECFLOAT           null                       null
> C           DECFLOAT  100000            10                      null          DECFLOAT           null                       null
> D           NUMERIC   1                 10                      0             NUMERIC            1                          null
> rows (ordered): 4

DROP TABLE TEST;
> ok

SET MODE Regular;
> ok

CREATE TABLE TEST(A NUMERIC(100000), B NUMERIC(100)) AS VALUES (1E99999, 1E99);
> ok

SELECT CHAR_LENGTH(CAST(A / B AS VARCHAR)) FROM TEST;
>> 99901

SELECT CHAR_LENGTH(CAST(A / CAST(B AS NUMERIC(200, 100)) AS VARCHAR)) FROM TEST;
>> 99901

DROP TABLE TEST;
> ok

SELECT 111_222_333_444_555_666_777_888_999, 111_222_333_444_555_666_777.333_444, 123_456., .333, 345_323.765_329, 1.;
> 111222333444555666777888999 111222333444555666777.333444 123456 0.333 345323.765329 1
> --------------------------- ---------------------------- ------ ----- ------------- -
> 111222333444555666777888999 111222333444555666777.333444 123456 0.333 345323.765329 1
> rows: 1

SELECT 1_.;
> exception SYNTAX_ERROR_2

SELECT 1_1._1;
> exception SYNTAX_ERROR_2

SELECT 9_9.9_;
> exception SYNTAX_ERROR_2

SELECT 132_134.3__3;
> exception SYNTAX_ERROR_2

SELECT 111_222_333_444_555_666__777;
> exception SYNTAX_ERROR_2
