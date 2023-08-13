-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE MEMORY TABLE TEST(D1 DECFLOAT, D2 DECFLOAT(5), D3 DECFLOAT(10), X NUMBER);
> ok

INSERT INTO TEST VALUES(1, 1, 9999999999, 1.23);
> update count: 1

TABLE TEST;
> D1 D2 D3         X
> -- -- ---------- ----
> 1  1  9999999999 1.23
> rows: 1

SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE,
    DECLARED_DATA_TYPE, DECLARED_NUMERIC_PRECISION, DECLARED_NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE NUMERIC_PRECISION NUMERIC_PRECISION_RADIX NUMERIC_SCALE DECLARED_DATA_TYPE DECLARED_NUMERIC_PRECISION DECLARED_NUMERIC_SCALE
> ----------- --------- ----------------- ----------------------- ------------- ------------------ -------------------------- ----------------------
> D1          DECFLOAT  100000            10                      null          DECFLOAT           null                       null
> D2          DECFLOAT  5                 10                      null          DECFLOAT           5                          null
> D3          DECFLOAT  10                10                      null          DECFLOAT           10                         null
> X           DECFLOAT  40                10                      null          DECFLOAT           40                         null
> rows (ordered): 4

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
>> SELECT CAST(1.1 AS DECFLOAT), CAST(1E+1 AS DECFLOAT)

CREATE MEMORY TABLE TEST(D DECFLOAT(8)) AS VALUES '-Infinity', '-1', '0', '1', '1.5', 'Infinity', 'NaN';
> ok

@reconnect

SELECT D, -D, SIGN(D) FROM TEST ORDER BY D;
> D         - D       SIGN(D)
> --------- --------- -------
> -Infinity Infinity  -1
> -1        1         -1
> 0         0         0
> 1         -1        1
> 1.5       -1.5      1
> Infinity  -Infinity 1
> NaN       NaN       0
> rows (ordered): 7

SELECT A.D, B.D, A.D + B.D, A.D - B.D, A.D * B.D FROM TEST A JOIN TEST B ORDER BY A.D, B.D;
> D         D         A.D + B.D A.D - B.D A.D * B.D
> --------- --------- --------- --------- ---------
> -Infinity -Infinity -Infinity NaN       Infinity
> -Infinity -1        -Infinity -Infinity Infinity
> -Infinity 0         -Infinity -Infinity NaN
> -Infinity 1         -Infinity -Infinity -Infinity
> -Infinity 1.5       -Infinity -Infinity -Infinity
> -Infinity Infinity  NaN       -Infinity -Infinity
> -Infinity NaN       NaN       NaN       NaN
> -1        -Infinity -Infinity Infinity  Infinity
> -1        -1        -2        0         1
> -1        0         -1        -1        0
> -1        1         0         -2        -1
> -1        1.5       0.5       -2.5      -1.5
> -1        Infinity  Infinity  -Infinity -Infinity
> -1        NaN       NaN       NaN       NaN
> 0         -Infinity -Infinity Infinity  NaN
> 0         -1        -1        1         0
> 0         0         0         0         0
> 0         1         1         -1        0
> 0         1.5       1.5       -1.5      0
> 0         Infinity  Infinity  -Infinity NaN
> 0         NaN       NaN       NaN       NaN
> 1         -Infinity -Infinity Infinity  -Infinity
> 1         -1        0         2         -1
> 1         0         1         1         0
> 1         1         2         0         1
> 1         1.5       2.5       -0.5      1.5
> 1         Infinity  Infinity  -Infinity Infinity
> 1         NaN       NaN       NaN       NaN
> 1.5       -Infinity -Infinity Infinity  -Infinity
> 1.5       -1        0.5       2.5       -1.5
> 1.5       0         1.5       1.5       0
> 1.5       1         2.5       0.5       1.5
> 1.5       1.5       3         0         2.25
> 1.5       Infinity  Infinity  -Infinity Infinity
> 1.5       NaN       NaN       NaN       NaN
> Infinity  -Infinity NaN       Infinity  -Infinity
> Infinity  -1        Infinity  Infinity  -Infinity
> Infinity  0         Infinity  Infinity  NaN
> Infinity  1         Infinity  Infinity  Infinity
> Infinity  1.5       Infinity  Infinity  Infinity
> Infinity  Infinity  Infinity  NaN       Infinity
> Infinity  NaN       NaN       NaN       NaN
> NaN       -Infinity NaN       NaN       NaN
> NaN       -1        NaN       NaN       NaN
> NaN       0         NaN       NaN       NaN
> NaN       1         NaN       NaN       NaN
> NaN       1.5       NaN       NaN       NaN
> NaN       Infinity  NaN       NaN       NaN
> NaN       NaN       NaN       NaN       NaN
> rows (ordered): 49

SELECT A.D, B.D, A.D / B.D, MOD(A.D, B.D) FROM TEST A JOIN TEST B WHERE B.D <> 0 ORDER BY A.D, B.D;
> D         D         A.D / B.D    MOD(A.D, B.D)
> --------- --------- ------------ -------------
> -Infinity -Infinity NaN          NaN
> -Infinity -1        Infinity     NaN
> -Infinity 1         -Infinity    NaN
> -Infinity 1.5       -Infinity    NaN
> -Infinity Infinity  NaN          NaN
> -Infinity NaN       NaN          NaN
> -1        -Infinity 0            -1
> -1        -1        1            0
> -1        1         -1           0
> -1        1.5       -0.666666667 -1
> -1        Infinity  0            -1
> -1        NaN       NaN          NaN
> 0         -Infinity 0            0
> 0         -1        0            0
> 0         1         0            0
> 0         1.5       0            0
> 0         Infinity  0            0
> 0         NaN       NaN          NaN
> 1         -Infinity 0            1
> 1         -1        -1           0
> 1         1         1            0
> 1         1.5       0.666666667  1
> 1         Infinity  0            1
> 1         NaN       NaN          NaN
> 1.5       -Infinity 0            1.5
> 1.5       -1        -1.5         0.5
> 1.5       1         1.5          0.5
> 1.5       1.5       1            0
> 1.5       Infinity  0            1.5
> 1.5       NaN       NaN          NaN
> Infinity  -Infinity NaN          NaN
> Infinity  -1        -Infinity    NaN
> Infinity  1         Infinity     NaN
> Infinity  1.5       Infinity     NaN
> Infinity  Infinity  NaN          NaN
> Infinity  NaN       NaN          NaN
> NaN       -Infinity NaN          NaN
> NaN       -1        NaN          NaN
> NaN       1         NaN          NaN
> NaN       1.5       NaN          NaN
> NaN       Infinity  NaN          NaN
> NaN       NaN       NaN          NaN
> rows (ordered): 42

SELECT A.D, B.D, A.D > B.D, A.D = B.D, A.D < B.D FROM TEST A JOIN TEST B ORDER BY A.D, B.D;
> D         D         A.D > B.D A.D = B.D A.D < B.D
> --------- --------- --------- --------- ---------
> -Infinity -Infinity FALSE     TRUE      FALSE
> -Infinity -1        FALSE     FALSE     TRUE
> -Infinity 0         FALSE     FALSE     TRUE
> -Infinity 1         FALSE     FALSE     TRUE
> -Infinity 1.5       FALSE     FALSE     TRUE
> -Infinity Infinity  FALSE     FALSE     TRUE
> -Infinity NaN       FALSE     FALSE     TRUE
> -1        -Infinity TRUE      FALSE     FALSE
> -1        -1        FALSE     TRUE      FALSE
> -1        0         FALSE     FALSE     TRUE
> -1        1         FALSE     FALSE     TRUE
> -1        1.5       FALSE     FALSE     TRUE
> -1        Infinity  FALSE     FALSE     TRUE
> -1        NaN       FALSE     FALSE     TRUE
> 0         -Infinity TRUE      FALSE     FALSE
> 0         -1        TRUE      FALSE     FALSE
> 0         0         FALSE     TRUE      FALSE
> 0         1         FALSE     FALSE     TRUE
> 0         1.5       FALSE     FALSE     TRUE
> 0         Infinity  FALSE     FALSE     TRUE
> 0         NaN       FALSE     FALSE     TRUE
> 1         -Infinity TRUE      FALSE     FALSE
> 1         -1        TRUE      FALSE     FALSE
> 1         0         TRUE      FALSE     FALSE
> 1         1         FALSE     TRUE      FALSE
> 1         1.5       FALSE     FALSE     TRUE
> 1         Infinity  FALSE     FALSE     TRUE
> 1         NaN       FALSE     FALSE     TRUE
> 1.5       -Infinity TRUE      FALSE     FALSE
> 1.5       -1        TRUE      FALSE     FALSE
> 1.5       0         TRUE      FALSE     FALSE
> 1.5       1         TRUE      FALSE     FALSE
> 1.5       1.5       FALSE     TRUE      FALSE
> 1.5       Infinity  FALSE     FALSE     TRUE
> 1.5       NaN       FALSE     FALSE     TRUE
> Infinity  -Infinity TRUE      FALSE     FALSE
> Infinity  -1        TRUE      FALSE     FALSE
> Infinity  0         TRUE      FALSE     FALSE
> Infinity  1         TRUE      FALSE     FALSE
> Infinity  1.5       TRUE      FALSE     FALSE
> Infinity  Infinity  FALSE     TRUE      FALSE
> Infinity  NaN       FALSE     FALSE     TRUE
> NaN       -Infinity TRUE      FALSE     FALSE
> NaN       -1        TRUE      FALSE     FALSE
> NaN       0         TRUE      FALSE     FALSE
> NaN       1         TRUE      FALSE     FALSE
> NaN       1.5       TRUE      FALSE     FALSE
> NaN       Infinity  TRUE      FALSE     FALSE
> NaN       NaN       FALSE     TRUE      FALSE
> rows (ordered): 49

SELECT D, CAST(D AS REAL) D1, CAST(D AS DOUBLE PRECISION) D2 FROM TEST ORDER BY D;
> D         D1        D2
> --------- --------- ---------
> -Infinity -Infinity -Infinity
> -1        -1.0      -1.0
> 0         0.0       0.0
> 1         1.0       1.0
> 1.5       1.5       1.5
> Infinity  Infinity  Infinity
> NaN       NaN       NaN
> rows (ordered): 7

EXPLAIN SELECT CAST('Infinity' AS DECFLOAT), CAST('-Infinity' AS DECFLOAT), CAST('NaN' AS DECFLOAT), CAST(0 AS DECFLOAT);
>> SELECT CAST('Infinity' AS DECFLOAT), CAST('-Infinity' AS DECFLOAT), CAST('NaN' AS DECFLOAT), CAST(0 AS DECFLOAT)

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION TABLE TEST;
> SCRIPT
> -----------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "D" DECFLOAT(8) );
> -- 7 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST" VALUES ('-Infinity'), (-1), (0), (1), (1.5), ('Infinity'), ('NaN');
> rows (ordered): 4

DROP TABLE TEST;
> ok

VALUES '1E100' > 0;
>> TRUE

SELECT 'NaN' = CAST('NaN' AS DECFLOAT);
>> TRUE

SELECT CAST('NaN' AS DOUBLE ) = CAST('NaN' AS DECFLOAT);
>> TRUE

SELECT 111222E+8_8, 111_222E+1_4;
> 1.11222E+93 1.11222E+19
> ----------- -----------
> 1.11222E+93 1.11222E+19
> rows: 1

SELECT 111222333444555666777E+8_8, 111_222_333_444_555_666_777E+1_4;
> 1.11222333444555666777E+108 1.11222333444555666777E+34
> --------------------------- --------------------------
> 1.11222333444555666777E+108 1.11222333444555666777E+34
> rows: 1

SELECT 111222333444555666777.123E+8_8, 111_222_333_444_555_666_777.888_999E+1_4;
> 1.11222333444555666777123E+108 1.11222333444555666777888999E+34
> ------------------------------ --------------------------------
> 1.11222333444555666777123E+108 1.11222333444555666777888999E+34
> rows: 1

SELECT 1E;
> exception SYNTAX_ERROR_2

SELECT 1E+;
> exception SYNTAX_ERROR_2

SELECT 1E-;
> exception SYNTAX_ERROR_2

SELECT 1E_3;
> exception SYNTAX_ERROR_2

SELECT 1E+_3;
> exception SYNTAX_ERROR_2

SELECT 1E+3__3;
> exception SYNTAX_ERROR_2

SELECT 1E+8_;
> exception SYNTAX_ERROR_2

SELECT 1.3_E+3__3;
> exception SYNTAX_ERROR_2

