-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
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

CREATE MEMORY TABLE TEST(D REAL) AS VALUES '-Infinity', '-1', '0', '1', '1.5', 'Infinity', 'NaN';
> ok

SELECT D, -D, SIGN(D) FROM TEST ORDER BY D;
> D         - D       SIGN(D)
> --------- --------- -------
> -Infinity Infinity  -1
> -1.0      1.0       -1
> 0.0       0.0       0
> 1.0       -1.0      1
> 1.5       -1.5      1
> Infinity  -Infinity 1
> NaN       NaN       0
> rows (ordered): 7

SELECT A.D, B.D, A.D + B.D, A.D - B.D, A.D * B.D FROM TEST A JOIN TEST B ORDER BY A.D, B.D;
> D         D         A.D + B.D A.D - B.D A.D * B.D
> --------- --------- --------- --------- ---------
> -Infinity -Infinity -Infinity NaN       Infinity
> -Infinity -1.0      -Infinity -Infinity Infinity
> -Infinity 0.0       -Infinity -Infinity NaN
> -Infinity 1.0       -Infinity -Infinity -Infinity
> -Infinity 1.5       -Infinity -Infinity -Infinity
> -Infinity Infinity  NaN       -Infinity -Infinity
> -Infinity NaN       NaN       NaN       NaN
> -1.0      -Infinity -Infinity Infinity  Infinity
> -1.0      -1.0      -2.0      0.0       1.0
> -1.0      0.0       -1.0      -1.0      0.0
> -1.0      1.0       0.0       -2.0      -1.0
> -1.0      1.5       0.5       -2.5      -1.5
> -1.0      Infinity  Infinity  -Infinity -Infinity
> -1.0      NaN       NaN       NaN       NaN
> 0.0       -Infinity -Infinity Infinity  NaN
> 0.0       -1.0      -1.0      1.0       0.0
> 0.0       0.0       0.0       0.0       0.0
> 0.0       1.0       1.0       -1.0      0.0
> 0.0       1.5       1.5       -1.5      0.0
> 0.0       Infinity  Infinity  -Infinity NaN
> 0.0       NaN       NaN       NaN       NaN
> 1.0       -Infinity -Infinity Infinity  -Infinity
> 1.0       -1.0      0.0       2.0       -1.0
> 1.0       0.0       1.0       1.0       0.0
> 1.0       1.0       2.0       0.0       1.0
> 1.0       1.5       2.5       -0.5      1.5
> 1.0       Infinity  Infinity  -Infinity Infinity
> 1.0       NaN       NaN       NaN       NaN
> 1.5       -Infinity -Infinity Infinity  -Infinity
> 1.5       -1.0      0.5       2.5       -1.5
> 1.5       0.0       1.5       1.5       0.0
> 1.5       1.0       2.5       0.5       1.5
> 1.5       1.5       3.0       0.0       2.25
> 1.5       Infinity  Infinity  -Infinity Infinity
> 1.5       NaN       NaN       NaN       NaN
> Infinity  -Infinity NaN       Infinity  -Infinity
> Infinity  -1.0      Infinity  Infinity  -Infinity
> Infinity  0.0       Infinity  Infinity  NaN
> Infinity  1.0       Infinity  Infinity  Infinity
> Infinity  1.5       Infinity  Infinity  Infinity
> Infinity  Infinity  Infinity  NaN       Infinity
> Infinity  NaN       NaN       NaN       NaN
> NaN       -Infinity NaN       NaN       NaN
> NaN       -1.0      NaN       NaN       NaN
> NaN       0.0       NaN       NaN       NaN
> NaN       1.0       NaN       NaN       NaN
> NaN       1.5       NaN       NaN       NaN
> NaN       Infinity  NaN       NaN       NaN
> NaN       NaN       NaN       NaN       NaN
> rows (ordered): 49

SELECT A.D, B.D, A.D / B.D, MOD(A.D, B.D) FROM TEST A JOIN TEST B WHERE B.D <> 0 ORDER BY A.D, B.D;
> D         D         A.D / B.D  MOD(A.D, B.D)
> --------- --------- ---------- -------------
> -Infinity -Infinity NaN        NaN
> -Infinity -1.0      Infinity   NaN
> -Infinity 1.0       -Infinity  NaN
> -Infinity 1.5       -Infinity  NaN
> -Infinity Infinity  NaN        NaN
> -Infinity NaN       NaN        NaN
> -1.0      -Infinity 0.0        -1.0
> -1.0      -1.0      1.0        0.0
> -1.0      1.0       -1.0       0.0
> -1.0      1.5       -0.6666667 -1.0
> -1.0      Infinity  0.0        -1.0
> -1.0      NaN       NaN        NaN
> 0.0       -Infinity 0.0        0.0
> 0.0       -1.0      0.0        0.0
> 0.0       1.0       0.0        0.0
> 0.0       1.5       0.0        0.0
> 0.0       Infinity  0.0        0.0
> 0.0       NaN       NaN        NaN
> 1.0       -Infinity 0.0        1.0
> 1.0       -1.0      -1.0       0.0
> 1.0       1.0       1.0        0.0
> 1.0       1.5       0.6666667  1.0
> 1.0       Infinity  0.0        1.0
> 1.0       NaN       NaN        NaN
> 1.5       -Infinity 0.0        1.5
> 1.5       -1.0      -1.5       0.5
> 1.5       1.0       1.5        0.5
> 1.5       1.5       1.0        0.0
> 1.5       Infinity  0.0        1.5
> 1.5       NaN       NaN        NaN
> Infinity  -Infinity NaN        NaN
> Infinity  -1.0      -Infinity  NaN
> Infinity  1.0       Infinity   NaN
> Infinity  1.5       Infinity   NaN
> Infinity  Infinity  NaN        NaN
> Infinity  NaN       NaN        NaN
> NaN       -Infinity NaN        NaN
> NaN       -1.0      NaN        NaN
> NaN       1.0       NaN        NaN
> NaN       1.5       NaN        NaN
> NaN       Infinity  NaN        NaN
> NaN       NaN       NaN        NaN
> rows (ordered): 42

SELECT A.D, B.D, A.D > B.D, A.D = B.D, A.D < B.D FROM TEST A JOIN TEST B ORDER BY A.D, B.D;
> D         D         A.D > B.D A.D = B.D A.D < B.D
> --------- --------- --------- --------- ---------
> -Infinity -Infinity FALSE     TRUE      FALSE
> -Infinity -1.0      FALSE     FALSE     TRUE
> -Infinity 0.0       FALSE     FALSE     TRUE
> -Infinity 1.0       FALSE     FALSE     TRUE
> -Infinity 1.5       FALSE     FALSE     TRUE
> -Infinity Infinity  FALSE     FALSE     TRUE
> -Infinity NaN       FALSE     FALSE     TRUE
> -1.0      -Infinity TRUE      FALSE     FALSE
> -1.0      -1.0      FALSE     TRUE      FALSE
> -1.0      0.0       FALSE     FALSE     TRUE
> -1.0      1.0       FALSE     FALSE     TRUE
> -1.0      1.5       FALSE     FALSE     TRUE
> -1.0      Infinity  FALSE     FALSE     TRUE
> -1.0      NaN       FALSE     FALSE     TRUE
> 0.0       -Infinity TRUE      FALSE     FALSE
> 0.0       -1.0      TRUE      FALSE     FALSE
> 0.0       0.0       FALSE     TRUE      FALSE
> 0.0       1.0       FALSE     FALSE     TRUE
> 0.0       1.5       FALSE     FALSE     TRUE
> 0.0       Infinity  FALSE     FALSE     TRUE
> 0.0       NaN       FALSE     FALSE     TRUE
> 1.0       -Infinity TRUE      FALSE     FALSE
> 1.0       -1.0      TRUE      FALSE     FALSE
> 1.0       0.0       TRUE      FALSE     FALSE
> 1.0       1.0       FALSE     TRUE      FALSE
> 1.0       1.5       FALSE     FALSE     TRUE
> 1.0       Infinity  FALSE     FALSE     TRUE
> 1.0       NaN       FALSE     FALSE     TRUE
> 1.5       -Infinity TRUE      FALSE     FALSE
> 1.5       -1.0      TRUE      FALSE     FALSE
> 1.5       0.0       TRUE      FALSE     FALSE
> 1.5       1.0       TRUE      FALSE     FALSE
> 1.5       1.5       FALSE     TRUE      FALSE
> 1.5       Infinity  FALSE     FALSE     TRUE
> 1.5       NaN       FALSE     FALSE     TRUE
> Infinity  -Infinity TRUE      FALSE     FALSE
> Infinity  -1.0      TRUE      FALSE     FALSE
> Infinity  0.0       TRUE      FALSE     FALSE
> Infinity  1.0       TRUE      FALSE     FALSE
> Infinity  1.5       TRUE      FALSE     FALSE
> Infinity  Infinity  FALSE     TRUE      FALSE
> Infinity  NaN       FALSE     FALSE     TRUE
> NaN       -Infinity TRUE      FALSE     FALSE
> NaN       -1.0      TRUE      FALSE     FALSE
> NaN       0.0       TRUE      FALSE     FALSE
> NaN       1.0       TRUE      FALSE     FALSE
> NaN       1.5       TRUE      FALSE     FALSE
> NaN       Infinity  TRUE      FALSE     FALSE
> NaN       NaN       FALSE     TRUE      FALSE
> rows (ordered): 49

SELECT D, CAST(D AS DOUBLE PRECISION) D1, CAST(D AS DECFLOAT) D2 FROM TEST ORDER BY D;
> D         D1        D2
> --------- --------- ---------
> -Infinity -Infinity -Infinity
> -1.0      -1.0      -1
> 0.0       0.0       0
> 1.0       1.0       1
> 1.5       1.5       1.5
> Infinity  Infinity  Infinity
> NaN       NaN       NaN
> rows (ordered): 7

EXPLAIN SELECT CAST('Infinity' AS REAL), CAST('-Infinity' AS REAL), CAST('NaN' AS REAL), CAST(0 AS REAL);
>> SELECT CAST('Infinity' AS REAL), CAST('-Infinity' AS REAL), CAST('NaN' AS REAL), CAST(0.0 AS REAL)

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION TABLE TEST;
> SCRIPT
> -----------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "D" REAL );
> -- 7 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST" VALUES ('-Infinity'), (-1.0), (0.0), (1.0), (1.5), ('Infinity'), ('NaN');
> rows (ordered): 4

DROP TABLE TEST;
> ok
