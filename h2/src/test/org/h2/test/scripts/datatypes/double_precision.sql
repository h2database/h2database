-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE MEMORY TABLE TEST(D1 DOUBLE, D2 DOUBLE PRECISION, D3 FLOAT, D4 FLOAT(25), D5 FLOAT(53));
> ok

ALTER TABLE TEST ADD COLUMN D6 FLOAT(54);
> exception INVALID_VALUE_PRECISION

SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE,
    DECLARED_DATA_TYPE, DECLARED_NUMERIC_PRECISION, DECLARED_NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE        NUMERIC_PRECISION NUMERIC_PRECISION_RADIX NUMERIC_SCALE DECLARED_DATA_TYPE DECLARED_NUMERIC_PRECISION DECLARED_NUMERIC_SCALE
> ----------- ---------------- ----------------- ----------------------- ------------- ------------------ -------------------------- ----------------------
> D1          DOUBLE PRECISION 53                2                       null          DOUBLE PRECISION   null                       null
> D2          DOUBLE PRECISION 53                2                       null          DOUBLE PRECISION   null                       null
> D3          DOUBLE PRECISION 53                2                       null          FLOAT              null                       null
> D4          DOUBLE PRECISION 53                2                       null          FLOAT              25                         null
> D5          DOUBLE PRECISION 53                2                       null          FLOAT              53                         null
> rows (ordered): 5

SCRIPT NODATA NOPASSWORDS NOSETTINGS NOVERSION TABLE TEST;
> SCRIPT
> --------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "D1" DOUBLE PRECISION, "D2" DOUBLE PRECISION, "D3" FLOAT, "D4" FLOAT(25), "D5" FLOAT(53) );
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> rows (ordered): 3

DROP TABLE TEST;
> ok

EXPLAIN VALUES CAST(0 AS DOUBLE);
>> VALUES (CAST(0.0 AS DOUBLE PRECISION))

CREATE MEMORY TABLE TEST(D DOUBLE PRECISION) AS VALUES '-Infinity', '-1', '0', '1', '1.5', 'Infinity', 'NaN';
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
> D         D         A.D / B.D           MOD(A.D, B.D)
> --------- --------- ------------------- -------------
> -Infinity -Infinity NaN                 NaN
> -Infinity -1.0      Infinity            NaN
> -Infinity 1.0       -Infinity           NaN
> -Infinity 1.5       -Infinity           NaN
> -Infinity Infinity  NaN                 NaN
> -Infinity NaN       NaN                 NaN
> -1.0      -Infinity 0.0                 -1.0
> -1.0      -1.0      1.0                 0.0
> -1.0      1.0       -1.0                0.0
> -1.0      1.5       -0.6666666666666666 -1.0
> -1.0      Infinity  0.0                 -1.0
> -1.0      NaN       NaN                 NaN
> 0.0       -Infinity 0.0                 0.0
> 0.0       -1.0      0.0                 0.0
> 0.0       1.0       0.0                 0.0
> 0.0       1.5       0.0                 0.0
> 0.0       Infinity  0.0                 0.0
> 0.0       NaN       NaN                 NaN
> 1.0       -Infinity 0.0                 1.0
> 1.0       -1.0      -1.0                0.0
> 1.0       1.0       1.0                 0.0
> 1.0       1.5       0.6666666666666666  1.0
> 1.0       Infinity  0.0                 1.0
> 1.0       NaN       NaN                 NaN
> 1.5       -Infinity 0.0                 1.5
> 1.5       -1.0      -1.5                0.5
> 1.5       1.0       1.5                 0.5
> 1.5       1.5       1.0                 0.0
> 1.5       Infinity  0.0                 1.5
> 1.5       NaN       NaN                 NaN
> Infinity  -Infinity NaN                 NaN
> Infinity  -1.0      -Infinity           NaN
> Infinity  1.0       Infinity            NaN
> Infinity  1.5       Infinity            NaN
> Infinity  Infinity  NaN                 NaN
> Infinity  NaN       NaN                 NaN
> NaN       -Infinity NaN                 NaN
> NaN       -1.0      NaN                 NaN
> NaN       1.0       NaN                 NaN
> NaN       1.5       NaN                 NaN
> NaN       Infinity  NaN                 NaN
> NaN       NaN       NaN                 NaN
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

SELECT D, CAST(D AS REAL) D1, CAST(D AS DECFLOAT) D2 FROM TEST ORDER BY D;
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

EXPLAIN SELECT CAST('Infinity' AS DOUBLE PRECISION), CAST('-Infinity' AS DOUBLE PRECISION), CAST('NaN' AS DOUBLE PRECISION), CAST(0 AS DOUBLE PRECISION);
>> SELECT CAST('Infinity' AS DOUBLE PRECISION), CAST('-Infinity' AS DOUBLE PRECISION), CAST('NaN' AS DOUBLE PRECISION), CAST(0.0 AS DOUBLE PRECISION)

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION TABLE TEST;
> SCRIPT
> -----------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "D" DOUBLE PRECISION );
> -- 7 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST" VALUES ('-Infinity'), (-1.0), (0.0), (1.0), (1.5), ('Infinity'), ('NaN');
> rows (ordered): 4

DROP TABLE TEST;
> ok

SELECT CAST(PI() AS DOUBLE PRECISION) / 1e0;
>> 3.141592653589793

SELECT 'NaN' = CAST('NaN' AS DOUBLE);
>> TRUE
