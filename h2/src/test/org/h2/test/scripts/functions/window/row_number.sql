-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST (ID INT PRIMARY KEY, CATEGORY INT, VALUE INT);
> ok

INSERT INTO TEST VALUES
    (1, 1, 11),
    (2, 1, 12),
    (3, 1, 13),
    (4, 2, 21),
    (5, 2, 22),
    (6, 3, 31),
    (7, 3, 32),
    (8, 3, 33),
    (9, 4, 41);
> update count: 9

SELECT *,
    ROW_NUMBER() OVER () RN,
    RANK() OVER () RK,
    DENSE_RANK() OVER () DR,
    ROUND(PERCENT_RANK() OVER (), 2) PR,
    ROUND(CUME_DIST() OVER (), 2) CD,

    ROW_NUMBER() OVER (ORDER BY ID) RNO,
    RANK() OVER (ORDER BY ID) RKO,
    DENSE_RANK() OVER (ORDER BY ID) DRO,
    ROUND(PERCENT_RANK() OVER (ORDER BY ID), 2) PRO,
    ROUND(CUME_DIST() OVER (ORDER BY ID), 2) CDO

    FROM TEST;
> ID CATEGORY VALUE RN RK DR PR  CD  RNO RKO DRO PRO  CDO
> -- -------- ----- -- -- -- --- --- --- --- --- ---- ----
> 1  1        11    1  1  1  0.0 1.0 1   1   1   0.0  0.11
> 2  1        12    2  1  1  0.0 1.0 2   2   2   0.13 0.22
> 3  1        13    3  1  1  0.0 1.0 3   3   3   0.25 0.33
> 4  2        21    4  1  1  0.0 1.0 4   4   4   0.38 0.44
> 5  2        22    5  1  1  0.0 1.0 5   5   5   0.5  0.56
> 6  3        31    6  1  1  0.0 1.0 6   6   6   0.63 0.67
> 7  3        32    7  1  1  0.0 1.0 7   7   7   0.75 0.78
> 8  3        33    8  1  1  0.0 1.0 8   8   8   0.88 0.89
> 9  4        41    9  1  1  0.0 1.0 9   9   9   1.0  1.0
> rows (ordered): 9

SELECT *,
    ROW_NUMBER() OVER (ORDER BY CATEGORY) RN,
    RANK() OVER (ORDER BY CATEGORY) RK,
    DENSE_RANK() OVER (ORDER BY CATEGORY) DR,
    ROUND(PERCENT_RANK() OVER (ORDER BY CATEGORY), 2) PR,
    ROUND(CUME_DIST() OVER (ORDER BY CATEGORY), 2) CD
    FROM TEST;
> ID CATEGORY VALUE RN RK DR PR   CD
> -- -------- ----- -- -- -- ---- ----
> 1  1        11    1  1  1  0.0  0.33
> 2  1        12    2  1  1  0.0  0.33
> 3  1        13    3  1  1  0.0  0.33
> 4  2        21    4  4  2  0.38 0.56
> 5  2        22    5  4  2  0.38 0.56
> 6  3        31    6  6  3  0.63 0.89
> 7  3        32    7  6  3  0.63 0.89
> 8  3        33    8  6  3  0.63 0.89
> 9  4        41    9  9  4  1.0  1.0
> rows (ordered): 9

SELECT *,
    ROW_NUMBER() OVER (PARTITION BY CATEGORY ORDER BY ID) RN,
    RANK() OVER (PARTITION BY CATEGORY ORDER BY ID) RK,
    DENSE_RANK() OVER (PARTITION BY CATEGORY ORDER BY ID) DR,
    ROUND(PERCENT_RANK() OVER (PARTITION BY CATEGORY ORDER BY ID), 2) PR,
    ROUND(CUME_DIST() OVER (PARTITION BY CATEGORY ORDER BY ID), 2) CD
    FROM TEST;
> ID CATEGORY VALUE RN RK DR PR  CD
> -- -------- ----- -- -- -- --- ----
> 1  1        11    1  1  1  0.0 0.33
> 2  1        12    2  2  2  0.5 0.67
> 3  1        13    3  3  3  1.0 1.0
> 4  2        21    1  1  1  0.0 0.5
> 5  2        22    2  2  2  1.0 1.0
> 6  3        31    1  1  1  0.0 0.33
> 7  3        32    2  2  2  0.5 0.67
> 8  3        33    3  3  3  1.0 1.0
> 9  4        41    1  1  1  0.0 1.0
> rows (ordered): 9

SELECT
    ROW_NUMBER() OVER () RN,
    RANK() OVER () RK,
    DENSE_RANK() OVER () DR,
    PERCENT_RANK() OVER () PR,
    CUME_DIST() OVER () CD
    FROM TEST GROUP BY CATEGORY;
> RN RK DR PR  CD
> -- -- -- --- ---
> 1  1  1  0.0 1.0
> 2  1  1  0.0 1.0
> 3  1  1  0.0 1.0
> 4  1  1  0.0 1.0
> rows: 4

DROP TABLE TEST;
> ok

CREATE TABLE TEST (ID INT PRIMARY KEY, TYPE VARCHAR, CNT INT);
> ok

INSERT INTO TEST VALUES
    (1, 'a', 1),
    (2, 'b', 2),
    (3, 'c', 4),
    (4, 'b', 8);
> update count: 4

SELECT ROW_NUMBER() OVER(ORDER /**/ BY TYPE) RN, TYPE, SUM(CNT) SUM FROM TEST GROUP BY TYPE;
> RN TYPE SUM
> -- ---- ---
> 1  a    1
> 2  b    10
> 3  c    4
> rows: 3

DROP TABLE TEST;
> ok
