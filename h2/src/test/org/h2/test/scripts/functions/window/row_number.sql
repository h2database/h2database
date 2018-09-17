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
    (8, 3, 33);
> update count: 8

SELECT *,
    ROW_NUMBER() OVER () RN, RANK() OVER () RK, DENSE_RANK() OVER () DR,
    ROW_NUMBER() OVER (ORDER BY ID) RNO, RANK() OVER (ORDER BY ID) RKO, DENSE_RANK() OVER (ORDER BY ID) DRO
    FROM TEST;
> ID CATEGORY VALUE RN RK DR RNO RKO DRO
> -- -------- ----- -- -- -- --- --- ---
> 1  1        11    1  1  1  1   1   1
> 2  1        12    2  1  1  2   2   2
> 3  1        13    3  1  1  3   3   3
> 4  2        21    4  1  1  4   4   4
> 5  2        22    5  1  1  5   5   5
> 6  3        31    6  1  1  6   6   6
> 7  3        32    7  1  1  7   7   7
> 8  3        33    8  1  1  8   8   8
> rows (ordered): 8

SELECT *,
    ROW_NUMBER() OVER (ORDER BY CATEGORY) RN, RANK() OVER (ORDER BY CATEGORY) RK, DENSE_RANK() OVER (ORDER BY CATEGORY) DR
    FROM TEST;
> ID CATEGORY VALUE RN RK DR
> -- -------- ----- -- -- --
> 1  1        11    1  1  1
> 2  1        12    2  1  1
> 3  1        13    3  1  1
> 4  2        21    4  4  2
> 5  2        22    5  4  2
> 6  3        31    6  6  3
> 7  3        32    7  6  3
> 8  3        33    8  6  3
> rows (ordered): 8

SELECT *,
    ROW_NUMBER() OVER (PARTITION BY CATEGORY ORDER BY ID) RN,
    RANK() OVER (PARTITION BY CATEGORY ORDER BY ID) RK,
    DENSE_RANK() OVER (PARTITION BY CATEGORY ORDER BY ID) DR
    FROM TEST;
> ID CATEGORY VALUE RN RK DR
> -- -------- ----- -- -- --
> 1  1        11    1  1  1
> 2  1        12    2  2  2
> 3  1        13    3  3  3
> 4  2        21    1  1  1
> 5  2        22    2  2  2
> 6  3        31    1  1  1
> 7  3        32    2  2  2
> 8  3        33    3  3  3
> rows (ordered): 8

SELECT
    ROW_NUMBER() OVER () RN, RANK() OVER () RK, DENSE_RANK() OVER () DR
    FROM TEST GROUP BY CATEGORY;
> RN RK DR
> -- -- --
> 1  1  1
> 2  1  1
> 3  1  1
> rows: 3

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

SELECT ROW_NUMBER() OVER() RN, TYPE, SUM(CNT) SUM FROM TEST GROUP BY TYPE;
> RN TYPE SUM
> -- ---- ---
> 1  a    1
> 2  b    10
> 3  c    4
> rows: 3

DROP TABLE TEST;
> ok
