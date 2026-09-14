-- Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST (ID INT PRIMARY KEY, CATEGORY INT, "VALUE" INT);
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
    ROUND(PERCENT_RANK() OVER (), 2) PR,
    ROUND(CUME_DIST() OVER (), 2) CD,
    ROW_NUMBER() OVER (ORDER BY ID) RNO,
    RANK() OVER (ORDER BY ID) RKO,
    DENSE_RANK() OVER (ORDER BY ID) DRO,
    ROUND(PERCENT_RANK() OVER (ORDER BY ID), 2) PRO,
    ROUND(CUME_DIST() OVER (ORDER BY ID), 2) CDO
    FROM TEST;
> ID CATEGORY VALUE RN PR  CD  RNO RKO DRO PRO  CDO
> -- -------- ----- -- --- --- --- --- --- ---- ----
> 1  1        11    1  0.0 1.0 1   1   1   0.0  0.11
> 2  1        12    2  0.0 1.0 2   2   2   0.13 0.22
> 3  1        13    3  0.0 1.0 3   3   3   0.25 0.33
> 4  2        21    4  0.0 1.0 4   4   4   0.38 0.44
> 5  2        22    5  0.0 1.0 5   5   5   0.5  0.56
> 6  3        31    6  0.0 1.0 6   6   6   0.63 0.67
> 7  3        32    7  0.0 1.0 7   7   7   0.75 0.78
> 8  3        33    8  0.0 1.0 8   8   8   0.88 0.89
> 9  4        41    9  0.0 1.0 9   9   9   1.0  1.0
> rows: 9

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
> rows: 9

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
> rows: 9

SELECT *,
    ROW_NUMBER() OVER W RN,
    RANK() OVER W RK,
    DENSE_RANK() OVER W DR,
    ROUND(PERCENT_RANK() OVER W, 2) PR,
    ROUND(CUME_DIST() OVER W, 2) CD
    FROM TEST WINDOW W AS (PARTITION BY CATEGORY ORDER BY ID) QUALIFY ROW_NUMBER() OVER W = 2;
> ID CATEGORY VALUE RN RK DR PR  CD
> -- -------- ----- -- -- -- --- ----
> 2  1        12    2  2  2  0.5 0.67
> 5  2        22    2  2  2  1.0 1.0
> 7  3        32    2  2  2  0.5 0.67
> rows: 3

SELECT *,
    ROW_NUMBER() OVER (PARTITION BY CATEGORY ORDER BY ID) RN,
    RANK() OVER (PARTITION BY CATEGORY ORDER BY ID) RK,
    DENSE_RANK() OVER (PARTITION BY CATEGORY ORDER BY ID) DR,
    ROUND(PERCENT_RANK() OVER (PARTITION BY CATEGORY ORDER BY ID), 2) PR,
    ROUND(CUME_DIST() OVER (PARTITION BY CATEGORY ORDER BY ID), 2) CD
    FROM TEST QUALIFY RN = 3;
> ID CATEGORY VALUE RN RK DR PR  CD
> -- -------- ----- -- -- -- --- ---
> 3  1        13    3  3  3  1.0 1.0
> 8  3        33    3  3  3  1.0 1.0
> rows: 2

SELECT
    ROW_NUMBER() OVER (ORDER BY CATEGORY) RN,
    RANK() OVER (ORDER BY CATEGORY) RK,
    DENSE_RANK() OVER (ORDER BY CATEGORY) DR,
    PERCENT_RANK() OVER () PR,
    CUME_DIST() OVER () CD,
    CATEGORY C
    FROM TEST GROUP BY CATEGORY ORDER BY RN;
> RN RK DR PR  CD  C
> -- -- -- --- --- -
> 1  1  1  0.0 1.0 1
> 2  2  2  0.0 1.0 2
> 3  3  3  0.0 1.0 3
> 4  4  4  0.0 1.0 4
> rows (ordered): 4

SELECT RANK() OVER () FROM TEST;
> exception SYNTAX_ERROR_2

SELECT DENSE_RANK() OVER () FROM TEST;
> exception SYNTAX_ERROR_2

SELECT ROW_NUMBER() OVER (ORDER BY ID RANGE CURRENT ROW) FROM TEST;
> exception SYNTAX_ERROR_1

SELECT RANK() OVER (ORDER BY ID RANGE CURRENT ROW) FROM TEST;
> exception SYNTAX_ERROR_1

SELECT DENSE_RANK() OVER (ORDER BY ID RANGE CURRENT ROW) FROM TEST;
> exception SYNTAX_ERROR_1

SELECT PERCENT_RANK() OVER (ORDER BY ID RANGE CURRENT ROW) FROM TEST;
> exception SYNTAX_ERROR_1

SELECT CUME_DIST() OVER (ORDER BY ID RANGE CURRENT ROW) FROM TEST;
> exception SYNTAX_ERROR_1

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

SELECT ROW_NUMBER() OVER (ORDER BY TYPE) RN, TYPE, SUM(CNT) SUM FROM TEST GROUP BY TYPE;
> RN TYPE SUM
> -- ---- ---
> 1  a    1
> 2  b    10
> 3  c    4
> rows: 3

SELECT A, B, C, ROW_NUMBER() OVER (PARTITION BY A, B) N FROM
    VALUES (1, 1, 1), (1, 1, 2), (1, 2, 3), (2, 1, 4) T(A, B, C);
> A B C N
> - - - -
> 1 1 1 1
> 1 1 2 2
> 1 2 3 1
> 2 1 4 1
> rows: 4

SELECT RANK () OVER () FROM TEST;
> exception SYNTAX_ERROR_2

SELECT DENSE_RANK () OVER () FROM TEST;
> exception SYNTAX_ERROR_2

DROP TABLE TEST;
> ok

SELECT ROW_NUMBER() OVER () FROM VALUES (1);
> ROW_NUMBER() OVER ()
> --------------------
> 1
> rows: 1

CREATE TABLE TEST(X INT) AS VALUES 1, 2, 3;
> ok

EXPLAIN SELECT ROW_NUMBER() OVER (ORDER BY 'a') FROM TEST;
>> SELECT ROW_NUMBER() OVER () FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT RANK() OVER (ORDER BY 'a') FROM TEST;
>> SELECT CAST(1 AS BIGINT) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

SELECT RANK() OVER (ORDER BY 'a') FROM TEST;
> 1
> -
> 1
> 1
> 1
> rows: 3

EXPLAIN SELECT DENSE_RANK() OVER (ORDER BY 'a') FROM TEST;
>> SELECT CAST(1 AS BIGINT) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

SELECT DENSE_RANK() OVER (ORDER BY 'a') FROM TEST;
> 1
> -
> 1
> 1
> 1
> rows: 3

EXPLAIN SELECT PERCENT_RANK() OVER (ORDER BY 'a') FROM TEST;
>> SELECT CAST(0.0 AS DOUBLE PRECISION) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

SELECT PERCENT_RANK() OVER (ORDER BY 'a') FROM TEST;
> 0.0
> ---
> 0.0
> 0.0
> 0.0
> rows: 3

EXPLAIN SELECT CUME_DIST() OVER (ORDER BY 'a') FROM TEST;
>> SELECT CAST(1.0 AS DOUBLE PRECISION) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

SELECT CUME_DIST() OVER (ORDER BY 'a') FROM TEST;
> 1.0
> ---
> 1.0
> 1.0
> 1.0
> rows: 3

DROP TABLE TEST;
> ok

-- Issue #4366: QUALIFY must be preserved when a derived table / CTE / view is
-- queried with an outer WHERE (global condition pushdown).
CREATE TABLE QUALIFY_TEST (
    ID INT,
    RAIL INT,
    ATTRIBUTE DECIMAL(4, 2)
);
> ok

INSERT INTO QUALIFY_TEST(ID, RAIL, ATTRIBUTE) VALUES
    (1, 1, 1.11),
    (1, 2, 2.22),
    (1, 3, 3.33),
    (2, 1, 4.44),
    (2, 2, 5.55);
> update count: 5

SELECT * FROM QUALIFY_TEST
    QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY RAIL DESC)
    ORDER BY ID;
> ID RAIL ATTRIBUTE
> -- ---- ---------
> 1  3    3.33
> 2  2    5.55
> rows (ordered): 2

-- Derived table with outer WHERE (global condition pushdown)
SELECT * FROM (
    SELECT * FROM QUALIFY_TEST
        QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY RAIL DESC)
) T WHERE T.ID = 1;
> ID RAIL ATTRIBUTE
> -- ---- ---------
> 1  3    3.33
> rows: 1

-- CTE with outer WHERE
WITH TEMP AS (
    SELECT * FROM QUALIFY_TEST
        QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY RAIL DESC)
)
SELECT * FROM TEMP WHERE TEMP.ID = 1;
> ID RAIL ATTRIBUTE
> -- ---- ---------
> 1  3    3.33
> rows: 1

-- VIEW with outer WHERE
CREATE VIEW QUALIFY_VIEW AS
    SELECT * FROM QUALIFY_TEST
        QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY RAIL DESC);
> ok

SELECT * FROM QUALIFY_VIEW WHERE ID = 1;
> ID RAIL ATTRIBUTE
> -- ---- ---------
> 1  3    3.33
> rows: 1

SELECT * FROM QUALIFY_VIEW WHERE ID = 2;
> ID RAIL ATTRIBUTE
> -- ---- ---------
> 2  2    5.55
> rows: 1

-- Range condition pushdown (multiple global conditions)
SELECT * FROM (
    SELECT * FROM QUALIFY_TEST
        QUALIFY 1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY RAIL DESC)
) T WHERE T.ID >= 1 AND T.ID <= 1;
> ID RAIL ATTRIBUTE
> -- ---- ---------
> 1  3    3.33
> rows: 1

DROP VIEW QUALIFY_VIEW;
> ok

DROP TABLE QUALIFY_TEST;
> ok
