-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(ID INT, R INT, CATEGORY INT);
> ok

INSERT INTO TEST VALUES
    (1, 4, 1),
    (2, 3, 1),
    (3, 2, 2),
    (4, 1, 2);
> update count: 4

SELECT *, ROW_NUMBER() OVER W FROM TEST;
> exception WINDOW_NOT_FOUND_1

SELECT * FROM TEST WINDOW W AS W1, W1 AS ();
> exception SYNTAX_ERROR_2

SELECT *, ROW_NUMBER() OVER W1, ROW_NUMBER() OVER W2 FROM TEST
    WINDOW W1 AS (W2 ORDER BY ID), W2 AS (PARTITION BY CATEGORY ORDER BY ID DESC);
> ID R CATEGORY ROW_NUMBER() OVER (PARTITION BY CATEGORY ORDER BY ID) ROW_NUMBER() OVER (PARTITION BY CATEGORY ORDER BY ID DESC)
> -- - -------- ----------------------------------------------------- ----------------------------------------------------------
> 1  4 1        1                                                     2
> 2  3 1        2                                                     1
> 3  2 2        1                                                     2
> 4  1 2        2                                                     1
> rows: 4

SELECT *, LAST_VALUE(ID) OVER W FROM TEST
    WINDOW W AS (PARTITION BY CATEGORY ORDER BY ID RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW);
> ID R CATEGORY LAST_VALUE(ID) OVER (PARTITION BY CATEGORY ORDER BY ID RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW)
> -- - -------- -------------------------------------------------------------------------------------------------------------------------------------
> 1  4 1        2
> 2  3 1        1
> 3  2 2        4
> 4  1 2        3
> rows: 4

DROP TABLE TEST;
> ok

SELECT MAX(MAX(X) OVER ()) OVER () FROM VALUES (1);
> exception INVALID_USE_OF_AGGREGATE_FUNCTION_1

SELECT MAX(MAX(X) OVER ()) FROM VALUES (1);
> exception INVALID_USE_OF_AGGREGATE_FUNCTION_1

SELECT MAX(MAX(X)) FROM VALUES (1);
> exception INVALID_USE_OF_AGGREGATE_FUNCTION_1

CREATE TABLE TEST(ID INT, CATEGORY INT);
> ok

INSERT INTO TEST VALUES
    (1, 1),
    (2, 1),
    (4, 2),
    (8, 2),
    (16, 3),
    (32, 3);
> update count: 6

SELECT ROW_NUMBER() OVER (ORDER  /**/ BY CATEGORY), SUM(ID) FROM TEST GROUP BY CATEGORY HAVING SUM(ID) = 12;
> ROW_NUMBER() OVER (ORDER BY CATEGORY) SUM(ID)
> ------------------------------------- -------
> 1                                     12
> rows: 1

SELECT ROW_NUMBER() OVER (ORDER  /**/ BY CATEGORY), SUM(ID) FROM TEST GROUP BY CATEGORY HAVING CATEGORY = 2;
> ROW_NUMBER() OVER (ORDER BY CATEGORY) SUM(ID)
> ------------------------------------- -------
> 1                                     12
> rows: 1

SELECT ROW_NUMBER() OVER (ORDER BY CATEGORY), SUM(ID) FROM TEST GROUP BY CATEGORY HAVING CATEGORY > 1;
> ROW_NUMBER() OVER (ORDER BY CATEGORY) SUM(ID)
> ------------------------------------- -------
> 1                                     12
> 2                                     48
> rows: 2

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT, CATEGORY BOOLEAN);
> ok

INSERT INTO TEST VALUES
    (1, FALSE),
    (2, FALSE),
    (4, TRUE),
    (8, TRUE),
    (16, FALSE),
    (32, FALSE);
> update count: 6

SELECT ROW_NUMBER() OVER (ORDER BY CATEGORY), SUM(ID) FROM TEST GROUP BY CATEGORY HAVING SUM(ID) = 12;
> ROW_NUMBER() OVER (ORDER BY CATEGORY) SUM(ID)
> ------------------------------------- -------
> 1                                     12
> rows: 1

SELECT ROW_NUMBER() OVER (ORDER BY CATEGORY), SUM(ID) FROM TEST GROUP BY CATEGORY HAVING CATEGORY;
> ROW_NUMBER() OVER (ORDER BY CATEGORY) SUM(ID)
> ------------------------------------- -------
> 1                                     12
> rows: 1

DROP TABLE TEST;
> ok
