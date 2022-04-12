-- Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(ID INT PRIMARY KEY, GR INT, A INT, B INT, C INT) AS VALUES
    (1, 1, NULL, NULL, NULL),
    (2, 1, NULL, NULL, NULL),
    (3, 1, NULL, 1, 1),
    (4, 1, NULL, 1, 1),
    (5, 1, 1, 1, 1),
    (6, 1, 1, 1, 2),
    (7, 2, 1, 2, 1);
> ok

SELECT UNIQUE(SELECT A, B FROM TEST);
>> FALSE

SELECT UNIQUE(TABLE TEST);
>> TRUE

SELECT UNIQUE(SELECT A, B, C FROM TEST);
>> TRUE

EXPLAIN SELECT UNIQUE(SELECT A, B FROM TEST);
>> SELECT UNIQUE( SELECT "A", "B" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */)

SELECT UNIQUE(SELECT A, B FROM TEST);
>> FALSE

EXPLAIN SELECT UNIQUE(SELECT DISTINCT A, B FROM TEST);
>> SELECT TRUE

SELECT UNIQUE(SELECT DISTINCT A, B FROM TEST);
>> TRUE

SELECT G, UNIQUE(SELECT A, B, C FROM TEST WHERE GR = G) FROM (VALUES 1, 2, 3) V(G);
> G UNIQUE( SELECT A, B, C FROM PUBLIC.TEST WHERE GR = G)
> - -----------------------------------------------------
> 1 TRUE
> 2 TRUE
> 3 TRUE
> rows: 3

SELECT G, UNIQUE(SELECT A, B FROM TEST WHERE GR = G ORDER BY A + B) FROM (VALUES 1, 2, 3) V(G);
> G UNIQUE( SELECT A, B FROM PUBLIC.TEST WHERE GR = G ORDER BY A + B)
> - -----------------------------------------------------------------
> 1 FALSE
> 2 TRUE
> 3 TRUE
> rows: 3

DROP TABLE TEST;
> ok
