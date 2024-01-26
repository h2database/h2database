-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- GROUP BY column index for MySQL/MariaDB/PostgreSQL compatibility mode

CREATE TABLE MYTAB(X INT , Y INT, Z INT) AS VALUES (1,123,2), (1,456,2), (3,789,4);
> ok

SET MODE MySQL;
> ok

SELECT SUM(Y) AS S , X + Z FROM MYTAB GROUP BY 2;
> S   X + Z
> --- -----
> 579 3
> 789 7
> rows: 2

EXPLAIN SELECT SUM(Y) AS S , X + Z FROM MYTAB GROUP BY 2;
> PLAN
> -------------------------------------------------------------------------------------------------------
> SELECT SUM("Y") AS "S", "X" + "Z" FROM "PUBLIC"."MYTAB" /* PUBLIC.MYTAB.tableScan */ GROUP BY "X" + "Z"
> rows: 1

SELECT SUM(Y) AS S , X + Z FROM MYTAB GROUP BY 3;
> exception GROUP_BY_NOT_IN_THE_RESULT

SELECT MYTAB.*,  SUM(Y) AS S  FROM MYTAB GROUP BY 1;
> exception SYNTAX_ERROR_2

SET MODE MariaDB;
> ok

SELECT SUM(Y) AS S , X + Z FROM MYTAB GROUP BY 2;
> S   X + Z
> --- -----
> 579 3
> 789 7
> rows: 2

SET MODE PostgreSQL;
> ok

SELECT SUM(Y) AS S , X + Z FROM MYTAB GROUP BY 2;
> S   ?column?
> --- --------
> 579 3
> 789 7
> rows: 2

SET MODE Oracle;
> ok

SELECT SUM(Y) AS S , X FROM MYTAB GROUP BY 2;
> exception MUST_GROUP_BY_COLUMN_1
