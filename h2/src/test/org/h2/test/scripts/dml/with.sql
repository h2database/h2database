-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create table folder(id int primary key, name varchar(255), parent int);
> ok

insert into folder values(1, null, null), (2, 'bin', 1), (3, 'docs', 1), (4, 'html', 3), (5, 'javadoc', 3), (6, 'ext', 1), (7, 'service', 1), (8, 'src', 1), (9, 'docsrc', 8), (10, 'installer', 8), (11, 'main', 8), (12, 'META-INF', 11), (13, 'org', 11), (14, 'h2', 13), (15, 'test', 8), (16, 'tools', 8);
> update count: 16

with recursive link(id, name, level) as (select id, name, 0 from folder where parent is null union all select folder.id, ifnull(link.name || '/', '') || folder.name, level + 1 from link inner join folder on link.id = folder.parent) select name from link where name is not null order by cast(id as int);
> NAME
> -----------------
> bin
> docs
> docs/html
> docs/javadoc
> ext
> service
> src
> src/docsrc
> src/installer
> src/main
> src/main/META-INF
> src/main/org
> src/main/org/h2
> src/test
> src/tools
> rows (ordered): 15

drop table folder;
> ok

explain with recursive r(n) as (
    (select 1) union all (select n+1 from r where n < 3)
)
select n from r;
>> WITH RECURSIVE "R"("N") AS ( (SELECT 1) UNION ALL (SELECT "N" + 1 FROM "R" WHERE "N" < 3) ) SELECT "N" FROM "R" "R" /* null */

explain with recursive "r"(n) as (
    (select 1) union all (select n+1 from "r" where n < 3)
)
select n from "r";
>> WITH RECURSIVE "r"("N") AS ( (SELECT 1) UNION ALL (SELECT "N" + 1 FROM "r" WHERE "N" < 3) ) SELECT "N" FROM "r" "r" /* null */

select sum(n) from (
    with recursive r(n) as (
        (select 1) union all (select n+1 from r where n < 3)
    )
    select n from r
);
>> 6

select sum(n) from (
    with recursive "r"(n) as (
        (select 1) union all (select n+1 from "r" where n < 3)
    )
    select n from "r"
);
>> 6

select sum(n) from (select 0) join (
    with recursive r(n) as (
        (select 1) union all (select n+1 from r where n < 3)
    )
    select n from r
) on 1=1;
>> 6

select 0 from (
    select 0 where 0 in (
        with recursive r(n) as (
            (select 1) union all (select n+1 from r where n < 3)
        )
        select n from r
    )
);
> 0
> -
> rows: 0

with recursive
    r0(n,k) as (select -1, 0),
    r1(n,k) as ((select 1, 0) union all (select n+1,k+1 from r1 where n <= 3)),
    r2(n,k) as ((select 10,0) union all (select n+1,k+1 from r2 where n <= 13))
    select r1.k, r0.n as N0, r1.n AS N1, r2.n AS n2 from r0 inner join r1 ON r1.k= r0.k inner join r2 ON r1.k= r2.k;
> K N0 N1 N2
> - -- -- --
> 0 -1 1  10
> rows: 1

CREATE SCHEMA SCH;
> ok

WITH RECURSIVE R1 AS (
(SELECT 1)
UNION ALL
(SELECT (N + 1) FROM R1 WHERE N < 3))
TABLE R1;
> exception SYNTAX_ERROR_2

WITH R1(N) AS (
(SELECT 1)
UNION ALL
(SELECT (N + 1) FROM R1 WHERE N < 3))
TABLE R1;
> exception TABLE_OR_VIEW_NOT_FOUND_DATABASE_EMPTY_1

WITH RECURSIVE R1(A) AS (SELECT 1)
SELECT A FROM R1 WHERE A IN (WITH RECURSIVE R2(B) AS (SELECT 1) TABLE R2);
>> 1

WITH RECURSIVE R1(A) AS (WITH RECURSIVE R2(B) AS (SELECT 1) TABLE R2)
TABLE R1;
> exception SYNTAX_ERROR_2

CREATE VIEW SCH.R2(N) AS
WITH RECURSIVE R1(N) AS (
(SELECT 1)
UNION ALL
(SELECT (N + 1) FROM R1 WHERE N < 3))
TABLE R1;
> ok

SELECT * FROM SCH.R2;
> N
> -
> 1
> 2
> 3
> rows: 3

WITH CTE_TEST AS (SELECT 1, 2) SELECT * FROM CTE_TEST;
> 1 2
> - -
> 1 2
> rows: 1

WITH CTE_TEST AS (SELECT 1, 2) (SELECT * FROM CTE_TEST);
> 1 2
> - -
> 1 2
> rows: 1

WITH CTE_TEST AS (SELECT 1, 2) ((SELECT * FROM CTE_TEST));
> 1 2
> - -
> 1 2
> rows: 1

CREATE TABLE TEST(A INT, B INT) AS SELECT 1, 2;
> ok

WITH CTE_TEST AS (TABLE TEST) ((SELECT * FROM CTE_TEST));
> A B
> - -
> 1 2
> rows: 1

WITH CTE_TEST AS (TABLE TEST) ((TABLE CTE_TEST));
> A B
> - -
> 1 2
> rows: 1

WITH CTE_TEST AS (VALUES (1, 2)) ((SELECT * FROM CTE_TEST));
> C1 C2
> -- --
> 1  2
> rows: 1

WITH CTE_TEST AS (TABLE TEST) ((SELECT A, B FROM CTE_TEST2));
> exception TABLE_OR_VIEW_NOT_FOUND_1

WITH CTE_TEST AS (TABLE TEST) ((SELECT A, B, C FROM CTE_TEST));
> exception COLUMN_NOT_FOUND_1

DROP TABLE TEST;
> ok

WITH RECURSIVE V(V1, V2) AS (
    SELECT 0 V1, 1 V2
    UNION ALL
    SELECT V1 + 1, V2 + 1 FROM V WHERE V2 < 4
)
SELECT V1, V2, COUNT(*) FROM V
LEFT JOIN (SELECT T1 / T2 R FROM (VALUES (10, 0)) T(T1, T2) WHERE T2*T2*T2*T2*T2*T2 <> 0) X ON X.R > V.V1 AND X.R < V.V2
GROUP BY V1, V2;
> V1 V2 COUNT(*)
> -- -- --------
> 0  1  1
> 1  2  1
> 2  3  1
> 3  4  1
> rows: 4

EXPLAIN WITH RECURSIVE V(V1, V2) AS (
    SELECT 0 V1, 1 V2
    UNION ALL
    SELECT V1 + 1, V2 + 1 FROM V WHERE V2 < 10
)
SELECT V1, V2, COUNT(*) FROM V
LEFT JOIN (SELECT T1 / T2 R FROM (VALUES (10, 0)) T(T1, T2) WHERE T2*T2*T2*T2*T2*T2 <> 0) X ON X.R > V.V1 AND X.R < V.V2
GROUP BY V1, V2;
>> WITH RECURSIVE "V"("V1", "V2") AS ( (SELECT 0 AS "V1", 1 AS "V2") UNION ALL (SELECT "V1" + 1, "V2" + 1 FROM "V" WHERE "V2" < 10) ) SELECT "V1", "V2", COUNT(*) FROM "V" "V" /* null */ LEFT OUTER JOIN ( SELECT "T1" / "T2" AS "R" FROM (VALUES (10, 0)) "T"("T1", "T2") WHERE ((((("T2" * "T2") * "T2") * "T2") * "T2") * "T2") <> 0 ) "X" /* SELECT T1 / T2 AS R FROM (VALUES (10, 0)) T(T1, T2) /* table scan */ WHERE ((((((T2 * T2) * T2) * T2) * T2) * T2) <> 0) _LOCAL_AND_GLOBAL_ (((T1 / T2) >= ?1) AND ((T1 / T2) <= ?2)): R > V.V1 AND R < V.V2 */ ON ("X"."R" > "V"."V1") AND ("X"."R" < "V"."V2") GROUP BY "V1", "V2"

-- Data change delta tables in WITH
CREATE TABLE TEST("VALUE" INT NOT NULL PRIMARY KEY);
> ok

WITH W AS (SELECT NULL FROM FINAL TABLE (INSERT INTO TEST VALUES 1, 2))
SELECT COUNT (*) FROM W;
>> 2

WITH W AS (SELECT NULL FROM FINAL TABLE (UPDATE TEST SET "VALUE" = 3 WHERE "VALUE" = 2))
SELECT COUNT (*) FROM W;
>> 1

WITH W AS (SELECT NULL FROM FINAL TABLE (MERGE INTO TEST VALUES 4, 5))
SELECT COUNT (*) FROM W;
>> 2

WITH W AS (SELECT NULL FROM OLD TABLE (DELETE FROM TEST WHERE "VALUE" = 4))
SELECT COUNT (*) FROM W;
>> 1

SET MODE MySQL;
> ok

WITH W AS (SELECT NULL FROM FINAL TABLE (REPLACE INTO TEST VALUES 4, 5))
SELECT COUNT (*) FROM W;
>> 2

SET MODE Regular;
> ok

DROP TABLE TEST;
> ok

CREATE TABLE T(C INT);
> ok

INSERT INTO T WITH W(C) AS (VALUES 1) SELECT C FROM W;
> update count: 1

TABLE W;
> exception TABLE_OR_VIEW_NOT_FOUND_1

TABLE T;
>> 1

DROP TABLE T;
> ok

WITH T(X) AS (SELECT 1)
(SELECT 2 Y) UNION (SELECT 3 Z) UNION (SELECT * FROM T);
> Y
> -
> 1
> 2
> 3
> rows: 3

WITH T1(F1, F2) AS (SELECT 1, 2)
SELECT A1.F1, A1.F2 FROM (SELECT * FROM T1) A1;
> F1 F2
> -- --
> 1  2
> rows: 1

CREATE VIEW V AS
WITH A AS (SELECT) TABLE A;
> ok

TABLE V;
>
>
>
> rows: 1

DROP VIEW V;
> ok

WITH
    Q1(X) AS (VALUES 1),
    Q2 AS (
        WITH
            Q1(Y) AS (VALUES 2)
        TABLE Q1
    )
SELECT Q1.X, Q2.Y FROM Q1, Q2;
> X Y
> - -
> 1 2
> rows: 1

WITH
    Q1(X) AS (
        WITH Q1(Y) AS (VALUES 1)
        SELECT Q1.Y FROM Q1
    )
SELECT Q1.X FROM Q1;
>> 1

WITH
    Q1(X) AS (VALUES 1),
    Q1(X) AS (VALUES 2)
TABLE Q1;
> exception TABLE_OR_VIEW_ALREADY_EXISTS_1
