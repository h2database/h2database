-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create table test(id int) as select 1;
> ok

select * from test where id in (select id from test order by 'x');
> ID
> --
> 1
> rows: 1

drop table test;
> ok

select x, x in(2, 3) i from system_range(1, 2) group by x;
> X I
> - -----
> 1 FALSE
> 2 TRUE
> rows: 2

select * from system_range(1, 1) where x = x + 1 or x in(2, 0);
> X
> -
> rows: 0

select * from system_range(1, 1) where cast('a' || x as varchar_ignorecase) in ('A1', 'B1');
> X
> -
> 1
> rows: 1

create table test(x int) as select x from system_range(1, 2);
> ok

select * from (select rownum r from test) where r in (1, 2);
> R
> -
> 1
> 2
> rows: 2

select * from (select rownum r from test) where r = 1 or r = 2;
> R
> -
> 1
> 2
> rows: 2

drop table test;
> ok

select x from system_range(1, 1) where x in (select x from system_range(1, 1) group by x order by max(x));
> X
> -
> 1
> rows: 1

create table test(id int) as (values 1, 2, 4);
> ok

select a.id, a.id in(select 4) x  from test a, test b where a.id in (b.id, b.id - 1);
> ID X
> -- -----
> 1  FALSE
> 1  FALSE
> 2  FALSE
> 4  TRUE
> rows: 4

select a.id, a.id in(select 4) x  from test a, test b where a.id in (b.id, b.id - 1) group by a.id;
> ID X
> -- -----
> 1  FALSE
> 2  FALSE
> 4  TRUE
> rows: 3

select a.id, 4 in(select a.id) x  from test a, test b where a.id in (b.id, b.id - 1) group by a.id;
> ID X
> -- -----
> 1  FALSE
> 2  FALSE
> 4  TRUE
> rows: 3

drop table test;
> ok

create table test(id int primary key, d int) as (values (1, 1), (2, 1));
> ok

select id from test where id in (1, 2) and d = 1;
> ID
> --
> 1
> 2
> rows: 2

drop table test;
> ok

create table test(id int) as (values null, 1);
> ok

select * from test where id not in (select id from test where 1=0);
> ID
> ----
> 1
> null
> rows: 2

select * from test where null not in (select id from test where 1=0);
> ID
> ----
> 1
> null
> rows: 2

select * from test where not (id in (select id from test where 1=0));
> ID
> ----
> 1
> null
> rows: 2

select * from test where not (null in (select id from test where 1=0));
> ID
> ----
> 1
> null
> rows: 2

drop table test;
> ok

create table t1 (id int primary key) as (select x from system_range(1, 1000));
> ok

create table t2 (id int primary key) as (select x from system_range(1, 1000));
> ok

explain select count(*) from t1 where t1.id in ( select t2.id from t2 );
>> SELECT COUNT(*) FROM "PUBLIC"."T1" /* PUBLIC.PRIMARY_KEY_A: ID IN(SELECT DISTINCT T2.ID FROM PUBLIC.T2 /* PUBLIC.T2.tableScan */) */ WHERE "T1"."ID" IN( SELECT DISTINCT "T2"."ID" FROM "PUBLIC"."T2" /* PUBLIC.T2.tableScan */)

select count(*) from t1 where t1.id in ( select t2.id from t2 );
> COUNT(*)
> --------
> 1000
> rows: 1

drop table t1, t2;
> ok

select count(*) from system_range(1, 2) where x in(1, 1, 1);
> COUNT(*)
> --------
> 1
> rows: 1

create table test(id int primary key) as (values 1, 2, 3);
> ok

explain select * from test where id in(1, 2, null);
>> SELECT "PUBLIC"."TEST"."ID" FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID IN(1, 2, NULL) */ WHERE "ID" IN(1, 2, NULL)

drop table test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255)) AS (VALUES (1, 'Hello'), (2, 'World'));
> ok

select * from test where id in (select id from test);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

select * from test where id in ((select id from test));
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

select * from test where id in (((select id from test)));
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

DROP TABLE TEST;
> ok

create table test(v boolean) as (values unknown, true, false);
> ok

SELECT CASE WHEN NOT (false IN (null)) THEN false END;
> NULL
> ----
> null
> rows: 1

select a.v as av, b.v as bv, a.v IN (b.v), not a.v IN (b.v) from test a, test b;
> AV    BV    A.V = B.V A.V <> B.V
> ----- ----- --------- ----------
> FALSE FALSE TRUE      FALSE
> FALSE TRUE  FALSE     TRUE
> FALSE null  null      null
> TRUE  FALSE FALSE     TRUE
> TRUE  TRUE  TRUE      FALSE
> TRUE  null  null      null
> null  FALSE null      null
> null  TRUE  null      null
> null  null  null      null
> rows: 9

select a.v as av, b.v as bv, a.v IN (b.v, null), not a.v IN (b.v, null) from test a, test b;
> AV    BV    A.V IN(B.V, NULL) A.V NOT IN(B.V, NULL)
> ----- ----- ----------------- ---------------------
> FALSE FALSE TRUE              FALSE
> FALSE TRUE  null              null
> FALSE null  null              null
> TRUE  FALSE null              null
> TRUE  TRUE  TRUE              FALSE
> TRUE  null  null              null
> null  FALSE null              null
> null  TRUE  null              null
> null  null  null              null
> rows: 9

drop table test;
> ok

SELECT CASE WHEN NOT (false IN (null)) THEN false END;
> NULL
> ----
> null
> rows: 1

create table test(a int, b int) as select 2, 0;
> ok

create index idx on test(b, a);
> ok

select count(*) from test where a in(2, 10) and b in(0, null);
>> 1

drop table test;
> ok

create table test(a int, b int) as select 1, 0;
> ok

create index idx on test(b, a);
> ok

select count(*) from test where b in(null, 0) and a in(1, null);
>> 1

drop table test;
> ok

create table test(a int, b int, unique(a, b));
> ok

insert into test values(1,1), (1,2);
> update count: 2

select count(*) from test where a in(1,2) and b in(1,2);
>> 2

drop table test;
> ok

SELECT * FROM SYSTEM_RANGE(1, 10) WHERE X IN ((SELECT 1), (SELECT 2));
> X
> -
> 1
> 2
> rows: 2

EXPLAIN SELECT * FROM SYSTEM_RANGE(1, 10) WHERE X IN ((SELECT X FROM SYSTEM_RANGE(1, 1)), (SELECT X FROM SYSTEM_RANGE(2, 2)));
>> SELECT "SYSTEM_RANGE"."X" FROM SYSTEM_RANGE(1, 10) /* range index: X IN((SELECT X FROM SYSTEM_RANGE(1, 1) /* range index */), (SELECT X FROM SYSTEM_RANGE(2, 2) /* range index */)) */ WHERE "X" IN((SELECT "X" FROM SYSTEM_RANGE(1, 1) /* range index */), (SELECT "X" FROM SYSTEM_RANGE(2, 2) /* range index */))

-- Tests for IN predicate with an empty list

SELECT 1 WHERE 1 IN ();
> 1
> -
> rows: 0

SELECT 1 WHERE 1 NOT IN ();
>> 1

SELECT CASE 1 WHEN IN() THEN 1 ELSE 2 END;
> exception SYNTAX_ERROR_2

SET MODE DB2;
> ok

SELECT 1 WHERE 1 IN ();
> exception SYNTAX_ERROR_2

SET MODE Derby;
> ok

SELECT 1 WHERE 1 IN ();
> exception SYNTAX_ERROR_2

SET MODE MSSQLServer;
> ok

SELECT 1 WHERE 1 IN ();
> exception SYNTAX_ERROR_2

SET MODE HSQLDB;
> ok

SELECT 1 WHERE 1 IN ();
> exception SYNTAX_ERROR_2

SET MODE MySQL;
> ok

SELECT 1 WHERE 1 IN ();
> exception SYNTAX_ERROR_2

SET MODE Oracle;
> ok

SELECT 1 WHERE 1 IN ();
> exception SYNTAX_ERROR_2

SET MODE PostgreSQL;
> ok

SELECT 1 WHERE 1 IN ();
> exception SYNTAX_ERROR_2

SET MODE Regular;
> ok

CREATE TABLE TEST(A INT, B INT) AS (VALUES (1, 1), (1, 2), (2, 1), (2, NULL));
> ok

SELECT * FROM TEST WHERE (A, B) IN ((1, 1), (2, 1), (2, 2), (2, NULL));
> A B
> - -
> 1 1
> 2 1
> rows: 2

DROP TABLE TEST;
> ok

SELECT LOCALTIME IN(DATE '2000-01-01', DATE '2010-01-01');
> exception TYPES_ARE_NOT_COMPARABLE_2

SELECT LOCALTIME IN ((VALUES DATE '2000-01-01', DATE '2010-01-01'));
> exception TYPES_ARE_NOT_COMPARABLE_2

CREATE TABLE TEST(V INT) AS VALUES 1, 2;
> ok

SELECT V, V IN (1, 1000000000000) FROM TEST;
> V V IN(1, 1000000000000)
> - ----------------------
> 1 TRUE
> 2 FALSE
> rows: 2

EXPLAIN SELECT V, V IN (1, 1000000000000) FROM TEST;
>> SELECT "V", "V" IN(1, 1000000000000) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

CREATE UNIQUE INDEX TEST_IDX ON TEST(V);
> ok

SELECT V, V IN (1, 1000000000000) FROM TEST;
> V V IN(1, 1000000000000)
> - ----------------------
> 1 TRUE
> 2 FALSE
> rows: 2

EXPLAIN SELECT V, V IN (1, 1000000000000) FROM TEST;
>> SELECT "V", "V" IN(1, 1000000000000) FROM "PUBLIC"."TEST" /* PUBLIC.TEST_IDX */

DROP TABLE TEST;
> ok

CREATE TABLE TEST(C BIGINT PRIMARY KEY) AS VALUES 1, 1000000000000;
> ok

SELECT V, V IN (SELECT * FROM TEST) FROM (VALUES 1, 2) T(V);
> V V IN( SELECT DISTINCT PUBLIC.TEST.C FROM PUBLIC.TEST)
> - -----------------------------------------------------
> 1 TRUE
> 2 FALSE
> rows: 2

EXPLAIN SELECT V, V IN (SELECT * FROM TEST) FROM (VALUES 1, 2) T(V);
>> SELECT "V", "V" IN( SELECT DISTINCT "PUBLIC"."TEST"."C" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */) FROM (VALUES (1), (2)) "T"("V") /* table scan */

DROP TABLE TEST;
> ok

CREATE TABLE TEST(C INTEGER PRIMARY KEY) AS VALUES 1, 2;
> ok

SELECT V, V IN (SELECT * FROM TEST) FROM (VALUES 1, 1000000000000) T(V);
> V             V IN( SELECT DISTINCT PUBLIC.TEST.C FROM PUBLIC.TEST)
> ------------- -----------------------------------------------------
> 1             TRUE
> 1000000000000 FALSE
> rows: 2

EXPLAIN SELECT V, V IN (SELECT * FROM TEST) FROM (VALUES 1, 1000000000000) T(V);
>> SELECT "V", "V" IN( SELECT DISTINCT "PUBLIC"."TEST"."C" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */) FROM (VALUES (1), (1000000000000)) "T"("V") /* table scan */

DROP TABLE TEST;
> ok
