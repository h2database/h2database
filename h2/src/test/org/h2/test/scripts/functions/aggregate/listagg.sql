-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- with filter condition

create table test(v varchar);
> ok

insert into test values ('1'), ('2'), ('3'), ('4'), ('5'), ('6'), ('7'), ('8'), ('9');
> update count: 9

select listagg(v, '-') within group (order by v asc),
    listagg(v, '-') within group (order by v desc) filter (where v >= '4')
    from test where v >= '2';
> LISTAGG(V, '-') WITHIN GROUP (ORDER BY V) LISTAGG(V, '-') WITHIN GROUP (ORDER BY V DESC) FILTER (WHERE V >= '4')
> ----------------------------------------- ----------------------------------------------------------------------
> 2-3-4-5-6-7-8-9                           9-8-7-6-5-4
> rows: 1

select group_concat(v order by v asc separator '-'),
    group_concat(v order by v desc separator '-') filter (where v >= '4')
    from test where v >= '2';
> LISTAGG(V, '-') WITHIN GROUP (ORDER BY V) LISTAGG(V, '-') WITHIN GROUP (ORDER BY V DESC) FILTER (WHERE V >= '4')
> ----------------------------------------- ----------------------------------------------------------------------
> 2-3-4-5-6-7-8-9                           9-8-7-6-5-4
> rows: 1

create index test_idx on test(v);
> ok

select group_concat(v order by v asc separator '-'),
    group_concat(v order by v desc separator '-') filter (where v >= '4')
    from test where v >= '2';
> LISTAGG(V, '-') WITHIN GROUP (ORDER BY V) LISTAGG(V, '-') WITHIN GROUP (ORDER BY V DESC) FILTER (WHERE V >= '4')
> ----------------------------------------- ----------------------------------------------------------------------
> 2-3-4-5-6-7-8-9                           9-8-7-6-5-4
> rows: 1

select group_concat(v order by v asc separator '-'),
    group_concat(v order by v desc separator '-') filter (where v >= '4')
    from test;
> LISTAGG(V, '-') WITHIN GROUP (ORDER BY V) LISTAGG(V, '-') WITHIN GROUP (ORDER BY V DESC) FILTER (WHERE V >= '4')
> ----------------------------------------- ----------------------------------------------------------------------
> 1-2-3-4-5-6-7-8-9                         9-8-7-6-5-4
> rows: 1

drop table test;
> ok

create table test (id int auto_increment primary key, v int);
> ok

insert into test(v) values (7), (2), (8), (3), (7), (3), (9), (-1);
> update count: 8

select group_concat(v) from test;
> LISTAGG(V) WITHIN GROUP (ORDER BY NULL)
> ---------------------------------------
> 7,2,8,3,7,3,9,-1
> rows: 1

select group_concat(distinct v) from test;
> LISTAGG(DISTINCT V) WITHIN GROUP (ORDER BY NULL)
> ------------------------------------------------
> -1,2,3,7,8,9
> rows: 1

select group_concat(distinct v order by v desc) from test;
> LISTAGG(DISTINCT V) WITHIN GROUP (ORDER BY V DESC)
> --------------------------------------------------
> 9,8,7,3,2,-1
> rows: 1

INSERT INTO TEST(V) VALUES NULL;
> update count: 1

SELECT LISTAGG(V, ',') WITHIN GROUP (ORDER BY ID) FROM TEST;
>> 7,2,8,3,7,3,9,-1

SELECT LISTAGG(COALESCE(CAST(V AS VARCHAR), 'null'), ',') WITHIN GROUP (ORDER BY ID) FROM TEST;
>> 7,2,8,3,7,3,9,-1,null

SELECT LISTAGG(V, ',') WITHIN GROUP (ORDER BY V) FROM TEST;
>> -1,2,3,3,7,7,8,9

drop table test;
> ok

create table test(g int, v int) as values (1, 1), (1, 2), (1, 3), (2, 4), (2, 5), (2, 6), (3, null);
> ok

select g, listagg(v, '-') from test group by g;
> G LISTAGG(V, '-') WITHIN GROUP (ORDER BY NULL)
> - --------------------------------------------
> 1 1-2-3
> 2 4-5-6
> 3 null
> rows: 3

select g, listagg(v, '-') over (partition by g) from test order by v;
> G LISTAGG(V, '-') WITHIN GROUP (ORDER BY NULL) OVER (PARTITION BY G)
> - ------------------------------------------------------------------
> 3 null
> 1 1-2-3
> 1 1-2-3
> 1 1-2-3
> 2 4-5-6
> 2 4-5-6
> 2 4-5-6
> rows (ordered): 7

select g, listagg(v, '-' on overflow error) within group (order by v) filter (where v <> 2) over (partition by g) from test order by v;
> G LISTAGG(V, '-') WITHIN GROUP (ORDER BY V) FILTER (WHERE V <> 2) OVER (PARTITION BY G)
> - -------------------------------------------------------------------------------------
> 3 null
> 1 1-3
> 1 1-3
> 1 1-3
> 2 4-5-6
> 2 4-5-6
> 2 4-5-6
> rows (ordered): 7

select listagg(distinct v, '-') from test;
> LISTAGG(DISTINCT V, '-') WITHIN GROUP (ORDER BY NULL)
> -----------------------------------------------------
> 1-2-3-4-5-6
> rows: 1

select g, group_concat(v separator v) from test group by g;
> exception SYNTAX_ERROR_2

drop table test;
> ok

CREATE TABLE TEST(A INT, B INT, C INT);
> ok

INSERT INTO TEST VALUES
    (1, NULL, NULL),
    (2, NULL, 1),
    (3, 1, NULL),
    (4, 1, 1),
    (5, NULL, 2),
    (6, 2, NULL),
    (7, 2, 2);
> update count: 7

SELECT LISTAGG(A) WITHIN GROUP (ORDER BY B ASC NULLS FIRST, C ASC NULLS FIRST) FROM TEST;
>> 1,2,5,3,4,6,7

SELECT LISTAGG(A) WITHIN GROUP (ORDER BY B ASC NULLS LAST, C ASC NULLS LAST) FROM TEST;
>> 4,3,7,6,2,5,1

DROP TABLE TEST;
> ok

SELECT LISTAGG(DISTINCT A, ' ') WITHIN GROUP (ORDER BY B) FROM (VALUES ('a', 2), ('a', 3), ('b', 1)) T(A, B);
>> b a

CREATE TABLE TEST(A INT NOT NULL, B VARCHAR(50) NOT NULL) AS VALUES (1, '1'), (1, '2'), (1, '3');
> ok

SELECT STRING_AGG(B, ', ') FROM TEST GROUP BY A;
>> 1, 2, 3

SELECT STRING_AGG(B, ', ' ORDER BY B DESC) FROM TEST GROUP BY A;
>> 3, 2, 1

DROP TABLE TEST;
> ok

EXPLAIN SELECT LISTAGG(A) WITHIN GROUP (ORDER BY 'a') FROM (VALUES 'a', 'b') T(A);
>> SELECT LISTAGG("A") WITHIN GROUP (ORDER BY NULL) FROM (VALUES ('a'), ('b')) "T"("A") /* table scan */

SET MODE Oracle;
> ok

SELECT LISTAGG(V, '') WITHIN GROUP(ORDER BY V) FROM (VALUES 'a', 'b') T(V);
>> ab

SET MODE Regular;
> ok

CREATE TABLE TEST(ID INT, V VARCHAR) AS VALUES (1, 'b'), (2, 'a');
> ok

EXPLAIN SELECT LISTAGG(V) FROM TEST;
>> SELECT LISTAGG("V") WITHIN GROUP (ORDER BY NULL) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT LISTAGG(V) WITHIN GROUP (ORDER BY ID) FROM TEST;
>> SELECT LISTAGG("V") WITHIN GROUP (ORDER BY "ID") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT LISTAGG(V, ';') WITHIN GROUP (ORDER BY ID) FROM TEST;
>> SELECT LISTAGG("V", ';') WITHIN GROUP (ORDER BY "ID") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT LISTAGG(V ON OVERFLOW ERROR) WITHIN GROUP (ORDER BY ID) FROM TEST;
>> SELECT LISTAGG("V") WITHIN GROUP (ORDER BY "ID") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT LISTAGG(V, ';' ON OVERFLOW ERROR) WITHIN GROUP (ORDER BY ID) FROM TEST;
>> SELECT LISTAGG("V", ';') WITHIN GROUP (ORDER BY "ID") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT LISTAGG(V ON OVERFLOW TRUNCATE WITH COUNT) WITHIN GROUP (ORDER BY ID) FROM TEST;
>> SELECT LISTAGG("V" ON OVERFLOW TRUNCATE WITH COUNT) WITHIN GROUP (ORDER BY "ID") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT LISTAGG(V ON OVERFLOW TRUNCATE WITHOUT COUNT) WITHIN GROUP (ORDER BY ID) FROM TEST;
>> SELECT LISTAGG("V" ON OVERFLOW TRUNCATE WITHOUT COUNT) WITHIN GROUP (ORDER BY "ID") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT LISTAGG(V ON OVERFLOW TRUNCATE '..' WITH COUNT) WITHIN GROUP (ORDER BY ID) FROM TEST;
>> SELECT LISTAGG("V" ON OVERFLOW TRUNCATE '..' WITH COUNT) WITHIN GROUP (ORDER BY "ID") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT LISTAGG(V ON OVERFLOW TRUNCATE '..' WITHOUT COUNT) WITHIN GROUP (ORDER BY ID) FROM TEST;
>> SELECT LISTAGG("V" ON OVERFLOW TRUNCATE '..' WITHOUT COUNT) WITHIN GROUP (ORDER BY "ID") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

DROP TABLE TEST;
> ok

SELECT LISTAGG(V, ?) L FROM (VALUES 'a', 'b', 'c') T(V);
{
:
> L
> -----
> a:b:c
> rows: 1
};
> update count: 0

SELECT LISTAGG(V, V) L FROM (VALUES 'a', 'b', 'c') T(V);
> exception SYNTAX_ERROR_2
