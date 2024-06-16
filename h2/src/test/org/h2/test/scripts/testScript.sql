-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--
CREATE TABLE TEST(A INT, B INT) AS VALUES (1, 2), (3, 4), (5, 6);
> ok

UPDATE TOP (1) TEST SET B = 10;
> exception TABLE_OR_VIEW_NOT_FOUND_1

SET MODE MSSQLServer;
> ok

UPDATE TOP (1) TEST SET B = 10;
> update count: 1

SELECT COUNT(*) FILTER (WHERE B = 10) N, COUNT(*) FILTER (WHERE B <> 10) O FROM TEST;
> N O
> - -
> 1 2
> rows: 1

UPDATE TEST SET B = 10 WHERE B <> 10;
> update count: 2

UPDATE TOP (1) TEST SET B = 10 LIMIT 1;
> exception SYNTAX_ERROR_1

SET MODE Regular;
> ok

DROP TABLE TEST;
> ok

--- special grammar and test cases ---------------------------------------------------------------------------------------------
select 0 as x from system_range(1, 2) d group by d.x;
> X
> -
> 0
> 0
> rows: 2

select 1 "a", count(*) from dual group by "a" order by "a";
> a COUNT(*)
> - --------
> 1 1
> rows (ordered): 1

create table results(eventId int, points int, studentId int);
> ok

insert into results values(1, 10, 1), (2, 20, 1), (3, 5, 1);
> update count: 3

insert into results values(1, 10, 2), (2, 20, 2), (3, 5, 2);
> update count: 3

insert into results values(1, 10, 3), (2, 20, 3), (3, 5, 3);
> update count: 3

SELECT SUM(points) FROM RESULTS
WHERE eventID IN
(SELECT eventID FROM RESULTS
WHERE studentID = 2
ORDER BY points DESC
LIMIT 2 )
AND studentID = 2;
> SUM(POINTS)
> -----------
> 30
> rows: 1

SELECT eventID X FROM RESULTS
WHERE studentID = 2
ORDER BY points DESC
LIMIT 2;
> X
> -
> 2
> 1
> rows (ordered): 2

SELECT SUM(r.points) FROM RESULTS r,
(SELECT eventID FROM RESULTS
WHERE studentID = 2
ORDER BY points DESC
LIMIT 2 ) r2
WHERE r2.eventID = r.eventId
AND studentID = 2;
> SUM(R.POINTS)
> -------------
> 30
> rows: 1

drop table results;
> ok

create table test(id int, name varchar) as select 1, 'a';
> ok

(select id from test order by id) union (select id from test order by name);
> ID
> --
> 1
> rows: 1

drop table test;
> ok

select * from system_range(1,1) order by x limit 3 offset 3;
> X
> -
> rows (ordered): 0

create sequence seq start with 65 increment by 1;
> ok

select char(nextval('seq')) as x;
> X
> -
> A
> rows: 1

select char(nextval('seq')) as x;
> X
> -
> B
> rows: 1

drop sequence seq;
> ok

create table test(id int, name varchar);
> ok

insert into test values(5, 'b'), (5, 'b'), (20, 'a');
> update count: 3

select id from test where name in(null, null);
> ID
> --
> rows: 0

select * from (select * from test order by name limit 1) where id < 10;
> ID NAME
> -- ----
> rows: 0

drop table test;
> ok

create table test (id int primary key, pid int);
> ok

alter table test add constraint fk_test foreign key (pid)
references test (id) index idx_test_pid;
> ok

insert into test values (2, null);
> update count: 1

update test set pid = 1 where id = 2;
> exception REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1

drop table test;
> ok

create table test(name varchar(255));
> ok

select * from test union select * from test order by test.name;
> exception ORDER_BY_NOT_IN_RESULT

insert into test values('a'), ('b'), ('c');
> update count: 3

select name from test where name > all(select name from test where name<'b');
> NAME
> ----
> b
> c
> rows: 2

select count(*) from (select name from test where name > all(select name from test where name<'b')) x;
> COUNT(*)
> --------
> 2
> rows: 1

drop table test;
> ok

create table test(id int) as select 1;
> ok

select * from test where id >= all(select id from test where 1=0);
> ID
> --
> 1
> rows: 1

select * from test where id = all(select id from test where 1=0);
> ID
> --
> 1
> rows: 1

select * from test where id = all(select id from test union all select id from test);
> ID
> --
> 1
> rows: 1

select * from test where null >= all(select id from test where 1=0);
> ID
> --
> 1
> rows: 1

select * from test where null = all(select id from test where 1=0);
> ID
> --
> 1
> rows: 1

select * from test where null = all(select id from test union all select id from test);
> ID
> --
> rows: 0

select * from test where id >= all(select cast(null as int) from test);
> ID
> --
> rows: 0

select * from test where id = all(select null from test union all select id from test);
> ID
> --
> rows: 0

select * from test where null >= all(select cast(null as int) from test);
> ID
> --
> rows: 0

select * from test where null = all(select null from test union all select id from test);
> ID
> --
> rows: 0

drop table test;
> ok

select x from dual order by y.x;
> exception COLUMN_NOT_FOUND_1

create table test(id int primary key, name varchar(255), row_number int);
> ok

insert into test values(1, 'hello', 10), (2, 'world', 20);
> update count: 2

select rownum(), id, name from test order by id;
> ROWNUM() ID NAME
> -------- -- -----
> 1        1  hello
> 2        2  world
> rows (ordered): 2

select rownum(), id, name from test order by name;
> ROWNUM() ID NAME
> -------- -- -----
> 1        1  hello
> 2        2  world
> rows (ordered): 2

select rownum(), id, name from test order by name desc;
> ROWNUM() ID NAME
> -------- -- -----
> 2        2  world
> 1        1  hello
> rows (ordered): 2

update test set (id)=(id);
> update count: 2

drop table test;
> ok

select 2^2;
> exception SYNTAX_ERROR_1

select * from dual where cast('xx' as varchar_ignorecase(1)) = 'X' and cast('x x ' as char(2)) = 'x';
>
>
>
> rows: 1

explain select -cast(0 as real), -cast(0 as double);
>> SELECT CAST(0.0 AS REAL), CAST(0.0 AS DOUBLE PRECISION)

select (1) one;
> ONE
> ---
> 1
> rows: 1

create table test(id int);
> ok

insert into test values(1), (2), (4);
> update count: 3

select * from test order by id limit -1;
> exception INVALID_VALUE_2

select * from test order by id limit 0;
> ID
> --
> rows (ordered): 0

select * from test order by id limit 1;
> ID
> --
> 1
> rows (ordered): 1

select * from test order by id limit 1+1;
> ID
> --
> 1
> 2
> rows (ordered): 2

select * from test order by id limit null;
> exception INVALID_VALUE_2

delete from test limit 0;
> ok

delete from test limit 1;
> update count: 1

delete from test limit -1;
> exception INVALID_VALUE_2

drop table test;
> ok

create table test(id int primary key);
> ok

insert into test(id) direct sorted select x from system_range(1, 100);
> update count: 100

explain insert into test(id) direct sorted select x from system_range(1, 100);
>> INSERT INTO "PUBLIC"."TEST"("ID") DIRECT SELECT "X" FROM SYSTEM_RANGE(1, 100) /* range index */

explain select * from test limit 10;
>> SELECT "PUBLIC"."TEST"."ID" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ FETCH FIRST 10 ROWS ONLY

drop table test;
> ok

create table test(id int primary key);
> ok

insert into test values(1), (2), (3), (4);
> update count: 4

explain analyze select * from test where id is null;
>> SELECT "PUBLIC"."TEST"."ID" FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID IS NULL */ /* scanCount: 1 */ WHERE "ID" IS NULL

drop table test;
> ok

explain analyze select 1;
>> SELECT 1

create table test(id int);
> ok

create view x as select * from test;
> ok

drop table test restrict;
> exception CANNOT_DROP_2

drop table test cascade;
> ok

select 1, 2 from (select * from dual) union all select 3, 4 from dual;
> 1 2
> - -
> 1 2
> 3 4
> rows: 2

select 3 from (select * from dual) union all select 2 from dual;
> 3
> -
> 2
> 3
> rows: 2

create table a(x int, y int);
> ok

alter table a add constraint a_xy unique(x, y);
> ok

create table b(x int, y int, foreign key(x, y) references a(x, y));
> ok

insert into a values(null, null), (null, 0), (0, null), (0, 0);
> update count: 4

insert into b values(null, null), (null, 0), (0, null), (0, 0);
> update count: 4

delete from a where x is null and y is null;
> update count: 1

delete from a where x is null and y = 0;
> update count: 1

delete from a where x = 0 and y is null;
> update count: 1

delete from a where x = 0 and y = 0;
> exception REFERENTIAL_INTEGRITY_VIOLATED_CHILD_EXISTS_1

drop table b;
> ok

drop table a;
> ok

select * from (select null as x) where x=1;
> X
> -
> rows: 0

create table test(id decimal(10, 2) primary key) as select 0;
> ok

select * from test where id = 0.00;
> ID
> ----
> 0.00
> rows: 1

select * from test where id = 0.0;
> ID
> ----
> 0.00
> rows: 1

drop table test;
> ok

select count(*) from (select 1 union (select 2 intersect select 2)) x;
> COUNT(*)
> --------
> 2
> rows: 1

create table test(id varchar(1) primary key) as select 'X';
> ok

select count(*) from (select 1 from dual where 1 in ((select 1 union select 1))) a;
> COUNT(*)
> --------
> 1
> rows: 1

insert into test ((select 1 union select 2) union select 3);
> update count: 3

select count(*) from test where id = 'X1';
> COUNT(*)
> --------
> 0
> rows: 1

drop table test;
> ok

create table test(id int, constraint pk primary key(id), constraint x unique(id));
> ok

SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME = 'TEST';
> CONSTRAINT_NAME
> ---------------
> PK
> X
> rows: 2

drop table test;
> ok

create table parent(id int primary key);
> ok

create table child(id int, parent_id int, constraint child_parent foreign key (parent_id) references parent(id));
> ok

SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME = 'CHILD';
> CONSTRAINT_NAME
> ---------------
> CHILD_PARENT
> rows: 1

drop table parent, child;
> ok

create table test(id int, name varchar(max));
> ok

alter table test alter column id identity;
> ok

drop table test;
> ok

create table test(id identity);
> ok

set password test;
> exception COLUMN_NOT_FOUND_1

alter user sa set password test;
> exception COLUMN_NOT_FOUND_1

comment on table test is test;
> exception COLUMN_NOT_FOUND_1

select 1 from test a where 1 in(select 1 from test b where b.id in(select 1 from test c where c.id=a.id));
> 1
> -
> rows: 0

drop table test;
> ok

select @n := case when x = 1 then 1 else @n * x end f from system_range(1, 4);
> F
> --
> 1
> 2
> 24
> 6
> rows: 4

select * from (select "x" from dual);
> exception COLUMN_NOT_FOUND_1

select * from(select 1 from system_range(1, 2) group by sin(x) order by sin(x));
> 1
> -
> 1
> 1
> rows: 2

create table parent(id int primary key, x int) as select 1 id, 2 x;
> ok

create table child(id int references parent(id)) as select 1;
> ok

delete from parent;
> exception REFERENTIAL_INTEGRITY_VIOLATED_CHILD_EXISTS_1

drop table parent, child;
> ok

create domain integer as varchar;
> exception DOMAIN_ALREADY_EXISTS_1

create domain int as varchar;
> ok

create memory table test(id int);
> ok

script nodata nopasswords nosettings noversion;
> SCRIPT
> -----------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE DOMAIN "PUBLIC"."INT" AS CHARACTER VARYING;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" "PUBLIC"."INT" );
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> rows (ordered): 4

SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST';
>> CHARACTER VARYING

drop table test;
> ok

drop domain int;
> ok

create table test(id identity, parent bigint, foreign key(parent) references(id));
> ok

insert into test values(0, 0), (1, NULL), (2, 1), (3, 3), (4, 3);
> update count: 5

delete from test where id = 3;
> exception REFERENTIAL_INTEGRITY_VIOLATED_CHILD_EXISTS_1

delete from test where id = 0;
> update count: 1

delete from test where id = 1;
> exception REFERENTIAL_INTEGRITY_VIOLATED_CHILD_EXISTS_1

drop table test;
> ok

create schema a;
> ok

set autocommit false;
> ok

set schema a;
> ok

create table t1 ( k int, v varchar(10) );
> ok

insert into t1 values ( 1, 't1' );
> update count: 1

create table t2 ( k int, v varchar(10) );
> ok

insert into t2 values ( 2, 't2' );
> update count: 1

create view v_test(a, b, c, d) as select t1.*, t2.* from t1 join t2 on ( t1.k = t2.k );
> ok

select * from v_test;
> A B C D
> - - - -
> rows: 0

set schema public;
> ok

drop schema a cascade;
> ok

set autocommit true;
> ok

select x/3 as a, count(*) c from system_range(1, 10) group by a having c>2;
> A C
> - -
> 1 3
> 2 3
> rows: 2

create table test(id int);
> ok

insert into test values(1), (2);
> update count: 2

select id+1 as x, count(*) from test group by x;
> X COUNT(*)
> - --------
> 2 1
> 3 1
> rows: 2

select 1 as id, id as b, count(*)  from test group by id;
> ID B COUNT(*)
> -- - --------
> 1  1 1
> 1  2 1
> rows: 2

select id+1 as x, count(*) from test group by -x;
> exception COLUMN_NOT_FOUND_1

select id+1 as x, count(*) from test group by x having x>2;
> exception MUST_GROUP_BY_COLUMN_1

select id+1 as x, count(*) from test group by 1;
> exception MUST_GROUP_BY_COLUMN_1

drop table test;
> ok

create table test(t0 timestamp(0), t1 timestamp(1), t4 timestamp(4));
> ok

select column_name, datetime_precision from information_schema.columns c where c.table_name = 'TEST' order by column_name;
> COLUMN_NAME DATETIME_PRECISION
> ----------- ------------------
> T0          0
> T1          1
> T4          4
> rows (ordered): 3

drop table test;
> ok

create table test(a int);
> ok

insert into test values(1), (2);
> update count: 2

select -test.a a from test order by test.a;
> A
> --
> -1
> -2
> rows (ordered): 2

select -test.a from test order by test.a;
> - TEST.A
> --------
> -1
> -2
> rows (ordered): 2

select -test.a aa from test order by a;
> AA
> --
> -1
> -2
> rows (ordered): 2

select -test.a aa from test order by aa;
> AA
> --
> -2
> -1
> rows (ordered): 2

select -test.a a from test order by a;
> A
> --
> -2
> -1
> rows (ordered): 2

drop table test;
> ok

CREATE TABLE table_a(a_id INT PRIMARY KEY, left_id INT, right_id INT);
> ok

CREATE TABLE table_b(b_id INT PRIMARY KEY, a_id INT);
> ok

CREATE TABLE table_c(left_id INT, right_id INT, center_id INT);
> ok

CREATE VIEW view_a AS
SELECT table_c.center_id, table_a.a_id, table_b.b_id
FROM table_c
INNER JOIN table_a ON table_c.left_id = table_a.left_id
AND table_c.right_id = table_a.right_id
LEFT JOIN table_b ON table_b.a_id = table_a.a_id;
> ok

SELECT * FROM table_c INNER JOIN view_a
ON table_c.center_id = view_a.center_id;
> LEFT_ID RIGHT_ID CENTER_ID CENTER_ID A_ID B_ID
> ------- -------- --------- --------- ---- ----
> rows: 0

drop view view_a;
> ok

drop table table_a, table_b, table_c;
> ok

create table t (pk int primary key, attr int);
> ok

insert into t values (1, 5), (5, 1);
> update count: 2

select t1.pk from t t1, t t2 where t1.pk = t2.attr order by t1.pk;
> PK
> --
> 1
> 5
> rows (ordered): 2

drop table t;
> ok

CREATE ROLE TEST_A;
> ok

GRANT TEST_A TO TEST_A;
> exception ROLE_ALREADY_GRANTED_1

CREATE ROLE TEST_B;
> ok

GRANT TEST_A TO TEST_B;
> ok

GRANT TEST_B TO TEST_A;
> exception ROLE_ALREADY_GRANTED_1

DROP ROLE TEST_A;
> ok

DROP ROLE TEST_B;
> ok

CREATE ROLE PUBLIC2;
> ok

GRANT PUBLIC2 TO SA;
> ok

GRANT PUBLIC2 TO SA;
> ok

REVOKE PUBLIC2 FROM SA;
> ok

REVOKE PUBLIC2 FROM SA;
> ok

DROP ROLE PUBLIC2;
> ok

create table test(id int primary key, lastname varchar, firstname varchar, parent int references(id));
> ok

alter table test add constraint name unique (lastname, firstname);
> ok

SELECT CONSTRAINT_NAME, INDEX_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS;
> CONSTRAINT_NAME INDEX_NAME
> --------------- ------------------
> CONSTRAINT_2    PRIMARY_KEY_2
> CONSTRAINT_27   CONSTRAINT_INDEX_2
> NAME            NAME_INDEX_2
> rows: 3

SELECT CONSTRAINT_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE;
> CONSTRAINT_NAME COLUMN_NAME
> --------------- -----------
> CONSTRAINT_2    ID
> CONSTRAINT_27   PARENT
> NAME            FIRSTNAME
> NAME            LASTNAME
> rows: 4

drop table test;
> ok

ALTER TABLE INFORMATION_SCHEMA.INFORMATION_SCHEMA_CATALOG_NAME RENAME TO INFORMATION_SCHEMA.CAT;
> exception FEATURE_NOT_SUPPORTED_1

CREATE TABLE test (id bigserial NOT NULL primary key);
> ok

drop table test;
> ok

CREATE TABLE test (id serial NOT NULL primary key);
> ok

drop table test;
> ok

CREATE MEMORY TABLE TEST(ID INT, D DOUBLE, F FLOAT);
> ok

insert into test values(0, POWER(0, -1), POWER(0, -1)), (1, -POWER(0, -1), -POWER(0, -1)), (2, SQRT(-1), SQRT(-1));
> update count: 3

select * from test order by id;
> ID D         F
> -- --------- ---------
> 0  Infinity  Infinity
> 1  -Infinity -Infinity
> 2  NaN       NaN
> rows (ordered): 3

script nopasswords nosettings noversion;
> SCRIPT
> -----------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER, "D" DOUBLE PRECISION, "F" FLOAT );
> -- 3 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST" VALUES (0, 'Infinity', 'Infinity'), (1, '-Infinity', '-Infinity'), (2, 'NaN', 'NaN');
> rows (ordered): 4

DROP TABLE TEST;
> ok

create schema a;
> ok

create table a.x(ax int);
> ok

create schema b;
> ok

create table b.x(bx int);
> ok

select * from a.x, b.x;
> AX BX
> -- --
> rows: 0

drop schema a cascade;
> ok

drop schema b cascade;
> ok

CREATE TABLE p(d date);
> ok

INSERT INTO p VALUES('-1-01-01'), ('0-01-01'), ('0001-01-01');
> update count: 3

select d, year(d), extract(year from d), cast(d as timestamp) from p;
> D           EXTRACT(YEAR FROM D) EXTRACT(YEAR FROM D) CAST(D AS TIMESTAMP)
> ----------- -------------------- -------------------- --------------------
> -0001-01-01 -1                   -1                   -0001-01-01 00:00:00
> 0000-01-01  0                    0                    0000-01-01 00:00:00
> 0001-01-01  1                    1                    0001-01-01 00:00:00
> rows: 3

drop table p;
> ok

create table test(a int, b int default 1);
> ok

insert into test values(1, default), (2, 2), (3, null);
> update count: 3

select * from test;
> A B
> - ----
> 1 1
> 2 2
> 3 null
> rows: 3

update test set b = default where a = 2;
> update count: 1

explain update test set b = default where a = 2;
>> UPDATE "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ SET "B" = DEFAULT WHERE "A" = 2

select * from test;
> A B
> - ----
> 1 1
> 2 1
> 3 null
> rows: 3

update test set a=default;
> update count: 3

drop table test;
> ok

CREATE ROLE X;
> ok

GRANT X TO X;
> exception ROLE_ALREADY_GRANTED_1

CREATE ROLE Y;
> ok

GRANT Y TO X;
> ok

DROP ROLE Y;
> ok

DROP ROLE X;
> ok

select top sum(1) 0 from dual;
> exception SYNTAX_ERROR_1

create table test(id int primary key, name varchar) as select 1, 'Hello World';
> ok

select * from test;
> ID NAME
> -- -----------
> 1  Hello World
> rows: 1

drop table test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, LABEL CHAR(20), LOOKUP CHAR(30));
> ok

INSERT INTO TEST VALUES (1, 'Mouse', 'MOUSE'), (2, 'MOUSE', 'Mouse');
> update count: 2

SELECT * FROM TEST;
> ID LABEL  LOOKUP
> -- ------ ------
> 1  Mouse  MOUSE
> 2  MOUSE  Mouse
> rows: 2

DROP TABLE TEST;
> ok

call 'a' regexp 'Ho.*\';
> exception LIKE_ESCAPE_ERROR_1

set @t = 0;
> ok

call set(1, 2);
> exception CAN_ONLY_ASSIGN_TO_VARIABLE_1

select x, set(@t, ifnull(@t, 0) + x) from system_range(1, 3);
> X SET(@T, COALESCE(@T, 0) + X)
> - ----------------------------
> 1 1
> 2 3
> 3 6
> rows: 3

select * from system_range(1, 2) a,
(select * from system_range(1, 2) union select * from system_range(1, 2)
union select * from system_range(1, 1)) v where a.x = v.x;
> X X
> - -
> 1 1
> 2 2
> rows: 2

create table test(id int);
> ok

select * from ((select * from test) union (select * from test)) where id = 0;
> ID
> --
> rows: 0

select * from ((test d1 inner join test d2 on d1.id = d2.id) inner join test d3 on d1.id = d3.id) inner join test d4 on d4.id = d1.id;
> ID ID ID ID
> -- -- -- --
> rows: 0

drop table test;
> ok

create table person(id bigint auto_increment, name varchar(100));
> ok

insert into person(name) values ('a'), ('b'), ('c');
> update count: 3

select * from person order by id;
> ID NAME
> -- ----
> 1  a
> 2  b
> 3  c
> rows (ordered): 3

select * from person order by id limit 2;
> ID NAME
> -- ----
> 1  a
> 2  b
> rows (ordered): 2

select * from person order by id limit 2 offset 1;
> ID NAME
> -- ----
> 2  b
> 3  c
> rows (ordered): 2

select * from person order by id limit 2147483647 offset 1;
> ID NAME
> -- ----
> 2  b
> 3  c
> rows (ordered): 2

select * from person order by id limit 2147483647-1 offset 1;
> ID NAME
> -- ----
> 2  b
> 3  c
> rows (ordered): 2

select * from person order by id limit 2147483647-1 offset 2;
> ID NAME
> -- ----
> 3  c
> rows (ordered): 1

select * from person order by id limit 2147483647-2 offset 2;
> ID NAME
> -- ----
> 3  c
> rows (ordered): 1

drop table person;
> ok

CREATE TABLE TEST(ID INTEGER NOT NULL, ID2 INTEGER DEFAULT 0);
> ok

ALTER TABLE test ALTER COLUMN ID2 RENAME TO ID;
> exception DUPLICATE_COLUMN_NAME_1

drop table test;
> ok

CREATE TABLE FOO (A CHAR(10));
> ok

CREATE TABLE BAR AS SELECT * FROM FOO;
> ok

select table_name, character_maximum_length from information_schema.columns where column_name = 'A';
> TABLE_NAME CHARACTER_MAXIMUM_LENGTH
> ---------- ------------------------
> BAR        10
> FOO        10
> rows: 2

DROP TABLE FOO, BAR;
> ok

create table multi_pages(dir_num int, bh_id int);
> ok

insert into multi_pages values(1, 1), (2, 2), (3, 3);
> update count: 3

create table b_holding(id int primary key, site varchar(255));
> ok

insert into b_holding values(1, 'Hello'), (2, 'Hello'), (3, 'Hello');
> update count: 3

select * from (select dir_num, count(*) as cnt from multi_pages  t, b_holding bh
where t.bh_id=bh.id and bh.site='Hello' group by dir_num) as x
where cnt < 1000 order by dir_num asc;
> DIR_NUM CNT
> ------- ---
> 1       1
> 2       1
> 3       1
> rows (ordered): 3

explain select * from (select dir_num, count(*) as cnt from multi_pages  t, b_holding bh
where t.bh_id=bh.id and bh.site='Hello' group by dir_num) as x
where cnt < 1000 order by dir_num asc;
>> SELECT "X"."DIR_NUM", "X"."CNT" FROM ( SELECT "DIR_NUM", COUNT(*) AS "CNT" FROM "PUBLIC"."MULTI_PAGES" "T" INNER JOIN "PUBLIC"."B_HOLDING" "BH" ON 1=1 WHERE ("BH"."SITE" = 'Hello') AND ("T"."BH_ID" = "BH"."ID") GROUP BY "DIR_NUM" ) "X" /* SELECT DIR_NUM, COUNT(*) AS CNT FROM PUBLIC.MULTI_PAGES T /* PUBLIC.MULTI_PAGES.tableScan */ INNER JOIN PUBLIC.B_HOLDING BH /* PUBLIC.PRIMARY_KEY_3: ID = T.BH_ID */ ON 1=1 WHERE (BH.SITE = 'Hello') AND (T.BH_ID = BH.ID) GROUP BY DIR_NUM HAVING COUNT(*) <= ?1: CNT < CAST(1000 AS BIGINT) */ WHERE "CNT" < CAST(1000 AS BIGINT) ORDER BY 1

select dir_num, count(*) as cnt from multi_pages  t, b_holding bh
where t.bh_id=bh.id and bh.site='Hello' group by dir_num
having count(*) < 1000 order by dir_num asc;
> DIR_NUM CNT
> ------- ---
> 1       1
> 2       1
> 3       1
> rows (ordered): 3

drop table multi_pages, b_holding;
> ok

create table test(id smallint primary key);
> ok

insert into test values(1), (2), (3);
> update count: 3

explain select * from test where id = 1;
>> SELECT "PUBLIC"."TEST"."ID" FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ WHERE "ID" = 1

EXPLAIN SELECT * FROM TEST WHERE ID = (SELECT MAX(ID) FROM TEST);
>> SELECT "PUBLIC"."TEST"."ID" FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID = (SELECT MAX(ID) FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */ /* direct lookup */) */ WHERE "ID" = (SELECT MAX("ID") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ /* direct lookup */)

drop table test;
> ok

create table test(id tinyint primary key);
> ok

insert into test values(1), (2), (3);
> update count: 3

explain select * from test where id = 3;
>> SELECT "PUBLIC"."TEST"."ID" FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID = 3 */ WHERE "ID" = 3

explain select * from test where id = 255;
>> SELECT "PUBLIC"."TEST"."ID" FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID = 255 */ WHERE "ID" = 255

drop table test;
> ok

CREATE TABLE PARENT(A INT, B INT, PRIMARY KEY(A, B));
> ok

CREATE TABLE CHILD(A INT, B INT, CONSTRAINT CP FOREIGN KEY(A, B) REFERENCES PARENT(A, B));
> ok

INSERT INTO PARENT VALUES(1, 2);
> update count: 1

INSERT INTO CHILD VALUES(2, NULL), (NULL, 3), (NULL, NULL), (1, 2);
> update count: 4

set autocommit false;
> ok

ALTER TABLE CHILD SET REFERENTIAL_INTEGRITY FALSE;
> ok

ALTER TABLE CHILD SET REFERENTIAL_INTEGRITY TRUE CHECK;
> ok

set autocommit true;
> ok

DROP TABLE CHILD, PARENT;
> ok

CREATE TABLE TEST(BIRTH TIMESTAMP);
> ok

INSERT INTO TEST VALUES('2006-04-03 10:20:30'), ('2006-04-03 10:20:31'), ('2006-05-05 00:00:00'), ('2006-07-03 22:30:00'), ('2006-07-03 22:31:00');
> update count: 5

SELECT * FROM (SELECT CAST(BIRTH AS DATE) B
FROM TEST GROUP BY CAST(BIRTH AS DATE)) A
WHERE A.B >= '2006-05-05';
> B
> ----------
> 2006-05-05
> 2006-07-03
> rows: 2

DROP TABLE TEST;
> ok

CREATE TABLE Parent(ID INT PRIMARY KEY, Name VARCHAR);
> ok

CREATE TABLE Child(ID INT);
> ok

ALTER TABLE Child ADD FOREIGN KEY(ID) REFERENCES Parent(ID);
> ok

INSERT INTO Parent VALUES(1,  '0'), (2,  '0'), (3,  '0');
> update count: 3

INSERT INTO Child VALUES(1);
> update count: 1

ALTER TABLE Parent ALTER COLUMN Name BOOLEAN NULL;
> ok

DELETE FROM Parent WHERE ID=3;
> update count: 1

DROP TABLE Parent, Child;
> ok

set autocommit false;
> ok

CREATE TABLE A(ID INT PRIMARY KEY, SK INT);
> ok

ALTER TABLE A ADD CONSTRAINT AC FOREIGN KEY(SK) REFERENCES A(ID);
> ok

INSERT INTO A VALUES(1, 1);
> update count: 1

INSERT INTO A VALUES(-2, NULL);
> update count: 1

ALTER TABLE A SET REFERENTIAL_INTEGRITY FALSE;
> ok

ALTER TABLE A SET REFERENTIAL_INTEGRITY TRUE CHECK;
> ok

ALTER TABLE A SET REFERENTIAL_INTEGRITY FALSE;
> ok

INSERT INTO A VALUES(2, 3);
> update count: 1

ALTER TABLE A SET REFERENTIAL_INTEGRITY TRUE;
> ok

ALTER TABLE A SET REFERENTIAL_INTEGRITY FALSE;
> ok

ALTER TABLE A SET REFERENTIAL_INTEGRITY TRUE CHECK;
> exception REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1

DROP TABLE A;
> ok

set autocommit true;
> ok

CREATE TABLE PARENT(ID INT PRIMARY KEY);
> ok

CREATE TABLE CHILD(PID INT);
> ok

INSERT INTO PARENT VALUES(1);
> update count: 1

INSERT INTO CHILD VALUES(2);
> update count: 1

ALTER TABLE CHILD ADD CONSTRAINT CP FOREIGN KEY(PID) REFERENCES PARENT(ID);
> exception REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1

UPDATE CHILD SET PID=1;
> update count: 1

ALTER TABLE CHILD ADD CONSTRAINT CP FOREIGN KEY(PID) REFERENCES PARENT(ID);
> ok

DROP TABLE CHILD, PARENT;
> ok

CREATE TABLE A(ID INT PRIMARY KEY, SK INT);
> ok

INSERT INTO A VALUES(1, 2);
> update count: 1

ALTER TABLE A ADD CONSTRAINT AC FOREIGN KEY(SK) REFERENCES A(ID);
> exception REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1

DROP TABLE A;
> ok

CREATE TABLE TEST(ID INT);
> ok

INSERT INTO TEST VALUES(0), (1), (100);
> update count: 3

ALTER TABLE TEST ADD CONSTRAINT T CHECK ID<100;
> exception CHECK_CONSTRAINT_VIOLATED_1

UPDATE TEST SET ID=20 WHERE ID=100;
> update count: 1

ALTER TABLE TEST ADD CONSTRAINT T CHECK ID<100;
> ok

DROP TABLE TEST;
> ok

create table test(id int);
> ok

set autocommit false;
> ok

insert into test values(1);
> update count: 1

prepare commit tx1;
> ok

commit transaction tx1;
> ok

rollback;
> ok

select * from test;
> ID
> --
> 1
> rows: 1

drop table test;
> ok

set autocommit true;
> ok

SELECT 'Hello' ~ 'He.*' T1, 'HELLO' ~ 'He.*' F2, CAST('HELLO' AS VARCHAR_IGNORECASE) ~ 'He.*' T3;
> T1   F2    T3
> ---- ----- ----
> TRUE FALSE TRUE
> rows: 1

SELECT 'Hello' ~* 'He.*' T1, 'HELLO' ~* 'He.*' T2, 'hallo' ~* 'He.*' F3;
> T1   T2   F3
> ---- ---- -----
> TRUE TRUE FALSE
> rows: 1

SELECT 'Hello' !~* 'Ho.*' T1, 'HELLO' !~* 'He.*' F2, 'hallo' !~* 'Ha.*' F3;
> T1   F2    F3
> ---- ----- -----
> TRUE FALSE FALSE
> rows: 1

create table test(parent int primary key, child int, foreign key(child) references (parent));
> ok

insert into test values(1, 1);
> update count: 1

insert into test values(2, 3);
> exception REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1

set autocommit false;
> ok

set referential_integrity false;
> ok

insert into test values(4, 4);
> update count: 1

insert into test values(5, 6);
> update count: 1

set referential_integrity true;
> ok

insert into test values(7, 7), (8, 9);
> exception REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1

set autocommit true;
> ok

drop table test;
> ok

create table test as select 1, space(10) from dual where 1=0 union all select x, cast(space(100) as varchar(101)) d from system_range(1, 100);
> ok

drop table test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(1, 'Hello'), (-1, '-1');
> update count: 2

select * from test where name = -1 and name = id;
> ID NAME
> -- ----
> -1 -1
> rows: 1

explain select * from test where name = -1 and name = id;
>> SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."NAME" FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID = -1 */ WHERE ("NAME" = -1) AND ("NAME" = "ID")

DROP TABLE TEST;
> ok

select * from system_range(1, 2) where x=x+1 and x=1;
> X
> -
> rows: 0

CREATE TABLE A as select 6 a;
> ok

CREATE TABLE B(B INT PRIMARY KEY);
> ok

CREATE VIEW V(V) AS (SELECT A FROM A UNION SELECT B FROM B);
> ok

create table C as select * from table(c int = (0,6));
> ok

select * from V, C where V.V  = C.C;
> V C
> - -
> 6 6
> rows: 1

drop table A, B, C, V cascade;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, FLAG BOOLEAN, NAME VARCHAR);
> ok

CREATE INDEX IDX_FLAG ON TEST(FLAG, NAME);
> ok

INSERT INTO TEST VALUES(1, TRUE, 'Hello'), (2, FALSE, 'World');
> update count: 2

EXPLAIN SELECT * FROM TEST WHERE FLAG;
>> SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."FLAG", "PUBLIC"."TEST"."NAME" FROM "PUBLIC"."TEST" /* PUBLIC.IDX_FLAG: FLAG = TRUE */ WHERE "FLAG"

EXPLAIN SELECT * FROM TEST WHERE FLAG AND NAME>'I';
>> SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."FLAG", "PUBLIC"."TEST"."NAME" FROM "PUBLIC"."TEST" /* PUBLIC.IDX_FLAG: FLAG = TRUE AND NAME > 'I' */ WHERE "FLAG" AND ("NAME" > 'I')

DROP TABLE TEST;
> ok

CREATE TABLE test_table (first_col varchar(20), second_col integer);
> ok

insert into test_table values('a', 10), ('a', 4), ('b', 30), ('b', 3);
> update count: 4

CREATE VIEW test_view AS SELECT first_col AS renamed_col, MIN(second_col) AS also_renamed FROM test_table GROUP BY first_col;
> ok

SELECT * FROM test_view WHERE renamed_col = 'a';
> RENAMED_COL ALSO_RENAMED
> ----------- ------------
> a           4
> rows: 1

drop view test_view;
> ok

drop table test_table;
> ok

create table test(id int);
> ok

explain select id+1 a from test group by id+1;
>> SELECT "ID" + 1 AS "A" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ GROUP BY "ID" + 1

drop table test;
> ok

set autocommit off;
> ok

set schema_search_path = public, information_schema;
> ok

select table_name from tables where 1=0;
> TABLE_NAME
> ----------
> rows: 0

set schema_search_path = public;
> ok

set autocommit on;
> ok

create table script.public.x(a int);
> ok

select * from script.PUBLIC.x;
> A
> -
> rows: 0

create index script.public.idx on script.public.x(a);
> ok

drop table script.public.x;
> ok

create table d(d double, r real);
> ok

insert into d(d, d, r) values(1.1234567890123456789, 1.1234567890123456789, 3);
> exception DUPLICATE_COLUMN_NAME_1

insert into d values(1.1234567890123456789, 1.1234567890123456789);
> update count: 1

select r+d, r+r, d+d from d;
> R + D             R + R     D + D
> ----------------- --------- ------------------
> 2.246913624759111 2.2469137 2.2469135780246914
> rows: 1

drop table d;
> ok

create table test(id int, c char(5), v varchar(5));
> ok

insert into test set id = 1, c = 'a', v = 'a';
> update count: 1

insert into test set id = 2, c = 'a ', v = 'a ';
> update count: 1

insert into test set id = 3, c = 'abcde      ', v = 'abcde';
> update count: 1

select distinct length(c) from test order by length(c);
> CHAR_LENGTH(C)
> --------------
> 5
> rows (ordered): 1

select id, c, v, length(c), length(v) from test order by id;
> ID C     V     CHAR_LENGTH(C) CHAR_LENGTH(V)
> -- ----- ----- -------------- --------------
> 1  a     a     5              1
> 2  a     a     5              2
> 3  abcde abcde 5              5
> rows (ordered): 3

select id from test where c='a' order by id;
> ID
> --
> 1
> 2
> rows (ordered): 2

select id from test where c='a ' order by id;
> ID
> --
> 1
> 2
> rows (ordered): 2

select id from test where c=v order by id;
> ID
> --
> 1
> 2
> 3
> rows (ordered): 3

drop table test;
> ok

create table people (family varchar(1) not null, person varchar(1) not null);
> ok

create table cars (family varchar(1) not null, car varchar(1) not null);
> ok

insert into people values(1, 1), (2, 1), (2, 2), (3, 1), (5, 1);
> update count: 5

insert into cars values(2, 1), (2, 2), (3, 1), (3, 2), (3, 3), (4, 1);
> update count: 6

select family, (select count(car) from cars where cars.family = people.family) as x
from people group by family order by family;
> FAMILY X
> ------ -
> 1      0
> 2      2
> 3      3
> 5      0
> rows (ordered): 4

drop table people, cars;
> ok

select (1, 2);
> ROW (1, 2)
> ----------
> ROW (1, 2)
> rows: 1

select * from (select 1), (select 2);
> 1 2
> - -
> 1 2
> rows: 1

create table t1(c1 int, c2 int);
> ok

create table t2(c1 int, c2 int);
> ok

insert into t1 values(1, null), (2, 2), (3, 3);
> update count: 3

insert into t2 values(1, 1), (1, 2), (2, null), (3, 3);
> update count: 4

select * from t2 where c1 not in(select c2 from t1);
> C1 C2
> -- --
> rows: 0

select * from t2 where c1 not in(null, 2, 3);
> C1 C2
> -- --
> rows: 0

select * from t1 where c2 not in(select c1 from t2);
> C1 C2
> -- --
> rows: 0

select * from t1 where not exists(select * from t2 where t1.c2=t2.c1);
> C1 C2
> -- ----
> 1  null
> rows: 1

drop table t1;
> ok

drop table t2;
> ok

CREATE TABLE test (family_name VARCHAR_IGNORECASE(63) NOT NULL);
> ok

INSERT INTO test VALUES('Smith'), ('de Smith'), ('el Smith'), ('von Smith');
> update count: 4

SELECT * FROM test WHERE family_name IN ('de Smith', 'Smith');
> FAMILY_NAME
> -----------
> Smith
> de Smith
> rows: 2

SELECT * FROM test WHERE family_name BETWEEN 'D' AND 'T';
> FAMILY_NAME
> -----------
> Smith
> de Smith
> el Smith
> rows: 3

CREATE INDEX family_name ON test(family_name);
> ok

SELECT * FROM test WHERE family_name IN ('de Smith', 'Smith');
> FAMILY_NAME
> -----------
> Smith
> de Smith
> rows: 2

drop table test;
> ok

create memory table test(id int primary key, data clob);
> ok

insert into test values(1, 'abc' || space(20));
> update count: 1

script nopasswords nosettings noversion blocksize 10;
> SCRIPT
> ----------------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL, "DATA" CHARACTER LARGE OBJECT );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE CACHED LOCAL TEMPORARY TABLE IF NOT EXISTS SYSTEM_LOB_STREAM(ID INT NOT NULL, PART INT NOT NULL, CDATA VARCHAR, BDATA VARBINARY);
> ALTER TABLE SYSTEM_LOB_STREAM ADD CONSTRAINT SYSTEM_LOB_STREAM_PRIMARY_KEY PRIMARY KEY(ID, PART);
> CREATE ALIAS IF NOT EXISTS SYSTEM_COMBINE_CLOB FOR 'org.h2.command.dml.ScriptCommand.combineClob';
> CREATE ALIAS IF NOT EXISTS SYSTEM_COMBINE_BLOB FOR 'org.h2.command.dml.ScriptCommand.combineBlob';
> INSERT INTO SYSTEM_LOB_STREAM VALUES(0, 0, 'abc ', NULL);
> INSERT INTO SYSTEM_LOB_STREAM VALUES(0, 1, ' ', NULL);
> INSERT INTO SYSTEM_LOB_STREAM VALUES(0, 2, ' ', NULL);
> INSERT INTO "PUBLIC"."TEST" VALUES (1, SYSTEM_COMBINE_CLOB(0));
> DROP TABLE IF EXISTS SYSTEM_LOB_STREAM;
> DROP ALIAS IF EXISTS SYSTEM_COMBINE_CLOB;
> DROP ALIAS IF EXISTS SYSTEM_COMBINE_BLOB;
> rows (ordered): 15

drop table test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World');
> update count: 2

SELECT DISTINCT * FROM TEST ORDER BY ID;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows (ordered): 2

DROP TABLE TEST;
> ok

create sequence main_seq;
> ok

create schema "TestSchema";
> ok

create sequence "TestSchema"."TestSeq";
> ok

create sequence "TestSchema"."ABC";
> ok

select currval('main_seq'), currval('TestSchema', 'TestSeq');
> exception CURRENT_SEQUENCE_VALUE_IS_NOT_DEFINED_IN_SESSION_1

select nextval('TestSchema', 'ABC');
>> 1

set autocommit off;
> ok

set schema "TestSchema";
> ok

select nextval('abc'), currval('Abc'), nextval('TestSchema', 'ABC');
> NEXTVAL('abc') CURRVAL('Abc') NEXTVAL('TestSchema', 'ABC')
> -------------- -------------- ----------------------------
> 2              2              3
> rows: 1

set schema public;
> ok

drop schema "TestSchema" cascade;
> ok

drop sequence main_seq;
> ok

create sequence "test";
> ok

select nextval('test');
> NEXTVAL('test')
> ---------------
> 1
> rows: 1

drop sequence "test";
> ok

set autocommit on;
> ok

CREATE TABLE parent(id int PRIMARY KEY);
> ok

CREATE TABLE child(parentid int REFERENCES parent);
> ok

TABLE INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS;
> CONSTRAINT_CATALOG CONSTRAINT_SCHEMA CONSTRAINT_NAME UNIQUE_CONSTRAINT_CATALOG UNIQUE_CONSTRAINT_SCHEMA UNIQUE_CONSTRAINT_NAME MATCH_OPTION UPDATE_RULE DELETE_RULE
> ------------------ ----------------- --------------- ------------------------- ------------------------ ---------------------- ------------ ----------- -----------
> SCRIPT             PUBLIC            CONSTRAINT_3    SCRIPT                    PUBLIC                   CONSTRAINT_8           NONE         RESTRICT    RESTRICT
> rows: 1

ALTER TABLE parent ADD COLUMN name varchar;
> ok

TABLE INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS;
> CONSTRAINT_CATALOG CONSTRAINT_SCHEMA CONSTRAINT_NAME UNIQUE_CONSTRAINT_CATALOG UNIQUE_CONSTRAINT_SCHEMA UNIQUE_CONSTRAINT_NAME MATCH_OPTION UPDATE_RULE DELETE_RULE
> ------------------ ----------------- --------------- ------------------------- ------------------------ ---------------------- ------------ ----------- -----------
> SCRIPT             PUBLIC            CONSTRAINT_3    SCRIPT                    PUBLIC                   CONSTRAINT_8           NONE         RESTRICT    RESTRICT
> rows: 1

drop table parent, child;
> ok

create table test(id int);
> ok

create schema TEST_SCHEMA;
> ok

set autocommit false;
> ok

set schema TEST_SCHEMA;
> ok

create table test(id int, name varchar);
> ok

explain select * from test;
>> SELECT "TEST_SCHEMA"."TEST"."ID", "TEST_SCHEMA"."TEST"."NAME" FROM "TEST_SCHEMA"."TEST" /* TEST_SCHEMA.TEST.tableScan */

explain select * from public.test;
>> SELECT "PUBLIC"."TEST"."ID" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

drop schema TEST_SCHEMA cascade;
> ok

set autocommit true;
> ok

set schema public;
> ok

select * from test;
> ID
> --
> rows: 0

drop table test;
> ok

create table content(thread_id int, parent_id int);
> ok

alter table content add constraint content_parent_id check (parent_id = thread_id) or (parent_id is null) or ( parent_id in (select thread_id from content));
> ok

create index content_thread_id ON content(thread_id);
> ok

insert into content values(0, 0), (0, 0);
> update count: 2

insert into content values(0, 1);
> exception CHECK_CONSTRAINT_VIOLATED_1

insert into content values(1, 1), (2, 2);
> update count: 2

insert into content values(2, 1);
> update count: 1

insert into content values(2, 3);
> exception CHECK_CONSTRAINT_VIOLATED_1

drop table content;
> ok

select x/10 y from system_range(1, 100) group by x/10;
> Y
> --
> 0
> 1
> 10
> 2
> 3
> 4
> 5
> 6
> 7
> 8
> 9
> rows: 11

select timestamp '2001-02-03T10:30:33';
> TIMESTAMP '2001-02-03 10:30:33'
> -------------------------------
> 2001-02-03 10:30:33
> rows: 1

create table test(id int);
> ok

insert into test (select x from system_range(1, 100));
> update count: 100

select id/1000 from test group by id/1000;
> ID / 1000
> ---------
> 0
> rows: 1

select id/(10*100) from test group by id/(10*100);
> ID / 1000
> ---------
> 0
> rows: 1

select id/1000 from test group by id/100;
> exception MUST_GROUP_BY_COLUMN_1

drop table test;
> ok

select (x/10000) from system_range(10, 20) group by (x/10000);
> X / 10000
> ---------
> 0
> rows: 1

select sum(x), (x/10) from system_range(10, 100) group by (x/10);
> SUM(X) X / 10
> ------ ------
> 100    10
> 145    1
> 245    2
> 345    3
> 445    4
> 545    5
> 645    6
> 745    7
> 845    8
> 945    9
> rows: 10

CREATE FORCE VIEW ADDRESS_VIEW AS SELECT * FROM ADDRESS;
> ok

CREATE memory TABLE ADDRESS(ID INT);
> ok

alter view address_view recompile;
> ok

alter view if exists address_view recompile;
> ok

alter view if exists does_not_exist recompile;
> ok

select * from ADDRESS_VIEW;
> ID
> --
> rows: 0

drop view address_view;
> ok

drop table address;
> ok

CREATE ALIAS PARSE_INT2 FOR "java.lang.Integer.parseInt(java.lang.String, int)";
> ok

select min(SUBSTRING(random_uuid(), 15,1)='4') from system_range(1, 10);
> MIN(SUBSTRING(RANDOM_UUID() FROM 15 FOR 1) = '4')
> -------------------------------------------------
> TRUE
> rows: 1

select min(8=bitand(12, PARSE_INT2(SUBSTRING(random_uuid(), 20,1), 16))) from system_range(1, 10);
> MIN(8 = BITAND(12, PUBLIC.PARSE_INT2(SUBSTRING(RANDOM_UUID() FROM 20 FOR 1), 16)))
> ----------------------------------------------------------------------------------
> TRUE
> rows: 1

select BITGET(x, 0) AS IS_SET from system_range(1, 2);
> IS_SET
> ------
> FALSE
> TRUE
> rows: 2

drop alias PARSE_INT2;
> ok

create memory table test(name varchar check(name = upper(name)));
> ok

insert into test values(null);
> update count: 1

insert into test values('aa');
> exception CHECK_CONSTRAINT_VIOLATED_1

insert into test values('AA');
> update count: 1

script nodata nopasswords nosettings noversion;
> SCRIPT
> ---------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "NAME" CHARACTER VARYING );
> -- 2 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" CHECK("NAME" = UPPER("NAME")) NOCHECK;
> rows (ordered): 4

drop table test;
> ok

create domain email as varchar(200) check (position('@' in value) > 1);
> ok

create domain gmail as email default '@gmail.com' check (position('gmail' in value) > 1);
> ok

create memory table address(id int primary key, name email, name2 gmail);
> ok

insert into address(id, name, name2) values(1, 'test@abc', 'test@gmail.com');
> update count: 1

insert into address(id, name, name2) values(2, 'test@abc', 'test@acme');
> exception CHECK_CONSTRAINT_VIOLATED_1

@reconnect

insert into address(id, name, name2) values(3, 'test_abc', 'test@gmail');
> exception CHECK_CONSTRAINT_VIOLATED_1

insert into address2(name) values('test@abc');
> exception TABLE_OR_VIEW_NOT_FOUND_1

CREATE DOMAIN STRING AS VARCHAR(255) DEFAULT '';
> ok

CREATE DOMAIN IF NOT EXISTS STRING AS VARCHAR(255) DEFAULT '';
> ok

CREATE DOMAIN STRING1 AS VARCHAR;
> ok

CREATE DOMAIN STRING2 AS VARCHAR DEFAULT '<empty>';
> ok

create domain string_x as string2;
> ok

create memory table test(a string, b string1, c string2);
> ok

insert into test(b) values('x');
> update count: 1

select * from test;
> A B C
> - - -------
>   x <empty>
> rows: 1

select DOMAIN_NAME, DOMAIN_DEFAULT, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, PARENT_DOMAIN_NAME, REMARKS from information_schema.domains;
> DOMAIN_NAME DOMAIN_DEFAULT DATA_TYPE         CHARACTER_MAXIMUM_LENGTH PARENT_DOMAIN_NAME REMARKS
> ----------- -------------- ----------------- ------------------------ ------------------ -------
> EMAIL       null           CHARACTER VARYING 200                      null               null
> GMAIL       '@gmail.com'   CHARACTER VARYING 200                      EMAIL              null
> STRING      ''             CHARACTER VARYING 255                      null               null
> STRING1     null           CHARACTER VARYING 1000000000               null               null
> STRING2     '<empty>'      CHARACTER VARYING 1000000000               null               null
> STRING_X    null           CHARACTER VARYING 1000000000               STRING2            null
> rows: 6

script nodata nopasswords nosettings noversion;
> SCRIPT
> -------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE DOMAIN "PUBLIC"."EMAIL" AS CHARACTER VARYING(200);
> CREATE DOMAIN "PUBLIC"."STRING" AS CHARACTER VARYING(255) DEFAULT '';
> CREATE DOMAIN "PUBLIC"."STRING1" AS CHARACTER VARYING;
> CREATE DOMAIN "PUBLIC"."STRING2" AS CHARACTER VARYING DEFAULT '<empty>';
> CREATE DOMAIN "PUBLIC"."GMAIL" AS "PUBLIC"."EMAIL" DEFAULT '@gmail.com';
> CREATE DOMAIN "PUBLIC"."STRING_X" AS "PUBLIC"."STRING2";
> CREATE MEMORY TABLE "PUBLIC"."ADDRESS"( "ID" INTEGER NOT NULL, "NAME" "PUBLIC"."EMAIL", "NAME2" "PUBLIC"."GMAIL" );
> ALTER TABLE "PUBLIC"."ADDRESS" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_E" PRIMARY KEY("ID");
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.ADDRESS;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "A" "PUBLIC"."STRING", "B" "PUBLIC"."STRING1", "C" "PUBLIC"."STRING2" );
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER DOMAIN "PUBLIC"."EMAIL" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_3" CHECK(LOCATE('@', VALUE) > 1) NOCHECK;
> ALTER DOMAIN "PUBLIC"."GMAIL" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_4" CHECK(LOCATE('gmail', VALUE) > 1) NOCHECK;
> rows (ordered): 14

drop table test;
> ok

drop domain string;
> ok

drop domain string1;
> ok

drop domain string2 cascade;
> ok

drop domain string_x;
> ok

drop table address;
> ok

drop domain email cascade;
> ok

drop domain gmail;
> ok

create force view address_view as select * from address;
> ok

create table address(id identity, name varchar check instr(value, '@') > 1);
> exception SYNTAX_ERROR_2

create table address(id identity, name varchar check instr(name, '@') > 1);
> ok

drop view if exists address_view;
> ok

drop table address;
> ok

create memory table a(k10 blob(10k), m20 blob(20m), g30 clob(30g));
> ok

SCRIPT NODATA NOPASSWORDS NOSETTINGS NOVERSION DROP;
> SCRIPT
> -----------------------------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> DROP TABLE IF EXISTS "PUBLIC"."A" CASCADE;
> CREATE MEMORY TABLE "PUBLIC"."A"( "K10" BINARY LARGE OBJECT(10240), "M20" BINARY LARGE OBJECT(20971520), "G30" CHARACTER LARGE OBJECT(32212254720) );
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.A;
> rows (ordered): 4

create table b();
> ok

create table c();
> ok

drop table information_schema.columns;
> exception CANNOT_DROP_TABLE_1

create table columns as select * from information_schema.columns;
> ok

create table tables as select * from information_schema.tables where false;
> ok

create table dual2 as select 1 from dual;
> ok

select * from dual2;
> 1
> -
> 1
> rows: 1

drop table dual2, columns, tables;
> ok

drop table a, a;
> ok

drop table b, c;
> ok

CREATE TABLE A (ID_A int primary key);
> ok

CREATE TABLE B (ID_B int primary key);
> ok

CREATE TABLE C (ID_C int primary key);
> ok

insert into A values (1);
> update count: 1

insert into A values (2);
> update count: 1

insert into B values (1);
> update count: 1

insert into C values (1);
> update count: 1

SELECT * FROM C WHERE NOT EXISTS ((SELECT ID_A FROM A) EXCEPT (SELECT ID_B FROM B));
> ID_C
> ----
> rows: 0

(SELECT ID_A FROM A) EXCEPT (SELECT ID_B FROM B);
> ID_A
> ----
> 2
> rows: 1

drop table a;
> ok

drop table b;
> ok

drop table c;
> ok

CREATE TABLE X (ID INTEGER PRIMARY KEY);
> ok

insert into x values(0), (1), (10);
> update count: 3

SELECT t1.ID, (SELECT t1.id || ':' || AVG(t2.ID) FROM X t2) AS col2 FROM X t1;
> ID COL2
> -- ---------------------
> 0  0:3.6666666666666665
> 1  1:3.6666666666666665
> 10 10:3.6666666666666665
> rows: 3

drop table x;
> ok

create table test(id int primary key, name varchar);
> ok

insert into test values(rownum, '11'), (rownum, '22'), (rownum, '33');
> update count: 3

select * from test order by id;
> ID NAME
> -- ----
> 1  11
> 2  22
> 3  33
> rows (ordered): 3

select rownum, (select count(*) from test) as col2, rownum from test;
> ROWNUM() COL2 ROWNUM()
> -------- ---- --------
> 1        3    1
> 2        3    2
> 3        3    3
> rows: 3

delete from test t0 where rownum<2;
> update count: 1

select rownum, * from (select * from test where id>1 order by id desc);
> ROWNUM() ID NAME
> -------- -- ----
> 1        3  33
> 2        2  22
> rows: 2

update test set name='x' where rownum<2;
> update count: 1

select * from test;
> ID NAME
> -- ----
> 2  x
> 3  33
> rows: 2

merge into test values(2, 'r' || rownum), (10, rownum), (11, rownum);
> update count: 3

select * from test;
> ID NAME
> -- ----
> 10 2
> 11 3
> 2  r1
> 3  33
> rows: 4

call rownum;
> ROWNUM()
> --------
> 1
> rows: 1

drop table test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

create index idx_test_name on test(name);
> ok

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

set ignorecase true;
> ok

CREATE TABLE TEST2(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

create unique index idx_test2_name on test2(name);
> ok

INSERT INTO TEST2 VALUES(1, 'hElLo');
> update count: 1

INSERT INTO TEST2 VALUES(2, 'World');
> update count: 1

INSERT INTO TEST2 VALUES(3, 'WoRlD');
> exception DUPLICATE_KEY_1

drop index idx_test2_name;
> ok

select * from test where name='HELLO';
> ID NAME
> -- ----
> rows: 0

select * from test2 where name='HELLO';
> ID NAME
> -- -----
> 1  hElLo
> rows: 1

select * from test where name like 'HELLO';
> ID NAME
> -- ----
> rows: 0

select * from test2 where name like 'HELLO';
> ID NAME
> -- -----
> 1  hElLo
> rows: 1

explain plan for select * from test2, test where test2.name = test.name;
>> SELECT "PUBLIC"."TEST2"."ID", "PUBLIC"."TEST2"."NAME", "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."NAME" FROM "PUBLIC"."TEST2" /* PUBLIC.TEST2.tableScan */ INNER JOIN "PUBLIC"."TEST" /* PUBLIC.IDX_TEST_NAME */ ON 1=1 WHERE "TEST2"."NAME" = "TEST"."NAME"

select * from test2, test where test2.name = test.name;
> ID NAME  ID NAME
> -- ----- -- -----
> 1  hElLo 1  Hello
> 2  World 2  World
> rows: 2

explain plan for select * from test, test2 where test2.name = test.name;
>> SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."NAME", "PUBLIC"."TEST2"."ID", "PUBLIC"."TEST2"."NAME" FROM "PUBLIC"."TEST2" /* PUBLIC.TEST2.tableScan */ INNER JOIN "PUBLIC"."TEST" /* PUBLIC.IDX_TEST_NAME */ ON 1=1 WHERE "TEST2"."NAME" = "TEST"."NAME"

select * from test, test2 where test2.name = test.name;
> ID NAME  ID NAME
> -- ----- -- -----
> 1  Hello 1  hElLo
> 2  World 2  World
> rows: 2

create index idx_test2_name on test2(name);
> ok

explain plan for select * from test2, test where test2.name = test.name;
>> SELECT "PUBLIC"."TEST2"."ID", "PUBLIC"."TEST2"."NAME", "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."NAME" FROM "PUBLIC"."TEST" /* PUBLIC.IDX_TEST_NAME */ INNER JOIN "PUBLIC"."TEST2" /* PUBLIC.IDX_TEST2_NAME: NAME = TEST.NAME */ ON 1=1 WHERE "TEST2"."NAME" = "TEST"."NAME"

select * from test2, test where test2.name = test.name;
> ID NAME  ID NAME
> -- ----- -- -----
> 1  hElLo 1  Hello
> 2  World 2  World
> rows: 2

explain plan for select * from test, test2 where test2.name = test.name;
>> SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."NAME", "PUBLIC"."TEST2"."ID", "PUBLIC"."TEST2"."NAME" FROM "PUBLIC"."TEST" /* PUBLIC.IDX_TEST_NAME */ INNER JOIN "PUBLIC"."TEST2" /* PUBLIC.IDX_TEST2_NAME: NAME = TEST.NAME */ ON 1=1 WHERE "TEST2"."NAME" = "TEST"."NAME"

select * from test, test2 where test2.name = test.name;
> ID NAME  ID NAME
> -- ----- -- -----
> 1  Hello 1  hElLo
> 2  World 2  World
> rows: 2

DROP TABLE IF EXISTS TEST;
> ok

DROP TABLE IF EXISTS TEST2;
> ok

set ignorecase false;
> ok

create table test(f1 varchar, f2 varchar);
> ok

insert into test values('abc','222');
> update count: 1

insert into test values('abc','111');
> update count: 1

insert into test values('abc','333');
> update count: 1

SELECT t.f1, t.f2 FROM test t ORDER BY t.f2;
> F1  F2
> --- ---
> abc 111
> abc 222
> abc 333
> rows (ordered): 3

SELECT t1.f1, t1.f2, t2.f1, t2.f2 FROM test t1, test t2 ORDER BY t2.f2, t1.f2;
> F1  F2  F1  F2
> --- --- --- ---
> abc 111 abc 111
> abc 222 abc 111
> abc 333 abc 111
> abc 111 abc 222
> abc 222 abc 222
> abc 333 abc 222
> abc 111 abc 333
> abc 222 abc 333
> abc 333 abc 333
> rows (ordered): 9

drop table if exists test;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

explain select t0.id, t1.id from test t0, test t1 order by t0.id, t1.id;
>> SELECT "T0"."ID", "T1"."ID" FROM "PUBLIC"."TEST" "T0" /* PUBLIC.PRIMARY_KEY_2 */ INNER JOIN "PUBLIC"."TEST" "T1" /* PUBLIC.TEST.tableScan */ ON 1=1 ORDER BY 1, 2 /* index sorted: 1 of 2 columns */

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

SELECT id, sum(id) FROM test GROUP BY id ORDER BY id*sum(id);
> ID SUM(ID)
> -- -------
> 1  1
> 2  2
> rows (ordered): 2

select *
from test t1
inner join test t2 on t2.id=t1.id
inner join test t3 on t3.id=t2.id
where exists (select 1 from test t4 where t2.id=t4.id);
> ID NAME  ID NAME  ID NAME
> -- ----- -- ----- -- -----
> 1  Hello 1  Hello 1  Hello
> 2  World 2  World 2  World
> rows: 2

explain select * from test t1 where id in(select id from test t2 where t1.id=t2.id);
>> SELECT "T1"."ID", "T1"."NAME" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.TEST.tableScan */ WHERE "ID" IN( SELECT DISTINCT "ID" FROM "PUBLIC"."TEST" "T2" /* PUBLIC.PRIMARY_KEY_2: ID = T1.ID */ WHERE "T1"."ID" = "T2"."ID")

select * from test t1 where id in(select id from test t2 where t1.id=t2.id);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

explain select * from test t1 where id in(id, id+1);
>> SELECT "T1"."ID", "T1"."NAME" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.TEST.tableScan */ WHERE "ID" IN("ID", "ID" + 1)

select * from test t1 where id in(id, id+1);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

explain select * from test t1 where id in(id);
>> SELECT "T1"."ID", "T1"."NAME" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.TEST.tableScan */ WHERE "ID" = "ID"

select * from test t1 where id in(id);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

explain select * from test t1 where id in(select id from test);
>> SELECT "T1"."ID", "T1"."NAME" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.PRIMARY_KEY_2: ID IN(SELECT DISTINCT ID FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */) */ WHERE "ID" IN( SELECT DISTINCT "ID" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */)

select * from test t1 where id in(select id from test);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

explain select * from test t1 where id in(1, select max(id) from test);
>> SELECT "T1"."ID", "T1"."NAME" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.PRIMARY_KEY_2: ID IN(1, (SELECT MAX(ID) FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */ /* direct lookup */)) */ WHERE "ID" IN(1, (SELECT MAX("ID") FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ /* direct lookup */))

select * from test t1 where id in(1, select max(id) from test);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

explain select * from test t1 where id in(1, select max(id) from test t2 where t1.id=t2.id);
>> SELECT "T1"."ID", "T1"."NAME" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.TEST.tableScan */ WHERE "ID" IN(1, (SELECT MAX("ID") FROM "PUBLIC"."TEST" "T2" /* PUBLIC.PRIMARY_KEY_2: ID = T1.ID */ WHERE "T1"."ID" = "T2"."ID"))

select * from test t1 where id in(1, select max(id) from test t2 where t1.id=t2.id);
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows: 2

DROP TABLE TEST;
> ok

create force view t1 as select * from t1;
> ok

select * from t1;
> exception VIEW_IS_INVALID_2

drop table t1;
> ok

CREATE TABLE TEST(id INT PRIMARY KEY, foo BIGINT);
> ok

INSERT INTO TEST VALUES(1, 100);
> update count: 1

INSERT INTO TEST VALUES(2, 123456789012345678);
> update count: 1

SELECT * FROM TEST WHERE foo = 123456789014567;
> ID FOO
> -- ---
> rows: 0

DROP TABLE IF EXISTS TEST;
> ok

create table test(id int);
> ok

insert into test values(1), (2), (3), (4);
> update count: 4

(select * from test a, test b) minus (select * from test a, test b);
> ID ID
> -- --
> rows: 0

drop table test;
> ok

call select 1.0/3.0*3.0, 100.0/2.0, -25.0/100.0, 0.0/3.0, 6.9/2.0, 0.72179425150347250912311550800000 / 5314251955.21;
> ROW (0.99990, 50.0000, -0.25000000, 0.0000, 3.4500, 0.000000000135822361752313607260107721120531135706133162)
> -------------------------------------------------------------------------------------------------------------
> ROW (0.99990, 50.0000, -0.25000000, 0.0000, 3.4500, 0.000000000135822361752313607260107721120531135706133162)
> rows: 1

create sequence test_seq;
> ok

create table test(id int primary key, parent int);
> ok

create index ni on test(parent);
> ok

alter table test add constraint nu unique(parent);
> ok

alter table test add constraint fk foreign key(parent) references(id);
> ok

SELECT TABLE_NAME, INDEX_NAME, INDEX_TYPE_NAME FROM INFORMATION_SCHEMA.INDEXES;
> TABLE_NAME INDEX_NAME    INDEX_TYPE_NAME
> ---------- ------------- ---------------
> TEST       NI            INDEX
> TEST       NU_INDEX_2    UNIQUE INDEX
> TEST       PRIMARY_KEY_2 PRIMARY KEY
> rows: 3

SELECT TABLE_NAME, INDEX_NAME, ORDINAL_POSITION, COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS;
> TABLE_NAME INDEX_NAME    ORDINAL_POSITION COLUMN_NAME
> ---------- ------------- ---------------- -----------
> TEST       NI            1                PARENT
> TEST       NU_INDEX_2    1                PARENT
> TEST       PRIMARY_KEY_2 1                ID
> rows: 3

select SEQUENCE_NAME, BASE_VALUE, INCREMENT, REMARKS from INFORMATION_SCHEMA.SEQUENCES;
> SEQUENCE_NAME BASE_VALUE INCREMENT REMARKS
> ------------- ---------- --------- -------
> TEST_SEQ      1          1         null
> rows: 1

drop table test;
> ok

drop sequence test_seq;
> ok

create table test(id int);
> ok

insert into test values(1), (2);
> update count: 2

select count(*) from test where id in ((select id from test where 1=0));
> COUNT(*)
> --------
> 0
> rows: 1

select count(*) from test where id = ((select id from test where 1=0)+1);
> COUNT(*)
> --------
> 0
> rows: 1

select count(*) from test where id = (select id from test where 1=0);
> COUNT(*)
> --------
> 0
> rows: 1

select count(*) from test where id in ((select id from test));
> COUNT(*)
> --------
> 2
> rows: 1

select count(*) from test where id = ((select id from test));
> exception SCALAR_SUBQUERY_CONTAINS_MORE_THAN_ONE_ROW

select count(*) from test where id = ARRAY [(select id from test), 1];
> exception TYPES_ARE_NOT_COMPARABLE_2

select count(*) from test where id = ((select id from test fetch first row only), 1);
> exception TYPES_ARE_NOT_COMPARABLE_2

select (select id from test where 1=0) from test;
> (SELECT ID FROM PUBLIC.TEST WHERE FALSE)
> ----------------------------------------
> null
> null
> rows: 2

drop table test;
> ok

create table test(id int primary key, a boolean);
> ok

insert into test values(1, 'Y');
> update count: 1

call select a from test order by id;
> (SELECT A FROM PUBLIC.TEST ORDER BY ID)
> ---------------------------------------
> TRUE
> rows (ordered): 1

select select a from test order by id;
> (SELECT A FROM PUBLIC.TEST ORDER BY ID)
> ---------------------------------------
> TRUE
> rows: 1

insert into test values(2, 'N');
> update count: 1

insert into test values(3, '1');
> update count: 1

insert into test values(4, '0');
> update count: 1

insert into test values(5, 'T');
> update count: 1

insert into test values(6, 'F');
> update count: 1

select max(id) from test where id = max(id) group by id;
> exception INVALID_USE_OF_AGGREGATE_FUNCTION_1

select * from test where a=TRUE=a;
> ID A
> -- -----
> 1  TRUE
> 2  FALSE
> 3  TRUE
> 4  FALSE
> 5  TRUE
> 6  FALSE
> rows: 6

drop table test;
> ok

CREATE memory TABLE TEST(ID INT PRIMARY KEY, PARENT INT REFERENCES TEST);
> ok

CREATE memory TABLE s(S_NO VARCHAR(5) PRIMARY KEY, name VARCHAR(16), city VARCHAR(16));
> ok

CREATE memory TABLE p(p_no VARCHAR(5) PRIMARY KEY, descr VARCHAR(16), color VARCHAR(8));
> ok

CREATE memory TABLE sp1(S_NO VARCHAR(5) REFERENCES s, p_no VARCHAR(5) REFERENCES p, qty INT, PRIMARY KEY (S_NO, p_no));
> ok

CREATE memory TABLE sp2(S_NO VARCHAR(5), p_no VARCHAR(5), qty INT, constraint c1 FOREIGN KEY (S_NO) references s, PRIMARY KEY (S_NO, p_no));
> ok

script NOPASSWORDS NOSETTINGS noversion;
> SCRIPT
> --------------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL, "PARENT" INTEGER );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE "PUBLIC"."S"( "S_NO" CHARACTER VARYING(5) NOT NULL, "NAME" CHARACTER VARYING(16), "CITY" CHARACTER VARYING(16) );
> ALTER TABLE "PUBLIC"."S" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_5" PRIMARY KEY("S_NO");
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.S;
> CREATE MEMORY TABLE "PUBLIC"."P"( "P_NO" CHARACTER VARYING(5) NOT NULL, "DESCR" CHARACTER VARYING(16), "COLOR" CHARACTER VARYING(8) );
> ALTER TABLE "PUBLIC"."P" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_50" PRIMARY KEY("P_NO");
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.P;
> CREATE MEMORY TABLE "PUBLIC"."SP1"( "S_NO" CHARACTER VARYING(5) NOT NULL, "P_NO" CHARACTER VARYING(5) NOT NULL, "QTY" INTEGER );
> ALTER TABLE "PUBLIC"."SP1" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_141" PRIMARY KEY("S_NO", "P_NO");
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.SP1;
> CREATE MEMORY TABLE "PUBLIC"."SP2"( "S_NO" CHARACTER VARYING(5) NOT NULL, "P_NO" CHARACTER VARYING(5) NOT NULL, "QTY" INTEGER );
> ALTER TABLE "PUBLIC"."SP2" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_1417" PRIMARY KEY("S_NO", "P_NO");
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.SP2;
> ALTER TABLE "PUBLIC"."SP1" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_1" FOREIGN KEY("S_NO") REFERENCES "PUBLIC"."S"("S_NO") NOCHECK;
> ALTER TABLE "PUBLIC"."SP1" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_14" FOREIGN KEY("P_NO") REFERENCES "PUBLIC"."P"("P_NO") NOCHECK;
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_27" FOREIGN KEY("PARENT") REFERENCES "PUBLIC"."TEST"("ID") NOCHECK;
> ALTER TABLE "PUBLIC"."SP2" ADD CONSTRAINT "PUBLIC"."C1" FOREIGN KEY("S_NO") REFERENCES "PUBLIC"."S"("S_NO") NOCHECK;
> rows (ordered): 20

drop table test;
> ok

drop table sp1;
> ok

drop table sp2;
> ok

drop table s;
> ok

drop table p;
> ok

create table test (id identity, "VALUE" int not null);
> ok

alter table test add primary key(id);
> exception SECOND_PRIMARY_KEY

alter table test drop primary key;
> ok

alter table test drop primary key;
> exception INDEX_NOT_FOUND_1

alter table test add primary key(id, id, id);
> ok

alter table test drop primary key;
> ok

drop table test;
> ok

set autocommit off;
> ok

create local temporary table test (id identity, b int, foreign key(b) references(id));
> ok

drop table test;
> ok

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION DROP;
> SCRIPT
> -------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> rows (ordered): 1

create local temporary table test1 (id identity);
> ok

create local temporary table test2 (id identity);
> ok

alter table test2 add constraint test2_test1 foreign key (id) references test1;
> ok

drop table test1, test2;
> ok

create local temporary table test1 (id identity);
> ok

create local temporary table test2 (id identity);
> ok

alter table test2 add constraint test2_test1 foreign key (id) references test1;
> ok

drop table test1, test2;
> ok

set autocommit on;
> ok

create table test(id int primary key, ref int, foreign key(ref) references(id));
> ok

insert into test values(1, 1), (2, 2);
> update count: 2

update test set ref=3-ref;
> update count: 2

alter table test add column dummy int;
> ok

insert into test values(4, 4, null);
> update count: 1

drop table test;
> ok

create table test(id int primary key);
> ok

-- Column A.ID cannot be referenced here
explain select * from test a inner join test b left outer join test c on c.id = a.id;
> exception COLUMN_NOT_FOUND_1

SELECT T.ID FROM TEST "T";
> ID
> --
> rows: 0

SELECT T."ID" FROM TEST "T";
> ID
> --
> rows: 0

SELECT "T".ID FROM TEST "T";
> ID
> --
> rows: 0

SELECT "T"."ID" FROM TEST "T";
> ID
> --
> rows: 0

SELECT T.ID FROM "TEST" T;
> ID
> --
> rows: 0

SELECT T."ID" FROM "TEST" T;
> ID
> --
> rows: 0

SELECT "T".ID FROM "TEST" T;
> ID
> --
> rows: 0

SELECT "T"."ID" FROM "TEST" T;
> ID
> --
> rows: 0

SELECT T.ID FROM "TEST" "T";
> ID
> --
> rows: 0

SELECT T."ID" FROM "TEST" "T";
> ID
> --
> rows: 0

SELECT "T".ID FROM "TEST" "T";
> ID
> --
> rows: 0

SELECT "T"."ID" FROM "TEST" "T";
> ID
> --
> rows: 0

select "TEST".id from test;
> ID
> --
> rows: 0

select test."ID" from test;
> ID
> --
> rows: 0

select test."id" from test;
> exception COLUMN_NOT_FOUND_1

select "TEST"."ID" from test;
> ID
> --
> rows: 0

select "test"."ID" from test;
> exception COLUMN_NOT_FOUND_1

select public."TEST".id from test;
> ID
> --
> rows: 0

select public.test."ID" from test;
> ID
> --
> rows: 0

select public."TEST"."ID" from test;
> ID
> --
> rows: 0

select public."test"."ID" from test;
> exception COLUMN_NOT_FOUND_1

select "PUBLIC"."TEST".id from test;
> ID
> --
> rows: 0

select "PUBLIC".test."ID" from test;
> ID
> --
> rows: 0

select public."TEST"."ID" from test;
> ID
> --
> rows: 0

select "public"."TEST"."ID" from test;
> exception COLUMN_NOT_FOUND_1

drop table test;
> ok

create schema s authorization sa;
> ok

create memory table s.test(id int);
> ok

create index if not exists idx_id on s.test(id);
> ok

create index if not exists idx_id on s.test(id);
> ok

alter index s.idx_id rename to s.x;
> ok

alter index if exists s.idx_id rename to s.x;
> ok

alter index if exists s.x rename to s.index_id;
> ok

alter table s.test add constraint cu_id unique(id);
> ok

alter table s.test add name varchar;
> ok

alter table s.test drop column name;
> ok

alter table s.test drop constraint cu_id;
> ok

alter table s.test rename to testtab;
> ok

alter table s.testtab rename to test;
> ok

create trigger test_trigger before insert on s.test call 'org.h2.test.db.TestTriggersConstraints';
> ok

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION DROP;
> SCRIPT
> -----------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE SCHEMA IF NOT EXISTS "S" AUTHORIZATION "SA";
> DROP TABLE IF EXISTS "S"."TEST" CASCADE;
> CREATE MEMORY TABLE "S"."TEST"( "ID" INTEGER );
> -- 0 +/- SELECT COUNT(*) FROM S.TEST;
> CREATE INDEX "S"."INDEX_ID" ON "S"."TEST"("ID" NULLS FIRST);
> CREATE FORCE TRIGGER "S"."TEST_TRIGGER" BEFORE INSERT ON "S"."TEST" QUEUE 1024 CALL 'org.h2.test.db.TestTriggersConstraints';
> rows (ordered): 7

drop trigger s.test_trigger;
> ok

drop schema s cascade;
> ok

CREATE MEMORY TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255), y int as id+1);
> ok

INSERT INTO TEST(id, name) VALUES(1, 'Hello');
> update count: 1

create index idx_n_id on test(name, id);
> ok

alter table test add constraint abc foreign key(id) references (id);
> ok

alter table test rename column id to i;
> ok

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION DROP;
> SCRIPT
> --------------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> DROP TABLE IF EXISTS "PUBLIC"."TEST" CASCADE;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "I" INTEGER NOT NULL, "NAME" CHARACTER VARYING(255), "Y" INTEGER GENERATED ALWAYS AS ("I" + 1) );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("I");
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST"("I", "NAME") VALUES (1, 'Hello');
> CREATE INDEX "PUBLIC"."IDX_N_ID" ON "PUBLIC"."TEST"("NAME" NULLS FIRST, "I" NULLS FIRST);
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."ABC" FOREIGN KEY("I") REFERENCES "PUBLIC"."TEST"("I") NOCHECK;
> rows (ordered): 8

INSERT INTO TEST(i, name) VALUES(2, 'World');
> update count: 1

SELECT * FROM TEST ORDER BY I;
> I NAME  Y
> - ----- -
> 1 Hello 2
> 2 World 3
> rows (ordered): 2

UPDATE TEST SET NAME='Hi' WHERE I=1;
> update count: 1

DELETE FROM TEST t0 WHERE t0.I=2;
> update count: 1

drop table test;
> ok

create table test(current int);
> ok

select current from test;
> CURRENT
> -------
> rows: 0

drop table test;
> ok

CREATE table my_table(my_int integer, my_char varchar);
> ok

INSERT INTO my_table VALUES(1, 'Testing');
> update count: 1

ALTER TABLE my_table ALTER COLUMN my_int RENAME to my_new_int;
> ok

SELECT my_new_int FROM my_table;
> MY_NEW_INT
> ----------
> 1
> rows: 1

UPDATE my_table SET my_new_int = 33;
> update count: 1

SELECT * FROM my_table;
> MY_NEW_INT MY_CHAR
> ---------- -------
> 33         Testing
> rows: 1

DROP TABLE my_table;
> ok

create sequence seq1;
> ok

create table test(ID INT default next value for seq1);
> ok

drop sequence seq1;
> exception CANNOT_DROP_2

alter table test add column name varchar;
> ok

insert into test(name) values('Hello');
> update count: 1

select * from test;
> ID NAME
> -- -----
> 1  Hello
> rows: 1

drop table test;
> ok

drop sequence seq1;
> ok

create table test(a int primary key, b int, c int);
> ok

alter table test add constraint unique_ba unique(b, a);
> ok

alter table test add constraint abc foreign key(c, a) references test(b, a);
> ok

insert into test values(1, 1, null);
> update count: 1

drop table test;
> ok

create table ADDRESS (ADDRESS_ID int primary key, ADDRESS_TYPE int not null, SERVER_ID int not null);
> ok

alter table address add constraint unique_a unique(ADDRESS_TYPE, SERVER_ID);
> ok

create table SERVER (SERVER_ID int primary key, SERVER_TYPE int not null, ADDRESS_TYPE int);
> ok

alter table ADDRESS add constraint addr foreign key (SERVER_ID) references SERVER;
> ok

alter table SERVER add constraint server_const foreign key (ADDRESS_TYPE, SERVER_ID) references ADDRESS (ADDRESS_TYPE, SERVER_ID);
> ok

insert into SERVER (SERVER_ID, SERVER_TYPE) values (1, 1);
> update count: 1

drop table address, server;
> ok

CREATE TABLE PlanElements(id int primary key, name varchar, parent_id int, foreign key(parent_id) references(id) on delete cascade);
> ok

INSERT INTO PlanElements(id,name,parent_id) VALUES(1, '#1', null), (2, '#1-A', 1), (3, '#1-A-1', 2), (4, '#1-A-2', 2);
> update count: 4

INSERT INTO PlanElements(id,name,parent_id) VALUES(5, '#1-B', 1), (6, '#1-B-1', 5), (7, '#1-B-2', 5);
> update count: 3

INSERT INTO PlanElements(id,name,parent_id) VALUES(8, '#1-C', 1), (9, '#1-C-1', 8), (10, '#1-C-2', 8);
> update count: 3

INSERT INTO PlanElements(id,name,parent_id) VALUES(11, '#1-D', 1), (12, '#1-D-1', 11), (13, '#1-D-2', 11), (14, '#1-D-3', 11);
> update count: 4

INSERT INTO PlanElements(id,name,parent_id) VALUES(15, '#1-E', 1), (16, '#1-E-1', 15), (17, '#1-E-2', 15), (18, '#1-E-3', 15), (19, '#1-E-4', 15);
> update count: 5

DELETE FROM PlanElements WHERE id = 1;
> update count: 1

SELECT * FROM PlanElements;
> ID NAME PARENT_ID
> -- ---- ---------
> rows: 0

DROP TABLE PlanElements;
> ok

CREATE TABLE PARENT(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE TABLE CHILD(ID INT PRIMARY KEY, NAME VARCHAR(255), FOREIGN KEY(NAME) REFERENCES PARENT(ID));
> ok

INSERT INTO PARENT VALUES(1, '1');
> update count: 1

INSERT INTO CHILD VALUES(1, '1');
> update count: 1

INSERT INTO CHILD VALUES(2, 'Hello');
> exception DATA_CONVERSION_ERROR_1

DROP TABLE IF EXISTS CHILD;
> ok

DROP TABLE IF EXISTS PARENT;
> ok

DECLARE GLOBAL TEMPORARY TABLE TEST(ID INT PRIMARY KEY);
> ok

SELECT * FROM TEST;
> ID
> --
> rows: 0

SELECT GROUP_CONCAT(ID) FROM TEST;
> LISTAGG(ID) WITHIN GROUP (ORDER BY NULL)
> ----------------------------------------
> null
> rows: 1

SELECT * FROM SESSION.TEST;
> ID
> --
> rows: 0

DROP TABLE TEST;
> ok

VALUES(1, 2);
> C1 C2
> -- --
> 1  2
> rows: 1

DROP TABLE IF EXISTS TEST;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

SELECT group_concat(name) FROM TEST group by id;
> LISTAGG(NAME) WITHIN GROUP (ORDER BY NULL)
> ------------------------------------------
> Hello
> World
> rows: 2

drop table test;
> ok

--- script drop ---------------------------------------------------------------------------------------------
create memory table test (id int primary key, im_ie varchar(10));
> ok

create sequence test_seq;
> ok

SCRIPT NODATA NOPASSWORDS NOSETTINGS NOVERSION DROP;
> SCRIPT
> --------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> DROP TABLE IF EXISTS "PUBLIC"."TEST" CASCADE;
> DROP SEQUENCE IF EXISTS "PUBLIC"."TEST_SEQ";
> CREATE SEQUENCE "PUBLIC"."TEST_SEQ" START WITH 1;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL, "IM_IE" CHARACTER VARYING(10) );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> rows (ordered): 7

drop sequence test_seq;
> ok

drop table test;
> ok

--- constraints ---------------------------------------------------------------------------------------------
CREATE MEMORY TABLE TEST(ID IDENTITY(100, 10), NAME VARCHAR);
> ok

INSERT INTO TEST(NAME) VALUES('Hello'), ('World');
> update count: 2

SELECT * FROM TEST;
> ID  NAME
> --- -----
> 100 Hello
> 110 World
> rows: 2

DROP TABLE TEST;
> ok

CREATE CACHED TABLE account(
id INTEGER GENERATED BY DEFAULT AS IDENTITY,
name VARCHAR NOT NULL,
mail_address VARCHAR NOT NULL,
UNIQUE(name),
PRIMARY KEY(id)
);
> ok

CREATE CACHED TABLE label(
id INTEGER GENERATED BY DEFAULT AS IDENTITY,
parent_id INTEGER NOT NULL,
account_id INTEGER NOT NULL,
name VARCHAR NOT NULL,
PRIMARY KEY(id),
UNIQUE(parent_id, name),
UNIQUE(id, account_id),
FOREIGN KEY(account_id) REFERENCES account (id),
FOREIGN KEY(parent_id, account_id) REFERENCES label (id, account_id)
);
> ok

INSERT INTO account VALUES (0, 'example', 'example@example.com');
> update count: 1

INSERT INTO label VALUES ( 0, 0, 0, 'TEST');
> update count: 1

INSERT INTO label VALUES ( 1, 0, 0, 'TEST');
> exception DUPLICATE_KEY_1

INSERT INTO label VALUES ( 1, 0, 0, 'TEST1');
> update count: 1

INSERT INTO label VALUES ( 2, 2, 1, 'TEST');
> exception REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1

drop table label;
> ok

drop table account;
> ok

--- constraints and alter table add column ---------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, PARENTID INT, FOREIGN KEY(PARENTID) REFERENCES(ID));
> ok

INSERT INTO TEST VALUES(0, 0);
> update count: 1

ALTER TABLE TEST ADD COLUMN CHILD_ID INT;
> ok

ALTER TABLE TEST ALTER COLUMN CHILD_ID VARCHAR;
> ok

ALTER TABLE TEST ALTER COLUMN PARENTID VARCHAR;
> ok

ALTER TABLE TEST DROP COLUMN PARENTID;
> ok

ALTER TABLE TEST DROP COLUMN CHILD_ID;
> ok

SELECT * FROM TEST;
> ID
> --
> 0
> rows: 1

DROP TABLE TEST;
> ok

CREATE MEMORY TABLE A(X INT PRIMARY KEY);
> ok

CREATE MEMORY TABLE B(XX INT, CONSTRAINT B2A FOREIGN KEY(XX) REFERENCES A(X));
> ok

CREATE MEMORY TABLE C(X_MASTER INT PRIMARY KEY);
> ok

ALTER TABLE A ADD CONSTRAINT A2C FOREIGN KEY(X) REFERENCES C(X_MASTER);
> ok

insert into c values(1);
> update count: 1

insert into a values(1);
> update count: 1

insert into b values(1);
> update count: 1

ALTER TABLE A ADD COLUMN Y INT;
> ok

insert into c values(2);
> update count: 1

insert into a values(2, 2);
> update count: 1

insert into b values(2);
> update count: 1

DROP TABLE IF EXISTS A, B, C;
> ok

--- quoted keywords ---------------------------------------------------------------------------------------------
CREATE TABLE "CREATE"("SELECT" INT, "PRIMARY" INT, "KEY" INT, "INDEX" INT, "ROWNUM" INT, "NEXTVAL" INT, "FROM" INT);
> ok

INSERT INTO "CREATE" default values;
> update count: 1

INSERT INTO "CREATE" default values;
> update count: 1

SELECT "ROWNUM", ROWNUM, "SELECT" "AS", "PRIMARY" AS "X", "KEY", "NEXTVAL", "INDEX", "SELECT" "FROM" FROM "CREATE";
> ROWNUM ROWNUM() AS   X    KEY  NEXTVAL INDEX FROM
> ------ -------- ---- ---- ---- ------- ----- ----
> null   1        null null null null    null  null
> null   2        null null null null    null  null
> rows: 2

DROP TABLE "CREATE";
> ok

CREATE TABLE PARENT(ID INT PRIMARY KEY, NAME VARCHAR);
> ok

CREATE TABLE CHILD(ID INT, PARENTID INT, FOREIGN KEY(PARENTID) REFERENCES PARENT(ID));
> ok

INSERT INTO PARENT VALUES(1, 'Mary'), (2, 'John');
> update count: 2

INSERT INTO CHILD VALUES(10, 1), (11, 1), (20, 2), (21, 2);
> update count: 4

MERGE INTO PARENT KEY(ID) VALUES(1, 'Marcy');
> update count: 1

SELECT * FROM PARENT;
> ID NAME
> -- -----
> 1  Marcy
> 2  John
> rows: 2

SELECT * FROM CHILD;
> ID PARENTID
> -- --------
> 10 1
> 11 1
> 20 2
> 21 2
> rows: 4

DROP TABLE PARENT, CHILD;
> ok

---
create table STRING_TEST(label varchar(31), label2 varchar(255));
> ok

create table STRING_TEST_ic(label varchar_ignorecase(31), label2
varchar_ignorecase(255));
> ok

insert into STRING_TEST values('HELLO','Bye');
> update count: 1

insert into STRING_TEST values('HELLO','Hello');
> update count: 1

insert into STRING_TEST_ic select * from STRING_TEST;
> update count: 2

-- Expect rows of STRING_TEST_ic and STRING_TEST to be identical
select * from STRING_TEST;
> LABEL LABEL2
> ----- ------
> HELLO Bye
> HELLO Hello
> rows: 2

-- correct
select * from STRING_TEST_ic;
> LABEL LABEL2
> ----- ------
> HELLO Bye
> HELLO Hello
> rows: 2

drop table STRING_TEST;
> ok

drop table STRING_TEST_ic;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR_IGNORECASE);
> ok

INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World'), (3, 'hallo'), (4, 'hoi');
> update count: 4

SELECT * FROM TEST WHERE NAME = 'HELLO';
> ID NAME
> -- -----
> 1  Hello
> rows: 1

SELECT * FROM TEST WHERE NAME = 'HE11O';
> ID NAME
> -- ----
> rows: 0

SELECT * FROM TEST ORDER BY NAME;
> ID NAME
> -- -----
> 3  hallo
> 1  Hello
> 4  hoi
> 2  World
> rows (ordered): 4

DROP TABLE IF EXISTS TEST;
> ok

--- update with list ---------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

SELECT * FROM TEST ORDER BY ID;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows (ordered): 2

UPDATE TEST t0 SET t0.NAME='Hi' WHERE t0.ID=1;
> update count: 1

update test set (id, name)=(id+1, name || 'Hi');
> update count: 2

update test set (id, name)=(select id+1, name || 'Ho' from test t1 where test.id=t1.id);
> update count: 2

explain update test set (id, name)=(id+1, name || 'Hi');
>> UPDATE "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ SET "ID" = "ID" + 1, "NAME" = "NAME" || 'Hi'

explain update test set (id, name)=(select id+1, name || 'Ho' from test t1 where test.id=t1.id);
>> UPDATE "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ SET ("ID", "NAME") = (SELECT "ID" + 1, "NAME" || 'Ho' FROM "PUBLIC"."TEST" "T1" /* PUBLIC.PRIMARY_KEY_2: ID = TEST.ID */ WHERE "TEST"."ID" = "T1"."ID")

select * from test;
> ID NAME
> -- ---------
> 3  HiHiHo
> 4  WorldHiHo
> rows: 2

DROP TABLE IF EXISTS TEST;
> ok

--- script ---------------------------------------------------------------------------------------------
create memory table test(id int primary key, c clob, b blob);
> ok

insert into test values(0, null, null);
> update count: 1

insert into test values(1, '', '');
> update count: 1

insert into test values(2, 'Cafe', X'cafe');
> update count: 1

script simple nopasswords nosettings noversion;
> SCRIPT
> ------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL, "C" CHARACTER LARGE OBJECT, "B" BINARY LARGE OBJECT );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> -- 3 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST" VALUES(0, NULL, NULL);
> INSERT INTO "PUBLIC"."TEST" VALUES(1, '', X'');
> INSERT INTO "PUBLIC"."TEST" VALUES(2, 'Cafe', X'cafe');
> rows (ordered): 7

drop table test;
> ok

--- optimizer ---------------------------------------------------------------------------------------------
create table b(id int primary key, p int);
> ok

create index bp on b(p);
> ok

insert into b values(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9);
> update count: 10

insert into b select id+10, p+10 from b;
> update count: 10

explain select * from b b0, b b1, b b2 where b1.p = b0.id and b2.p = b1.id and b0.id=10;
>> SELECT "B0"."ID", "B0"."P", "B1"."ID", "B1"."P", "B2"."ID", "B2"."P" FROM "PUBLIC"."B" "B0" /* PUBLIC.PRIMARY_KEY_4: ID = 10 */ /* WHERE B0.ID = 10 */ INNER JOIN "PUBLIC"."B" "B1" /* PUBLIC.BP: P = B0.ID */ ON 1=1 /* WHERE B1.P = B0.ID */ INNER JOIN "PUBLIC"."B" "B2" /* PUBLIC.BP: P = B1.ID */ ON 1=1 WHERE ("B0"."ID" = 10) AND ("B1"."P" = "B0"."ID") AND ("B2"."P" = "B1"."ID")

explain select * from b b0, b b1, b b2, b b3 where b1.p = b0.id and b2.p = b1.id and b3.p = b2.id and b0.id=10;
>> SELECT "B0"."ID", "B0"."P", "B1"."ID", "B1"."P", "B2"."ID", "B2"."P", "B3"."ID", "B3"."P" FROM "PUBLIC"."B" "B0" /* PUBLIC.PRIMARY_KEY_4: ID = 10 */ /* WHERE B0.ID = 10 */ INNER JOIN "PUBLIC"."B" "B1" /* PUBLIC.BP: P = B0.ID */ ON 1=1 /* WHERE B1.P = B0.ID */ INNER JOIN "PUBLIC"."B" "B2" /* PUBLIC.BP: P = B1.ID */ ON 1=1 /* WHERE B2.P = B1.ID */ INNER JOIN "PUBLIC"."B" "B3" /* PUBLIC.BP: P = B2.ID */ ON 1=1 WHERE ("B0"."ID" = 10) AND ("B3"."P" = "B2"."ID") AND ("B1"."P" = "B0"."ID") AND ("B2"."P" = "B1"."ID")

explain select * from b b0, b b1, b b2, b b3, b b4 where b1.p = b0.id and b2.p = b1.id and b3.p = b2.id and b4.p = b3.id and b0.id=10;
>> SELECT "B0"."ID", "B0"."P", "B1"."ID", "B1"."P", "B2"."ID", "B2"."P", "B3"."ID", "B3"."P", "B4"."ID", "B4"."P" FROM "PUBLIC"."B" "B0" /* PUBLIC.PRIMARY_KEY_4: ID = 10 */ /* WHERE B0.ID = 10 */ INNER JOIN "PUBLIC"."B" "B1" /* PUBLIC.BP: P = B0.ID */ ON 1=1 /* WHERE B1.P = B0.ID */ INNER JOIN "PUBLIC"."B" "B2" /* PUBLIC.BP: P = B1.ID */ ON 1=1 /* WHERE B2.P = B1.ID */ INNER JOIN "PUBLIC"."B" "B3" /* PUBLIC.BP: P = B2.ID */ ON 1=1 /* WHERE B3.P = B2.ID */ INNER JOIN "PUBLIC"."B" "B4" /* PUBLIC.BP: P = B3.ID */ ON 1=1 WHERE ("B0"."ID" = 10) AND ("B3"."P" = "B2"."ID") AND ("B4"."P" = "B3"."ID") AND ("B1"."P" = "B0"."ID") AND ("B2"."P" = "B1"."ID")

analyze;
> ok

explain select * from b b0, b b1, b b2, b b3, b b4 where b1.p = b0.id and b2.p = b1.id and b3.p = b2.id and b4.p = b3.id and b0.id=10;
>> SELECT "B0"."ID", "B0"."P", "B1"."ID", "B1"."P", "B2"."ID", "B2"."P", "B3"."ID", "B3"."P", "B4"."ID", "B4"."P" FROM "PUBLIC"."B" "B0" /* PUBLIC.PRIMARY_KEY_4: ID = 10 */ /* WHERE B0.ID = 10 */ INNER JOIN "PUBLIC"."B" "B1" /* PUBLIC.BP: P = B0.ID */ ON 1=1 /* WHERE B1.P = B0.ID */ INNER JOIN "PUBLIC"."B" "B2" /* PUBLIC.BP: P = B1.ID */ ON 1=1 /* WHERE B2.P = B1.ID */ INNER JOIN "PUBLIC"."B" "B3" /* PUBLIC.BP: P = B2.ID */ ON 1=1 /* WHERE B3.P = B2.ID */ INNER JOIN "PUBLIC"."B" "B4" /* PUBLIC.BP: P = B3.ID */ ON 1=1 WHERE ("B0"."ID" = 10) AND ("B3"."P" = "B2"."ID") AND ("B4"."P" = "B3"."ID") AND ("B1"."P" = "B0"."ID") AND ("B2"."P" = "B1"."ID")

drop table if exists b;
> ok

create table test(id int primary key, first_name varchar, name varchar, state int);
> ok

create index idx_first_name on test(first_name);
> ok

create index idx_name on test(name);
> ok

create index idx_state on test(state);
> ok

insert into test values
(0, 'Anne', 'Smith', 0), (1, 'Tom', 'Smith', 0),
(2, 'Tom', 'Jones', 0), (3, 'Steve', 'Johnson', 0),
(4, 'Steve', 'Martin', 0), (5, 'Jon', 'Jones', 0),
(6, 'Marc', 'Scott', 0), (7, 'Marc', 'Miller', 0),
(8, 'Susan', 'Wood', 0), (9, 'Jon', 'Bennet', 0);
> update count: 10

EXPLAIN SELECT * FROM TEST WHERE ID = 3;
>> SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."FIRST_NAME", "PUBLIC"."TEST"."NAME", "PUBLIC"."TEST"."STATE" FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID = 3 */ WHERE "ID" = 3

explain select * from test where name='Smith' and first_name='Tom' and state=0;
>> SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."FIRST_NAME", "PUBLIC"."TEST"."NAME", "PUBLIC"."TEST"."STATE" FROM "PUBLIC"."TEST" /* PUBLIC.IDX_FIRST_NAME: FIRST_NAME = 'Tom' */ WHERE ("STATE" = 0) AND ("NAME" = 'Smith') AND ("FIRST_NAME" = 'Tom')

alter table test alter column name selectivity 100;
> ok

explain select * from test where name='Smith' and first_name='Tom' and state=0;
>> SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."FIRST_NAME", "PUBLIC"."TEST"."NAME", "PUBLIC"."TEST"."STATE" FROM "PUBLIC"."TEST" /* PUBLIC.IDX_NAME: NAME = 'Smith' */ WHERE ("STATE" = 0) AND ("NAME" = 'Smith') AND ("FIRST_NAME" = 'Tom')

drop table test;
> ok

CREATE TABLE O(X INT PRIMARY KEY, Y INT);
> ok

INSERT INTO O SELECT X, X+1 FROM SYSTEM_RANGE(1, 1000);
> update count: 1000

EXPLAIN SELECT A.X FROM O B, O A, O F, O D, O C, O E, O G, O H, O I, O J
WHERE 1=J.X and J.Y=I.X AND I.Y=H.X AND H.Y=G.X AND G.Y=F.X AND F.Y=E.X
AND E.Y=D.X AND D.Y=C.X AND C.Y=B.X AND B.Y=A.X;
>> SELECT "A"."X" FROM "PUBLIC"."O" "J" /* PUBLIC.PRIMARY_KEY_4: X = 1 */ /* WHERE J.X = 1 */ INNER JOIN "PUBLIC"."O" "I" /* PUBLIC.PRIMARY_KEY_4: X = J.Y */ ON 1=1 /* WHERE J.Y = I.X */ INNER JOIN "PUBLIC"."O" "H" /* PUBLIC.PRIMARY_KEY_4: X = I.Y */ ON 1=1 /* WHERE I.Y = H.X */ INNER JOIN "PUBLIC"."O" "G" /* PUBLIC.PRIMARY_KEY_4: X = H.Y */ ON 1=1 /* WHERE H.Y = G.X */ INNER JOIN "PUBLIC"."O" "F" /* PUBLIC.PRIMARY_KEY_4: X = G.Y */ ON 1=1 /* WHERE G.Y = F.X */ INNER JOIN "PUBLIC"."O" "E" /* PUBLIC.PRIMARY_KEY_4: X = F.Y */ ON 1=1 /* WHERE F.Y = E.X */ INNER JOIN "PUBLIC"."O" "D" /* PUBLIC.PRIMARY_KEY_4: X = E.Y */ ON 1=1 /* WHERE E.Y = D.X */ INNER JOIN "PUBLIC"."O" "C" /* PUBLIC.PRIMARY_KEY_4: X = D.Y */ ON 1=1 /* WHERE D.Y = C.X */ INNER JOIN "PUBLIC"."O" "B" /* PUBLIC.PRIMARY_KEY_4: X = C.Y */ ON 1=1 /* WHERE C.Y = B.X */ INNER JOIN "PUBLIC"."O" "A" /* PUBLIC.PRIMARY_KEY_4: X = B.Y */ ON 1=1 WHERE ("J"."X" = 1) AND ("I"."Y" = "H"."X") AND ("H"."Y" = "G"."X") AND ("G"."Y" = "F"."X") AND ("F"."Y" = "E"."X") AND ("E"."Y" = "D"."X") AND ("D"."Y" = "C"."X") AND ("C"."Y" = "B"."X") AND ("B"."Y" = "A"."X") AND ("J"."Y" = "I"."X")

DROP TABLE O;
> ok

CREATE TABLE PARENT(ID INT PRIMARY KEY, AID INT, BID INT, CID INT, DID INT, EID INT, FID INT, GID INT, HID INT);
> ok

CREATE TABLE CHILD(ID INT PRIMARY KEY);
> ok

INSERT INTO PARENT SELECT X, 1, 2, 1, 2, 1, 2, 1, 2 FROM SYSTEM_RANGE(0, 1000);
> update count: 1001

INSERT INTO CHILD SELECT X FROM SYSTEM_RANGE(0, 1000);
> update count: 1001

SELECT COUNT(*) FROM PARENT, CHILD A, CHILD B, CHILD C, CHILD D, CHILD E, CHILD F, CHILD G, CHILD H
WHERE AID=A.ID AND BID=B.ID AND CID=C.ID
AND DID=D.ID AND EID=E.ID AND FID=F.ID AND GID=G.ID AND HID=H.ID;
> COUNT(*)
> --------
> 1001
> rows: 1

EXPLAIN SELECT COUNT(*) FROM PARENT, CHILD A, CHILD B, CHILD C, CHILD D, CHILD E, CHILD F, CHILD G, CHILD H
WHERE AID=A.ID AND BID=B.ID AND CID=C.ID
AND DID=D.ID AND EID=E.ID AND FID=F.ID AND GID=G.ID AND HID=H.ID;
>> SELECT COUNT(*) FROM "PUBLIC"."PARENT" /* PUBLIC.PARENT.tableScan */ INNER JOIN "PUBLIC"."CHILD" "A" /* PUBLIC.PRIMARY_KEY_3: ID = AID */ ON 1=1 /* WHERE AID = A.ID */ INNER JOIN "PUBLIC"."CHILD" "B" /* PUBLIC.PRIMARY_KEY_3: ID = BID */ ON 1=1 /* WHERE BID = B.ID */ INNER JOIN "PUBLIC"."CHILD" "C" /* PUBLIC.PRIMARY_KEY_3: ID = CID */ ON 1=1 /* WHERE CID = C.ID */ INNER JOIN "PUBLIC"."CHILD" "D" /* PUBLIC.PRIMARY_KEY_3: ID = DID */ ON 1=1 /* WHERE DID = D.ID */ INNER JOIN "PUBLIC"."CHILD" "E" /* PUBLIC.PRIMARY_KEY_3: ID = EID */ ON 1=1 /* WHERE EID = E.ID */ INNER JOIN "PUBLIC"."CHILD" "F" /* PUBLIC.PRIMARY_KEY_3: ID = FID */ ON 1=1 /* WHERE FID = F.ID */ INNER JOIN "PUBLIC"."CHILD" "G" /* PUBLIC.PRIMARY_KEY_3: ID = GID */ ON 1=1 /* WHERE GID = G.ID */ INNER JOIN "PUBLIC"."CHILD" "H" /* PUBLIC.PRIMARY_KEY_3: ID = HID */ ON 1=1 WHERE ("CID" = "C"."ID") AND ("DID" = "D"."ID") AND ("EID" = "E"."ID") AND ("FID" = "F"."ID") AND ("GID" = "G"."ID") AND ("HID" = "H"."ID") AND ("AID" = "A"."ID") AND ("BID" = "B"."ID")

CREATE TABLE FAMILY(ID INT PRIMARY KEY, PARENTID INT);
> ok

INSERT INTO FAMILY SELECT X, X-1 FROM SYSTEM_RANGE(0, 1000);
> update count: 1001

EXPLAIN SELECT COUNT(*) FROM CHILD A, CHILD B, FAMILY, CHILD C, CHILD D, PARENT, CHILD E, CHILD F, CHILD G
WHERE FAMILY.ID=1 AND FAMILY.PARENTID=PARENT.ID
AND AID=A.ID AND BID=B.ID AND CID=C.ID AND DID=D.ID AND EID=E.ID AND FID=F.ID AND GID=G.ID;
>> SELECT COUNT(*) FROM "PUBLIC"."FAMILY" /* PUBLIC.PRIMARY_KEY_7: ID = 1 */ /* WHERE FAMILY.ID = 1 */ INNER JOIN "PUBLIC"."PARENT" /* PUBLIC.PRIMARY_KEY_8: ID = FAMILY.PARENTID */ ON 1=1 /* WHERE FAMILY.PARENTID = PARENT.ID */ INNER JOIN "PUBLIC"."CHILD" "A" /* PUBLIC.PRIMARY_KEY_3: ID = AID */ ON 1=1 /* WHERE AID = A.ID */ INNER JOIN "PUBLIC"."CHILD" "B" /* PUBLIC.PRIMARY_KEY_3: ID = BID */ ON 1=1 /* WHERE BID = B.ID */ INNER JOIN "PUBLIC"."CHILD" "C" /* PUBLIC.PRIMARY_KEY_3: ID = CID */ ON 1=1 /* WHERE CID = C.ID */ INNER JOIN "PUBLIC"."CHILD" "D" /* PUBLIC.PRIMARY_KEY_3: ID = DID */ ON 1=1 /* WHERE DID = D.ID */ INNER JOIN "PUBLIC"."CHILD" "E" /* PUBLIC.PRIMARY_KEY_3: ID = EID */ ON 1=1 /* WHERE EID = E.ID */ INNER JOIN "PUBLIC"."CHILD" "F" /* PUBLIC.PRIMARY_KEY_3: ID = FID */ ON 1=1 /* WHERE FID = F.ID */ INNER JOIN "PUBLIC"."CHILD" "G" /* PUBLIC.PRIMARY_KEY_3: ID = GID */ ON 1=1 WHERE ("FAMILY"."ID" = 1) AND ("AID" = "A"."ID") AND ("BID" = "B"."ID") AND ("CID" = "C"."ID") AND ("DID" = "D"."ID") AND ("EID" = "E"."ID") AND ("FID" = "F"."ID") AND ("GID" = "G"."ID") AND ("FAMILY"."PARENTID" = "PARENT"."ID")

DROP TABLE FAMILY;
> ok

DROP TABLE PARENT;
> ok

DROP TABLE CHILD;
> ok

--- is null / not is null ---------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT UNIQUE, NAME VARCHAR CHECK LENGTH(NAME)>3);
> ok

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT, NAME VARCHAR(255), B INT);
> ok

CREATE UNIQUE INDEX IDXNAME ON TEST(NAME);
> ok

CREATE UNIQUE INDEX IDX_NAME_B ON TEST(NAME, B);
> ok

INSERT INTO TEST(ID, NAME, B) VALUES (0, NULL, NULL);
> update count: 1

INSERT INTO TEST(ID, NAME, B) VALUES (1, 'Hello', NULL);
> update count: 1

INSERT INTO TEST(ID, NAME, B) VALUES (2, NULL, NULL);
> update count: 1

INSERT INTO TEST(ID, NAME, B) VALUES (3, 'World', NULL);
> update count: 1

select * from test;
> ID NAME  B
> -- ----- ----
> 0  null  null
> 1  Hello null
> 2  null  null
> 3  World null
> rows: 4

UPDATE test SET name='Hi';
> exception DUPLICATE_KEY_1

select * from test;
> ID NAME  B
> -- ----- ----
> 0  null  null
> 1  Hello null
> 2  null  null
> 3  World null
> rows: 4

UPDATE test SET name=NULL;
> update count: 4

UPDATE test SET B=1;
> update count: 4

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT, NAME VARCHAR);
> ok

INSERT INTO TEST VALUES(NULL, NULL), (0, 'Hello'), (1, 'World');
> update count: 3

SELECT * FROM TEST WHERE NOT (1=1);
> ID NAME
> -- ----
> rows: 0

DROP TABLE TEST;
> ok

create table test_null(a int, b int);
> ok

insert into test_null values(0, 0);
> update count: 1

insert into test_null values(0, null);
> update count: 1

insert into test_null values(null, null);
> update count: 1

insert into test_null values(null, 0);
> update count: 1

select * from test_null where a=0;
> A B
> - ----
> 0 0
> 0 null
> rows: 2

select * from test_null where not a=0;
> A B
> - -
> rows: 0

select * from test_null where (a=0 or b=0);
> A    B
> ---- ----
> 0    0
> 0    null
> null 0
> rows: 3

select * from test_null where not (a=0 or b=0);
> A B
> - -
> rows: 0

select * from test_null where (a=1 or b=0);
> A    B
> ---- -
> 0    0
> null 0
> rows: 2

select * from test_null where not( a=1 or b=0);
> A B
> - -
> rows: 0

select * from test_null where not(not( a=1 or b=0));
> A    B
> ---- -
> 0    0
> null 0
> rows: 2

select * from test_null where a=0 or b=0;
> A    B
> ---- ----
> 0    0
> 0    null
> null 0
> rows: 3

SELECT count(*) FROM test_null WHERE not ('X'=null and 1=0);
> COUNT(*)
> --------
> 4
> rows: 1

drop table if exists test_null;
> ok

--- schema ----------------------------------------------------------------------------------------------
SELECT DISTINCT TABLE_SCHEMA, TABLE_CATALOG FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA;
> TABLE_SCHEMA       TABLE_CATALOG
> ------------------ -------------
> INFORMATION_SCHEMA SCRIPT
> rows (ordered): 1

SELECT * FROM INFORMATION_SCHEMA.SCHEMATA;
> CATALOG_NAME SCHEMA_NAME        SCHEMA_OWNER DEFAULT_CHARACTER_SET_CATALOG DEFAULT_CHARACTER_SET_SCHEMA DEFAULT_CHARACTER_SET_NAME SQL_PATH DEFAULT_COLLATION_NAME REMARKS
> ------------ ------------------ ------------ ----------------------------- ---------------------------- -------------------------- -------- ---------------------- -------
> SCRIPT       INFORMATION_SCHEMA SA           SCRIPT                        PUBLIC                       Unicode                    null     OFF                    null
> SCRIPT       PUBLIC             SA           SCRIPT                        PUBLIC                       Unicode                    null     OFF                    null
> rows: 2

SELECT * FROM INFORMATION_SCHEMA.INFORMATION_SCHEMA_CATALOG_NAME;
> CATALOG_NAME
> ------------
> SCRIPT
> rows: 1

SELECT INFORMATION_SCHEMA.SCHEMATA.SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA;
> SCHEMA_NAME
> ------------------
> INFORMATION_SCHEMA
> PUBLIC
> rows: 2

SELECT INFORMATION_SCHEMA.SCHEMATA.* FROM INFORMATION_SCHEMA.SCHEMATA;
> CATALOG_NAME SCHEMA_NAME        SCHEMA_OWNER DEFAULT_CHARACTER_SET_CATALOG DEFAULT_CHARACTER_SET_SCHEMA DEFAULT_CHARACTER_SET_NAME SQL_PATH DEFAULT_COLLATION_NAME REMARKS
> ------------ ------------------ ------------ ----------------------------- ---------------------------- -------------------------- -------- ---------------------- -------
> SCRIPT       INFORMATION_SCHEMA SA           SCRIPT                        PUBLIC                       Unicode                    null     OFF                    null
> SCRIPT       PUBLIC             SA           SCRIPT                        PUBLIC                       Unicode                    null     OFF                    null
> rows: 2

CREATE SCHEMA TEST_SCHEMA AUTHORIZATION SA;
> ok

DROP SCHEMA TEST_SCHEMA RESTRICT;
> ok

create schema Contact_Schema AUTHORIZATION SA;
> ok

CREATE TABLE Contact_Schema.Address (
address_id           BIGINT NOT NULL
CONSTRAINT address_id_check
CHECK (address_id > 0),
address_type         VARCHAR(20) NOT NULL
CONSTRAINT address_type
CHECK (address_type in ('postal','email','web')),
CONSTRAINT X_PKAddress
PRIMARY KEY (address_id)
);
> ok

create schema ClientServer_Schema AUTHORIZATION SA;
> ok

CREATE TABLE ClientServer_Schema.PrimaryKey_Seq (
sequence_name VARCHAR(100) NOT NULL,
seq_number BIGINT NOT NULL UNIQUE,
CONSTRAINT X_PKPrimaryKey_Seq
PRIMARY KEY (sequence_name)
);
> ok

alter table Contact_Schema.Address add constraint abc foreign key(address_id)
references ClientServer_Schema.PrimaryKey_Seq(seq_number);
> ok

drop table ClientServer_Schema.PrimaryKey_Seq, Contact_Schema.Address;
> ok

drop schema Contact_Schema restrict;
> ok

drop schema ClientServer_Schema restrict;
> ok

--- alter table add / drop / rename column ----------------------------------------------------------------------------------------------
CREATE MEMORY TABLE TEST(ID INT PRIMARY KEY);
> ok

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> -------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> rows (ordered): 4

ALTER TABLE TEST ADD CREATEDATE VARCHAR(255) DEFAULT '2001-01-01' NOT NULL;
> ok

ALTER TABLE TEST ADD NAME VARCHAR(255) NULL BEFORE CREATEDATE;
> ok

CREATE INDEX IDXNAME ON TEST(NAME);
> ok

INSERT INTO TEST(ID, NAME) VALUES(1, 'Hi');
> update count: 1

ALTER TABLE TEST ALTER COLUMN NAME SET NOT NULL;
> ok

ALTER TABLE TEST ALTER COLUMN NAME SET NOT NULL;
> ok

ALTER TABLE TEST ALTER COLUMN NAME SET NULL;
> ok

ALTER TABLE TEST ALTER COLUMN NAME SET NULL;
> ok

ALTER TABLE TEST ALTER COLUMN NAME SET DEFAULT 1;
> ok

SELECT * FROM TEST;
> ID NAME CREATEDATE
> -- ---- ----------
> 1  Hi   2001-01-01
> rows: 1

ALTER TABLE TEST ADD MODIFY_DATE TIMESTAMP;
> ok

CREATE MEMORY TABLE TEST_SEQ(ID INT, NAME VARCHAR);
> ok

INSERT INTO TEST_SEQ VALUES(-1, '-1');
> update count: 1

ALTER TABLE TEST_SEQ ALTER COLUMN ID IDENTITY;
> ok

INSERT INTO TEST_SEQ VALUES(NULL, '1');
> exception NULL_NOT_ALLOWED

INSERT INTO TEST_SEQ VALUES(DEFAULT, '1');
> update count: 1

ALTER TABLE TEST_SEQ ALTER COLUMN ID RESTART WITH 10;
> ok

INSERT INTO TEST_SEQ VALUES(DEFAULT, '10');
> update count: 1

alter table test_seq drop primary key;
> ok

ALTER TABLE TEST_SEQ ALTER COLUMN ID INT DEFAULT 20;
> ok

INSERT INTO TEST_SEQ VALUES(DEFAULT, '20');
> update count: 1

ALTER TABLE TEST_SEQ ALTER COLUMN NAME RENAME TO DATA;
> ok

SELECT * FROM TEST_SEQ ORDER BY ID;
> ID DATA
> -- ----
> -1 -1
> 1  1
> 10 10
> 20 20
> rows (ordered): 4

SCRIPT SIMPLE NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST_SEQ"( "ID" INTEGER DEFAULT 20 NOT NULL, "DATA" CHARACTER VARYING );
> -- 4 +/- SELECT COUNT(*) FROM PUBLIC.TEST_SEQ;
> INSERT INTO "PUBLIC"."TEST_SEQ" VALUES(-1, '-1');
> INSERT INTO "PUBLIC"."TEST_SEQ" VALUES(1, '1');
> INSERT INTO "PUBLIC"."TEST_SEQ" VALUES(10, '10');
> INSERT INTO "PUBLIC"."TEST_SEQ" VALUES(20, '20');
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL, "NAME" CHARACTER VARYING(255) DEFAULT 1, "CREATEDATE" CHARACTER VARYING(255) DEFAULT '2001-01-01' NOT NULL, "MODIFY_DATE" TIMESTAMP );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST" VALUES(1, 'Hi', '2001-01-01', NULL);
> CREATE INDEX "PUBLIC"."IDXNAME" ON "PUBLIC"."TEST"("NAME" NULLS FIRST);
> rows (ordered): 12

CREATE UNIQUE INDEX IDX_NAME_ID ON TEST(ID, NAME);
> ok

ALTER TABLE TEST DROP COLUMN NAME;
> exception COLUMN_IS_REFERENCED_1

DROP INDEX IDX_NAME_ID;
> ok

DROP INDEX IDX_NAME_ID IF EXISTS;
> ok

ALTER TABLE TEST DROP NAME;
> ok

DROP TABLE TEST_SEQ;
> ok

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> ---------------------------------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL, "CREATEDATE" CHARACTER VARYING(255) DEFAULT '2001-01-01' NOT NULL, "MODIFY_DATE" TIMESTAMP );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST" VALUES (1, '2001-01-01', NULL);
> rows (ordered): 5

ALTER TABLE TEST ADD NAME VARCHAR(255) NULL BEFORE CREATEDATE;
> ok

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL, "NAME" CHARACTER VARYING(255), "CREATEDATE" CHARACTER VARYING(255) DEFAULT '2001-01-01' NOT NULL, "MODIFY_DATE" TIMESTAMP );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST" VALUES (1, NULL, '2001-01-01', NULL);
> rows (ordered): 5

UPDATE TEST SET NAME = 'Hi';
> update count: 1

INSERT INTO TEST VALUES(2, 'Hello', DEFAULT, DEFAULT);
> update count: 1

SELECT * FROM TEST;
> ID NAME  CREATEDATE MODIFY_DATE
> -- ----- ---------- -----------
> 1  Hi    2001-01-01 null
> 2  Hello 2001-01-01 null
> rows: 2

DROP TABLE TEST;
> ok

CREATE MEMORY TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR);
> ok

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> ---------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL, "NAME" CHARACTER VARYING );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> rows (ordered): 4

INSERT INTO TEST(ID, NAME) VALUES(1, 'Hi'), (2, 'World');
> update count: 2

SELECT * FROM TEST;
> ID NAME
> -- -----
> 1  Hi
> 2  World
> rows: 2

SELECT * FROM TEST WHERE ? IS NULL;
{
Hello
> ID NAME
> -- ----
> rows: 0
};
> update count: 0

DROP TABLE TEST;
> ok

--- limit/offset ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR);
> ok

INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World'), (3, 'with'), (4, 'limited'), (5, 'resources');
> update count: 5

SELECT TOP 2 * FROM TEST ORDER BY ID;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows (ordered): 2

SELECT * FROM TEST ORDER BY ID LIMIT 2+0 OFFSET 1+0;
> ID NAME
> -- -----
> 2  World
> 3  with
> rows (ordered): 2

SELECT * FROM TEST UNION ALL SELECT * FROM TEST ORDER BY ID LIMIT 2+0 OFFSET 1+0;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> rows (ordered): 2

SELECT * FROM TEST ORDER BY ID OFFSET 4;
> ID NAME
> -- ---------
> 5  resources
> rows (ordered): 1

SELECT ID FROM TEST GROUP BY ID UNION ALL SELECT ID FROM TEST GROUP BY ID;
> ID
> --
> 1
> 1
> 2
> 2
> 3
> 3
> 4
> 4
> 5
> 5
> rows: 10

SELECT * FROM (SELECT ID FROM TEST GROUP BY ID);
> ID
> --
> 1
> 2
> 3
> 4
> 5
> rows: 5

EXPLAIN SELECT * FROM TEST UNION ALL SELECT * FROM TEST ORDER BY ID LIMIT 2+0 OFFSET 1+0;
>> (SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."NAME" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */) UNION ALL (SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."NAME" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */) ORDER BY 1 OFFSET 1 ROW FETCH NEXT 2 ROWS ONLY

EXPLAIN DELETE FROM TEST WHERE ID=1;
>> DELETE FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ WHERE "ID" = 1

DROP TABLE TEST;
> ok

CREATE TABLE TEST2COL(A INT, B INT, C VARCHAR(255), PRIMARY KEY(A, B));
> ok

INSERT INTO TEST2COL VALUES(0, 0, 'Hallo'), (0, 1, 'Welt'), (1, 0, 'Hello'), (1, 1, 'World');
> update count: 4

SELECT * FROM TEST2COL WHERE A=0 AND B=0;
> A B C
> - - -----
> 0 0 Hallo
> rows: 1

EXPLAIN SELECT * FROM TEST2COL WHERE A=0 AND B=0;
>> SELECT "PUBLIC"."TEST2COL"."A", "PUBLIC"."TEST2COL"."B", "PUBLIC"."TEST2COL"."C" FROM "PUBLIC"."TEST2COL" /* PUBLIC.PRIMARY_KEY_E: A = 0 AND B = 0 */ WHERE ("A" = 0) AND ("B" = 0)

SELECT * FROM TEST2COL WHERE A=0;
> A B C
> - - -----
> 0 0 Hallo
> 0 1 Welt
> rows: 2

EXPLAIN SELECT * FROM TEST2COL WHERE A=0;
>> SELECT "PUBLIC"."TEST2COL"."A", "PUBLIC"."TEST2COL"."B", "PUBLIC"."TEST2COL"."C" FROM "PUBLIC"."TEST2COL" /* PUBLIC.PRIMARY_KEY_E: A = 0 */ WHERE "A" = 0

SELECT * FROM TEST2COL WHERE B=0;
> A B C
> - - -----
> 0 0 Hallo
> 1 0 Hello
> rows: 2

EXPLAIN SELECT * FROM TEST2COL WHERE B=0;
>> SELECT "PUBLIC"."TEST2COL"."A", "PUBLIC"."TEST2COL"."B", "PUBLIC"."TEST2COL"."C" FROM "PUBLIC"."TEST2COL" /* PUBLIC.TEST2COL.tableScan */ WHERE "B" = 0

DROP TABLE TEST2COL;
> ok

--- testCases ----------------------------------------------------------------------------------------------
CREATE TABLE t_1 (ch CHARACTER(10), dec DECIMAL(10,2), do DOUBLE, lo BIGINT, "IN" INTEGER, sm SMALLINT, ty TINYINT,
da DATE DEFAULT CURRENT_DATE, ti TIME DEFAULT CURRENT_TIME, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP );
> ok

INSERT INTO T_1 (ch, dec, do) VALUES ('name', 10.23, 0);
> update count: 1

SELECT COUNT(*) FROM T_1;
> COUNT(*)
> --------
> 1
> rows: 1

DROP TABLE T_1;
> ok

--- rights ----------------------------------------------------------------------------------------------
CREATE USER TEST_USER PASSWORD '123';
> ok

CREATE TABLE TEST(ID INT);
> ok

CREATE ROLE TEST_ROLE;
> ok

CREATE ROLE IF NOT EXISTS TEST_ROLE;
> ok

GRANT SELECT, INSERT ON TEST TO TEST_USER;
> ok

GRANT UPDATE ON TEST TO TEST_ROLE;
> ok

GRANT TEST_ROLE TO TEST_USER;
> ok

SELECT ROLE_NAME FROM INFORMATION_SCHEMA.ROLES;
> ROLE_NAME
> ---------
> PUBLIC
> TEST_ROLE
> rows: 2

SELECT GRANTEE, GRANTEETYPE, GRANTEDROLE, RIGHTS, TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.RIGHTS;
> GRANTEE   GRANTEETYPE GRANTEDROLE RIGHTS         TABLE_SCHEMA TABLE_NAME
> --------- ----------- ----------- -------------- ------------ ----------
> TEST_ROLE ROLE        null        UPDATE         PUBLIC       TEST
> TEST_USER USER        TEST_ROLE   null           null         null
> TEST_USER USER        null        SELECT, INSERT PUBLIC       TEST
> rows: 3

SELECT * FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES;
> GRANTOR GRANTEE   TABLE_CATALOG TABLE_SCHEMA TABLE_NAME PRIVILEGE_TYPE IS_GRANTABLE WITH_HIERARCHY
> ------- --------- ------------- ------------ ---------- -------------- ------------ --------------
> null    TEST_ROLE SCRIPT        PUBLIC       TEST       UPDATE         NO           NO
> null    TEST_USER SCRIPT        PUBLIC       TEST       INSERT         NO           NO
> null    TEST_USER SCRIPT        PUBLIC       TEST       SELECT         NO           NO
> rows: 3

SELECT * FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES;
> GRANTOR GRANTEE   TABLE_CATALOG TABLE_SCHEMA TABLE_NAME COLUMN_NAME PRIVILEGE_TYPE IS_GRANTABLE
> ------- --------- ------------- ------------ ---------- ----------- -------------- ------------
> null    TEST_ROLE SCRIPT        PUBLIC       TEST       ID          UPDATE         NO
> null    TEST_USER SCRIPT        PUBLIC       TEST       ID          INSERT         NO
> null    TEST_USER SCRIPT        PUBLIC       TEST       ID          SELECT         NO
> rows: 3

REVOKE INSERT ON TEST FROM TEST_USER;
> ok

REVOKE TEST_ROLE FROM TEST_USER;
> ok

SELECT GRANTEE, GRANTEETYPE, GRANTEDROLE, RIGHTS, TABLE_NAME FROM INFORMATION_SCHEMA.RIGHTS;
> GRANTEE   GRANTEETYPE GRANTEDROLE RIGHTS TABLE_NAME
> --------- ----------- ----------- ------ ----------
> TEST_ROLE ROLE        null        UPDATE TEST
> TEST_USER USER        null        SELECT TEST
> rows: 2

SELECT * FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES;
> GRANTOR GRANTEE   TABLE_CATALOG TABLE_SCHEMA TABLE_NAME PRIVILEGE_TYPE IS_GRANTABLE WITH_HIERARCHY
> ------- --------- ------------- ------------ ---------- -------------- ------------ --------------
> null    TEST_ROLE SCRIPT        PUBLIC       TEST       UPDATE         NO           NO
> null    TEST_USER SCRIPT        PUBLIC       TEST       SELECT         NO           NO
> rows: 2

DROP USER TEST_USER;
> ok

DROP TABLE TEST;
> ok

DROP ROLE TEST_ROLE;
> ok

SELECT * FROM INFORMATION_SCHEMA.ROLES;
> ROLE_NAME REMARKS
> --------- -------
> PUBLIC    null
> rows: 1

SELECT * FROM INFORMATION_SCHEMA.RIGHTS;
> GRANTEE GRANTEETYPE GRANTEDROLE RIGHTS TABLE_SCHEMA TABLE_NAME
> ------- ----------- ----------- ------ ------------ ----------
> rows: 0

--- plan ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(?, ?);
{
1, Hello
2, World
3, Peace
};
> update count: 3

EXPLAIN INSERT INTO TEST VALUES(1, 'Test');
>> INSERT INTO "PUBLIC"."TEST"("ID", "NAME") VALUES (1, 'Test')

EXPLAIN INSERT INTO TEST VALUES(1, 'Test'), (2, 'World');
>> INSERT INTO "PUBLIC"."TEST"("ID", "NAME") VALUES (1, 'Test'), (2, 'World')

EXPLAIN INSERT INTO TEST SELECT DISTINCT ID+1, NAME FROM TEST;
>> INSERT INTO "PUBLIC"."TEST"("ID", "NAME") SELECT DISTINCT "ID" + 1, "NAME" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT DISTINCT ID + 1, NAME FROM TEST;
>> SELECT DISTINCT "ID" + 1, "NAME" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN SELECT * FROM TEST WHERE 1=0;
>> SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."NAME" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan: FALSE */ WHERE FALSE

EXPLAIN SELECT TOP 1 * FROM TEST FOR UPDATE;
>> SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."NAME" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ FETCH FIRST ROW ONLY FOR UPDATE

EXPLAIN SELECT COUNT(NAME) FROM TEST WHERE ID=1;
>> SELECT COUNT("NAME") FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ WHERE "ID" = 1

EXPLAIN SELECT * FROM TEST WHERE (ID>=1 AND ID<=2)  OR (ID>0 AND ID<3) AND (ID<>6) ORDER BY NAME NULLS FIRST, 1 NULLS LAST, (1+1) DESC;
>> SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."NAME" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ WHERE (("ID" >= 1) AND ("ID" <= 2)) OR (("ID" <> 6) AND ("ID" > 0) AND ("ID" < 3)) ORDER BY 2 NULLS FIRST, 1 NULLS LAST

EXPLAIN SELECT * FROM TEST WHERE ID=1 GROUP BY NAME, ID;
>> SELECT "PUBLIC"."TEST"."ID", "PUBLIC"."TEST"."NAME" FROM "PUBLIC"."TEST" /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ WHERE "ID" = 1 GROUP BY "NAME", "ID"

EXPLAIN PLAN FOR UPDATE TEST SET NAME='Hello', ID=1 WHERE NAME LIKE 'T%' ESCAPE 'x';
>> UPDATE "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ SET "ID" = 1, "NAME" = 'Hello' WHERE "NAME" LIKE 'T%' ESCAPE 'x'

EXPLAIN PLAN FOR DELETE FROM TEST;
>> DELETE FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN PLAN FOR SELECT NAME, COUNT(*) FROM TEST GROUP BY NAME HAVING COUNT(*) > 1;
>> SELECT "NAME", COUNT(*) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ GROUP BY "NAME" HAVING COUNT(*) > 1

EXPLAIN PLAN FOR SELECT * FROM test t1 inner join test t2 on t1.id=t2.id and t2.name is not null where t1.id=1;
>> SELECT "T1"."ID", "T1"."NAME", "T2"."ID", "T2"."NAME" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ /* WHERE T1.ID = 1 */ INNER JOIN "PUBLIC"."TEST" "T2" /* PUBLIC.PRIMARY_KEY_2: ID = T1.ID */ ON 1=1 WHERE ("T1"."ID" = 1) AND ("T2"."NAME" IS NOT NULL) AND ("T1"."ID" = "T2"."ID")

EXPLAIN PLAN FOR SELECT * FROM test t1 left outer join test t2 on t1.id=t2.id and t2.name is not null where t1.id=1;
>> SELECT "T1"."ID", "T1"."NAME", "T2"."ID", "T2"."NAME" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ /* WHERE T1.ID = 1 */ LEFT OUTER JOIN "PUBLIC"."TEST" "T2" /* PUBLIC.PRIMARY_KEY_2: ID = T1.ID */ ON ("T2"."NAME" IS NOT NULL) AND ("T1"."ID" = "T2"."ID") WHERE "T1"."ID" = 1

EXPLAIN PLAN FOR SELECT * FROM test t1 left outer join test t2 on t1.id=t2.id and t2.name is null where t1.id=1;
>> SELECT "T1"."ID", "T1"."NAME", "T2"."ID", "T2"."NAME" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.PRIMARY_KEY_2: ID = 1 */ /* WHERE T1.ID = 1 */ LEFT OUTER JOIN "PUBLIC"."TEST" "T2" /* PUBLIC.PRIMARY_KEY_2: ID = T1.ID */ ON ("T2"."NAME" IS NULL) AND ("T1"."ID" = "T2"."ID") WHERE "T1"."ID" = 1

EXPLAIN PLAN FOR SELECT * FROM TEST T1 WHERE EXISTS(SELECT * FROM TEST T2 WHERE T1.ID-1 = T2.ID);
>> SELECT "T1"."ID", "T1"."NAME" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.TEST.tableScan */ WHERE EXISTS( SELECT "T2"."ID", "T2"."NAME" FROM "PUBLIC"."TEST" "T2" /* PUBLIC.PRIMARY_KEY_2: ID = (T1.ID - 1) */ WHERE ("T1"."ID" - 1) = "T2"."ID")

EXPLAIN PLAN FOR SELECT * FROM TEST T1 WHERE ID IN(1, 2);
>> SELECT "T1"."ID", "T1"."NAME" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.PRIMARY_KEY_2: ID IN(1, 2) */ WHERE "ID" IN(1, 2)

EXPLAIN PLAN FOR SELECT * FROM TEST T1 WHERE ID IN(SELECT ID FROM TEST);
>> SELECT "T1"."ID", "T1"."NAME" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.PRIMARY_KEY_2: ID IN(SELECT DISTINCT ID FROM PUBLIC.TEST /* PUBLIC.TEST.tableScan */) */ WHERE "ID" IN( SELECT DISTINCT "ID" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */)

EXPLAIN PLAN FOR SELECT * FROM TEST T1 WHERE ID NOT IN(SELECT ID FROM TEST);
>> SELECT "T1"."ID", "T1"."NAME" FROM "PUBLIC"."TEST" "T1" /* PUBLIC.TEST.tableScan */ WHERE "ID" NOT IN( SELECT DISTINCT "ID" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */)

EXPLAIN PLAN FOR SELECT CAST(ID AS VARCHAR(255)) FROM TEST;
>> SELECT CAST("ID" AS CHARACTER VARYING(255)) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

EXPLAIN PLAN FOR SELECT LEFT(NAME, 2) FROM TEST;
>> SELECT LEFT("NAME", 2) FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

SELECT * FROM test t1 inner join test t2 on t1.id=t2.id and t2.name is not null where t1.id=1;
> ID NAME  ID NAME
> -- ----- -- -----
> 1  Hello 1  Hello
> rows: 1

SELECT * FROM test t1 left outer join test t2 on t1.id=t2.id and t2.name is not null where t1.id=1;
> ID NAME  ID NAME
> -- ----- -- -----
> 1  Hello 1  Hello
> rows: 1

SELECT * FROM test t1 left outer join test t2 on t1.id=t2.id and t2.name is null where t1.id=1;
> ID NAME  ID   NAME
> -- ----- ---- ----
> 1  Hello null null
> rows: 1

DROP TABLE TEST;
> ok

--- union ----------------------------------------------------------------------------------------------
SELECT * FROM SYSTEM_RANGE(1,2) UNION ALL SELECT * FROM SYSTEM_RANGE(1,2) ORDER BY 1;
> X
> -
> 1
> 1
> 2
> 2
> rows (ordered): 4

EXPLAIN (SELECT * FROM SYSTEM_RANGE(1,2) UNION ALL SELECT * FROM SYSTEM_RANGE(1,2) ORDER BY 1);
>> (SELECT "SYSTEM_RANGE"."X" FROM SYSTEM_RANGE(1, 2) /* range index */) UNION ALL (SELECT "SYSTEM_RANGE"."X" FROM SYSTEM_RANGE(1, 2) /* range index */) ORDER BY 1

CREATE TABLE CHILDREN(ID INT PRIMARY KEY, NAME VARCHAR(255), CLASS INT);
> ok

CREATE TABLE CLASSES(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO CHILDREN VALUES(?, ?, ?);
{
0, Joe, 0
1, Anne, 1
2, Joerg, 1
3, Petra, 2
};
> update count: 4

INSERT INTO CLASSES VALUES(?, ?);
{
0, Kindergarden
1, Class 1
2, Class 2
3, Class 3
4, Class 4
};
> update count: 5

SELECT * FROM CHILDREN UNION ALL SELECT * FROM CHILDREN ORDER BY ID, NAME FOR UPDATE;
> ID NAME  CLASS
> -- ----- -----
> 0  Joe   0
> 0  Joe   0
> 1  Anne  1
> 1  Anne  1
> 2  Joerg 1
> 2  Joerg 1
> 3  Petra 2
> 3  Petra 2
> rows (ordered): 8

EXPLAIN SELECT * FROM CHILDREN UNION ALL SELECT * FROM CHILDREN ORDER BY ID, NAME FOR UPDATE;
>> (SELECT "PUBLIC"."CHILDREN"."ID", "PUBLIC"."CHILDREN"."NAME", "PUBLIC"."CHILDREN"."CLASS" FROM "PUBLIC"."CHILDREN" /* PUBLIC.CHILDREN.tableScan */ FOR UPDATE) UNION ALL (SELECT "PUBLIC"."CHILDREN"."ID", "PUBLIC"."CHILDREN"."NAME", "PUBLIC"."CHILDREN"."CLASS" FROM "PUBLIC"."CHILDREN" /* PUBLIC.CHILDREN.tableScan */ FOR UPDATE) ORDER BY 1, 2 FOR UPDATE

SELECT 'Child', ID, NAME FROM CHILDREN UNION SELECT 'Class', ID, NAME FROM CLASSES;
> 'Child' ID NAME
> ------- -- ------------
> Child   0  Joe
> Child   1  Anne
> Child   2  Joerg
> Child   3  Petra
> Class   0  Kindergarden
> Class   1  Class1
> Class   2  Class2
> Class   3  Class3
> Class   4  Class4
> rows: 9

EXPLAIN SELECT 'Child', ID, NAME FROM CHILDREN UNION SELECT 'Class', ID, NAME FROM CLASSES;
>> (SELECT 'Child', "ID", "NAME" FROM "PUBLIC"."CHILDREN" /* PUBLIC.CHILDREN.tableScan */) UNION (SELECT 'Class', "ID", "NAME" FROM "PUBLIC"."CLASSES" /* PUBLIC.CLASSES.tableScan */)

SELECT * FROM CHILDREN EXCEPT SELECT * FROM CHILDREN WHERE CLASS=0;
> ID NAME  CLASS
> -- ----- -----
> 1  Anne  1
> 2  Joerg 1
> 3  Petra 2
> rows: 3

EXPLAIN SELECT * FROM CHILDREN EXCEPT SELECT * FROM CHILDREN WHERE CLASS=0;
>> (SELECT "PUBLIC"."CHILDREN"."ID", "PUBLIC"."CHILDREN"."NAME", "PUBLIC"."CHILDREN"."CLASS" FROM "PUBLIC"."CHILDREN" /* PUBLIC.CHILDREN.tableScan */) EXCEPT (SELECT "PUBLIC"."CHILDREN"."ID", "PUBLIC"."CHILDREN"."NAME", "PUBLIC"."CHILDREN"."CLASS" FROM "PUBLIC"."CHILDREN" /* PUBLIC.CHILDREN.tableScan */ WHERE "CLASS" = 0)

EXPLAIN SELECT CLASS FROM CHILDREN INTERSECT SELECT ID FROM CLASSES;
>> (SELECT "CLASS" FROM "PUBLIC"."CHILDREN" /* PUBLIC.CHILDREN.tableScan */) INTERSECT (SELECT "ID" FROM "PUBLIC"."CLASSES" /* PUBLIC.CLASSES.tableScan */)

SELECT CLASS FROM CHILDREN INTERSECT SELECT ID FROM CLASSES;
> CLASS
> -----
> 0
> 1
> 2
> rows: 3

EXPLAIN SELECT * FROM CHILDREN EXCEPT SELECT * FROM CHILDREN WHERE CLASS=0;
>> (SELECT "PUBLIC"."CHILDREN"."ID", "PUBLIC"."CHILDREN"."NAME", "PUBLIC"."CHILDREN"."CLASS" FROM "PUBLIC"."CHILDREN" /* PUBLIC.CHILDREN.tableScan */) EXCEPT (SELECT "PUBLIC"."CHILDREN"."ID", "PUBLIC"."CHILDREN"."NAME", "PUBLIC"."CHILDREN"."CLASS" FROM "PUBLIC"."CHILDREN" /* PUBLIC.CHILDREN.tableScan */ WHERE "CLASS" = 0)

SELECT * FROM CHILDREN CH, CLASSES CL WHERE CH.CLASS = CL.ID;
> ID NAME  CLASS ID NAME
> -- ----- ----- -- ------------
> 0  Joe   0     0  Kindergarden
> 1  Anne  1     1  Class1
> 2  Joerg 1     1  Class1
> 3  Petra 2     2  Class2
> rows: 4

SELECT CH.ID CH_ID, CH.NAME CH_NAME, CL.ID CL_ID, CL.NAME CL_NAME FROM CHILDREN CH, CLASSES CL WHERE CH.CLASS = CL.ID;
> CH_ID CH_NAME CL_ID CL_NAME
> ----- ------- ----- ------------
> 0     Joe     0     Kindergarden
> 1     Anne    1     Class1
> 2     Joerg   1     Class1
> 3     Petra   2     Class2
> rows: 4

CREATE VIEW CHILDREN_CLASSES(CH_ID, CH_NAME, CL_ID, CL_NAME) AS
SELECT CH.ID CH_ID1, CH.NAME CH_NAME2, CL.ID CL_ID3, CL.NAME CL_NAME4
FROM CHILDREN CH, CLASSES CL WHERE CH.CLASS = CL.ID;
> ok

SELECT * FROM CHILDREN_CLASSES WHERE CH_NAME <> 'X';
> CH_ID CH_NAME CL_ID CL_NAME
> ----- ------- ----- ------------
> 0     Joe     0     Kindergarden
> 1     Anne    1     Class1
> 2     Joerg   1     Class1
> 3     Petra   2     Class2
> rows: 4

CREATE VIEW CHILDREN_CLASS1 AS SELECT * FROM CHILDREN_CLASSES WHERE CL_ID=1;
> ok

SELECT * FROM CHILDREN_CLASS1;
> CH_ID CH_NAME CL_ID CL_NAME
> ----- ------- ----- -------
> 1     Anne    1     Class1
> 2     Joerg   1     Class1
> rows: 2

CREATE VIEW CHILDREN_CLASS2 AS SELECT * FROM CHILDREN_CLASSES WHERE CL_ID=2;
> ok

SELECT * FROM CHILDREN_CLASS2;
> CH_ID CH_NAME CL_ID CL_NAME
> ----- ------- ----- -------
> 3     Petra   2     Class2
> rows: 1

CREATE VIEW CHILDREN_CLASS12 AS SELECT * FROM CHILDREN_CLASS1 UNION ALL SELECT * FROM CHILDREN_CLASS1;
> ok

SELECT * FROM CHILDREN_CLASS12;
> CH_ID CH_NAME CL_ID CL_NAME
> ----- ------- ----- -------
> 1     Anne    1     Class1
> 1     Anne    1     Class1
> 2     Joerg   1     Class1
> 2     Joerg   1     Class1
> rows: 4

DROP VIEW CHILDREN_CLASS2;
> ok

DROP VIEW CHILDREN_CLASS1 cascade;
> ok

DROP VIEW CHILDREN_CLASSES;
> ok

DROP VIEW CHILDREN_CLASS12;
> exception VIEW_NOT_FOUND_1

CREATE VIEW V_UNION AS SELECT * FROM CHILDREN UNION ALL SELECT * FROM CHILDREN;
> ok

SELECT * FROM V_UNION WHERE ID=1;
> ID NAME CLASS
> -- ---- -----
> 1  Anne 1
> 1  Anne 1
> rows: 2

EXPLAIN SELECT * FROM V_UNION WHERE ID=1;
>> SELECT "PUBLIC"."V_UNION"."ID", "PUBLIC"."V_UNION"."NAME", "PUBLIC"."V_UNION"."CLASS" FROM "PUBLIC"."V_UNION" /* (SELECT PUBLIC.CHILDREN.ID, PUBLIC.CHILDREN.NAME, PUBLIC.CHILDREN.CLASS FROM PUBLIC.CHILDREN /* PUBLIC.PRIMARY_KEY_9: ID IS NOT DISTINCT FROM ?1 */ /* scanCount: 2 */ WHERE PUBLIC.CHILDREN.ID IS NOT DISTINCT FROM ?1) UNION ALL (SELECT PUBLIC.CHILDREN.ID, PUBLIC.CHILDREN.NAME, PUBLIC.CHILDREN.CLASS FROM PUBLIC.CHILDREN /* PUBLIC.PRIMARY_KEY_9: ID IS NOT DISTINCT FROM ?1 */ /* scanCount: 2 */ WHERE PUBLIC.CHILDREN.ID IS NOT DISTINCT FROM ?1): ID = 1 */ WHERE "ID" = 1

CREATE VIEW V_EXCEPT AS SELECT * FROM CHILDREN EXCEPT SELECT * FROM CHILDREN WHERE ID=2;
> ok

SELECT * FROM V_EXCEPT WHERE ID=1;
> ID NAME CLASS
> -- ---- -----
> 1  Anne 1
> rows: 1

EXPLAIN SELECT * FROM V_EXCEPT WHERE ID=1;
>> SELECT "PUBLIC"."V_EXCEPT"."ID", "PUBLIC"."V_EXCEPT"."NAME", "PUBLIC"."V_EXCEPT"."CLASS" FROM "PUBLIC"."V_EXCEPT" /* (SELECT DISTINCT PUBLIC.CHILDREN.ID, PUBLIC.CHILDREN.NAME, PUBLIC.CHILDREN.CLASS FROM PUBLIC.CHILDREN /* PUBLIC.PRIMARY_KEY_9: ID IS NOT DISTINCT FROM ?1 */ /* scanCount: 2 */ WHERE PUBLIC.CHILDREN.ID IS NOT DISTINCT FROM ?1) EXCEPT (SELECT DISTINCT PUBLIC.CHILDREN.ID, PUBLIC.CHILDREN.NAME, PUBLIC.CHILDREN.CLASS FROM PUBLIC.CHILDREN /* PUBLIC.PRIMARY_KEY_9: ID = 2 */ /* scanCount: 2 */ WHERE ID = 2): ID = 1 */ WHERE "ID" = 1

CREATE VIEW V_INTERSECT AS SELECT ID, NAME FROM CHILDREN INTERSECT SELECT * FROM CLASSES;
> ok

SELECT * FROM V_INTERSECT WHERE ID=1;
> ID NAME
> -- ----
> rows: 0

EXPLAIN SELECT * FROM V_INTERSECT WHERE ID=1;
>> SELECT "PUBLIC"."V_INTERSECT"."ID", "PUBLIC"."V_INTERSECT"."NAME" FROM "PUBLIC"."V_INTERSECT" /* (SELECT DISTINCT ID, NAME FROM PUBLIC.CHILDREN /* PUBLIC.PRIMARY_KEY_9: ID IS NOT DISTINCT FROM ?1 */ /* scanCount: 2 */ WHERE ID IS NOT DISTINCT FROM ?1) INTERSECT (SELECT DISTINCT PUBLIC.CLASSES.ID, PUBLIC.CLASSES.NAME FROM PUBLIC.CLASSES /* PUBLIC.PRIMARY_KEY_5: ID IS NOT DISTINCT FROM ?1 */ /* scanCount: 2 */ WHERE PUBLIC.CLASSES.ID IS NOT DISTINCT FROM ?1): ID = 1 */ WHERE "ID" = 1

DROP VIEW V_UNION;
> ok

DROP VIEW V_EXCEPT;
> ok

DROP VIEW V_INTERSECT;
> ok

DROP TABLE CHILDREN;
> ok

DROP TABLE CLASSES;
> ok

--- view ----------------------------------------------------------------------------------------------
CREATE CACHED TABLE TEST_A(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE CACHED TABLE TEST_B(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

SELECT A.ID AID, A.NAME A_NAME, B.ID BID, B.NAME B_NAME FROM TEST_A A INNER JOIN TEST_B B WHERE A.ID = B.ID;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> rows: 0

INSERT INTO TEST_B VALUES(1, 'Hallo'), (2, 'Welt'), (3, 'Rekord');
> update count: 3

CREATE VIEW IF NOT EXISTS TEST_ALL AS SELECT A.ID AID, A.NAME A_NAME, B.ID BID, B.NAME B_NAME FROM TEST_A A, TEST_B B WHERE A.ID = B.ID;
> ok

SELECT COUNT(*) FROM TEST_ALL;
> COUNT(*)
> --------
> 0
> rows: 1

CREATE VIEW IF NOT EXISTS TEST_ALL AS
SELECT * FROM TEST_A;
> ok

INSERT INTO TEST_A VALUES(1, 'Hello'), (2, 'World'), (3, 'Record');
> update count: 3

SELECT * FROM TEST_ALL;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> 1   Hello  1   Hallo
> 2   World  2   Welt
> 3   Record 3   Rekord
> rows: 3

SELECT * FROM TEST_ALL WHERE  AID=1;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> 1   Hello  1   Hallo
> rows: 1

SELECT * FROM TEST_ALL WHERE AID>0;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> 1   Hello  1   Hallo
> 2   World  2   Welt
> 3   Record 3   Rekord
> rows: 3

SELECT * FROM TEST_ALL WHERE AID<2;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> 1   Hello  1   Hallo
> rows: 1

SELECT * FROM TEST_ALL WHERE AID<=2;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> 1   Hello  1   Hallo
> 2   World  2   Welt
> rows: 2

SELECT * FROM TEST_ALL WHERE AID>=2;
> AID A_NAME BID B_NAME
> --- ------ --- ------
> 2   World  2   Welt
> 3   Record 3   Rekord
> rows: 2

CREATE VIEW TEST_A_SUB AS SELECT * FROM TEST_A WHERE ID < 2;
> ok

SELECT TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'PUBLIC';
> TABLE_NAME VIEW_DEFINITION
> ---------- ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> TEST_ALL   SELECT "A"."ID" AS "AID", "A"."NAME" AS "A_NAME", "B"."ID" AS "BID", "B"."NAME" AS "B_NAME" FROM "PUBLIC"."TEST_A" "A" INNER JOIN "PUBLIC"."TEST_B" "B" ON 1=1 WHERE "A"."ID" = "B"."ID"
> TEST_A_SUB SELECT "PUBLIC"."TEST_A"."ID", "PUBLIC"."TEST_A"."NAME" FROM "PUBLIC"."TEST_A" WHERE "ID" < 2
> rows: 2

SELECT * FROM TEST_A_SUB WHERE NAME IS NOT NULL;
> ID NAME
> -- -----
> 1  Hello
> rows: 1

DROP VIEW TEST_A_SUB;
> ok

DROP TABLE TEST_A cascade;
> ok

DROP TABLE TEST_B cascade;
> ok

DROP VIEW TEST_ALL;
> exception VIEW_NOT_FOUND_1

DROP VIEW IF EXISTS TEST_ALL;
> ok

--- commit/rollback ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

SET AUTOCOMMIT FALSE;
> ok

INSERT INTO TEST VALUES(1, 'Test');
> update count: 1

ROLLBACK;
> ok

SELECT * FROM TEST;
> ID NAME
> -- ----
> rows: 0

INSERT INTO TEST VALUES(1, 'Test2');
> update count: 1

SAVEPOINT TEST;
> ok

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

ROLLBACK TO SAVEPOINT NOT_EXISTING;
> exception SAVEPOINT_IS_INVALID_1

ROLLBACK TO SAVEPOINT TEST;
> ok

SELECT * FROM TEST;
> ID NAME
> -- -----
> 1  Test2
> rows: 1

ROLLBACK WORK;
> ok

SELECT * FROM TEST;
> ID NAME
> -- ----
> rows: 0

INSERT INTO TEST VALUES(1, 'Test3');
> update count: 1

SAVEPOINT TEST3;
> ok

INSERT INTO TEST VALUES(2, 'World2');
> update count: 1

ROLLBACK TO SAVEPOINT TEST3;
> ok

COMMIT WORK;
> ok

SELECT * FROM TEST;
> ID NAME
> -- -----
> 1  Test3
> rows: 1

SET AUTOCOMMIT TRUE;
> ok

DROP TABLE TEST;
> ok

--- insert..select ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(0, 'Hello');
> update count: 1

INSERT INTO TEST SELECT ID+1, NAME||'+' FROM TEST;
> update count: 1

INSERT INTO TEST SELECT ID+2, NAME||'+' FROM TEST;
> update count: 2

INSERT INTO TEST SELECT ID+4, NAME||'+' FROM TEST;
> update count: 4

SELECT * FROM TEST;
> ID NAME
> -- --------
> 0  Hello
> 1  Hello+
> 2  Hello+
> 3  Hello++
> 4  Hello+
> 5  Hello++
> 6  Hello++
> 7  Hello+++
> rows: 8

DROP TABLE TEST;
> ok

--- syntax errors ----------------------------------------------------------------------------------------------
CREATE SOMETHING STRANGE;
> exception SYNTAX_ERROR_2

SELECT T1.* T2;
> exception SYNTAX_ERROR_1

select replace('abchihihi', 'i', 'o') abcehohoho, replace('this is tom', 'i') 1e_th_st_om from test;
> exception SYNTAX_ERROR_2

select monthname(date )'005-0E9-12') d_set fm test;
> exception SYNTAX_ERROR_1

call substring('bob', 2, -1);
> ''
> --
>
> rows: 1

--- exists ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(0, NULL);
> update count: 1

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

SELECT * FROM TEST T WHERE NOT EXISTS(
SELECT * FROM TEST T2 WHERE T.ID > T2.ID);
> ID NAME
> -- ----
> 0  null
> rows: 1

DROP TABLE TEST;
> ok

--- subquery ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(0, NULL);
> update count: 1

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

select * from test where (select max(t1.id) from test t1) between 0 and 100;
> ID NAME
> -- -----
> 0  null
> 1  Hello
> rows: 2

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

SELECT * FROM TEST T WHERE T.ID = (SELECT T2.ID FROM TEST T2 WHERE T2.ID=T.ID);
> ID NAME
> -- -----
> 0  null
> 1  Hello
> 2  World
> rows: 3

SELECT (SELECT T2.NAME FROM TEST T2 WHERE T2.ID=T.ID), T.NAME FROM TEST T;
> (SELECT T2.NAME FROM PUBLIC.TEST T2 WHERE T2.ID = T.ID) NAME
> ------------------------------------------------------- -----
> Hello                                                   Hello
> World                                                   World
> null                                                    null
> rows: 3

SELECT (SELECT SUM(T2.ID) FROM TEST T2 WHERE T2.ID>T.ID), T.ID FROM TEST T;
> (SELECT SUM(T2.ID) FROM PUBLIC.TEST T2 WHERE T2.ID > T.ID) ID
> ---------------------------------------------------------- --
> 2                                                          1
> 3                                                          0
> null                                                       2
> rows: 3

select * from test t where t.id+1 in (select id from test);
> ID NAME
> -- -----
> 0  null
> 1  Hello
> rows: 2

select * from test t where t.id in (select id from test where id=t.id);
> ID NAME
> -- -----
> 0  null
> 1  Hello
> 2  World
> rows: 3

select 1 from test, test where 1 in (select 1 from test where id=1);
> 1
> -
> 1
> 1
> 1
> 1
> 1
> 1
> 1
> 1
> 1
> rows: 9

select * from test, test where id=id;
> exception AMBIGUOUS_COLUMN_NAME_1

select 1 from test, test where id=id;
> exception AMBIGUOUS_COLUMN_NAME_1

select 1 from test where id in (select id from test, test);
> exception AMBIGUOUS_COLUMN_NAME_1

DROP TABLE TEST;
> ok

--- group by ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(A INT, B INT, "VALUE" INT, UNIQUE(A, B));
> ok

INSERT INTO TEST VALUES(?, ?, ?);
{
NULL, NULL, NULL
NULL, 0, 0
NULL, 1, 10
0, 0, -1
0, 1, 100
1, 0, 200
1, 1, 300
};
> update count: 7

SELECT A, B, COUNT(*) CAL, COUNT(A) CA, COUNT(B) CB, MIN("VALUE") MI, MAX("VALUE") MA, SUM("VALUE") S FROM TEST GROUP BY A, B;
> A    B    CAL CA CB MI   MA   S
> ---- ---- --- -- -- ---- ---- ----
> 0    0    1   1  1  -1   -1   -1
> 0    1    1   1  1  100  100  100
> 1    0    1   1  1  200  200  200
> 1    1    1   1  1  300  300  300
> null 0    1   0  1  0    0    0
> null 1    1   0  1  10   10   10
> null null 1   0  0  null null null
> rows: 7

DROP TABLE TEST;
> ok

--- data types (blob, clob, varchar_ignorecase) ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT, XB BINARY(3), XBL BLOB, XO OTHER, XCL CLOB, XVI VARCHAR_IGNORECASE);
> ok

INSERT INTO TEST VALUES(0, X'', X'', X'', '', '');
> update count: 1

INSERT INTO TEST VALUES(1, X'0101', X'0101', X'0101', 'abc', 'aa');
> update count: 1

INSERT INTO TEST VALUES(2, X'0AFF', X'08FE', X'F0F1', 'AbCdEfG', 'ZzAaBb');
> update count: 1

INSERT INTO TEST VALUES(3,
    X'112233',
    X'112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff',
    X'112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff',
    'AbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYz',
    'AbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYz');
> update count: 1

INSERT INTO TEST VALUES(4, NULL, NULL, NULL, NULL, NULL);
> update count: 1

SELECT ID, XB, XBL, XO, XCL, XVI FROM TEST;
> ID XB        XBL                                                                                                                                                                                                                                                                                                             XO                                                                                                                                                                                                                                                                                                              XCL                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      XVI
> -- --------- --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> 0  X'000000' X''                                                                                                                                                                                                                                                                                                             X''
> 1  X'010100' X'0101'                                                                                                                                                                                                                                                                                                         X'0101'                                                                                                                                                                                                                                                                                                         abc                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      aa
> 2  X'0aff00' X'08fe'                                                                                                                                                                                                                                                                                                         X'f0f1'                                                                                                                                                                                                                                                                                                         AbCdEfG                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  ZzAaBb
> 3  X'112233' X'112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff' X'112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff112233445566778899aabbccddeeff' AbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYz AbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYzAbCdEfGhIjKlMnOpQrStUvWxYz
> 4  null      null                                                                                                                                                                                                                                                                                                            null                                                                                                                                                                                                                                                                                                            null                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     null
> rows: 5

SELECT ID FROM TEST WHERE XCL = XCL;
> ID
> --
> 0
> 1
> 2
> 3
> rows: 4

SELECT ID FROM TEST WHERE XCL LIKE 'abc%';
> ID
> --
> 1
> rows: 1

SELECT ID FROM TEST WHERE XVI LIKE 'abc%';
> ID
> --
> 3
> rows: 1

SELECT 'abc', 'Papa Joe''s', CAST(-1 AS SMALLINT), CAST(2 AS BIGINT), CAST(0 AS DOUBLE), CAST('0a0f' AS BINARY(4)) B, CAST(125 AS TINYINT), TRUE, FALSE FROM TEST WHERE ID=1;
> 'abc' 'Papa Joe''s' -1 2 0.0 B           125 TRUE FALSE
> ----- ------------- -- - --- ----------- --- ---- -----
> abc   Papa Joe's    -1 2 0.0 X'30613066' 125 TRUE FALSE
> rows: 1

-- ' This apostrophe is here to fix syntax highlighting in the text editors.

SELECT CAST('abcd' AS VARCHAR(255)) C1, CAST('ef_gh' AS VARCHAR(3)) C2;
> C1   C2
> ---- ---
> abcd ef_
> rows: 1

DROP TABLE TEST;
> ok

--- data types (date and time) ----------------------------------------------------------------------------------------------
CREATE MEMORY TABLE TEST(ID INT, XT TIME, XD DATE, XTS TIMESTAMP(9));
> ok

INSERT INTO TEST VALUES(0, '0:0:0','1-2-3','2-3-4 0:0:0');
> update count: 1

INSERT INTO TEST VALUES(1, '01:02:03','2001-02-03','2001-02-29 0:0:0');
> exception INVALID_DATETIME_CONSTANT_2

INSERT INTO TEST VALUES(1, '24:62:03','2001-02-03','2001-02-01 0:0:0');
> exception INVALID_DATETIME_CONSTANT_2

INSERT INTO TEST VALUES(1, '23:02:03','2001-04-31','2001-02-01 0:0:0');
> exception INVALID_DATETIME_CONSTANT_2

INSERT INTO TEST VALUES(1,'1:2:3','4-5-6','7-8-9 0:1:2');
> update count: 1

INSERT INTO TEST VALUES(2,'23:59:59','1999-12-31','1999-12-31 23:59:59.123456789');
> update count: 1

INSERT INTO TEST VALUES(NULL,NULL,NULL,NULL);
> update count: 1

SELECT * FROM TEST;
> ID   XT       XD         XTS
> ---- -------- ---------- -----------------------------
> 0    00:00:00 0001-02-03 0002-03-04 00:00:00
> 1    01:02:03 0004-05-06 0007-08-09 00:01:02
> 2    23:59:59 1999-12-31 1999-12-31 23:59:59.123456789
> null null     null       null
> rows: 4

SELECT XD+1, XD-1, XD-XD FROM TEST;
> DATEADD(DAY, 1, XD) DATEADD(DAY, -1, XD) XD - XD
> ------------------- -------------------- ----------------
> 0001-02-04          0001-02-02           INTERVAL '0' DAY
> 0004-05-07          0004-05-05           INTERVAL '0' DAY
> 2000-01-01          1999-12-30           INTERVAL '0' DAY
> null                null                 null
> rows: 4

SELECT ID, CAST(XTS AS DATE) TS2D,
CAST(XTS AS TIME(9)) TS2T,
CAST(XD AS TIMESTAMP) D2TS FROM TEST;
> ID   TS2D       TS2T               D2TS
> ---- ---------- ------------------ -------------------
> 0    0002-03-04 00:00:00           0001-02-03 00:00:00
> 1    0007-08-09 00:01:02           0004-05-06 00:00:00
> 2    1999-12-31 23:59:59.123456789 1999-12-31 00:00:00
> null null       null               null
> rows: 4

SCRIPT SIMPLE NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> ---------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER, "XT" TIME, "XD" DATE, "XTS" TIMESTAMP(9) );
> -- 4 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> INSERT INTO "PUBLIC"."TEST" VALUES(0, TIME '00:00:00', DATE '0001-02-03', TIMESTAMP '0002-03-04 00:00:00');
> INSERT INTO "PUBLIC"."TEST" VALUES(1, TIME '01:02:03', DATE '0004-05-06', TIMESTAMP '0007-08-09 00:01:02');
> INSERT INTO "PUBLIC"."TEST" VALUES(2, TIME '23:59:59', DATE '1999-12-31', TIMESTAMP '1999-12-31 23:59:59.123456789');
> INSERT INTO "PUBLIC"."TEST" VALUES(NULL, NULL, NULL, NULL);
> rows (ordered): 7

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, t0 timestamp(23, 0), t1 timestamp(23, 1), t2 timestamp(23, 2), t5 timestamp(23, 5));
> ok

INSERT INTO TEST VALUES(1, '2001-01-01 12:34:56.789123', '2001-01-01 12:34:56.789123', '2001-01-01 12:34:56.789123', '2001-01-01 12:34:56.789123');
> update count: 1

select * from test;
> ID T0                  T1                    T2                     T5
> -- ------------------- --------------------- ---------------------- -------------------------
> 1  2001-01-01 12:34:57 2001-01-01 12:34:56.8 2001-01-01 12:34:56.79 2001-01-01 12:34:56.78912
> rows: 1

DROP TABLE IF EXISTS TEST;
> ok

--- data types (decimal) ----------------------------------------------------------------------------------------------
CALL 1.2E10+1;
> 12000000001
> -----------
> 12000000001
> rows: 1

CALL -1.2E-10-1;
> -1.00000000012
> --------------
> -1.00000000012
> rows: 1

CALL 1E-1;
> 0.1
> ---
> 0.1
> rows: 1

CREATE TABLE TEST(ID INT, X1 BIT, XT TINYINT, X_SM SMALLINT, XB BIGINT, XD DECIMAL(10,2), XD2 DOUBLE PRECISION, XR REAL);
> ok

INSERT INTO TEST VALUES(?, ?, ?, ?, ?, ?, ?, ?);
{
0,FALSE,0,0,0,0.0,0.0,0.0
1,TRUE,1,1,1,1.0,1.0,1.0
4,TRUE,4,4,4,4.0,4.0,4.0
-1,FALSE,-1,-1,-1,-1.0,-1.0,-1.0
NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
};
> update count: 5

SELECT *, 0xFF, -0x1234567890abcd FROM TEST;
> ID   X1    XT   X_SM XB   XD    XD2  XR   255 -5124095575370701
> ---- ----- ---- ---- ---- ----- ---- ---- --- -----------------
> -1   FALSE -1   -1   -1   -1.00 -1.0 -1.0 255 -5124095575370701
> 0    FALSE 0    0    0    0.00  0.0  0.0  255 -5124095575370701
> 1    TRUE  1    1    1    1.00  1.0  1.0  255 -5124095575370701
> 4    TRUE  4    4    4    4.00  4.0  4.0  255 -5124095575370701
> null null  null null null null  null null 255 -5124095575370701
> rows: 5

SELECT XD, CAST(XD AS DECIMAL(10,1)) D2DE, CAST(XD2 AS DECIMAL(4, 3)) DO2DE, CAST(XR AS DECIMAL(20,3)) R2DE FROM TEST;
> XD    D2DE DO2DE  R2DE
> ----- ---- ------ ------
> -1.00 -1.0 -1.000 -1.000
> 0.00  0.0  0.000  0.000
> 1.00  1.0  1.000  1.000
> 4.00  4.0  4.000  4.000
> null  null null   null
> rows: 5

SELECT ID, CAST(XB AS DOUBLE) L2D, CAST(X_SM AS DOUBLE) S2D, CAST(XT AS DOUBLE) X2D FROM TEST;
> ID   L2D  S2D  X2D
> ---- ---- ---- ----
> -1   -1.0 -1.0 -1.0
> 0    0.0  0.0  0.0
> 1    1.0  1.0  1.0
> 4    4.0  4.0  4.0
> null null null null
> rows: 5

SELECT ID, CAST(XB AS REAL) L2D, CAST(X_SM AS REAL) S2D, CAST(XT AS REAL) T2R FROM TEST;
> ID   L2D  S2D  T2R
> ---- ---- ---- ----
> -1   -1.0 -1.0 -1.0
> 0    0.0  0.0  0.0
> 1    1.0  1.0  1.0
> 4    4.0  4.0  4.0
> null null null null
> rows: 5

SELECT ID, CAST(X_SM AS BIGINT) S2L, CAST(XT AS BIGINT) B2L, CAST(XD2 AS BIGINT) D2L, CAST(XR AS BIGINT) R2L FROM TEST;
> ID   S2L  B2L  D2L  R2L
> ---- ---- ---- ---- ----
> -1   -1   -1   -1   -1
> 0    0    0    0    0
> 1    1    1    1    1
> 4    4    4    4    4
> null null null null null
> rows: 5

SELECT ID, CAST(XB AS INT) L2I, CAST(XD2 AS INT) D2I, CAST(XD2 AS SMALLINT) DO2I, CAST(XR AS SMALLINT) R2I FROM TEST;
> ID   L2I  D2I  DO2I R2I
> ---- ---- ---- ---- ----
> -1   -1   -1   -1   -1
> 0    0    0    0    0
> 1    1    1    1    1
> 4    4    4    4    4
> null null null null null
> rows: 5

SELECT ID, CAST(XD AS SMALLINT) D2S, CAST(XB AS SMALLINT) L2S, CAST(XT AS SMALLINT) B2S FROM TEST;
> ID   D2S  L2S  B2S
> ---- ---- ---- ----
> -1   -1   -1   -1
> 0    0    0    0
> 1    1    1    1
> 4    4    4    4
> null null null null
> rows: 5

SELECT ID, CAST(XD2 AS TINYINT) D2B, CAST(XD AS TINYINT) DE2B, CAST(XB AS TINYINT) L2B, CAST(X_SM AS TINYINT) S2B FROM TEST;
> ID   D2B  DE2B L2B  S2B
> ---- ---- ---- ---- ----
> -1   -1   -1   -1   -1
> 0    0    0    0    0
> 1    1    1    1    1
> 4    4    4    4    4
> null null null null null
> rows: 5

SELECT ID, CAST(XD2 AS BIT) D2B, CAST(XD AS BIT) DE2B, CAST(XB AS BIT) L2B, CAST(X_SM AS BIT) S2B FROM TEST;
> ID   D2B   DE2B  L2B   S2B
> ---- ----- ----- ----- -----
> -1   TRUE  TRUE  TRUE  TRUE
> 0    FALSE FALSE FALSE FALSE
> 1    TRUE  TRUE  TRUE  TRUE
> 4    TRUE  TRUE  TRUE  TRUE
> null null  null  null  null
> rows: 5

SELECT CAST('TRUE' AS BIT) NT, CAST('1.0' AS BIT) N1, CAST('0.0' AS BIT) N0;
> NT   N1   N0
> ---- ---- -----
> TRUE TRUE FALSE
> rows: 1

SELECT ID, ID+X1, ID+XT, ID+X_SM, ID+XB, ID+XD, ID+XD2, ID+XR FROM TEST;
> ID   ID + X1 ID + XT ID + X_SM ID + XB ID + XD ID + XD2 ID + XR
> ---- ------- ------- --------- ------- ------- -------- -------
> -1   -1      -2      -2        -2      -2.00   -2.0     -2.0
> 0    0       0       0         0       0.00    0.0      0.0
> 1    2       2       2         2       2.00    2.0      2.0
> 4    5       8       8         8       8.00    8.0      8.0
> null null    null    null      null    null    null     null
> rows: 5

SELECT ID, 10-X1, 10-XT, 10-X_SM, 10-XB, 10-XD, 10-XD2, 10-XR FROM TEST;
> ID   10 - X1 10 - XT 10 - X_SM 10 - XB 10 - XD 10 - XD2 10 - XR
> ---- ------- ------- --------- ------- ------- -------- -------
> -1   10      11      11        11      11.00   11.0     11.0
> 0    10      10      10        10      10.00   10.0     10.0
> 1    9       9       9         9       9.00    9.0      9.0
> 4    9       6       6         6       6.00    6.0      6.0
> null null    null    null      null    null    null     null
> rows: 5

SELECT ID, 10*X1, 10*XT, 10*X_SM, 10*XB, 10*XD, 10*XD2, 10*XR FROM TEST;
> ID   10 * X1 10 * XT 10 * X_SM 10 * XB 10 * XD 10 * XD2 10 * XR
> ---- ------- ------- --------- ------- ------- -------- -------
> -1   0       -10     -10       -10     -10.00  -10.0    -10.0
> 0    0       0       0         0       0.00    0.0      0.0
> 1    10      10      10        10      10.00   10.0     10.0
> 4    10      40      40        40      40.00   40.0     40.0
> null null    null    null      null    null    null     null
> rows: 5

SELECT ID, SIGN(XT), SIGN(X_SM), SIGN(XB), SIGN(XD), SIGN(XD2), SIGN(XR) FROM TEST;
> ID   SIGN(XT) SIGN(X_SM) SIGN(XB) SIGN(XD) SIGN(XD2) SIGN(XR)
> ---- -------- ---------- -------- -------- --------- --------
> -1   -1       -1         -1       -1       -1        -1
> 0    0        0          0        0        0         0
> 1    1        1          1        1        1         1
> 4    1        1          1        1        1         1
> null null     null       null     null     null      null
> rows: 5

SELECT ID, XT-XT-XT, X_SM-X_SM-X_SM, XB-XB-XB, XD-XD-XD, XD2-XD2-XD2, XR-XR-XR FROM TEST;
> ID   (XT - XT) - XT (X_SM - X_SM) - X_SM (XB - XB) - XB (XD - XD) - XD (XD2 - XD2) - XD2 (XR - XR) - XR
> ---- -------------- -------------------- -------------- -------------- ----------------- --------------
> -1   1              1                    1              1.00           1.0               1.0
> 0    0              0                    0              0.00           0.0               0.0
> 1    -1             -1                   -1             -1.00          -1.0              -1.0
> 4    -4             -4                   -4             -4.00          -4.0              -4.0
> null null           null                 null           null           null              null
> rows: 5

SELECT ID, XT+XT, X_SM+X_SM, XB+XB, XD+XD, XD2+XD2, XR+XR FROM TEST;
> ID   XT + XT X_SM + X_SM XB + XB XD + XD XD2 + XD2 XR + XR
> ---- ------- ----------- ------- ------- --------- -------
> -1   -2      -2          -2      -2.00   -2.0      -2.0
> 0    0       0           0       0.00    0.0       0.0
> 1    2       2           2       2.00    2.0       2.0
> 4    8       8           8       8.00    8.0       8.0
> null null    null        null    null    null      null
> rows: 5

SELECT ID, XT*XT, X_SM*X_SM, XB*XB, XD*XD, XD2*XD2, XR*XR FROM TEST;
> ID   XT * XT X_SM * X_SM XB * XB XD * XD XD2 * XD2 XR * XR
> ---- ------- ----------- ------- ------- --------- -------
> -1   1       1           1       1.0000  1.0       1.0
> 0    0       0           0       0.0000  0.0       0.0
> 1    1       1           1       1.0000  1.0       1.0
> 4    16      16          16      16.0000 16.0      16.0
> null null    null        null    null    null      null
> rows: 5

SELECT 2/3 FROM TEST WHERE ID=1;
> 0
> -
> 0
> rows: 1

SELECT ID/ID FROM TEST;
> exception DIVISION_BY_ZERO_1

SELECT XT/XT FROM TEST;
> exception DIVISION_BY_ZERO_1

SELECT X_SM/X_SM FROM TEST;
> exception DIVISION_BY_ZERO_1

SELECT XB/XB FROM TEST;
> exception DIVISION_BY_ZERO_1

SELECT XD/XD FROM TEST;
> exception DIVISION_BY_ZERO_1

SELECT XD2/XD2 FROM TEST;
> exception DIVISION_BY_ZERO_1

SELECT XR/XR FROM TEST;
> exception DIVISION_BY_ZERO_1

SELECT ID++0, -X1, -XT, -X_SM, -XB, -XD, -XD2, -XR FROM TEST;
> ID + 0 - X1  - XT - X_SM - XB - XD  - XD2 - XR
> ------ ----- ---- ------ ---- ----- ----- ----
> -1     TRUE  1    1      1    1.00  1.0   1.0
> 0      TRUE  0    0      0    0.00  0.0   0.0
> 1      FALSE -1   -1     -1   -1.00 -1.0  -1.0
> 4      FALSE -4   -4     -4   -4.00 -4.0  -4.0
> null   null  null null   null null  null  null
> rows: 5

SELECT ID, X1||'!', XT||'!', X_SM||'!', XB||'!', XD||'!', XD2||'!', XR||'!' FROM TEST;
> ID   X1 || '!' XT || '!' X_SM || '!' XB || '!' XD || '!' XD2 || '!' XR || '!'
> ---- --------- --------- ----------- --------- --------- ---------- ---------
> -1   FALSE!    -1!       -1!         -1!       -1.00!    -1.0!      -1.0!
> 0    FALSE!    0!        0!          0!        0.00!     0.0!       0.0!
> 1    TRUE!     1!        1!          1!        1.00!     1.0!       1.0!
> 4    TRUE!     4!        4!          4!        4.00!     4.0!       4.0!
> null null      null      null        null      null      null       null
> rows: 5

DROP TABLE TEST;
> ok

--- in ----------------------------------------------------------------------------------------------
CREATE TABLE CUSTOMER(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE TABLE INVOICE(ID INT, CUSTOMER_ID INT, PRIMARY KEY(CUSTOMER_ID, ID), "VALUE" DECIMAL(10,2));
> ok

INSERT INTO CUSTOMER VALUES(?, ?);
{
1,Lehmann
2,Meier
3,Scott
4,NULL
};
> update count: 4

INSERT INTO INVOICE VALUES(?, ?, ?);
{
10,1,100.10
11,1,10.01
12,1,1.001
20,2,22.2
21,2,200.02
};
> update count: 5

SELECT * FROM CUSTOMER WHERE ID IN(1,2,4,-1);
> ID NAME
> -- -------
> 1  Lehmann
> 2  Meier
> 4  null
> rows: 3

SELECT * FROM CUSTOMER WHERE ID NOT IN(3,4,5,'1');
> ID NAME
> -- -----
> 2  Meier
> rows: 1

SELECT * FROM CUSTOMER WHERE ID NOT IN(SELECT CUSTOMER_ID FROM INVOICE);
> ID NAME
> -- -----
> 3  Scott
> 4  null
> rows: 2

SELECT * FROM INVOICE WHERE CUSTOMER_ID IN(SELECT C.ID FROM CUSTOMER C);
> ID CUSTOMER_ID VALUE
> -- ----------- ------
> 10 1           100.10
> 11 1           10.01
> 12 1           1.00
> 20 2           22.20
> 21 2           200.02
> rows: 5

SELECT * FROM CUSTOMER WHERE NAME IN('Lehmann', 20);
> exception DATA_CONVERSION_ERROR_1

SELECT * FROM CUSTOMER WHERE NAME NOT IN('Scott');
> ID NAME
> -- -------
> 1  Lehmann
> 2  Meier
> rows: 2

SELECT * FROM CUSTOMER WHERE NAME IN(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- -------
> 1  Lehmann
> 2  Meier
> 3  Scott
> rows: 3

SELECT * FROM CUSTOMER WHERE NAME NOT IN(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- ----
> rows: 0

SELECT * FROM CUSTOMER WHERE NAME = ANY(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- -------
> 1  Lehmann
> 2  Meier
> 3  Scott
> rows: 3

SELECT * FROM CUSTOMER WHERE NAME = ALL(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- ----
> rows: 0

SELECT * FROM CUSTOMER WHERE NAME > ALL(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- ----
> rows: 0

SELECT * FROM CUSTOMER WHERE NAME > ANY(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- -----
> 2  Meier
> 3  Scott
> rows: 2

SELECT * FROM CUSTOMER WHERE NAME < ANY(SELECT NAME FROM CUSTOMER);
> ID NAME
> -- -------
> 1  Lehmann
> 2  Meier
> rows: 2

DROP TABLE INVOICE;
> ok

DROP TABLE CUSTOMER;
> ok

--- aggregates ----------------------------------------------------------------------------------------------
drop table if exists t;
> ok

create table t(x double precision, y double precision);
> ok

create view s as
select stddev_pop(x) s_px, stddev_samp(x) s_sx, var_pop(x) v_px, var_samp(x) v_sx,
stddev_pop(y) s_py, stddev_samp(y) s_sy, var_pop(y) v_py, var_samp(y) v_sy from t;
> ok

select var(100000000.1) z from system_range(1, 1000000);
> Z
> ---
> 0.0
> rows: 1

select * from s;
> S_PX S_SX V_PX V_SX S_PY S_SY V_PY V_SY
> ---- ---- ---- ---- ---- ---- ---- ----
> null null null null null null null null
> rows: 1

select some(y>10), every(y>10), min(y), max(y) from t;
> ANY(Y > 10.0) EVERY(Y > 10.0) MIN(Y) MAX(Y)
> ------------- --------------- ------ ------
> null          null            null   null
> rows: 1

insert into t values(1000000004, 4);
> update count: 1

select * from s;
> S_PX S_SX V_PX V_SX S_PY S_SY V_PY V_SY
> ---- ---- ---- ---- ---- ---- ---- ----
> 0.0  null 0.0  null 0.0  null 0.0  null
> rows: 1

insert into t values(1000000007, 7);
> update count: 1

select * from s;
> S_PX S_SX               V_PX V_SX S_PY S_SY               V_PY V_SY
> ---- ------------------ ---- ---- ---- ------------------ ---- ----
> 1.5  2.1213203435596424 2.25 4.5  1.5  2.1213203435596424 2.25 4.5
> rows: 1

insert into t values(1000000013, 13);
> update count: 1

select * from s;
> S_PX               S_SX             V_PX V_SX S_PY               S_SY             V_PY V_SY
> ------------------ ---------------- ---- ---- ------------------ ---------------- ---- ----
> 3.7416573867739413 4.58257569495584 14.0 21.0 3.7416573867739413 4.58257569495584 14.0 21.0
> rows: 1

insert into t values(1000000016, 16);
> update count: 1

select * from s;
> S_PX              S_SX              V_PX V_SX S_PY              S_SY              V_PY V_SY
> ----------------- ----------------- ---- ---- ----------------- ----------------- ---- ----
> 4.743416490252569 5.477225575051661 22.5 30.0 4.743416490252569 5.477225575051661 22.5 30.0
> rows: 1

insert into t values(1000000016, 16);
> update count: 1

select * from s;
> S_PX              S_SX              V_PX              V_SX               S_PY              S_SY              V_PY  V_SY
> ----------------- ----------------- ----------------- ------------------ ----------------- ----------------- ----- ------------------
> 4.874423036912116 5.449770630813229 23.75999994277954 29.699999928474426 4.874423042781577 5.449770637375485 23.76 29.700000000000003
> rows: 1

select stddev_pop(distinct x) s_px, stddev_samp(distinct x) s_sx, var_pop(distinct x) v_px, var_samp(distinct x) v_sx,
stddev_pop(distinct y) s_py, stddev_samp(distinct y) s_sy, var_pop(distinct y) v_py, var_samp(distinct y) V_SY from t;
> S_PX              S_SX              V_PX V_SX S_PY              S_SY              V_PY V_SY
> ----------------- ----------------- ---- ---- ----------------- ----------------- ---- ----
> 4.743416490252569 5.477225575051661 22.5 30.0 4.743416490252569 5.477225575051661 22.5 30.0
> rows: 1

select some(y>10), every(y>10), min(y), max(y) from t;
> ANY(Y > 10.0) EVERY(Y > 10.0) MIN(Y) MAX(Y)
> ------------- --------------- ------ ------
> TRUE          FALSE           4.0    16.0
> rows: 1

drop view s;
> ok

drop table t;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255), "VALUE" DECIMAL(10,2));
> ok

INSERT INTO TEST VALUES(?, ?, ?);
{
1,Apples,1.20
2,Oranges,2.05
3,Cherries,5.10
4,Apples,1.50
5,Apples,1.10
6,Oranges,1.80
7,Bananas,2.50
8,NULL,3.10
9,NULL,-10.0
};
> update count: 9

SELECT IFNULL(NAME, '') || ': ' || GROUP_CONCAT("VALUE" ORDER BY NAME, "VALUE" DESC SEPARATOR ', ') FROM TEST GROUP BY NAME ORDER BY 1;
> COALESCE(NAME, '') || ': ' || LISTAGG("VALUE", ', ') WITHIN GROUP (ORDER BY NAME, "VALUE" DESC)
> -----------------------------------------------------------------------------------------------
> : 3.10, -10.00
> Apples: 1.50, 1.20, 1.10
> Bananas: 2.50
> Cherries: 5.10
> Oranges: 2.05, 1.80
> rows (ordered): 5

SELECT GROUP_CONCAT(ID ORDER BY ID) FROM TEST;
> LISTAGG(ID) WITHIN GROUP (ORDER BY ID)
> --------------------------------------
> 1,2,3,4,5,6,7,8,9
> rows: 1

SELECT STRING_AGG(ID,';') FROM TEST;
> LISTAGG(ID, ';') WITHIN GROUP (ORDER BY NULL)
> ---------------------------------------------
> 1;2;3;4;5;6;7;8;9
> rows: 1

SELECT DISTINCT NAME FROM TEST;
> NAME
> --------
> Apples
> Bananas
> Cherries
> Oranges
> null
> rows: 5

SELECT DISTINCT NAME FROM TEST ORDER BY NAME DESC NULLS LAST;
> NAME
> --------
> Oranges
> Cherries
> Bananas
> Apples
> null
> rows (ordered): 5

SELECT DISTINCT NAME FROM TEST ORDER BY NAME DESC NULLS LAST LIMIT 2 OFFSET 1;
> NAME
> --------
> Cherries
> Bananas
> rows (ordered): 2

SELECT NAME, COUNT(*), SUM("VALUE"), MAX("VALUE"), MIN("VALUE"), AVG("VALUE"), COUNT(DISTINCT "VALUE") FROM TEST GROUP BY NAME;
> NAME     COUNT(*) SUM("VALUE") MAX("VALUE") MIN("VALUE") AVG("VALUE")    COUNT(DISTINCT "VALUE")
> -------- -------- ------------ ------------ ------------ --------------- -----------------------
> Apples   3        3.80         1.50         1.10         1.266666666667  3
> Bananas  1        2.50         2.50         2.50         2.500000000000  1
> Cherries 1        5.10         5.10         5.10         5.100000000000  1
> Oranges  2        3.85         2.05         1.80         1.925000000000  2
> null     2        -6.90        3.10         -10.00       -3.450000000000 2
> rows: 5

SELECT NAME, MAX("VALUE"), MIN("VALUE"), MAX("VALUE"+1)*MIN("VALUE"+1) FROM TEST GROUP BY NAME;
> NAME     MAX("VALUE") MIN("VALUE") MAX("VALUE" + 1) * MIN("VALUE" + 1)
> -------- ------------ ------------ -----------------------------------
> Apples   1.50         1.10         5.2500
> Bananas  2.50         2.50         12.2500
> Cherries 5.10         5.10         37.2100
> Oranges  2.05         1.80         8.5400
> null     3.10         -10.00       -36.9000
> rows: 5

DROP TABLE TEST;
> ok

--- order by ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE UNIQUE INDEX IDXNAME ON TEST(NAME);
> ok

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(2, 'World');
> update count: 1

INSERT INTO TEST VALUES(3, NULL);
> update count: 1

SELECT * FROM TEST ORDER BY NAME;
> ID NAME
> -- -----
> 3  null
> 1  Hello
> 2  World
> rows (ordered): 3

SELECT * FROM TEST ORDER BY NAME DESC;
> ID NAME
> -- -----
> 2  World
> 1  Hello
> 3  null
> rows (ordered): 3

SELECT * FROM TEST ORDER BY NAME NULLS FIRST;
> ID NAME
> -- -----
> 3  null
> 1  Hello
> 2  World
> rows (ordered): 3

SELECT * FROM TEST ORDER BY NAME DESC NULLS FIRST;
> ID NAME
> -- -----
> 3  null
> 2  World
> 1  Hello
> rows (ordered): 3

SELECT * FROM TEST ORDER BY NAME NULLS LAST;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> 3  null
> rows (ordered): 3

SELECT * FROM TEST ORDER BY NAME DESC NULLS LAST;
> ID NAME
> -- -----
> 2  World
> 1  Hello
> 3  null
> rows (ordered): 3

SELECT ID, '=', NAME FROM TEST ORDER BY 2 FOR UPDATE;
> ID '=' NAME
> -- --- -----
> 1  =   Hello
> 2  =   World
> 3  =   null
> rows: 3

DROP TABLE TEST;
> ok

--- having ----------------------------------------------------------------------------------------------
CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE INDEX IDXNAME ON TEST(NAME);
> ok

INSERT INTO TEST VALUES(1, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(2, 'Hello');
> update count: 1

INSERT INTO TEST VALUES(3, 'World');
> update count: 1

INSERT INTO TEST VALUES(4, 'World');
> update count: 1

INSERT INTO TEST VALUES(5, 'Orange');
> update count: 1

SELECT NAME, SUM(ID) FROM TEST GROUP BY NAME HAVING COUNT(*)>1 ORDER BY NAME;
> NAME  SUM(ID)
> ----- -------
> Hello 3
> World 7
> rows (ordered): 2

DROP INDEX IF EXISTS IDXNAME;
> ok

DROP TABLE TEST;
> ok

--- sequence ----------------------------------------------------------------------------------------------
CREATE CACHED TABLE TEST(ID INT PRIMARY KEY);
> ok

CREATE CACHED TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY);
> ok

CREATE SEQUENCE IF NOT EXISTS TEST_SEQ START WITH 10;
> ok

CREATE SEQUENCE IF NOT EXISTS TEST_SEQ START WITH 20;
> ok

INSERT INTO TEST VALUES(NEXT VALUE FOR TEST_SEQ);
> update count: 1

CALL CURRVAL('test_seq');
> CURRVAL('test_seq')
> -------------------
> 10
> rows: 1

INSERT INTO TEST VALUES(NEXT VALUE FOR TEST_SEQ);
> update count: 1

CALL NEXT VALUE FOR TEST_SEQ;
> NEXT VALUE FOR PUBLIC.TEST_SEQ
> ------------------------------
> 12
> rows: 1

INSERT INTO TEST VALUES(NEXT VALUE FOR TEST_SEQ);
> update count: 1

SELECT * FROM TEST;
> ID
> --
> 10
> 11
> 13
> rows: 3

SELECT TOP 2 * FROM TEST;
> ID
> --
> 10
> 11
> rows: 2

SELECT TOP 2 * FROM TEST ORDER BY ID DESC;
> ID
> --
> 13
> 11
> rows (ordered): 2

ALTER SEQUENCE TEST_SEQ RESTART WITH 20 INCREMENT BY -1;
> ok

INSERT INTO TEST VALUES(NEXT VALUE FOR TEST_SEQ);
> update count: 1

INSERT INTO TEST VALUES(NEXT VALUE FOR TEST_SEQ);
> update count: 1

SELECT * FROM TEST ORDER BY ID ASC;
> ID
> --
> 10
> 11
> 13
> 19
> 20
> rows (ordered): 5

CALL NEXTVAL('test_seq');
> NEXTVAL('test_seq')
> -------------------
> 18
> rows: 1

DROP SEQUENCE IF EXISTS TEST_SEQ;
> ok

DROP SEQUENCE IF EXISTS TEST_SEQ;
> ok

CREATE SEQUENCE TEST_LONG START WITH 90123456789012345 MAXVALUE 90123456789012345 INCREMENT BY -1;
> ok

SET AUTOCOMMIT FALSE;
> ok

CALL NEXT VALUE FOR TEST_LONG;
> NEXT VALUE FOR PUBLIC.TEST_LONG
> -------------------------------
> 90123456789012345
> rows: 1

SELECT SEQUENCE_NAME, BASE_VALUE, INCREMENT FROM INFORMATION_SCHEMA.SEQUENCES;
> SEQUENCE_NAME BASE_VALUE        INCREMENT
> ------------- ----------------- ---------
> TEST_LONG     90123456789012344 -1
> rows: 1

SET AUTOCOMMIT TRUE;
> ok

DROP SEQUENCE TEST_LONG;
> ok

DROP TABLE TEST;
> ok

--- call ----------------------------------------------------------------------------------------------
CALL PI();
> 3.141592653589793
> -----------------
> 3.141592653589793
> rows: 1

CALL 1+1;
> 2
> -
> 2
> rows: 1

--- constraints ----------------------------------------------------------------------------------------------
CREATE TABLE PARENT(A INT, B INT, PRIMARY KEY(A, B));
> ok

CREATE TABLE CHILD(ID INT PRIMARY KEY, PA INT, PB INT, CONSTRAINT AB FOREIGN KEY(PA, PB) REFERENCES PARENT(A, B));
> ok

TABLE INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS;
> CONSTRAINT_CATALOG CONSTRAINT_SCHEMA CONSTRAINT_NAME UNIQUE_CONSTRAINT_CATALOG UNIQUE_CONSTRAINT_SCHEMA UNIQUE_CONSTRAINT_NAME MATCH_OPTION UPDATE_RULE DELETE_RULE
> ------------------ ----------------- --------------- ------------------------- ------------------------ ---------------------- ------------ ----------- -----------
> SCRIPT             PUBLIC            AB              SCRIPT                    PUBLIC                   CONSTRAINT_8           NONE         RESTRICT    RESTRICT
> rows: 1

TABLE INFORMATION_SCHEMA.KEY_COLUMN_USAGE;
> CONSTRAINT_CATALOG CONSTRAINT_SCHEMA CONSTRAINT_NAME TABLE_CATALOG TABLE_SCHEMA TABLE_NAME COLUMN_NAME ORDINAL_POSITION POSITION_IN_UNIQUE_CONSTRAINT
> ------------------ ----------------- --------------- ------------- ------------ ---------- ----------- ---------------- -----------------------------
> SCRIPT             PUBLIC            AB              SCRIPT        PUBLIC       CHILD      PA          1                1
> SCRIPT             PUBLIC            AB              SCRIPT        PUBLIC       CHILD      PB          2                2
> SCRIPT             PUBLIC            CONSTRAINT_3    SCRIPT        PUBLIC       CHILD      ID          1                null
> SCRIPT             PUBLIC            CONSTRAINT_8    SCRIPT        PUBLIC       PARENT     A           1                null
> SCRIPT             PUBLIC            CONSTRAINT_8    SCRIPT        PUBLIC       PARENT     B           2                null
> rows: 5

DROP TABLE PARENT, CHILD;
> ok

drop table if exists test;
> ok

create table test(id int primary key, parent int unique, foreign key(id) references test(parent));
> ok

insert into test values(1, 1);
> update count: 1

delete from test;
> update count: 1

drop table test;
> ok

drop table if exists child;
> ok

drop table if exists parent;
> ok

create table child(a int, id int);
> ok

create table parent(id int primary key);
> ok

alter table child add foreign key(id) references parent;
> ok

insert into parent values(1);
> update count: 1

delete from parent;
> update count: 1

drop table if exists child;
> ok

drop table if exists parent;
> ok

CREATE MEMORY TABLE PARENT(ID INT PRIMARY KEY);
> ok

CREATE MEMORY TABLE CHILD(ID INT, PARENT_ID INT, FOREIGN KEY(PARENT_ID) REFERENCES PARENT);
> ok

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> ----------------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."PARENT"( "ID" INTEGER NOT NULL );
> ALTER TABLE "PUBLIC"."PARENT" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_8" PRIMARY KEY("ID");
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.PARENT;
> CREATE MEMORY TABLE "PUBLIC"."CHILD"( "ID" INTEGER, "PARENT_ID" INTEGER );
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.CHILD;
> ALTER TABLE "PUBLIC"."CHILD" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_3" FOREIGN KEY("PARENT_ID") REFERENCES "PUBLIC"."PARENT"("ID") NOCHECK;
> rows (ordered): 7

DROP TABLE PARENT, CHILD;
> ok

CREATE TABLE TEST(ID INT, CONSTRAINT PK PRIMARY KEY(ID), NAME VARCHAR, PARENT INT, CONSTRAINT P FOREIGN KEY(PARENT) REFERENCES(ID));
> ok

ALTER TABLE TEST DROP PRIMARY KEY;
> exception INDEX_BELONGS_TO_CONSTRAINT_2

ALTER TABLE TEST DROP CONSTRAINT PK;
> exception CONSTRAINT_IS_USED_BY_CONSTRAINT_2

INSERT INTO TEST VALUES(1, 'Frank', 1);
> update count: 1

INSERT INTO TEST VALUES(2, 'Sue', 1);
> update count: 1

INSERT INTO TEST VALUES(3, 'Karin', 2);
> update count: 1

INSERT INTO TEST VALUES(4, 'Joe', 5);
> exception REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1

INSERT INTO TEST VALUES(4, 'Joe', 3);
> update count: 1

DROP TABLE TEST;
> ok

CREATE MEMORY TABLE TEST(A_INT INT NOT NULL, B_INT INT NOT NULL, PRIMARY KEY(A_INT, B_INT), CONSTRAINT U_B UNIQUE(B_INT));
> ok

ALTER TABLE TEST ADD CONSTRAINT A_UNIQUE UNIQUE(A_INT);
> ok

ALTER TABLE TEST DROP PRIMARY KEY;
> ok

ALTER TABLE TEST DROP PRIMARY KEY;
> exception INDEX_NOT_FOUND_1

ALTER TABLE TEST DROP CONSTRAINT A_UNIQUE;
> ok

ALTER TABLE TEST ADD CONSTRAINT C1 FOREIGN KEY(A_INT) REFERENCES TEST(B_INT);
> ok

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> --------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "A_INT" INTEGER NOT NULL, "B_INT" INTEGER NOT NULL );
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."U_B" UNIQUE NULLS DISTINCT ("B_INT");
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."C1" FOREIGN KEY("A_INT") REFERENCES "PUBLIC"."TEST"("B_INT") NOCHECK;
> rows (ordered): 5

ALTER TABLE TEST DROP CONSTRAINT C1;
> ok

ALTER TABLE TEST DROP CONSTRAINT C1;
> exception CONSTRAINT_NOT_FOUND_1

DROP TABLE TEST;
> ok

CREATE MEMORY TABLE A_TEST(A_INT INT NOT NULL, A_VARCHAR VARCHAR(255) DEFAULT 'x', A_DATE DATE, A_DECIMAL DECIMAL(10,2));
> ok

ALTER TABLE A_TEST ADD PRIMARY KEY(A_INT);
> ok

ALTER TABLE A_TEST ADD CONSTRAINT MIN_LENGTH CHECK LENGTH(A_VARCHAR)>1;
> ok

ALTER TABLE A_TEST ADD CONSTRAINT DATE_UNIQUE UNIQUE(A_DATE);
> ok

ALTER TABLE A_TEST ADD CONSTRAINT DATE_UNIQUE_2 UNIQUE(A_DATE);
> ok

INSERT INTO A_TEST VALUES(NULL, NULL, NULL, NULL);
> exception NULL_NOT_ALLOWED

INSERT INTO A_TEST VALUES(1, 'A', NULL, NULL);
> exception CHECK_CONSTRAINT_VIOLATED_1

INSERT INTO A_TEST VALUES(1, 'AB', NULL, NULL);
> update count: 1

INSERT INTO A_TEST VALUES(1, 'AB', NULL, NULL);
> exception DUPLICATE_KEY_1

INSERT INTO A_TEST VALUES(2, 'AB', NULL, NULL);
> update count: 1

INSERT INTO A_TEST VALUES(3, 'AB', '2004-01-01', NULL);
> update count: 1

INSERT INTO A_TEST VALUES(4, 'AB', '2004-01-01', NULL);
> exception DUPLICATE_KEY_1

INSERT INTO A_TEST VALUES(5, 'ABC', '2004-01-02', NULL);
> update count: 1

CREATE MEMORY TABLE B_TEST(B_INT INT DEFAULT -1 NOT NULL , B_VARCHAR VARCHAR(255) DEFAULT NULL NULL, CONSTRAINT B_UNIQUE UNIQUE(B_INT));
> ok

ALTER TABLE B_TEST ADD CHECK LENGTH(B_VARCHAR)>1;
> ok

ALTER TABLE B_TEST ADD CONSTRAINT C1 FOREIGN KEY(B_INT) REFERENCES A_TEST(A_INT) ON DELETE CASCADE ON UPDATE CASCADE;
> ok

ALTER TABLE B_TEST ADD PRIMARY KEY(B_INT);
> ok

INSERT INTO B_TEST VALUES(10, 'X');
> exception CHECK_CONSTRAINT_VIOLATED_1

INSERT INTO B_TEST VALUES(1, 'X');
> exception CHECK_CONSTRAINT_VIOLATED_1

INSERT INTO B_TEST VALUES(1, 'XX');
> update count: 1

SELECT * FROM B_TEST;
> B_INT B_VARCHAR
> ----- ---------
> 1     XX
> rows: 1

UPDATE A_TEST SET A_INT = A_INT*10;
> update count: 4

SELECT * FROM B_TEST;
> B_INT B_VARCHAR
> ----- ---------
> 10    XX
> rows: 1

ALTER TABLE B_TEST DROP CONSTRAINT C1;
> ok

ALTER TABLE B_TEST ADD CONSTRAINT C2 FOREIGN KEY(B_INT) REFERENCES A_TEST(A_INT) ON DELETE SET NULL ON UPDATE SET NULL;
> ok

UPDATE A_TEST SET A_INT = A_INT*10;
> exception NULL_NOT_ALLOWED

SELECT * FROM B_TEST;
> B_INT B_VARCHAR
> ----- ---------
> 10    XX
> rows: 1

ALTER TABLE B_TEST DROP CONSTRAINT C2;
> ok

UPDATE B_TEST SET B_INT = 20;
> update count: 1

SELECT A_INT FROM A_TEST;
> A_INT
> -----
> 10
> 20
> 30
> 50
> rows: 4

ALTER TABLE B_TEST ADD CONSTRAINT C3 FOREIGN KEY(B_INT) REFERENCES A_TEST(A_INT) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT;
> ok

UPDATE A_TEST SET A_INT = A_INT*10;
> update count: 4

SELECT * FROM B_TEST;
> B_INT B_VARCHAR
> ----- ---------
> -1    XX
> rows: 1

DELETE FROM A_TEST;
> update count: 4

SELECT * FROM B_TEST;
> B_INT B_VARCHAR
> ----- ---------
> -1    XX
> rows: 1

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."A_TEST"( "A_INT" INTEGER NOT NULL, "A_VARCHAR" CHARACTER VARYING(255) DEFAULT 'x', "A_DATE" DATE, "A_DECIMAL" DECIMAL(10, 2) );
> ALTER TABLE "PUBLIC"."A_TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_7" PRIMARY KEY("A_INT");
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.A_TEST;
> CREATE MEMORY TABLE "PUBLIC"."B_TEST"( "B_INT" INTEGER DEFAULT -1 NOT NULL, "B_VARCHAR" CHARACTER VARYING(255) DEFAULT NULL );
> ALTER TABLE "PUBLIC"."B_TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_760" PRIMARY KEY("B_INT");
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.B_TEST;
> INSERT INTO "PUBLIC"."B_TEST" VALUES (-1, 'XX');
> ALTER TABLE "PUBLIC"."A_TEST" ADD CONSTRAINT "PUBLIC"."MIN_LENGTH" CHECK(CHAR_LENGTH("A_VARCHAR") > 1) NOCHECK;
> ALTER TABLE "PUBLIC"."B_TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_76" CHECK(CHAR_LENGTH("B_VARCHAR") > 1) NOCHECK;
> ALTER TABLE "PUBLIC"."A_TEST" ADD CONSTRAINT "PUBLIC"."DATE_UNIQUE" UNIQUE NULLS DISTINCT ("A_DATE");
> ALTER TABLE "PUBLIC"."A_TEST" ADD CONSTRAINT "PUBLIC"."DATE_UNIQUE_2" UNIQUE NULLS DISTINCT ("A_DATE");
> ALTER TABLE "PUBLIC"."B_TEST" ADD CONSTRAINT "PUBLIC"."B_UNIQUE" UNIQUE NULLS DISTINCT ("B_INT");
> ALTER TABLE "PUBLIC"."B_TEST" ADD CONSTRAINT "PUBLIC"."C3" FOREIGN KEY("B_INT") REFERENCES "PUBLIC"."A_TEST"("A_INT") ON DELETE SET DEFAULT ON UPDATE SET DEFAULT NOCHECK;
> rows (ordered): 14

DROP TABLE A_TEST, B_TEST;
> ok

CREATE MEMORY TABLE FAMILY(ID INT PRIMARY KEY, NAME VARCHAR(20));
> ok

CREATE INDEX FAMILY_ID_NAME ON FAMILY(ID, NAME);
> ok

CREATE MEMORY TABLE PARENT(ID INT, FAMILY_ID INT, NAME VARCHAR(20), UNIQUE(ID, FAMILY_ID));
> ok

ALTER TABLE PARENT ADD CONSTRAINT PARENT_FAMILY FOREIGN KEY(FAMILY_ID)
REFERENCES FAMILY(ID);
> ok

CREATE MEMORY TABLE CHILD(
ID INT,
PARENTID INT,
FAMILY_ID INT,
UNIQUE(ID, PARENTID),
CONSTRAINT PARENT_CHILD FOREIGN KEY(PARENTID, FAMILY_ID)
REFERENCES PARENT(ID, FAMILY_ID)
ON UPDATE CASCADE
ON DELETE SET NULL,
NAME VARCHAR(20));
> ok

INSERT INTO FAMILY VALUES(1, 'Capone');
> update count: 1

INSERT INTO CHILD VALUES(100, 1, 1, 'early');
> exception REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1

INSERT INTO PARENT VALUES(1, 1, 'Sue');
> update count: 1

INSERT INTO PARENT VALUES(2, 1, 'Joe');
> update count: 1

INSERT INTO CHILD VALUES(100, 1, 1, 'Simon');
> update count: 1

INSERT INTO CHILD VALUES(101, 1, 1, 'Sabine');
> update count: 1

INSERT INTO CHILD VALUES(200, 2, 1, 'Jim');
> update count: 1

INSERT INTO CHILD VALUES(201, 2, 1, 'Johann');
> update count: 1

UPDATE PARENT SET ID=3 WHERE ID=1;
> update count: 1

SELECT * FROM CHILD;
> ID  PARENTID FAMILY_ID NAME
> --- -------- --------- ------
> 100 3        1         Simon
> 101 3        1         Sabine
> 200 2        1         Jim
> 201 2        1         Johann
> rows: 4

UPDATE CHILD SET PARENTID=-1 WHERE PARENTID IS NOT NULL;
> exception REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1

DELETE FROM PARENT WHERE ID=2;
> update count: 1

SELECT * FROM CHILD;
> ID  PARENTID FAMILY_ID NAME
> --- -------- --------- ------
> 100 3        1         Simon
> 101 3        1         Sabine
> 200 null     null      Jim
> 201 null     null      Johann
> rows: 4

SCRIPT SIMPLE NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."FAMILY"( "ID" INTEGER NOT NULL, "NAME" CHARACTER VARYING(20) );
> ALTER TABLE "PUBLIC"."FAMILY" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_7" PRIMARY KEY("ID");
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.FAMILY;
> INSERT INTO "PUBLIC"."FAMILY" VALUES(1, 'Capone');
> CREATE INDEX "PUBLIC"."FAMILY_ID_NAME" ON "PUBLIC"."FAMILY"("ID" NULLS FIRST, "NAME" NULLS FIRST);
> CREATE MEMORY TABLE "PUBLIC"."PARENT"( "ID" INTEGER, "FAMILY_ID" INTEGER, "NAME" CHARACTER VARYING(20) );
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.PARENT;
> INSERT INTO "PUBLIC"."PARENT" VALUES(3, 1, 'Sue');
> CREATE MEMORY TABLE "PUBLIC"."CHILD"( "ID" INTEGER, "PARENTID" INTEGER, "FAMILY_ID" INTEGER, "NAME" CHARACTER VARYING(20) );
> -- 4 +/- SELECT COUNT(*) FROM PUBLIC.CHILD;
> INSERT INTO "PUBLIC"."CHILD" VALUES(100, 3, 1, 'Simon');
> INSERT INTO "PUBLIC"."CHILD" VALUES(101, 3, 1, 'Sabine');
> INSERT INTO "PUBLIC"."CHILD" VALUES(200, NULL, NULL, 'Jim');
> INSERT INTO "PUBLIC"."CHILD" VALUES(201, NULL, NULL, 'Johann');
> ALTER TABLE "PUBLIC"."CHILD" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_3" UNIQUE NULLS DISTINCT ("ID", "PARENTID");
> ALTER TABLE "PUBLIC"."PARENT" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_8" UNIQUE NULLS DISTINCT ("ID", "FAMILY_ID");
> ALTER TABLE "PUBLIC"."CHILD" ADD CONSTRAINT "PUBLIC"."PARENT_CHILD" FOREIGN KEY("PARENTID", "FAMILY_ID") REFERENCES "PUBLIC"."PARENT"("ID", "FAMILY_ID") ON DELETE SET NULL ON UPDATE CASCADE NOCHECK;
> ALTER TABLE "PUBLIC"."PARENT" ADD CONSTRAINT "PUBLIC"."PARENT_FAMILY" FOREIGN KEY("FAMILY_ID") REFERENCES "PUBLIC"."FAMILY"("ID") NOCHECK;
> rows (ordered): 19

ALTER TABLE CHILD DROP CONSTRAINT PARENT_CHILD;
> ok

SCRIPT SIMPLE NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> ------------------------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."FAMILY"( "ID" INTEGER NOT NULL, "NAME" CHARACTER VARYING(20) );
> ALTER TABLE "PUBLIC"."FAMILY" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_7" PRIMARY KEY("ID");
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.FAMILY;
> INSERT INTO "PUBLIC"."FAMILY" VALUES(1, 'Capone');
> CREATE INDEX "PUBLIC"."FAMILY_ID_NAME" ON "PUBLIC"."FAMILY"("ID" NULLS FIRST, "NAME" NULLS FIRST);
> CREATE MEMORY TABLE "PUBLIC"."PARENT"( "ID" INTEGER, "FAMILY_ID" INTEGER, "NAME" CHARACTER VARYING(20) );
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.PARENT;
> INSERT INTO "PUBLIC"."PARENT" VALUES(3, 1, 'Sue');
> CREATE MEMORY TABLE "PUBLIC"."CHILD"( "ID" INTEGER, "PARENTID" INTEGER, "FAMILY_ID" INTEGER, "NAME" CHARACTER VARYING(20) );
> -- 4 +/- SELECT COUNT(*) FROM PUBLIC.CHILD;
> INSERT INTO "PUBLIC"."CHILD" VALUES(100, 3, 1, 'Simon');
> INSERT INTO "PUBLIC"."CHILD" VALUES(101, 3, 1, 'Sabine');
> INSERT INTO "PUBLIC"."CHILD" VALUES(200, NULL, NULL, 'Jim');
> INSERT INTO "PUBLIC"."CHILD" VALUES(201, NULL, NULL, 'Johann');
> ALTER TABLE "PUBLIC"."CHILD" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_3" UNIQUE NULLS DISTINCT ("ID", "PARENTID");
> ALTER TABLE "PUBLIC"."PARENT" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_8" UNIQUE NULLS DISTINCT ("ID", "FAMILY_ID");
> ALTER TABLE "PUBLIC"."PARENT" ADD CONSTRAINT "PUBLIC"."PARENT_FAMILY" FOREIGN KEY("FAMILY_ID") REFERENCES "PUBLIC"."FAMILY"("ID") NOCHECK;
> rows (ordered): 18

DELETE FROM PARENT;
> update count: 1

SELECT * FROM CHILD;
> ID  PARENTID FAMILY_ID NAME
> --- -------- --------- ------
> 100 3        1         Simon
> 101 3        1         Sabine
> 200 null     null      Jim
> 201 null     null      Johann
> rows: 4

DROP TABLE PARENT;
> ok

DROP TABLE CHILD;
> ok

DROP TABLE FAMILY;
> ok

CREATE TABLE INVOICE(CUSTOMER_ID INT, ID INT, TOTAL_AMOUNT DECIMAL(10,2), PRIMARY KEY(CUSTOMER_ID, ID));
> ok

CREATE TABLE INVOICE_LINE(CUSTOMER_ID INT, INVOICE_ID INT, LINE_ID INT, TEXT VARCHAR, AMOUNT DECIMAL(10,2));
> ok

CREATE INDEX ON INVOICE_LINE(CUSTOMER_ID);
> ok

ALTER TABLE INVOICE_LINE ADD FOREIGN KEY(CUSTOMER_ID, INVOICE_ID) REFERENCES INVOICE(CUSTOMER_ID, ID) ON DELETE CASCADE;
> ok

INSERT INTO INVOICE VALUES(1, 100, NULL), (1, 101, NULL);
> update count: 2

INSERT INTO INVOICE_LINE VALUES(1, 100, 10, 'Apples', 20.35), (1, 100, 20, 'Paper', 10.05), (1, 101, 10, 'Pencil', 1.10), (1, 101, 20, 'Chair', 540.40);
> update count: 4

INSERT INTO INVOICE_LINE VALUES(1, 102, 20, 'Nothing', 30.00);
> exception REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1

DELETE FROM INVOICE WHERE ID = 100;
> update count: 1

SELECT * FROM INVOICE_LINE;
> CUSTOMER_ID INVOICE_ID LINE_ID TEXT   AMOUNT
> ----------- ---------- ------- ------ ------
> 1           101        10      Pencil 1.10
> 1           101        20      Chair  540.40
> rows: 2

DROP TABLE INVOICE, INVOICE_LINE;
> ok

CREATE MEMORY TABLE TEST(A INT PRIMARY KEY, B INT, FOREIGN KEY (B) REFERENCES(A) ON UPDATE RESTRICT ON DELETE NO ACTION);
> ok

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> -----------------------------------------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "A" INTEGER NOT NULL, "B" INTEGER );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("A");
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_27" FOREIGN KEY("B") REFERENCES "PUBLIC"."TEST"("A") NOCHECK;
> rows (ordered): 5

DROP TABLE TEST;
> ok

--- users ----------------------------------------------------------------------------------------------
CREATE USER TEST PASSWORD 'abc';
> ok

CREATE USER TEST_ADMIN_X PASSWORD 'def' ADMIN;
> ok

ALTER USER TEST_ADMIN_X RENAME TO TEST_ADMIN;
> ok

ALTER USER TEST_ADMIN ADMIN TRUE;
> ok

CREATE USER TEST2 PASSWORD '123' ADMIN;
> ok

ALTER USER TEST2 SET PASSWORD 'abc';
> ok

ALTER USER TEST2 ADMIN FALSE;
> ok

CREATE MEMORY TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

CREATE MEMORY TABLE TEST2_X(ID INT);
> ok

CREATE INDEX IDX_ID ON TEST2_X(ID);
> ok

ALTER TABLE TEST2_X RENAME TO TEST2;
> ok

ALTER INDEX IDX_ID RENAME TO IDX_ID2;
> ok

SCRIPT NOPASSWORDS NOSETTINGS NOVERSION;
> SCRIPT
> --------------------------------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> CREATE USER IF NOT EXISTS "TEST_ADMIN" PASSWORD '' ADMIN;
> CREATE USER IF NOT EXISTS "TEST" PASSWORD '';
> CREATE USER IF NOT EXISTS "TEST2" PASSWORD '';
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "ID" INTEGER NOT NULL, "NAME" CHARACTER VARYING(255) );
> ALTER TABLE "PUBLIC"."TEST" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_2" PRIMARY KEY("ID");
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE "PUBLIC"."TEST2"( "ID" INTEGER );
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST2;
> CREATE INDEX "PUBLIC"."IDX_ID2" ON "PUBLIC"."TEST2"("ID" NULLS FIRST);
> rows (ordered): 10

SELECT USER_NAME, IS_ADMIN FROM INFORMATION_SCHEMA.USERS;
> USER_NAME  IS_ADMIN
> ---------- --------
> SA         TRUE
> TEST       FALSE
> TEST2      FALSE
> TEST_ADMIN TRUE
> rows: 4

DROP TABLE TEST2;
> ok

DROP TABLE TEST;
> ok

DROP USER TEST;
> ok

DROP USER IF EXISTS TEST;
> ok

DROP USER IF EXISTS TEST2;
> ok

DROP USER TEST_ADMIN;
> ok

SET AUTOCOMMIT FALSE;
> ok

SET SALT '' HASH '';
> ok

CREATE USER SECURE SALT '001122' HASH '1122334455';
> ok

ALTER USER SECURE SET SALT '112233' HASH '2233445566';
> ok

SCRIPT NOSETTINGS NOVERSION;
> SCRIPT
> -------------------------------------------------------------------
> CREATE USER IF NOT EXISTS "SA" SALT '' HASH '' ADMIN;
> CREATE USER IF NOT EXISTS "SECURE" SALT '112233' HASH '2233445566';
> rows (ordered): 2

SET PASSWORD '123';
> ok

SET AUTOCOMMIT TRUE;
> ok

DROP USER SECURE;
> ok

--- test cases ---------------------------------------------------------------------------------------------
create table test(id int, name varchar);
> ok

insert into test values(5, 'b'), (5, 'b'), (20, 'a');
> update count: 3

drop table test;
> ok

select 0 from ((
select 0 as f from dual u1 where null in (?, ?, ?, ?, ?)
) union all (
select u2.f from (
select 0 as f from (
select 0 from dual u2f1f1 where now() = ?
) u2f1
) u2
)) where f = 12345;
{
11, 22, 33, 44, 55, null
> 0
> -
> rows: 0
};
> update count: 0

create table x(id int not null);
> ok

alter table if exists y add column a varchar;
> ok

alter table if exists x add column a varchar;
> ok

alter table if exists x add column a varchar;
> exception DUPLICATE_COLUMN_NAME_1

alter table if exists y alter column a rename to b;
> ok

alter table if exists x alter column a rename to b;
> ok

alter table if exists x alter column a rename to b;
> exception COLUMN_NOT_FOUND_1

alter table if exists y alter column b set default 'a';
> ok

alter table if exists x alter column b set default 'a';
> ok

insert into x(id) values(1);
> update count: 1

select b from x;
>> a

delete from x;
> update count: 1

alter table if exists y alter column b drop default;
> ok

alter table if exists x alter column b drop default;
> ok

alter table if exists y alter column b set not null;
> ok

alter table if exists x alter column b set not null;
> ok

insert into x(id) values(1);
> exception NULL_NOT_ALLOWED

alter table if exists y alter column b drop not null;
> ok

alter table if exists x alter column b drop not null;
> ok

insert into x(id) values(1);
> update count: 1

select b from x;
>> null

delete from x;
> update count: 1

alter table if exists y add constraint x_pk primary key (id);
> ok

alter table if exists x add constraint x_pk primary key (id);
> ok

alter table if exists x add constraint x_pk primary key (id);
> exception CONSTRAINT_ALREADY_EXISTS_1

insert into x(id) values(1);
> update count: 1

insert into x(id) values(1);
> exception DUPLICATE_KEY_1

delete from x;
> update count: 1

alter table if exists y add constraint x_check check (b = 'a');
> ok

alter table if exists x add constraint x_check check (b = 'a');
> ok

alter table if exists x add constraint x_check check (b = 'a');
> exception CONSTRAINT_ALREADY_EXISTS_1

insert into x(id, b) values(1, 'b');
> exception CHECK_CONSTRAINT_VIOLATED_1

alter table if exists y rename constraint x_check to x_check1;
> ok

alter table if exists x rename constraint x_check to x_check1;
> ok

alter table if exists x rename constraint x_check to x_check1;
> exception CONSTRAINT_NOT_FOUND_1

alter table if exists y drop constraint x_check1;
> ok

alter table if exists x drop constraint x_check1;
> ok

alter table if exists y rename to z;
> ok

alter table if exists x rename to z;
> ok

alter table if exists x rename to z;
> ok

insert into z(id, b) values(1, 'b');
> update count: 1

delete from z;
> update count: 1

alter table if exists y add constraint z_uk unique (b);
> ok

alter table if exists z add constraint z_uk unique (b);
> ok

alter table if exists z add constraint z_uk unique (b);
> exception CONSTRAINT_ALREADY_EXISTS_1

insert into z(id, b) values(1, 'b');
> update count: 1

insert into z(id, b) values(1, 'b');
> exception DUPLICATE_KEY_1

delete from z;
> update count: 1

alter table if exists y drop column b;
> ok

alter table if exists z drop column b;
> ok

alter table if exists z drop column b;
> exception COLUMN_NOT_FOUND_1

alter table if exists y drop primary key;
> ok

alter table if exists z drop primary key;
> ok

alter table if exists z drop primary key;
> exception INDEX_NOT_FOUND_1

create table x (id int not null primary key);
> ok

alter table if exists y add constraint z_fk foreign key (id) references x (id);
> ok

alter table if exists z add constraint z_fk foreign key (id) references x (id);
> ok

alter table if exists z add constraint z_fk foreign key (id) references x (id);
> exception CONSTRAINT_ALREADY_EXISTS_1

insert into z (id) values (1);
> exception REFERENTIAL_INTEGRITY_VIOLATED_PARENT_MISSING_1

SET MODE MySQL;
> ok

alter table if exists y drop foreign key z_fk;
> ok

alter table if exists z drop foreign key z_fk;
> ok

alter table if exists z drop foreign key z_fk;
> exception CONSTRAINT_NOT_FOUND_1

SET MODE Regular;
> ok

insert into z (id) values (1);
> update count: 1

delete from z;
> update count: 1

drop table x;
> ok

drop table z;
> ok

create schema x;
> ok

alter schema if exists y rename to z;
> ok

alter schema if exists x rename to z;
> ok

alter schema if exists x rename to z;
> ok

create table z.z (id int);
> ok

drop schema z cascade;
> ok

----- Issue#493 -----
create table test ("YEAR" int, action varchar(10));
> ok

insert into test values (2015, 'order'), (2016, 'order'), (2014, 'order');
> update count: 3

insert into test values (2014, 'execution'), (2015, 'execution'), (2016, 'execution');
> update count: 3

select * from test where "YEAR" in (select distinct "YEAR" from test order by "YEAR" desc limit 1 offset 0);
> YEAR ACTION
> ---- ---------
> 2016 execution
> 2016 order
> rows: 2

drop table test;
> ok
