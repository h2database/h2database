-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(T INT);
> ok

SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'T';
>> INT

-- SET DEFAULT
ALTER TABLE TEST ALTER COLUMN T SET DEFAULT 1;
> ok

SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'T';
>> 1

-- DROP DEFAULT
ALTER TABLE TEST ALTER COLUMN T DROP DEFAULT;
> ok

SELECT COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'T';
>> null

-- SET NOT NULL
ALTER TABLE TEST ALTER COLUMN T SET NOT NULL;
> ok

SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'T';
>> NO

-- DROP NOT NULL
ALTER TABLE TEST ALTER COLUMN T DROP NOT NULL;
> ok

SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'T';
>> YES

ALTER TABLE TEST ALTER COLUMN T SET NOT NULL;
> ok

-- SET NULL
ALTER TABLE TEST ALTER COLUMN T SET NULL;
> ok

SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'T';
>> YES

-- SET DATA TYPE
ALTER TABLE TEST ALTER COLUMN T SET DATA TYPE BIGINT;
> ok

SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'T';
>> BIGINT

ALTER TABLE TEST ALTER COLUMN T INT INVISIBLE DEFAULT 1 ON UPDATE 2 NOT NULL COMMENT 'C';
> ok

SELECT COLUMN_TYPE, IS_VISIBLE, COLUMN_DEFAULT, COLUMN_ON_UPDATE, REMARKS, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'T';
> COLUMN_TYPE IS_VISIBLE COLUMN_DEFAULT COLUMN_ON_UPDATE REMARKS IS_NULLABLE
> ----------- ---------- -------------- ---------------- ------- -----------
> INT         FALSE      1              2                C       NO
> rows: 1

ALTER TABLE TEST ALTER COLUMN T SET DATA TYPE BIGINT;
> ok

SELECT COLUMN_TYPE, IS_VISIBLE, COLUMN_DEFAULT, COLUMN_ON_UPDATE, REMARKS, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'T';
> COLUMN_TYPE IS_VISIBLE COLUMN_DEFAULT COLUMN_ON_UPDATE REMARKS IS_NULLABLE
> ----------- ---------- -------------- ---------------- ------- -----------
> BIGINT      FALSE      1              2                C       NO
> rows: 1

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT AUTO_INCREMENT PRIMARY KEY, V INT NOT NULL);
> ok

ALTER TABLE TEST ALTER COLUMN ID RESTART WITH 100;
> ok

INSERT INTO TEST(V) VALUES (1);
> update count: 1

ALTER TABLE TEST AUTO_INCREMENT = 200;
> exception SYNTAX_ERROR_2

SET MODE MySQL;
> ok

ALTER TABLE TEST AUTO_INCREMENT = 200;
> ok

INSERT INTO TEST(V) VALUES (2);
> update count: 1

ALTER TABLE TEST AUTO_INCREMENT 300;
> ok

INSERT INTO TEST(V) VALUES (3);
> update count: 1

SELECT * FROM TEST ORDER BY ID;
> ID  V
> --- -
> 100 1
> 200 2
> 300 3
> rows (ordered): 3

ALTER TABLE TEST DROP PRIMARY KEY;
> ok

ALTER TABLE TEST AUTO_INCREMENT = 400;
> exception COLUMN_NOT_FOUND_1

ALTER TABLE TEST ADD PRIMARY KEY(V);
> ok

ALTER TABLE TEST AUTO_INCREMENT = 400;
> exception COLUMN_NOT_FOUND_1

SET MODE Regular;
> ok

DROP TABLE TEST;
> ok

-- Compatibility syntax

SET MODE MySQL;
> ok

create table test(id int primary key, name varchar);
> ok

insert into test(id) values(1);
> update count: 1

alter table test change column id id2 int;
> ok

select id2 from test;
> ID2
> ---
> 1
> rows: 1

drop table test;
> ok

SET MODE Oracle;
> ok

CREATE MEMORY TABLE TEST(V INT NOT NULL);
> ok

ALTER TABLE TEST MODIFY COLUMN V BIGINT;
> ok

SCRIPT NODATA NOPASSWORDS NOSETTINGS TABLE TEST;
> SCRIPT
> -----------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "V" BIGINT NOT NULL );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> rows: 3

SET MODE MySQL;
> ok

ALTER TABLE TEST MODIFY COLUMN V INT;
> ok

SCRIPT NODATA NOPASSWORDS NOSETTINGS TABLE TEST;
> SCRIPT
> -------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "V" INT );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> rows: 3

ALTER TABLE TEST MODIFY COLUMN V BIGINT NOT NULL;
> ok

SCRIPT NODATA NOPASSWORDS NOSETTINGS TABLE TEST;
> SCRIPT
> -----------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "V" BIGINT NOT NULL );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> rows: 3

SET MODE Regular;
> ok

DROP TABLE TEST;
> ok

create table test(id int, name varchar);
> ok

alter table test alter column id int as id+1;
> exception COLUMN_NOT_FOUND_1

drop table test;
> ok

create table t(x varchar) as select 'x';
> ok

alter table t alter column x int;
> exception DATA_CONVERSION_ERROR_1

drop table t;
> ok

create table t(id identity, x varchar) as select null, 'x';
> ok

alter table t alter column x int;
> exception DATA_CONVERSION_ERROR_1

drop table t;
> ok

-- ensure that increasing a VARCHAR columns length takes effect because we optimize this case
create table t(x varchar(2)) as select 'x';
> ok

alter table t alter column x varchar(20);
> ok

insert into t values 'Hello';
> update count: 1

drop table t;
> ok

SET MODE MySQL;
> ok

create table t(x int);
> ok

alter table t modify column x varchar(20);
> ok

insert into t values('Hello');
> update count: 1

drop table t;
> ok

-- This worked in v1.4.196
create table T (C varchar not null);
> ok

alter table T modify C int null;
> ok

insert into T values(null);
> update count: 1

drop table T;
> ok

-- This failed in v1.4.196
create table T (C int not null);
> ok

-- Silently corrupted column C
alter table T modify C null;
> ok

insert into T values(null);
> update count: 1

drop table T;
> ok

SET MODE Oracle;
> ok

create table foo (bar varchar(255));
> ok

alter table foo modify (bar varchar(255) not null);
> ok

insert into foo values(null);
> exception NULL_NOT_ALLOWED

SET MODE Regular;
> ok

-- Tests a bug we used to have where altering the name of a column that had
-- a check constraint that referenced itself would result in not being able
-- to re-open the DB.
create table test(id int check(id in (1,2)) );
> ok

alter table test alter id rename to id2;
> ok

@reconnect

insert into test values 1;
> update count: 1

insert into test values 3;
> exception CHECK_CONSTRAINT_VIOLATED_1

drop table test;
> ok

CREATE MEMORY TABLE TEST(C INT);
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS D RENAME TO E;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS C RENAME TO D;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS E SET NOT NULL;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS D SET NOT NULL;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS E SET DEFAULT 1;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS D SET DEFAULT 1;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS E SET ON UPDATE 2;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS D SET ON UPDATE 2;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS E SET DATA TYPE BIGINT;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS D SET DATA TYPE BIGINT;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS E SET INVISIBLE;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS D SET INVISIBLE;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS E SELECTIVITY 3;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS D SELECTIVITY 3;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS E RESTART WITH 4;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS D RESTART WITH 4;
> exception SEQUENCE_NOT_FOUND_1

SCRIPT NODATA NOPASSWORDS NOSETTINGS TABLE TEST;
> SCRIPT
> ---------------------------------------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "D" BIGINT INVISIBLE DEFAULT 1 ON UPDATE 2 SELECTIVITY 3 NOT NULL );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> rows: 3

ALTER TABLE TEST ALTER COLUMN IF EXISTS E DROP NOT NULL;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS D DROP NOT NULL;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS E DROP DEFAULT;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS D DROP DEFAULT;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS E DROP ON UPDATE;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS D DROP ON UPDATE;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS E INT;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS D INT;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS E SET VISIBLE;
> ok

ALTER TABLE TEST ALTER COLUMN IF EXISTS D SET VISIBLE;
> ok

SCRIPT NODATA NOPASSWORDS NOSETTINGS TABLE TEST;
> SCRIPT
> -------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "D" INT );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> rows: 3

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT GENERATED ALWAYS AS IDENTITY (MINVALUE 1 MAXVALUE 10 INCREMENT BY -1), V INT);
> ok

INSERT INTO TEST(V) VALUES 1;
> update count: 1

TABLE TEST;
> ID V
> -- -
> 10 1
> rows: 1

DELETE FROM TEST;
> update count: 1

ALTER TABLE TEST ALTER COLUMN ID RESTART;
> ok

INSERT INTO TEST(V) VALUES 1;
> update count: 1

TABLE TEST;
> ID V
> -- -
> 10 1
> rows: 1

ALTER TABLE TEST ALTER COLUMN ID RESTART WITH 5;
> ok

INSERT INTO TEST(V) VALUES 2;
> update count: 1

TABLE TEST;
> ID V
> -- -
> 10 1
> 5  2
> rows: 2

DROP TABLE TEST;
> ok

CREATE TABLE TEST(A INT) AS VALUES 1, 2, 3;
> ok

ALTER TABLE TEST ALTER COLUMN A SET DATA TYPE BIGINT USING A * 10;
> ok

TABLE TEST;
> A
> --
> 10
> 20
> 30
> rows: 3

ALTER TABLE TEST ADD COLUMN B INT NOT NULL USING A + 1;
> ok

TABLE TEST;
> A  B
> -- --
> 10 11
> 20 21
> 30 31
> rows: 3

ALTER TABLE TEST ADD COLUMN C VARCHAR(2) USING A;
> ok

TABLE TEST;
> A  B  C
> -- -- --
> 10 11 10
> 20 21 20
> 30 31 30
> rows: 3

ALTER TABLE TEST ALTER COLUMN C SET DATA TYPE VARCHAR(3) USING C || '*';
> ok

TABLE TEST;
> A  B  C
> -- -- ---
> 10 11 10*
> 20 21 20*
> 30 31 30*
> rows: 3

DROP TABLE TEST;
> ok

CREATE TABLE TEST(B BINARY) AS VALUES X'00';
> ok

ALTER TABLE TEST ALTER COLUMN B SET DATA TYPE BINARY(2);
> ok

TABLE TEST;
>> X'0000'

DROP TABLE TEST;
> ok
