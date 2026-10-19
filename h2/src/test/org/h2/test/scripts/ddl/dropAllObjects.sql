-- Copyright 2004-2026 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

@reconnect off

-- Test table depends on view

create table a(x int);
> ok

create view b as select * from a;
> ok

create table c(y int check (select count(*) from b) = 0);
> ok

drop all objects;
> ok

-- Test inter-schema dependency

create schema table_view;
> ok

set schema table_view;
> ok

create table test1 (id int, name varchar(20));
> ok

create view test_view_1 as (select * from test1);
> ok

set schema public;
> ok

create schema test_run;
> ok

set schema test_run;
> ok

create table test2 (id int, address varchar(20), constraint a_cons check (id in (select id from table_view.test1)));
> ok

set schema public;
> ok

drop all objects;
> ok

CREATE DOMAIN D INT;
> ok

DROP ALL OBJECTS;
> ok

SELECT COUNT(*) FROM INFORMATION_SCHEMA.DOMAINS WHERE DOMAIN_SCHEMA = 'PUBLIC';
>> 0

-- Domains may use sequences and functions

CREATE SEQUENCE SEQ;
> ok

CREATE ALIAS F1 FOR 'java.lang.Math.abs(int)';
> ok

CREATE DOMAIN D1 INT DEFAULT NEXT VALUE FOR SEQ CHECK (F1(VALUE) < 1000);
> ok

DROP ALL OBJECTS;
> ok

SELECT COUNT(*) FROM INFORMATION_SCHEMA.DOMAINS WHERE DOMAIN_SCHEMA = 'PUBLIC';
>> 0

SELECT COUNT(*) FROM INFORMATION_SCHEMA.SEQUENCES WHERE SEQUENCE_SCHEMA = 'PUBLIC';
>> 0

SELECT COUNT(*) FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'PUBLIC';
>> 0

-- Local temporary tables are dropped too

CREATE LOCAL TEMPORARY TABLE T1(ID BIGINT PRIMARY KEY);
> ok

INSERT INTO T1 VALUES 1, 2;
> update count: 2

CREATE LOCAL TEMPORARY TABLE T2(ID BIGINT GENERATED ALWAYS AS IDENTITY, T1_ID BIGINT REFERENCES T1(ID));
> ok

CREATE INDEX T2_IDX ON T2(T1_ID);
> ok

CREATE GLOBAL TEMPORARY TABLE T3(ID INT);
> ok

DROP ALL OBJECTS;
> ok

TABLE T1;
> exception TABLE_OR_VIEW_NOT_FOUND_DATABASE_EMPTY_1

TABLE T2;
> exception TABLE_OR_VIEW_NOT_FOUND_DATABASE_EMPTY_1

TABLE T3;
> exception TABLE_OR_VIEW_NOT_FOUND_DATABASE_EMPTY_1

CREATE LOCAL TEMPORARY TABLE T1(ID BIGINT PRIMARY KEY);
> ok

DROP TABLE T1;
> ok
