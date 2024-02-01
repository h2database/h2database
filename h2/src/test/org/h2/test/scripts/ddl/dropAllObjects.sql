-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
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
