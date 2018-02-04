-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- ASC
create table test(v tinyint);
> ok

create index test_idx on test(v asc);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 15

insert into test values (10);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 15

drop table test;
> ok

-- ASC NULLS FIRST
create table test(v tinyint);
> ok

create index test_idx on test(v asc nulls first);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 15

insert into test values (10);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 15

drop table test;
> ok

-- ASC NULLS LAST
create table test(v tinyint);
> ok

create index test_idx on test(v asc nulls last);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 15

insert into test values (10);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 15

drop table test;
> ok

-- DESC
create table test(v tinyint);
> ok

create index test_idx on test(v desc);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 15

insert into test values (10);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 15

drop table test;
> ok

-- DESC NULLS FIRST
create table test(v tinyint);
> ok

create index test_idx on test(v desc nulls first);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 15

insert into test values (10);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 15

drop table test;
> ok

-- DESC NULLS LAST
create table test(v tinyint);
> ok

create index test_idx on test(v desc nulls last);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 15

insert into test values (10);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 15

drop table test;
> ok

create table test(v tinyint);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 15

insert into test values (10);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 15

drop table test;
> ok

create table test(v smallint);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 15

insert into test values (10);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 15

drop table test;
> ok

create table test(v int);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 15

insert into test values (10);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 15

drop table test;
> ok

create table test(v bigint);
> ok

insert into test values (20), (20), (10);
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 20

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 15

insert into test values (10);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 15

drop table test;
> ok

create table test(v real);
> ok

insert into test values (2), (2), (1);
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 2.0

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 2.0

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 1.5

insert into test values (1);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 1.5

drop table test;
> ok

create table test(v double);
> ok

insert into test values (2), (2), (1);
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 2.0

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 2.0

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 1.5

insert into test values (1);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 1.5

drop table test;
> ok

create table test(v numeric(1));
> ok

insert into test values (2), (2), (1);
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 2

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 2

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 1.5

insert into test values (1);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 1.5

drop table test;
> ok

create table test(v time);
> ok

insert into test values ('20:00:00'), ('20:00:00'), ('10:00:00');
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------
> 20:00:00

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 20:00:00

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 15:00:00

insert into test values ('10:00:00');
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------
> 15:00:00

drop table test;
> ok

create table test(v date);
> ok

insert into test values ('2000-01-20'), ('2000-01-20'), ('2000-01-10');
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ----------
> 2000-01-20

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ----------
> 2000-01-20

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------
> 2000-01-15

insert into test values ('2000-01-10');
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ----------
> 2000-01-15

drop table test;
> ok

create table test(v timestamp);
> ok

insert into test values ('2000-01-20 20:00:00'), ('2000-01-20 20:00:00'), ('2000-01-10 10:00:00');
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ---------------------
> 2000-01-20 20:00:00.0

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------------------
> 2000-01-20 20:00:00.0

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ---------------------
> 2000-01-15 15:00:00.0

insert into test values ('2000-01-10 10:00:00');
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ---------------------
> 2000-01-15 15:00:00.0

delete from test;
> update count: 5

insert into test values ('2000-01-20 20:00:00'), ('2000-01-21 20:00:00');
> update count: 2

select median(v) from test;
> MEDIAN(V)
> ---------------------
> 2000-01-21 08:00:00.0

drop table test;
> ok

create table test(v timestamp with time zone);
> ok

insert into test values ('2000-01-20 20:00:00+04'), ('2000-01-20 20:00:00+04'), ('2000-01-10 10:00:00+02');
> update count: 3

select median(v) from test;
> MEDIAN(V)
> ------------------------
> 2000-01-20 20:00:00.0+04

insert into test values (null);
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ------------------------
> 2000-01-20 20:00:00.0+04

select median(distinct v) from test;
> MEDIAN(DISTINCT V)
> ------------------------
> 2000-01-15 15:00:00.0+03

insert into test values ('2000-01-10 10:00:00+02');
> update count: 1

select median(v) from test;
> MEDIAN(V)
> ------------------------
> 2000-01-15 15:00:00.0+03

delete from test;
> update count: 5

insert into test values ('2000-01-20 20:00:00+10'), ('2000-01-21 20:00:00-09');
> update count: 2

select median(v) from test;
> MEDIAN(V)
> ---------------------------
> 2000-01-21 08:00:00.0+00:30

drop table test;
> ok

-- with group by
create table test(name varchar, value int);
> ok

insert into test values ('Group 2A', 10), ('Group 2A', 10), ('Group 2A', 20),
    ('Group 1X', 40), ('Group 1X', 50), ('Group 3B', null);
> update count: 6

select name, median(value) from test group by name order by name;
> NAME     MEDIAN(VALUE)
> -------- -------------
> Group 1X 45
> Group 2A 10
> Group 3B null
> rows (ordered): 3

drop table test;
> ok
