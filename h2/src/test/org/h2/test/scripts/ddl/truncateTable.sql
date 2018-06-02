-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create table FOO(id integer primary key);
> ok

create table BAR(fooId integer);
> ok

alter table bar add foreign key (fooId) references foo (id);
> ok

truncate table bar;
> ok

truncate table foo;
> exception CANNOT_TRUNCATE_1

drop table bar, foo;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR);
> ok

INSERT INTO TEST VALUES(1, 'Hello'), (2, 'World');
> update count: 2

TRUNCATE TABLE TEST;
> ok

SELECT * FROM TEST;
> ID NAME
> -- ----
> rows: 0

DROP TABLE TEST;
> ok

CREATE TABLE PARENT(ID INT PRIMARY KEY, NAME VARCHAR);
> ok

CREATE TABLE CHILD(PARENTID INT, FOREIGN KEY(PARENTID) REFERENCES PARENT(ID), NAME VARCHAR);
> ok

TRUNCATE TABLE CHILD;
> ok

TRUNCATE TABLE PARENT;
> exception CANNOT_TRUNCATE_1

DROP TABLE CHILD;
> ok

DROP TABLE PARENT;
> ok
