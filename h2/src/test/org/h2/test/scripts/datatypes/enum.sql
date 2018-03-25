-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

----------------
--- ENUM support
----------------

--- ENUM basic operations

create table card (rank int, suit enum('hearts', 'clubs', 'spades'));
> ok

insert into card (rank, suit) values (0, 'clubs'), (3, 'hearts'), (4, NULL);
> update count: 3

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

select * from card;
> RANK SUIT
> ---- ------
> 0    clubs
> 3    hearts
> 4    null

select * from card order by suit;
> RANK SUIT
> ---- ------
> 4    null
> 3    hearts
> 0    clubs

insert into card (rank, suit) values (8, 'diamonds'), (10, 'clubs'), (7, 'hearts');
> update count: 3

select suit, count(rank) from card group by suit order by suit, count(rank);
> SUIT     COUNT(RANK)
> -------- -----------
> null     1
> hearts   2
> clubs    2
> diamonds 1

select rank from card where suit = 'diamonds';
> RANK
> ----
> 8

select column_type from information_schema.columns where COLUMN_NAME = 'SUIT';
> COLUMN_TYPE
> ------------------------------------------
> ENUM('hearts','clubs','spades','diamonds')
> rows: 1

--- ENUM integer-based operations

select rank from card where suit = 1;
> RANK
> ----
> 0
> 10

insert into card (rank, suit) values(5, 2);
> update count: 1

select * from card where rank = 5;
> RANK SUIT
> ---- ------
> 5    spades

--- ENUM edge cases

insert into card (rank, suit) values(6, ' ');
> exception

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds', 'clubs');
> exception

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds', '');
> exception

drop table card;
> ok

--- ENUM as custom user data type

create type CARD_SUIT as enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

create table card (rank int, suit CARD_SUIT);
> ok

insert into card (rank, suit) values (0, 'clubs'), (3, 'hearts');
> update count: 2

select * from card;
> RANK SUIT
> ---- ------
> 0    clubs
> 3    hearts

drop table card;
> ok

drop type CARD_SUIT;
> ok

--- ENUM in primary key with another column
create type CARD_SUIT as enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

create table card (rank int, suit CARD_SUIT, primary key(rank, suit));
> ok

insert into card (rank, suit) values (0, 'clubs'), (3, 'hearts'), (1, 'clubs');
> update count: 3

insert into card (rank, suit) values (0, 'clubs');
> exception

select rank from card where suit = 'clubs';
> RANK
> ----
> 0
> 1

drop table card;
> ok

drop type CARD_SUIT;
> ok

--- ENUM with index
create type CARD_SUIT as enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

create table card (rank int, suit CARD_SUIT, primary key(rank, suit));
> ok

insert into card (rank, suit) values (0, 'clubs'), (3, 'hearts'), (1, 'clubs');
> update count: 3

create index idx_card_suite on card(`suit`);

select rank from card where suit = 'clubs';
> RANK
> ----
> 0
> 1

select rank from card where suit in ('clubs');
> RANK
> ----
> 0
> 1

drop table card;
> ok

drop type CARD_SUIT;
> ok

CREATE TABLE TEST(ID INT, E1 ENUM('A', 'B') DEFAULT 'A', E2 ENUM('C', 'D') DEFAULT 'C' ON UPDATE 'D');
> ok

INSERT INTO TEST(ID) VALUES (1);
> update count: 1

SELECT * FROM TEST;
> ID E1 E2
> -- -- --
> 1  A  C
> rows: 1

UPDATE TEST SET E1 = 'B';
> update count: 1

SELECT * FROM TEST;
> ID E1 E2
> -- -- --
> 1  B  D
> rows: 1

DROP TABLE TEST;
> ok

CREATE TABLE TEST(E ENUM('A', 'B'));
> ok

INSERT INTO TEST VALUES ('B');
> update count: 1

CREATE VIEW V AS SELECT * FROM TEST;
> ok

SELECT * FROM V;
> E
> -
> B
> rows: 1

CREATE VIEW V1 AS SELECT E + 2 AS E FROM TEST;
> ok

SELECT * FROM V1;
> E
> -
> 3
> rows: 1

CREATE VIEW V2 AS SELECT E + E AS E FROM TEST;
> ok

SELECT * FROM V2;
> E
> -
> 2
> rows: 1

CREATE VIEW V3 AS SELECT -E AS E FROM TEST;
> ok

SELECT * FROM V3;
> E
> --
> -1
> rows: 1

SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'E' ORDER BY TABLE_NAME;
> TABLE_CATALOG TABLE_SCHEMA TABLE_NAME COLUMN_NAME ORDINAL_POSITION COLUMN_DEFAULT IS_NULLABLE DATA_TYPE CHARACTER_MAXIMUM_LENGTH CHARACTER_OCTET_LENGTH NUMERIC_PRECISION NUMERIC_PRECISION_RADIX NUMERIC_SCALE CHARACTER_SET_NAME COLLATION_NAME TYPE_NAME NULLABLE IS_COMPUTED SELECTIVITY CHECK_CONSTRAINT SEQUENCE_NAME REMARKS SOURCE_DATA_TYPE COLUMN_TYPE   COLUMN_ON_UPDATE
> ------------- ------------ ---------- ----------- ---------------- -------------- ----------- --------- ------------------------ ---------------------- ----------------- ----------------------- ------------- ------------------ -------------- --------- -------- ----------- ----------- ---------------- ------------- ------- ---------------- ------------- ----------------
> SCRIPT        PUBLIC       TEST       E           1                null           YES         1111      2147483647               2147483647             2147483647        10                      0             Unicode            OFF            ENUM      1        FALSE       50                           null                  null             ENUM('A','B') null
> SCRIPT        PUBLIC       V          E           1                null           YES         1111      2147483647               2147483647             2147483647        10                      0             Unicode            OFF            ENUM      1        FALSE       50                           null                  null             ENUM('A','B') null
> SCRIPT        PUBLIC       V1         E           1                null           YES         4         2147483647               2147483647             2147483647        10                      0             Unicode            OFF            INTEGER   1        FALSE       50                           null                  null             INTEGER       null
> SCRIPT        PUBLIC       V2         E           1                null           YES         4         2147483647               2147483647             2147483647        10                      0             Unicode            OFF            INTEGER   1        FALSE       50                           null                  null             INTEGER       null
> SCRIPT        PUBLIC       V3         E           1                null           YES         4         2147483647               2147483647             2147483647        10                      0             Unicode            OFF            INTEGER   1        FALSE       50                           null                  null             INTEGER       null
> rows (ordered): 5

DROP VIEW V;
> ok

DROP VIEW V1;
> ok

DROP VIEW V2;
> ok

DROP VIEW V3;
> ok

DROP TABLE TEST;
> ok
