-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
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
> rows: 3

@reconnect

select suit from card where rank = 0;
>> clubs

alter table card alter column suit enum('a', 'b', 'c', 'd');
> exception ENUM_VALUE_NOT_PERMITTED

alter table card alter column suit enum('''none''', 'hearts', 'clubs', 'spades', 'diamonds');
> ok

select * from card order by suit;
> RANK SUIT
> ---- ------
> 4    null
> 3    hearts
> 0    clubs
> rows (ordered): 3

insert into card (rank, suit) values (8, 'diamonds'), (10, 'clubs'), (7, 'hearts');
> update count: 3

select suit, count(rank) from card group by suit order by suit, count(rank);
> SUIT     COUNT(RANK)
> -------- -----------
> null     1
> hearts   2
> clubs    2
> diamonds 1
> rows (ordered): 4

SELECT JSON_ARRAYAGG(DISTINCT SUIT ORDER BY SUIT) FROM CARD;
>> ["hearts","clubs","diamonds"]

select rank from card where suit = 'diamonds';
>> 8

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds');
> ok

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds', 'long_enum_value_of_128_chars_00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');
> ok

insert into card (rank, suit) values (11, 'long_enum_value_of_128_chars_00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000');
> update count: 1

--- ENUM integer-based operations

select rank from card where suit = 2;
> exception TYPES_ARE_NOT_COMPARABLE_2

select rank from card where cast(suit as integer) = 2;
> RANK
> ----
> 0
> 10
> rows: 2

insert into card (rank, suit) values(5, 3);
> update count: 1

select * from card where cast(rank as integer) = 5;
> RANK SUIT
> ---- ------
> 5    spades
> rows: 1

--- ENUM edge cases

insert into card (rank, suit) values(6, ' ');
> exception ENUM_VALUE_NOT_PERMITTED

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds', 'clubs');
> exception ENUM_DUPLICATE

alter table card alter column suit enum('hearts', 'clubs', 'spades', 'diamonds', '');
> exception ENUM_EMPTY

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
> rows: 2

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
> exception DUPLICATE_KEY_1

select rank from card where suit = 'clubs';
> RANK
> ----
> 0
> 1
> rows: 2

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
> ok

select rank from card where suit = 'clubs';
> RANK
> ----
> 0
> 1
> rows: 2

select rank from card where suit in ('clubs');
> RANK
> ----
> 0
> 1
> rows: 2

insert into card values (2, 'diamonds');
> update count: 1

select rank from card where suit in ('clubs', 'hearts');
> RANK
> ----
> 0
> 1
> 3
> rows: 3

select rank from card where suit in ('clubs', 'hearts') or suit = 'diamonds';
> RANK
> ----
> 0
> 1
> 2
> 3
> rows: 4

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
>> B

CREATE VIEW V1 AS SELECT E + 2 AS E FROM TEST;
> ok

SELECT * FROM V1;
>> 4

CREATE VIEW V2 AS SELECT E + E AS E FROM TEST;
> ok

SELECT * FROM V2;
>> 4

CREATE VIEW V3 AS SELECT -E AS E FROM TEST;
> ok

SELECT * FROM V3;
>> -2

SELECT TABLE_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = 'E' ORDER BY TABLE_NAME;
> TABLE_NAME DATA_TYPE
> ---------- ---------
> TEST       ENUM
> V          ENUM
> V1         INTEGER
> V2         INTEGER
> V3         INTEGER
> rows (ordered): 5

SELECT OBJECT_NAME, OBJECT_TYPE, ENUM_IDENTIFIER, VALUE_NAME, VALUE_ORDINAL FROM INFORMATION_SCHEMA.ENUM_VALUES
    WHERE OBJECT_SCHEMA = 'PUBLIC';
> OBJECT_NAME OBJECT_TYPE ENUM_IDENTIFIER VALUE_NAME VALUE_ORDINAL
> ----------- ----------- --------------- ---------- -------------
> TEST        TABLE       1               A          1
> TEST        TABLE       1               B          2
> V           TABLE       1               A          1
> V           TABLE       1               B          2
> rows: 4

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

SELECT CAST (2 AS ENUM('a', 'b', 'c', 'd'));
>> b

CREATE TABLE TEST(E ENUM('a', 'b'));
> ok

EXPLAIN SELECT * FROM TEST WHERE E = 'a';
>> SELECT "PUBLIC"."TEST"."E" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ WHERE "E" = CAST('a' AS ENUM('a', 'b'))

INSERT INTO TEST VALUES ('a');
> update count: 1

(SELECT * FROM TEST A) UNION ALL (SELECT * FROM TEST A);
> E
> -
> a
> a
> rows: 2

(SELECT * FROM TEST A) MINUS (SELECT * FROM TEST A);
> E
> -
> rows: 0

DROP TABLE TEST;
> ok

EXPLAIN VALUES CAST('A' AS ENUM('A', 'B'));
>> VALUES (CAST('A' AS ENUM('A', 'B')))

CREATE TABLE TEST(E1 ENUM('a', 'b'), E2 ENUM('e', 'c') ARRAY, E3 ROW(E ENUM('x', 'y')));
> ok

SELECT COLUMN_NAME, DATA_TYPE, DTD_IDENTIFIER FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST';
> COLUMN_NAME DATA_TYPE DTD_IDENTIFIER
> ----------- --------- --------------
> E1          ENUM      1
> E2          ARRAY     2
> E3          ROW       3
> rows: 3

SELECT COLLECTION_TYPE_IDENTIFIER, DATA_TYPE, DTD_IDENTIFIER FROM INFORMATION_SCHEMA.ELEMENT_TYPES WHERE OBJECT_NAME = 'TEST';
> COLLECTION_TYPE_IDENTIFIER DATA_TYPE DTD_IDENTIFIER
> -------------------------- --------- --------------
> 2                          ENUM      2_
> rows: 1

SELECT ROW_IDENTIFIER, FIELD_NAME, DATA_TYPE, DTD_IDENTIFIER FROM INFORMATION_SCHEMA.FIELDS WHERE OBJECT_NAME = 'TEST';
> ROW_IDENTIFIER FIELD_NAME DATA_TYPE DTD_IDENTIFIER
> -------------- ---------- --------- --------------
> 3              E          ENUM      3_1
> rows: 1

SELECT * FROM INFORMATION_SCHEMA.ENUM_VALUES WHERE OBJECT_NAME = 'TEST';
> OBJECT_CATALOG OBJECT_SCHEMA OBJECT_NAME OBJECT_TYPE ENUM_IDENTIFIER VALUE_NAME VALUE_ORDINAL
> -------------- ------------- ----------- ----------- --------------- ---------- -------------
> SCRIPT         PUBLIC        TEST        TABLE       1               a          1
> SCRIPT         PUBLIC        TEST        TABLE       1               b          2
> SCRIPT         PUBLIC        TEST        TABLE       2_              c          2
> SCRIPT         PUBLIC        TEST        TABLE       2_              e          1
> SCRIPT         PUBLIC        TEST        TABLE       3_1             x          1
> SCRIPT         PUBLIC        TEST        TABLE       3_1             y          2
> rows: 6

DROP TABLE TEST;
> ok

CREATE TABLE TEST(A ENUM('A', 'B') ARRAY, B ROW(V ENUM('C', 'D')));
> ok

INSERT INTO TEST VALUES (ARRAY['A', 'B'], ROW('C'));
> update count: 1

TABLE TEST;
> A      B
> ------ -------
> [A, B] ROW (C)
> rows: 1

@reconnect

TABLE TEST;
> A      B
> ------ -------
> [A, B] ROW (C)
> rows: 1

DROP TABLE TEST;
> ok
