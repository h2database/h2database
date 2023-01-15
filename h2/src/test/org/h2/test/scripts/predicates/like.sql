-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

create table Foo (A varchar(20), B integer);
> ok

insert into Foo (A, B) values ('abcd', 1), ('abcd', 2);
> update count: 2

select * from Foo where A like 'abc%' escape '\' AND B=1;
> A    B
> ---- -
> abcd 1
> rows: 1

drop table Foo;
> ok

--- test case for number like string ---------------------------------------------------------------------------------------------
CREATE TABLE test (one bigint primary key, two bigint, three bigint);
> ok

CREATE INDEX two ON test(two);
> ok

INSERT INTO TEST VALUES(1, 2, 3), (10, 20, 30), (100, 200, 300);
> update count: 3

INSERT INTO TEST VALUES(2, 6, 9), (20, 60, 90), (200, 600, 900);
> update count: 3

SELECT * FROM test WHERE one LIKE '2%';
> ONE TWO THREE
> --- --- -----
> 2   6   9
> 20  60  90
> 200 600 900
> rows: 3

SELECT * FROM test WHERE two LIKE '2%';
> ONE TWO THREE
> --- --- -----
> 1   2   3
> 10  20  30
> 100 200 300
> rows: 3

SELECT * FROM test WHERE three LIKE '2%';
> ONE TWO THREE
> --- --- -----
> rows: 0

DROP TABLE TEST;
> ok

CREATE TABLE TEST(ID INT PRIMARY KEY, NAME VARCHAR(255));
> ok

INSERT INTO TEST VALUES(0, NULL), (1, 'Hello'), (2, 'World'), (3, 'Word'), (4, 'Wo%');
> update count: 5

SELECT * FROM TEST WHERE NAME IS NULL;
> ID NAME
> -- ----
> 0  null
> rows: 1

SELECT * FROM TEST WHERE NAME IS NOT NULL;
> ID NAME
> -- -----
> 1  Hello
> 2  World
> 3  Word
> 4  Wo%
> rows: 4

SELECT * FROM TEST WHERE NAME BETWEEN 'H' AND 'Word';
> ID NAME
> -- -----
> 1  Hello
> 3  Word
> 4  Wo%
> rows: 3

SELECT * FROM TEST WHERE ID >= 2 AND ID <= 3 AND ID <> 2;
> ID NAME
> -- ----
> 3  Word
> rows: 1

SELECT * FROM TEST WHERE ID>0 AND ID<4 AND ID!=2;
> ID NAME
> -- -----
> 1  Hello
> 3  Word
> rows: 2

SELECT * FROM TEST WHERE 'Hello' LIKE '_el%';
> ID NAME
> -- -----
> 0  null
> 1  Hello
> 2  World
> 3  Word
> 4  Wo%
> rows: 5

SELECT * FROM TEST WHERE NAME LIKE 'Hello%';
> ID NAME
> -- -----
> 1  Hello
> rows: 1

SELECT * FROM TEST WHERE NAME ILIKE 'hello%';
> ID NAME
> -- -----
> 1  Hello
> rows: 1

SELECT * FROM TEST WHERE NAME ILIKE 'xxx%';
> ID NAME
> -- ----
> rows: 0

SELECT * FROM TEST WHERE NAME LIKE 'Wo%';
> ID NAME
> -- -----
> 2  World
> 3  Word
> 4  Wo%
> rows: 3

SELECT * FROM TEST WHERE NAME LIKE 'Wo\%';
> ID NAME
> -- ----
> 4  Wo%
> rows: 1

SELECT * FROM TEST WHERE NAME LIKE 'WoX%' ESCAPE 'X';
> ID NAME
> -- ----
> 4  Wo%
> rows: 1

SELECT * FROM TEST WHERE NAME LIKE 'Word_';
> ID NAME
> -- ----
> rows: 0

SELECT * FROM TEST WHERE NAME LIKE '%Hello%';
> ID NAME
> -- -----
> 1  Hello
> rows: 1

SELECT * FROM TEST WHERE 'Hello' LIKE NAME;
> ID NAME
> -- -----
> 1  Hello
> rows: 1

SELECT T1.*, T2.* FROM TEST AS T1, TEST AS T2 WHERE T1.ID = T2.ID AND T1.NAME LIKE T2.NAME || '%';
> ID NAME  ID NAME
> -- ----- -- -----
> 1  Hello 1  Hello
> 2  World 2  World
> 3  Word  3  Word
> 4  Wo%   4  Wo%
> rows: 4

SELECT ID, MAX(NAME) FROM TEST GROUP BY ID HAVING MAX(NAME) = 'World';
> ID MAX(NAME)
> -- ---------
> 2  World
> rows: 1

SELECT ID, MAX(NAME) FROM TEST GROUP BY ID HAVING MAX(NAME) LIKE 'World%';
> ID MAX(NAME)
> -- ---------
> 2  World
> rows: 1

EXPLAIN SELECT ID FROM TEST WHERE NAME ILIKE 'w%';
>> SELECT "ID" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */ WHERE "NAME" ILIKE 'w%'

DROP TABLE TEST;
> ok

SELECT S, S LIKE '%', S ILIKE '%', S REGEXP '%' FROM (VALUES NULL, '', '1') T(S);
> S    CASE WHEN S IS NOT NULL THEN TRUE ELSE UNKNOWN END CASE WHEN S IS NOT NULL THEN TRUE ELSE UNKNOWN END S REGEXP '%'
> ---- -------------------------------------------------- -------------------------------------------------- ------------
>      TRUE                                               TRUE                                               FALSE
> 1    TRUE                                               TRUE                                               FALSE
> null null                                               null                                               null
> rows: 3

SELECT S, S NOT LIKE '%', S NOT ILIKE '%', S NOT REGEXP '%' FROM (VALUES NULL, '', '1') T(S);
> S    CASE WHEN S IS NOT NULL THEN FALSE ELSE UNKNOWN END CASE WHEN S IS NOT NULL THEN FALSE ELSE UNKNOWN END S NOT REGEXP '%'
> ---- --------------------------------------------------- --------------------------------------------------- ----------------
>      FALSE                                               FALSE                                               TRUE
> 1    FALSE                                               FALSE                                               TRUE
> null null                                                null                                                null
> rows: 3

CREATE TABLE TEST(ID BIGINT PRIMARY KEY, V VARCHAR UNIQUE) AS VALUES (1, 'aa'), (2, 'bb');
> ok

SELECT ID FROM (SELECT * FROM TEST) WHERE V NOT LIKE 'a%';
>> 2

DROP TABLE TEST;
> ok
