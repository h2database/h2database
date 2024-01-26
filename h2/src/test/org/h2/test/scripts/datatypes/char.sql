-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(C1 CHAR, C2 CHARACTER, C3 NCHAR, C4 NATIONAL CHARACTER, C5 NATIONAL CHAR);
> ok

SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE
> ----------- ---------
> C1          CHARACTER
> C2          CHARACTER
> C3          CHARACTER
> C4          CHARACTER
> C5          CHARACTER
> rows (ordered): 5

DROP TABLE TEST;
> ok

CREATE TABLE TEST(C CHAR(2));
> ok

INSERT INTO TEST VALUES 'aa', 'b';
> update count: 2

SELECT * FROM TEST WHERE C = 'b';
>> b

SELECT * FROM TEST WHERE C = 'b ';
>> b

SELECT * FROM TEST WHERE C = 'b  ';
>> b

SELECT C || 'x' V FROM TEST;
> V
> ---
> aax
> b x
> rows: 2

DROP TABLE TEST;
> ok

SET MODE PostgreSQL;
> ok

CREATE TABLE TEST(C CHAR(2));
> ok

INSERT INTO TEST VALUES 'aa', 'b';
> update count: 2

SELECT * FROM TEST WHERE C = 'b';
>> b

SELECT * FROM TEST WHERE C = 'b ';
>> b

SELECT * FROM TEST WHERE C = 'b  ';
>> b

SELECT C || 'x' V FROM TEST;
> V
> ---
> aax
> bx
> rows: 2

DROP TABLE TEST;
> ok

SET MODE Regular;
> ok

EXPLAIN VALUES CAST('a' AS CHAR(1));
>> VALUES (CAST('a' AS CHAR(1)))

EXPLAIN VALUES CAST('' AS CHAR(1));
>> VALUES (CAST(' ' AS CHAR(1)))

CREATE TABLE T(C CHAR(0));
> exception INVALID_VALUE_2

CREATE TABLE T(C1 CHAR(1 CHARACTERS), C2 CHAR(1 OCTETS));
> ok

DROP TABLE T;
> ok

VALUES CAST('ab' AS CHAR);
>> a

CREATE TABLE TEST(A CHAR(2) NOT NULL, B CHAR(3) NOT NULL);
> ok

INSERT INTO TEST VALUES ('a', 'a'), ('aa', 'aaa'), ('bb   ', 'bb');
> update count: 3

INSERT INTO TEST VALUES ('a a', 'a a');
> exception VALUE_TOO_LONG_2

VALUES CAST('a a' AS CHAR(2)) || '*';
>> a *

SELECT A || '*', B || '*', A || B || '*', CHAR_LENGTH(A), A = B FROM TEST;
> A || '*' B || '*' A || B || '*' CHAR_LENGTH(A) A = B
> -------- -------- ------------- -------------- -----
> a *      a *      a a *         2              TRUE
> aa*      aaa*     aaaaa*        2              FALSE
> bb*      bb *     bbbb *        2              TRUE
> rows: 3

DROP TABLE TEST;
> ok

SET MODE MySQL;
> ok

CREATE TABLE TEST(A CHAR(2) NOT NULL, B CHAR(3) NOT NULL);
> ok

INSERT INTO TEST VALUES ('a', 'a'), ('aa', 'aaa'), ('bb   ', 'bb');
> update count: 3

INSERT INTO TEST VALUES ('a a', 'a a');
> exception VALUE_TOO_LONG_2

VALUES CAST('a a' AS CHAR(2)) || '*';
>> a*

SELECT A || '*', B || '*', A || B || '*', CHAR_LENGTH(A), A = B FROM TEST;
> A || '*' B || '*' A || B || '*' CHAR_LENGTH(A) A = B
> -------- -------- ------------- -------------- -----
> a*       a*       aa*           1              TRUE
> aa*      aaa*     aaaaa*        2              FALSE
> bb*      bb*      bbbb*         2              TRUE
> rows: 3

DROP TABLE TEST;
> ok

SET MODE PostgreSQL;
> ok

CREATE TABLE TEST(A CHAR(2) NOT NULL, B CHAR(3) NOT NULL);
> ok

INSERT INTO TEST VALUES ('a', 'a'), ('aa', 'aaa'), ('bb   ', 'bb');
> update count: 3

INSERT INTO TEST VALUES ('a a', 'a a');
> exception VALUE_TOO_LONG_2

VALUES CAST('a a' AS CHAR(2)) || '*';
>> a*

SELECT A || '*', B || '*', A || B || '*', CHAR_LENGTH(A), A = B FROM TEST;
> ?column? ?column? ?column? char_length ?column?
> -------- -------- -------- ----------- --------
> a*       a*       aa*      1           TRUE
> aa*      aaa*     aaaaa*   2           FALSE
> bb*      bb*      bbbb*    2           TRUE
> rows: 3

DROP TABLE TEST;
> ok

SET MODE Regular;
> ok

CREATE TABLE T1(A CHARACTER(1000000000));
> ok

CREATE TABLE T2(A CHARACTER(1000000001));
> exception INVALID_VALUE_PRECISION

SET TRUNCATE_LARGE_LENGTH TRUE;
> ok

CREATE TABLE T2(A CHARACTER(1000000000));
> ok

SELECT TABLE_NAME, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'PUBLIC';
> TABLE_NAME CHARACTER_MAXIMUM_LENGTH
> ---------- ------------------------
> T1         1000000000
> T2         1000000000
> rows: 2

SET TRUNCATE_LARGE_LENGTH FALSE;
> ok

DROP TABLE T1, T2;
> ok
