-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(C1 CHAR, C2 CHARACTER, C3 NCHAR, C4 NATIONAL CHARACTER, C5 NATIONAL CHAR);
> ok

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE
> ----------- --------- --------- ------------------
> C1          1         CHAR      CHAR
> C2          1         CHAR      CHARACTER
> C3          1         CHAR      NCHAR
> C4          1         CHAR      NATIONAL CHARACTER
> C5          1         CHAR      NATIONAL CHAR
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
> bx
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
>> VALUES (CAST('' AS CHAR(1)))

CREATE TABLE T(C CHAR(0));
> exception INVALID_VALUE_2

CREATE TABLE T(C1 CHAR(1 CHARACTERS), C2 CHAR(1 OCTETS));
> ok

DROP TABLE T;
> ok

VALUES CAST('ab' AS CHAR);
>> a
