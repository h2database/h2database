-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

-- EXEC and EXECUTE in MSSQLServer mode

CREATE ALIAS MY_NO_ARG AS 'int f() { return 1; }';
> ok

CREATE ALIAS MY_SQRT FOR "java.lang.Math.sqrt";
> ok

CREATE ALIAS MY_REMAINDER FOR "java.lang.Math.IEEEremainder";
> ok

EXEC MY_SQRT 4;
> exception SYNTAX_ERROR_2

-- PostgreSQL-style EXECUTE doesn't work with MSSQLServer-style arguments
EXECUTE MY_SQRT 4;
> exception FUNCTION_ALIAS_NOT_FOUND_1

SET MODE MSSQLServer;
> ok

-- PostgreSQL-style PREPARE is not available in MSSQLServer mode
PREPARE TEST AS SELECT 1;
> exception SYNTAX_ERROR_1

-- PostgreSQL-style DEALLOCATE is not available in MSSQLServer mode
DEALLOCATE TEST;
> exception SYNTAX_ERROR_2

EXEC MY_NO_ARG;
>> 1

EXEC MY_SQRT 4;
>> 2.0

EXEC MY_REMAINDER 4, 3;
>> 1.0

EXECUTE MY_SQRT 4;
>> 2.0

EXEC PUBLIC.MY_SQRT 4;
>> 2.0

EXEC SCRIPT.PUBLIC.MY_SQRT 4;
>> 2.0

EXEC UNKNOWN_PROCEDURE;
> exception FUNCTION_NOT_FOUND_1

EXEC UNKNOWN_SCHEMA.MY_SQRT 4;
> exception SCHEMA_NOT_FOUND_1

EXEC UNKNOWN_DATABASE.PUBLIC.MY_SQRT 4;
> exception DATABASE_NOT_FOUND_1

SET MODE Regular;
> ok

DROP ALIAS MY_NO_ARG;
> ok

DROP ALIAS MY_SQRT;
> ok

DROP ALIAS MY_REMAINDER;
> ok

-- UPDATE TOP (n) in MSSQLServer mode

CREATE TABLE TEST(A INT, B INT) AS VALUES (1, 2), (3, 4), (5, 6);
> ok

UPDATE TOP (1) TEST SET B = 10;
> exception TABLE_OR_VIEW_NOT_FOUND_1

SET MODE MSSQLServer;
> ok

UPDATE TOP (1) TEST SET B = 10;
> update count: 1

SELECT COUNT(*) FILTER (WHERE B = 10) N, COUNT(*) FILTER (WHERE B <> 10) O FROM TEST;
> N O
> - -
> 1 2
> rows: 1

UPDATE TEST SET B = 10 WHERE B <> 10;
> update count: 2

UPDATE TOP (1) TEST SET B = 10 LIMIT 1;
> exception SYNTAX_ERROR_1

SET MODE Regular;
> ok

DROP TABLE TEST;
> ok

SET MODE MySQL;
> ok

CREATE TABLE A (A INT PRIMARY KEY, X INT);
> ok

ALTER TABLE A ADD INDEX A_IDX(X);
> ok

-- MariaDB compatibility
ALTER TABLE A DROP INDEX A_IDX_1;
> exception CONSTRAINT_NOT_FOUND_1

ALTER TABLE A DROP INDEX IF EXISTS A_IDX_1;
> ok

ALTER TABLE A DROP INDEX IF EXISTS A_IDX;
> ok

ALTER TABLE A DROP INDEX A_IDX;
> exception CONSTRAINT_NOT_FOUND_1

CREATE TABLE B (B INT PRIMARY KEY, A INT);
> ok

ALTER TABLE B ADD CONSTRAINT B_FK FOREIGN KEY (A) REFERENCES A(A);
> ok

ALTER TABLE B DROP FOREIGN KEY B_FK_1;
> exception CONSTRAINT_NOT_FOUND_1

-- MariaDB compatibility
ALTER TABLE B DROP FOREIGN KEY IF EXISTS B_FK_1;
> ok

ALTER TABLE B DROP FOREIGN KEY IF EXISTS B_FK;
> ok

ALTER TABLE B DROP FOREIGN KEY B_FK;
> exception CONSTRAINT_NOT_FOUND_1

DROP TABLE A, B;
> ok

SET MODE Regular;
> ok

-- PostgreSQL-style CREATE INDEX ... USING
CREATE TABLE TEST(B1 INT, B2 INT, H INT, R GEOMETRY, T INT);
> ok

CREATE INDEX TEST_BTREE_IDX ON TEST USING BTREE(B1, B2);
> ok

CREATE INDEX TEST_HASH_IDX ON TEST USING HASH(H);
> ok

CREATE INDEX TEST_RTREE_IDX ON TEST USING RTREE(R);
> ok

SELECT INDEX_NAME, COLUMN_NAME, ORDINAL_POSITION, INDEX_TYPE_NAME FROM INFORMATION_SCHEMA.INDEXES
    WHERE TABLE_NAME = 'TEST';
> INDEX_NAME     COLUMN_NAME ORDINAL_POSITION INDEX_TYPE_NAME
> -------------- ----------- ---------------- ---------------
> TEST_BTREE_IDX B1          1                INDEX
> TEST_BTREE_IDX B2          2                INDEX
> TEST_HASH_IDX  H           1                HASH INDEX
> TEST_RTREE_IDX R           1                SPATIAL INDEX
> rows: 4

CREATE HASH INDEX TEST_BAD_IDX ON TEST USING HASH(T);
> exception SYNTAX_ERROR_2

CREATE SPATIAL INDEX TEST_BAD_IDX ON TEST USING RTREE(T);
> exception SYNTAX_ERROR_2

DROP TABLE TEST;
> ok

SET MODE MySQL;
> ok

CREATE TABLE test (id int(25) NOT NULL auto_increment, name varchar NOT NULL, PRIMARY KEY  (id,name));
> ok

drop table test;
> ok

create memory table word(word_id integer, name varchar);
> ok

alter table word alter column word_id integer(10) auto_increment;
> ok

insert into word(name) values('Hello');
> update count: 1

alter table word alter column word_id restart with 30872;
> ok

insert into word(name) values('World');
> update count: 1

select * from word;
> WORD_ID NAME
> ------- -----
> 1       Hello
> 30872   World
> rows: 2

drop table word;
> ok

CREATE MEMORY TABLE TEST1(ID BIGINT(20) NOT NULL PRIMARY KEY COMMENT 'COMMENT1', FIELD_NAME VARCHAR(100) NOT NULL COMMENT 'COMMENT2');
> ok

SCRIPT NOPASSWORDS NOSETTINGS TABLE TEST1;
> SCRIPT
> ---------------------------------------------------------------------------------------------------------------------------------------
> -- 0 +/- SELECT COUNT(*) FROM PUBLIC.TEST1;
> ALTER TABLE "PUBLIC"."TEST1" ADD CONSTRAINT "PUBLIC"."CONSTRAINT_4" PRIMARY KEY("ID");
> CREATE MEMORY TABLE "PUBLIC"."TEST1"( "ID" BIGINT COMMENT 'COMMENT1' NOT NULL, "FIELD_NAME" VARCHAR(100) COMMENT 'COMMENT2' NOT NULL );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> rows: 4

CREATE TABLE TEST2(ID BIGINT(20) NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'COMMENT1', FIELD_NAME VARCHAR(100) NOT NULL COMMENT 'COMMENT2' COMMENT 'COMMENT3');
> exception SYNTAX_ERROR_2

CREATE TABLE TEST3(ID BIGINT(20) NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'COMMENT1' CHECK(ID > 0), FIELD_NAME VARCHAR(100) NOT NULL COMMENT 'COMMENT2');
> ok

CREATE TABLE TEST4(ID BIGINT(20) NOT NULL AUTO_INCREMENT PRIMARY KEY CHECK(ID > 0) COMMENT 'COMMENT1', FIELD_NAME VARCHAR(100) NOT NULL COMMENT 'COMMENT2');
> ok

DROP TABLE TEST1, TEST3, TEST4;
> ok

SET MODE Regular;
> ok

-- Keywords as identifiers

CREATE TABLE TEST(KEY INT, VALUE INT);
> exception SYNTAX_ERROR_2

@reconnect off

SET NON_KEYWORDS KEY, VALUE, AS, SET, DAY;
> ok

CREATE TABLE TEST(KEY INT, VALUE INT, AS INT, SET INT, DAY INT);
> ok

INSERT INTO TEST(KEY, VALUE, AS, SET, DAY) VALUES (1, 2, 3, 4, 5), (6, 7, 8, 9, 10);
> update count: 2

SELECT KEY, VALUE, AS, SET, DAY FROM TEST WHERE KEY <> 6 AND VALUE <> 7 AND AS <> 8 AND SET <> 9 AND DAY <> 10;
> KEY VALUE AS SET DAY
> --- ----- -- --- ---
> 1   2     3  4   5
> rows: 1

DROP TABLE TEST;
> ok

SELECT "VALUE" FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME = 'NON_KEYWORDS';
>> AS,DAY,KEY,SET,VALUE

SET NON_KEYWORDS;
> ok

@reconnect on

SELECT COUNT(*) FROM INFORMATION_SCHEMA.SETTINGS WHERE NAME = 'NON_KEYWORDS';
>> 0

CREATE TABLE TEST(KEY INT, VALUE INT);
> exception SYNTAX_ERROR_2

CREATE TABLE TEST1(C VARCHAR(1 CHAR));
> exception SYNTAX_ERROR_2

CREATE TABLE TEST2(C VARCHAR(1 BYTE));
> exception SYNTAX_ERROR_2

CREATE TABLE TEST3(C BINARY_FLOAT);
> exception UNKNOWN_DATA_TYPE_1

CREATE TABLE TEST4(C BINARY_DOUBLE);
> exception UNKNOWN_DATA_TYPE_1

SET MODE Oracle;
> ok

CREATE TABLE TEST1(C VARCHAR(1 CHAR));
> ok

CREATE TABLE TEST2(C VARCHAR(1 BYTE));
> ok

CREATE TABLE TEST3(C BINARY_FLOAT);
> ok

CREATE TABLE TEST4(C BINARY_DOUBLE);
> ok

SELECT TABLE_NAME, DATA_TYPE, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME IN ('TEST3', 'TEST4');
> TABLE_NAME DATA_TYPE        COLUMN_TYPE
> ---------- ---------------- -------------
> TEST3      REAL             BINARY_FLOAT
> TEST4      DOUBLE PRECISION BINARY_DOUBLE
> rows: 2

DROP TABLE TEST1, TEST2, TEST3, TEST4;
> ok

SET MODE PostgreSQL;
> ok

EXPLAIN VALUES VERSION();
>> VALUES (VERSION())

SET MODE Regular;
> ok

CREATE TABLE TEST(A INT) AS VALUES 0;
> ok

SELECT SIN(A), A+1, A FROM TEST;
> SIN(A) A + 1 A
> ------ ----- -
> 0.0    1     0
> rows: 1

CREATE VIEW V AS SELECT SIN(A), A+1, (((((A + 1) * A + 1) * A + 1) * A + 1) * A + 1) * A + 1 FROM TEST;
> ok

TABLE V;
> SIN(A) A + 1 ((((((((((A + 1) * A) + 1) * A) + 1) * A) + 1) * A) + 1) * A) + 1
> ------ ----- -----------------------------------------------------------------
> 0.0    1     1
> rows: 1

DROP VIEW V;
> ok

CREATE VIEW V AS SELECT SIN(0), COS(0);
> ok

TABLE V;
> 0.0 1.0
> --- ---
> 0.0 1.0
> rows: 1

DROP VIEW V;
> ok

SET MODE DB2;
> ok

SELECT SIN(A), A+1, A FROM TEST;
> 1   2 A
> --- - -
> 0.0 1 0
> rows: 1

CREATE VIEW V AS SELECT SIN(A), A+1, (((((A + 1) * A + 1) * A + 1) * A + 1) * A + 1) * A + 1 FROM TEST;
> exception COLUMN_ALIAS_IS_NOT_SPECIFIED_1

SET MODE Derby;
> ok

SELECT SIN(A), A+1, A FROM TEST;
> 1   2 A
> --- - -
> 0.0 1 0
> rows: 1

CREATE VIEW V AS SELECT SIN(A), A+1, (((((A + 1) * A + 1) * A + 1) * A + 1) * A + 1) * A + 1 FROM TEST;
> exception COLUMN_ALIAS_IS_NOT_SPECIFIED_1

SET MODE MSSQLServer;
> ok

SELECT SIN(A), A+1, A FROM TEST;
>       A
> --- - -
> 0.0 1 0
> rows: 1

CREATE VIEW V AS SELECT SIN(A), A+1, (((((A + 1) * A + 1) * A + 1) * A + 1) * A + 1) * A + 1 FROM TEST;
> exception COLUMN_ALIAS_IS_NOT_SPECIFIED_1

SET MODE HSQLDB;
> ok

SELECT SIN(A), A+1, A FROM TEST;
> C1  C2 A
> --- -- -
> 0.0 1  0
> rows: 1

CREATE VIEW V AS SELECT SIN(A), A+1, (((((A + 1) * A + 1) * A + 1) * A + 1) * A + 1) * A + 1 FROM TEST;
> ok

TABLE V;
> C1  C2 C3
> --- -- --
> 0.0 1  1
> rows: 1

DROP VIEW V;
> ok

SET MODE MySQL;
> ok

SELECT SIN(A), A+1, A FROM TEST;
> SIN(A) A + 1 A
> ------ ----- -
> 0.0    1     0
> rows: 1

CREATE VIEW V AS SELECT SIN(A), A+1, (((((A + 1) * A + 1) * A + 1) * A + 1) * A + 1) * A + 1 FROM TEST;
> ok

TABLE V;
> SIN(A) A + 1 Name_exp_3
> ------ ----- ----------
> 0.0    1     1
> rows: 1

DROP VIEW V;
> ok

CREATE VIEW V AS SELECT SIN(0), COS(0);
> ok

TABLE V;
> SIN(0) COS(0)
> ------ ------
> 0.0    1.0
> rows: 1

DROP VIEW V;
> ok

SET MODE Oracle;
> ok

SELECT SIN(A), A+1, A FROM TEST;
> SIN(A) A + 1 A
> ------ ----- -
> 0.0    1     0
> rows: 1

SET MODE PostgreSQL;
> ok

SELECT SIN(A), A+1, A FROM TEST;
> sin ?column? A
> --- -------- -
> 0.0 1        0
> rows: 1

CREATE VIEW V AS SELECT SIN(A), A+1, (((((A + 1) * A + 1) * A + 1) * A + 1) * A + 1) * A + 1 FROM TEST;
> exception DUPLICATE_COLUMN_NAME_1

CREATE VIEW V AS SELECT SIN(0), COS(0);
> ok

TABLE V;
> sin cos
> --- ---
> 0.0 1.0
> rows: 1

DROP VIEW V;
> ok

SET MODE Regular;
> ok

DROP TABLE TEST;
> ok
