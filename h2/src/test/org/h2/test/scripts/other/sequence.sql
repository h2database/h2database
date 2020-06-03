-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE SEQUENCE SEQ NO CACHE;
> ok

CREATE TABLE TEST(NEXT INT, CURRENT INT) AS (VALUES (10, 11), (20, 21));
> ok

SELECT NEXT "VALUE", NEXT VALUE FOR SEQ, CURRENT "VALUE", CURRENT VALUE FOR SEQ FROM TEST;
> VALUE NEXT VALUE FOR PUBLIC.SEQ VALUE CURRENT VALUE FOR PUBLIC.SEQ
> ----- ------------------------- ----- ----------------------------
> 10    1                         11    1
> 20    2                         21    2
> rows: 2

EXPLAIN SELECT NEXT "VALUE", NEXT VALUE FOR SEQ, CURRENT "VALUE", CURRENT VALUE FOR SEQ FROM TEST;
>> SELECT "NEXT" AS "VALUE", NEXT VALUE FOR "PUBLIC"."SEQ", "CURRENT" AS "VALUE", CURRENT VALUE FOR "PUBLIC"."SEQ" FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

DROP TABLE TEST;
> ok

DROP SEQUENCE SEQ;
> ok

CREATE SEQUENCE S1 START WITH 11;
> ok

CREATE SEQUENCE S2 START WITH 61;
> ok

SELECT NEXT VALUE FOR S1 A, NEXT VALUE FOR S2 B, NEXT VALUE FOR S1 C, NEXT VALUE FOR S2 D FROM SYSTEM_RANGE(1, 2);
> A  B  C  D
> -- -- -- --
> 11 61 11 61
> 12 62 12 62
> rows: 2

CREATE TABLE TEST(A BIGINT, B BIGINT, C BIGINT, D BIGINT, V INT) AS
    SELECT NEXT VALUE FOR S1, NEXT VALUE FOR S2, NEXT VALUE FOR S1, NEXT VALUE FOR S2, X FROM SYSTEM_RANGE(1, 2);
> ok

INSERT INTO TEST
    SELECT NEXT VALUE FOR S1, NEXT VALUE FOR S2, NEXT VALUE FOR S1, NEXT VALUE FOR S2, X FROM SYSTEM_RANGE(3, 4);
> update count: 2

INSERT INTO TEST VALUES
    (NEXT VALUE FOR S1, NEXT VALUE FOR S2, NEXT VALUE FOR S1, NEXT VALUE FOR S2, 5),
    (NEXT VALUE FOR S1, NEXT VALUE FOR S2, NEXT VALUE FOR S1, NEXT VALUE FOR S2, 6);
> update count: 2

TABLE TEST;
> A  B  C  D  V
> -- -- -- -- -
> 13 63 13 63 1
> 14 64 14 64 2
> 15 65 15 65 3
> 16 66 16 66 4
> 17 67 17 67 5
> 18 68 18 68 6
> rows: 6

UPDATE TEST SET A = NEXT VALUE FOR S1, B = NEXT VALUE FOR S2, C = NEXT VALUE FOR S1, D = NEXT VALUE FOR S2
    WHERE V BETWEEN 3 AND 4;
> update count: 2

TABLE TEST;
> A  B  C  D  V
> -- -- -- -- -
> 13 63 13 63 1
> 14 64 14 64 2
> 17 67 17 67 5
> 18 68 18 68 6
> 19 69 19 69 3
> 20 70 20 70 4
> rows: 6

MERGE INTO TEST D USING (VALUES 7, 8) S ON D.V = S.C1
    WHEN NOT MATCHED THEN INSERT VALUES
        (NEXT VALUE FOR S1, NEXT VALUE FOR S2, NEXT VALUE FOR S1, NEXT VALUE FOR S2, S.C1);
> update count: 2

TABLE TEST;
> A  B  C  D  V
> -- -- -- -- -
> 13 63 13 63 1
> 14 64 14 64 2
> 17 67 17 67 5
> 18 68 18 68 6
> 19 69 19 69 3
> 20 70 20 70 4
> 21 71 21 71 7
> 22 72 22 72 8
> rows: 8

MERGE INTO TEST D USING (VALUES 7, 8) S ON D.V = S.C1
    WHEN MATCHED THEN UPDATE
        SET A = NEXT VALUE FOR S1, B = NEXT VALUE FOR S2, C = NEXT VALUE FOR S1, D = NEXT VALUE FOR S2;
> update count: 2

TABLE TEST;
> A  B  C  D  V
> -- -- -- -- -
> 13 63 13 63 1
> 14 64 14 64 2
> 17 67 17 67 5
> 18 68 18 68 6
> 19 69 19 69 3
> 20 70 20 70 4
> 23 73 23 73 7
> 24 74 24 74 8
> rows: 8

DROP TABLE TEST;
> ok

-- MariaDB
SET MODE MySQL;
> ok

SELECT NEXT VALUE FOR S1 A, NEXT VALUE FOR S2 B, NEXT VALUE FOR S1 C, NEXT VALUE FOR S2 D FROM SYSTEM_RANGE(1, 2);
> A  B  C  D
> -- -- -- --
> 25 75 26 76
> 27 77 28 78
> rows: 2

SET MODE Regular;
> ok

DROP SEQUENCE S1;
> ok

DROP SEQUENCE S2;
> ok

CREATE SEQUENCE SEQ;
> ok

SELECT SEQ.NEXTVAL;
> exception COLUMN_NOT_FOUND_1

SELECT SEQ.CURRVAL;
> exception COLUMN_NOT_FOUND_1

DROP SEQUENCE SEQ;
> ok

SET MODE Oracle;
> ok

create sequence seq;
> ok

select case seq.nextval when 2 then 'two' when 3 then 'three' when 1 then 'one' else 'other' end result from dual;
> RESULT
> ------
> one
> rows: 1

drop sequence seq;
> ok

create schema s authorization sa;
> ok

alter sequence if exists s.seq restart with 10;
> ok

create sequence s.seq cache 0;
> ok

alter sequence if exists s.seq restart with 3;
> ok

select s.seq.nextval as x;
> X
> -
> 3
> rows: 1

drop sequence s.seq;
> ok

create sequence s.seq cache 0;
> ok

alter sequence s.seq restart with 10;
> ok

script NOPASSWORDS NOSETTINGS drop;
> SCRIPT
> ------------------------------------------------------------------
> ALTER SEQUENCE "S"."SEQ" RESTART WITH 10;
> CREATE SCHEMA IF NOT EXISTS "S" AUTHORIZATION "SA";
> CREATE SEQUENCE "S"."SEQ" AS NUMERIC(19, 0) START WITH 1 NO CACHE;
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> DROP SEQUENCE IF EXISTS "S"."SEQ";
> rows: 5

drop schema s cascade;
> ok

create schema TEST_SCHEMA;
> ok

create sequence TEST_SCHEMA.TEST_SEQ;
> ok

select TEST_SCHEMA.TEST_SEQ.CURRVAL;
> exception CURRENT_SEQUENCE_VALUE_IS_NOT_DEFINED_IN_SESSION_1

select TEST_SCHEMA.TEST_SEQ.nextval;
>> 1

select TEST_SCHEMA.TEST_SEQ.CURRVAL;
>> 1

drop schema TEST_SCHEMA cascade;
> ok

CREATE TABLE TEST(CURRVAL INT, NEXTVAL INT);
> ok

INSERT INTO TEST VALUES (3, 4);
> update count: 1

SELECT TEST.CURRVAL, TEST.NEXTVAL FROM TEST;
> CURRVAL NEXTVAL
> ------- -------
> 3       4
> rows: 1

DROP TABLE TEST;
> ok

SET MODE Regular;
> ok

CREATE SEQUENCE SEQ1 AS TINYINT;
> ok

CREATE SEQUENCE SEQ2 AS SMALLINT;
> ok

CREATE SEQUENCE SEQ3 AS INTEGER;
> ok

CREATE SEQUENCE SEQ4 AS BIGINT;
> ok

CREATE SEQUENCE SEQ5 AS REAL;
> ok

CREATE SEQUENCE SEQ6 AS DOUBLE PRECISION;
> ok

CREATE SEQUENCE SEQ7 AS NUMERIC(10, 2);
> ok

CREATE SEQUENCE SEQ8 AS NUMERIC(100, 20);
> ok

SELECT SEQUENCE_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, NUMERIC_SCALE, MAXIMUM_VALUE,
    DECLARED_DATA_TYPE, DECLARED_NUMERIC_PRECISION, DECLARED_NUMERIC_SCALE FROM INFORMATION_SCHEMA.SEQUENCES;
> SEQUENCE_NAME DATA_TYPE        NUMERIC_PRECISION NUMERIC_PRECISION_RADIX NUMERIC_SCALE MAXIMUM_VALUE       DECLARED_DATA_TYPE DECLARED_NUMERIC_PRECISION DECLARED_NUMERIC_SCALE
> ------------- ---------------- ----------------- ----------------------- ------------- ------------------- ------------------ -------------------------- ----------------------
> SEQ1          TINYINT          8                 2                       0             127                 TINYINT            8                          0
> SEQ2          SMALLINT         16                2                       0             32767               SMALLINT           16                         0
> SEQ3          INTEGER          32                2                       0             2147483647          INTEGER            32                         0
> SEQ4          BIGINT           64                2                       0             9223372036854775807 BIGINT             64                         0
> SEQ5          REAL             24                2                       0             16777216            REAL               24                         0
> SEQ6          DOUBLE PRECISION 53                2                       0             9007199254740992    DOUBLE PRECISION   53                         0
> SEQ7          NUMERIC          10                10                      2             99999999            NUMERIC            10                         2
> SEQ8          NUMERIC          39                10                      20            9223372036854775807 NUMERIC            100                        20
> rows: 8

SELECT NEXT VALUE FOR SEQ1 IS OF (TINYINT);
>> TRUE

DROP SEQUENCE SEQ1;
> ok

DROP SEQUENCE SEQ2;
> ok

DROP SEQUENCE SEQ3;
> ok

DROP SEQUENCE SEQ4;
> ok

DROP SEQUENCE SEQ5;
> ok

DROP SEQUENCE SEQ6;
> ok

DROP SEQUENCE SEQ7;
> ok

DROP SEQUENCE SEQ8;
> ok

CREATE SEQUENCE SEQ AS NUMERIC(10, 20);
> exception FEATURE_NOT_SUPPORTED_1

CREATE SEQUENCE SEQ AS VARCHAR(10);
> exception FEATURE_NOT_SUPPORTED_1

CREATE SEQUENCE SEQ NO;
> exception SYNTAX_ERROR_2
