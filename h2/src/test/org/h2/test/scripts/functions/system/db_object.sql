-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE ROLE A;
> ok

CREATE ROLE B;
> ok

SELECT ID_A <> ID_B, SQL_A, SQL_B FROM (VALUES (
    DB_OBJECT_ID('ROLE', 'A'),
    DB_OBJECT_ID('ROLE', 'B'),
    DB_OBJECT_SQL('ROLE', 'A'),
    DB_OBJECT_SQL('ROLE', 'B')
)) T(ID_A, ID_B, SQL_A, SQL_B);
> ID_A <> ID_B SQL_A           SQL_B
> ------------ --------------- ---------------
> TRUE         CREATE ROLE "A" CREATE ROLE "B"
> rows: 1

DROP ROLE A;
> ok

DROP ROLE B;
> ok

CALL DB_OBJECT_ID('SETTING', 'CREATE_BUILD') IS NOT NULL;
>> TRUE

CALL DB_OBJECT_SQL('SETTING', 'CREATE_BUILD') IS NOT NULL;
>> TRUE

CREATE SCHEMA A;
> ok

CREATE SCHEMA B;
> ok

SELECT ID_A <> ID_B, SQL_A, SQL_B FROM (VALUES (
    DB_OBJECT_ID('SCHEMA', 'A'),
    DB_OBJECT_ID('SCHEMA', 'B'),
    DB_OBJECT_SQL('SCHEMA', 'A'),
    DB_OBJECT_SQL('SCHEMA', 'B')
)) T(ID_A, ID_B, SQL_A, SQL_B);
> ID_A <> ID_B SQL_A                                              SQL_B
> ------------ -------------------------------------------------- --------------------------------------------------
> TRUE         CREATE SCHEMA IF NOT EXISTS "A" AUTHORIZATION "SA" CREATE SCHEMA IF NOT EXISTS "B" AUTHORIZATION "SA"
> rows: 1

DROP SCHEMA A;
> ok

DROP SCHEMA B;
> ok

CREATE USER A SALT X'00' HASH X'00';
> ok

CREATE USER B SALT X'00' HASH X'00';
> ok

SELECT ID_A <> ID_B, SQL_A, SQL_B FROM (VALUES (
    DB_OBJECT_ID('USER', 'A'),
    DB_OBJECT_ID('USER', 'B'),
    DB_OBJECT_SQL('USER', 'A'),
    DB_OBJECT_SQL('USER', 'B')
)) T(ID_A, ID_B, SQL_A, SQL_B);
> ID_A <> ID_B SQL_A                                             SQL_B
> ------------ ------------------------------------------------- -------------------------------------------------
> TRUE         CREATE USER IF NOT EXISTS "A" SALT '00' HASH '00' CREATE USER IF NOT EXISTS "B" SALT '00' HASH '00'
> rows: 1

DROP USER A;
> ok

DROP USER B;
> ok

CREATE CONSTANT A VALUE 1;
> ok

CREATE CONSTANT B VALUE 2;
> ok

SELECT ID_A <> ID_B, SQL_A, SQL_B FROM (VALUES (
    DB_OBJECT_ID('CONSTANT', 'PUBLIC', 'A'),
    DB_OBJECT_ID('CONSTANT', 'PUBLIC', 'B'),
    DB_OBJECT_SQL('CONSTANT', 'PUBLIC', 'A'),
    DB_OBJECT_SQL('CONSTANT', 'PUBLIC', 'B')
)) T(ID_A, ID_B, SQL_A, SQL_B);
> ID_A <> ID_B SQL_A                                SQL_B
> ------------ ------------------------------------ ------------------------------------
> TRUE         CREATE CONSTANT "PUBLIC"."A" VALUE 1 CREATE CONSTANT "PUBLIC"."B" VALUE 2
> rows: 1

DROP CONSTANT A;
> ok

DROP CONSTANT B;
> ok

CREATE DOMAIN A AS CHAR;
> ok

CREATE DOMAIN B AS CHAR;
> ok

SELECT ID_A <> ID_B, SQL_A, SQL_B FROM (VALUES (
    DB_OBJECT_ID('DOMAIN', 'PUBLIC', 'A'),
    DB_OBJECT_ID('DOMAIN', 'PUBLIC', 'B'),
    DB_OBJECT_SQL('DOMAIN', 'PUBLIC', 'A'),
    DB_OBJECT_SQL('DOMAIN', 'PUBLIC', 'B')
)) T(ID_A, ID_B, SQL_A, SQL_B);
> ID_A <> ID_B SQL_A                                   SQL_B
> ------------ --------------------------------------- ---------------------------------------
> TRUE         CREATE DOMAIN "PUBLIC"."A" AS CHARACTER CREATE DOMAIN "PUBLIC"."B" AS CHARACTER
> rows: 1

DROP DOMAIN A;
> ok

DROP DOMAIN B;
> ok

CREATE ALIAS A FOR 'java.lang.Math.sqrt';
> ok

CREATE AGGREGATE B FOR 'org.h2.test.scripts.Aggregate1';
> ok

SELECT ID_A <> ID_B, SQL_A, SQL_B FROM (VALUES (
    DB_OBJECT_ID('ROUTINE', 'PUBLIC', 'A'),
    DB_OBJECT_ID('ROUTINE', 'PUBLIC', 'B'),
    DB_OBJECT_SQL('ROUTINE', 'PUBLIC', 'A'),
    DB_OBJECT_SQL('ROUTINE', 'PUBLIC', 'B')
)) T(ID_A, ID_B, SQL_A, SQL_B);
> ID_A <> ID_B SQL_A                                                     SQL_B
> ------------ --------------------------------------------------------- ------------------------------------------------------------------------
> TRUE         CREATE FORCE ALIAS "PUBLIC"."A" FOR 'java.lang.Math.sqrt' CREATE FORCE AGGREGATE "PUBLIC"."B" FOR 'org.h2.test.scripts.Aggregate1'
> rows: 1

DROP ALIAS A;
> ok

DROP AGGREGATE B;
> ok

CREATE SEQUENCE A;
> ok

CREATE SEQUENCE B;
> ok

SELECT ID_A <> ID_B, SQL_A, SQL_B FROM (VALUES (
    DB_OBJECT_ID('SEQUENCE', 'PUBLIC', 'A'),
    DB_OBJECT_ID('SEQUENCE', 'PUBLIC', 'B'),
    DB_OBJECT_SQL('SEQUENCE', 'PUBLIC', 'A'),
    DB_OBJECT_SQL('SEQUENCE', 'PUBLIC', 'B')
)) T(ID_A, ID_B, SQL_A, SQL_B);
> ID_A <> ID_B SQL_A                                     SQL_B
> ------------ ----------------------------------------- -----------------------------------------
> TRUE         CREATE SEQUENCE "PUBLIC"."A" START WITH 1 CREATE SEQUENCE "PUBLIC"."B" START WITH 1
> rows: 1

DROP SEQUENCE A;
> ok

DROP SEQUENCE B;
> ok

CREATE MEMORY TABLE T_A(ID INT);
> ok

CREATE UNIQUE INDEX I_A ON T_A(ID);
> ok

ALTER TABLE T_A ADD CONSTRAINT C_A UNIQUE(ID);
> ok

CREATE SYNONYM S_A FOR T_A;
> ok

CREATE TRIGGER G_A BEFORE INSERT ON T_A FOR EACH ROW CALL 'org.h2.test.scripts.Trigger1';
> ok

CREATE MEMORY TABLE T_B(ID INT);
> ok

CREATE UNIQUE INDEX I_B ON T_B(ID);
> ok

ALTER TABLE T_B ADD CONSTRAINT C_B UNIQUE(ID);
> ok

CREATE SYNONYM S_B FOR T_B;
> ok

CREATE TRIGGER G_B BEFORE INSERT ON T_B FOR EACH ROW CALL 'org.h2.test.scripts.Trigger1';
> ok

SELECT T, ID_A <> ID_B, SQL_A, SQL_B FROM (VALUES
(
    'CONSTRAINT',
    DB_OBJECT_ID('CONSTRAINT', 'PUBLIC', 'C_A'),
    DB_OBJECT_ID('CONSTRAINT', 'PUBLIC', 'C_B'),
    DB_OBJECT_SQL('CONSTRAINT', 'PUBLIC', 'C_A'),
    DB_OBJECT_SQL('CONSTRAINT', 'PUBLIC', 'C_B')
), (
    'INDEX',
    DB_OBJECT_ID('INDEX', 'PUBLIC', 'I_A'),
    DB_OBJECT_ID('INDEX', 'PUBLIC', 'I_B'),
    DB_OBJECT_SQL('INDEX', 'PUBLIC', 'I_A'),
    DB_OBJECT_SQL('INDEX', 'PUBLIC', 'I_B')
), (
    'SYNONYM',
    DB_OBJECT_ID('SYNONYM', 'PUBLIC', 'S_A'),
    DB_OBJECT_ID('SYNONYM', 'PUBLIC', 'S_B'),
    DB_OBJECT_SQL('SYNONYM', 'PUBLIC', 'S_A'),
    DB_OBJECT_SQL('SYNONYM', 'PUBLIC', 'S_B')
), (
    'TABLE',
    DB_OBJECT_ID('TABLE', 'PUBLIC', 'T_A'),
    DB_OBJECT_ID('TABLE', 'PUBLIC', 'T_B'),
    DB_OBJECT_SQL('TABLE', 'PUBLIC', 'T_A'),
    DB_OBJECT_SQL('TABLE', 'PUBLIC', 'T_B')
), (
    'TRIGGER',
    DB_OBJECT_ID('TRIGGER', 'PUBLIC', 'G_A'),
    DB_OBJECT_ID('TRIGGER', 'PUBLIC', 'G_B'),
    DB_OBJECT_SQL('TRIGGER', 'PUBLIC', 'G_A'),
    DB_OBJECT_SQL('TRIGGER', 'PUBLIC', 'G_B')
)) T(T, ID_A, ID_B, SQL_A, SQL_B);
> T          ID_A <> ID_B SQL_A                                                                                                                           SQL_B
> ---------- ------------ ------------------------------------------------------------------------------------------------------------------------------- -------------------------------------------------------------------------------------------------------------------------------
> CONSTRAINT TRUE         ALTER TABLE "PUBLIC"."T_A" ADD CONSTRAINT "PUBLIC"."C_A" UNIQUE NULLS DISTINCT ("ID")                                           ALTER TABLE "PUBLIC"."T_B" ADD CONSTRAINT "PUBLIC"."C_B" UNIQUE NULLS DISTINCT ("ID")
> INDEX      TRUE         CREATE UNIQUE NULLS DISTINCT INDEX "PUBLIC"."I_A" ON "PUBLIC"."T_A"("ID" NULLS FIRST)                                           CREATE UNIQUE NULLS DISTINCT INDEX "PUBLIC"."I_B" ON "PUBLIC"."T_B"("ID" NULLS FIRST)
> SYNONYM    TRUE         CREATE SYNONYM "PUBLIC"."S_A" FOR "PUBLIC"."T_A"                                                                                CREATE SYNONYM "PUBLIC"."S_B" FOR "PUBLIC"."T_B"
> TABLE      TRUE         CREATE MEMORY TABLE "PUBLIC"."T_A"( "ID" INTEGER )                                                                              CREATE MEMORY TABLE "PUBLIC"."T_B"( "ID" INTEGER )
> TRIGGER    TRUE         CREATE FORCE TRIGGER "PUBLIC"."G_A" BEFORE INSERT ON "PUBLIC"."T_A" FOR EACH ROW QUEUE 1024 CALL 'org.h2.test.scripts.Trigger1' CREATE FORCE TRIGGER "PUBLIC"."G_B" BEFORE INSERT ON "PUBLIC"."T_B" FOR EACH ROW QUEUE 1024 CALL 'org.h2.test.scripts.Trigger1'
> rows: 5

DROP SYNONYM S_A;
> ok

DROP SYNONYM S_B;
> ok

DROP TABLE T_B, T_A;
> ok

CALL DB_OBJECT_ID(NULL, NULL);
>> null

CALL DB_OBJECT_ID(NULL, NULL, NULL);
>> null

CALL DB_OBJECT_ID('UNKNOWN', NULL);
>> null

CALL DB_OBJECT_ID('UNKNOWN', 'UNKNOWN');
>> null

CALL DB_OBJECT_ID('UNKNOWN', 'PUBLIC', 'UNKNOWN');
>> null

CALL DB_OBJECT_ID('UNKNOWN', 'UNKNOWN', 'UNKNOWN');
>> null

CALL DB_OBJECT_ID('TABLE', 'UNKNOWN', 'UNKNOWN');
>> null

CALL DB_OBJECT_ID('TABLE', 'PUBLIC', 'UNKNOWN');
>> null

CALL DB_OBJECT_ID('TABLE', 'PUBLIC', NULL);
>> null

CALL DB_OBJECT_ID('TABLE', 'INFORMATION_SCHEMA', 'TABLES') IS NOT NULL;
>> TRUE

CALL DB_OBJECT_SQL('TABLE', 'INFORMATION_SCHEMA', 'TABLES');
>> null

CREATE TABLE T_B(V INT);
> ok

INSERT INTO T_B VALUES 1, 2;
> update count: 2

CREATE INDEX I_B ON T_B(V);
> ok

CHECKPOINT SYNC;
> ok

SELECT DB_OBJECT_TOTAL_SIZE('TABLE', 'PUBLIC', 'T_B') >= DB_OBJECT_SIZE('TABLE', 'PUBLIC', 'T_B');
>> TRUE

SELECT DB_OBJECT_TOTAL_SIZE('TABLE', 'PUBLIC', 'T_B') =
    DB_OBJECT_SIZE('TABLE', 'PUBLIC', 'T_B') + DB_OBJECT_SIZE('INDEX', 'PUBLIC', 'I_B');
>> TRUE

SELECT DB_OBJECT_APPROXIMATE_TOTAL_SIZE('TABLE', 'PUBLIC', 'T_B') >= DB_OBJECT_APPROXIMATE_SIZE('TABLE', 'PUBLIC', 'T_B');
>> TRUE

SELECT DB_OBJECT_APPROXIMATE_TOTAL_SIZE('TABLE', 'PUBLIC', 'T_B') =
    DB_OBJECT_APPROXIMATE_SIZE('TABLE', 'PUBLIC', 'T_B') + DB_OBJECT_APPROXIMATE_SIZE('INDEX', 'PUBLIC', 'I_B');
>> TRUE

DROP TABLE T_B;
> ok
