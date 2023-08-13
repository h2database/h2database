-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

TABLE INFORMATION_SCHEMA.INFORMATION_SCHEMA_CATALOG_NAME;
> CATALOG_NAME
> ------------
> SCRIPT
> rows: 1

CREATE TABLE T1(C1 INT NOT NULL, C2 INT NOT NULL, C3 INT, C4 INT);
> ok

ALTER TABLE T1 ADD CONSTRAINT PK_1 PRIMARY KEY(C1, C2);
> ok

ALTER TABLE T1 ADD CONSTRAINT U_1 UNIQUE(C3, C4);
> ok

CREATE TABLE T2(C1 INT, C2 INT, C3 INT, C4 INT);
> ok

ALTER TABLE T2 ADD CONSTRAINT FK_1 FOREIGN KEY (C3, C4) REFERENCES T1(C1, C3) ON DELETE SET NULL;
> exception CONSTRAINT_NOT_FOUND_1

SET MODE MySQL;
> ok

ALTER TABLE T2 ADD CONSTRAINT FK_1 FOREIGN KEY (C3, C4) REFERENCES T1(C1, C3) ON DELETE SET NULL;
> ok

ALTER TABLE T2 ADD CONSTRAINT FK_2 FOREIGN KEY (C3, C4) REFERENCES T1(C4, C3) ON UPDATE CASCADE ON DELETE SET DEFAULT;
> ok

SET MODE Regular;
> ok

ALTER TABLE T2 ADD CONSTRAINT CH_1 CHECK (C4 > 0 AND NOT EXISTS(SELECT 1 FROM T1 WHERE T1.C1 + T1.C2 = T2.C4));
> ok

SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS LIMIT 0;
> CONSTRAINT_CATALOG CONSTRAINT_SCHEMA CONSTRAINT_NAME CONSTRAINT_TYPE TABLE_CATALOG TABLE_SCHEMA TABLE_NAME IS_DEFERRABLE INITIALLY_DEFERRED ENFORCED NULLS_DISTINCT INDEX_CATALOG INDEX_SCHEMA INDEX_NAME REMARKS
> ------------------ ----------------- --------------- --------------- ------------- ------------ ---------- ------------- ------------------ -------- -------------- ------------- ------------ ---------- -------
> rows: 0

SELECT CONSTRAINT_NAME, CONSTRAINT_TYPE, TABLE_NAME, IS_DEFERRABLE, INITIALLY_DEFERRED FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
    WHERE CONSTRAINT_CATALOG = DATABASE() AND CONSTRAINT_SCHEMA = SCHEMA() AND TABLE_CATALOG = DATABASE() AND TABLE_SCHEMA = SCHEMA()
    ORDER BY TABLE_NAME, CONSTRAINT_NAME;
> CONSTRAINT_NAME CONSTRAINT_TYPE TABLE_NAME IS_DEFERRABLE INITIALLY_DEFERRED
> --------------- --------------- ---------- ------------- ------------------
> CONSTRAINT_A    UNIQUE          T1         NO            NO
> PK_1            PRIMARY KEY     T1         NO            NO
> U_1             UNIQUE          T1         NO            NO
> CH_1            CHECK           T2         NO            NO
> FK_1            FOREIGN KEY     T2         NO            NO
> FK_2            FOREIGN KEY     T2         NO            NO
> rows (ordered): 6

SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE LIMIT 0;
> CONSTRAINT_CATALOG CONSTRAINT_SCHEMA CONSTRAINT_NAME TABLE_CATALOG TABLE_SCHEMA TABLE_NAME COLUMN_NAME ORDINAL_POSITION POSITION_IN_UNIQUE_CONSTRAINT
> ------------------ ----------------- --------------- ------------- ------------ ---------- ----------- ---------------- -----------------------------
> rows: 0

SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, POSITION_IN_UNIQUE_CONSTRAINT FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE CONSTRAINT_CATALOG = DATABASE() AND CONSTRAINT_SCHEMA = SCHEMA() AND TABLE_CATALOG = DATABASE() AND TABLE_SCHEMA = SCHEMA()
    ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION;
> CONSTRAINT_NAME TABLE_NAME COLUMN_NAME ORDINAL_POSITION POSITION_IN_UNIQUE_CONSTRAINT
> --------------- ---------- ----------- ---------------- -----------------------------
> CONSTRAINT_A    T1         C1          1                null
> CONSTRAINT_A    T1         C3          2                null
> PK_1            T1         C1          1                null
> PK_1            T1         C2          2                null
> U_1             T1         C3          1                null
> U_1             T1         C4          2                null
> FK_1            T2         C3          1                1
> FK_1            T2         C4          2                2
> FK_2            T2         C3          1                2
> FK_2            T2         C4          2                1
> rows (ordered): 10

SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS LIMIT 0;
> CONSTRAINT_CATALOG CONSTRAINT_SCHEMA CONSTRAINT_NAME UNIQUE_CONSTRAINT_CATALOG UNIQUE_CONSTRAINT_SCHEMA UNIQUE_CONSTRAINT_NAME MATCH_OPTION UPDATE_RULE DELETE_RULE
> ------------------ ----------------- --------------- ------------------------- ------------------------ ---------------------- ------------ ----------- -----------
> rows: 0

SELECT CONSTRAINT_NAME, UNIQUE_CONSTRAINT_NAME, MATCH_OPTION, UPDATE_RULE, DELETE_RULE FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS
    WHERE CONSTRAINT_CATALOG = DATABASE() AND CONSTRAINT_SCHEMA = SCHEMA() AND UNIQUE_CONSTRAINT_CATALOG = DATABASE() AND UNIQUE_CONSTRAINT_SCHEMA = SCHEMA()
    ORDER BY CONSTRAINT_NAME, UNIQUE_CONSTRAINT_NAME;
> CONSTRAINT_NAME UNIQUE_CONSTRAINT_NAME MATCH_OPTION UPDATE_RULE DELETE_RULE
> --------------- ---------------------- ------------ ----------- -----------
> FK_1            CONSTRAINT_A           NONE         RESTRICT    SET NULL
> FK_2            U_1                    NONE         CASCADE     SET DEFAULT
> rows (ordered): 2

SELECT U1.TABLE_NAME T1, U1.COLUMN_NAME C1, U2.TABLE_NAME T2, U2.COLUMN_NAME C2
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE U1 JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC ON U1.CONSTRAINT_NAME = RC.CONSTRAINT_NAME
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE U2 ON RC.UNIQUE_CONSTRAINT_NAME = U2.CONSTRAINT_NAME AND U1.POSITION_IN_UNIQUE_CONSTRAINT = U2.ORDINAL_POSITION
    WHERE U1.CONSTRAINT_NAME = 'FK_2' ORDER BY U1.COLUMN_NAME;
> T1 C1 T2 C2
> -- -- -- --
> T2 C3 T1 C4
> T2 C4 T1 C3
> rows (ordered): 2

TABLE INFORMATION_SCHEMA.CHECK_CONSTRAINTS;
> CONSTRAINT_CATALOG CONSTRAINT_SCHEMA CONSTRAINT_NAME CHECK_CLAUSE
> ------------------ ----------------- --------------- ---------------------------------------------------------------------------------------------------
> SCRIPT             PUBLIC            CH_1            ("C4" > 0) AND (NOT EXISTS( SELECT 1 FROM "PUBLIC"."T1" WHERE ("T1"."C1" + "T1"."C2") = "T2"."C4"))
> rows: 1

TABLE INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE;
> TABLE_CATALOG TABLE_SCHEMA TABLE_NAME COLUMN_NAME CONSTRAINT_CATALOG CONSTRAINT_SCHEMA CONSTRAINT_NAME
> ------------- ------------ ---------- ----------- ------------------ ----------------- ---------------
> SCRIPT        PUBLIC       T1         C1          SCRIPT             PUBLIC            CH_1
> SCRIPT        PUBLIC       T1         C1          SCRIPT             PUBLIC            CONSTRAINT_A
> SCRIPT        PUBLIC       T1         C1          SCRIPT             PUBLIC            FK_1
> SCRIPT        PUBLIC       T1         C1          SCRIPT             PUBLIC            PK_1
> SCRIPT        PUBLIC       T1         C2          SCRIPT             PUBLIC            CH_1
> SCRIPT        PUBLIC       T1         C2          SCRIPT             PUBLIC            PK_1
> SCRIPT        PUBLIC       T1         C3          SCRIPT             PUBLIC            CONSTRAINT_A
> SCRIPT        PUBLIC       T1         C3          SCRIPT             PUBLIC            FK_1
> SCRIPT        PUBLIC       T1         C3          SCRIPT             PUBLIC            FK_2
> SCRIPT        PUBLIC       T1         C3          SCRIPT             PUBLIC            U_1
> SCRIPT        PUBLIC       T1         C4          SCRIPT             PUBLIC            FK_2
> SCRIPT        PUBLIC       T1         C4          SCRIPT             PUBLIC            U_1
> SCRIPT        PUBLIC       T2         C3          SCRIPT             PUBLIC            FK_1
> SCRIPT        PUBLIC       T2         C3          SCRIPT             PUBLIC            FK_2
> SCRIPT        PUBLIC       T2         C4          SCRIPT             PUBLIC            CH_1
> SCRIPT        PUBLIC       T2         C4          SCRIPT             PUBLIC            FK_1
> SCRIPT        PUBLIC       T2         C4          SCRIPT             PUBLIC            FK_2
> rows: 17

DROP TABLE T2;
> ok

DROP TABLE T1;
> ok

@reconnect off

CREATE TABLE T1(C1 INT PRIMARY KEY);
> ok

CREATE TABLE T2(C2 INT PRIMARY KEY REFERENCES T1);
> ok

SELECT ENFORCED FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'FOREIGN KEY';
>> YES

SET REFERENTIAL_INTEGRITY FALSE;
> ok

SELECT ENFORCED FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'FOREIGN KEY';
>> NO

SET REFERENTIAL_INTEGRITY TRUE;
> ok

ALTER TABLE T1 SET REFERENTIAL_INTEGRITY FALSE;
> ok

SELECT ENFORCED FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'FOREIGN KEY';
>> NO

ALTER TABLE T1 SET REFERENTIAL_INTEGRITY TRUE;
> ok

ALTER TABLE T2 SET REFERENTIAL_INTEGRITY FALSE;
> ok

SELECT ENFORCED FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'FOREIGN KEY';
>> NO

DROP TABLE T2, T1;
> ok

@reconnect on

SELECT TABLE_NAME, ROW_COUNT_ESTIMATE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'INFORMATION_SCHEMA'
    AND TABLE_NAME IN ('INFORMATION_SCHEMA_CATALOG_NAME', 'SCHEMATA', 'ROLES', 'SESSIONS', 'IN_DOUBT', 'USERS');
> TABLE_NAME                      ROW_COUNT_ESTIMATE
> ------------------------------- ------------------
> INFORMATION_SCHEMA_CATALOG_NAME 1
> IN_DOUBT                        0
> ROLES                           1
> SCHEMATA                        2
> SESSIONS                        1
> USERS                           1
> rows: 6

EXPLAIN SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLLATIONS;
>> SELECT COUNT(*) FROM "INFORMATION_SCHEMA"."COLLATIONS" /* meta */ /* direct lookup */
