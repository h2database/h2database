-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE COUNT(X INT);
> ok

CREATE FORCE TRIGGER T_COUNT BEFORE INSERT ON COUNT CALL 'com.Unknown';
> ok

INSERT INTO COUNT VALUES(NULL);
> exception ERROR_CREATING_TRIGGER_OBJECT_3

DROP TRIGGER T_COUNT;
> ok

CREATE TABLE ITEMS(ID INT CHECK ID < SELECT MAX(ID) FROM COUNT);
> ok

insert into items values(DEFAULT);
> update count: 1

DROP TABLE COUNT;
> exception CANNOT_DROP_2

insert into items values(DEFAULT);
> update count: 1

drop table items, count;
> ok

CREATE TABLE TEST(A VARCHAR, B VARCHAR, C VARCHAR);
> ok

CREATE TRIGGER T1 BEFORE INSERT, UPDATE ON TEST FOR EACH ROW CALL 'org.h2.test.scripts.Trigger1';
> ok

INSERT INTO TEST VALUES ('a', 'b', 'c');
> exception ERROR_EXECUTING_TRIGGER_3

DROP TABLE TEST;
> ok

CREATE TABLE TEST(A VARCHAR, B VARCHAR, C INT);
> ok

CREATE TRIGGER T1 BEFORE INSERT ON TEST FOR EACH ROW CALL 'org.h2.test.scripts.Trigger1';
> ok

INSERT INTO TEST VALUES ('1', 'a', 1);
> update count: 1

DROP TRIGGER T1;
> ok

CREATE TRIGGER T1 BEFORE INSERT ON TEST FOR EACH STATEMENT CALL 'org.h2.test.scripts.Trigger1';
> ok

INSERT INTO TEST VALUES ('2', 'b', 2);
> update count: 1

DROP TRIGGER T1;
> ok

TABLE TEST;
> A B C
> - - --
> 1 a 10
> 2 b 2
> rows: 2

DROP TABLE TEST;
> ok

-- ---------------------------------------------------------------------------
-- Checking multiple classes in trigger source
-- ---------------------------------------------------------------------------

CREATE TABLE TEST(A VARCHAR, B VARCHAR, C VARCHAR);
> ok

CREATE TRIGGER T1 BEFORE INSERT, UPDATE ON TEST FOR EACH ROW AS STRINGDECODE(
'org.h2.api.Trigger create() {
    return new org.h2.api.Trigger() {
        public void fire(Connection conn, Object[] oldRow, Object[] newRow) {
            if (newRow != null) {
                newRow[2] = newRow[2] + "1"\u003B
            }
        }
    }\u003B
}');
> ok

INSERT INTO TEST VALUES ('a', 'b', 'c');
> update count: 1

TABLE TEST;
> A B C
> - - --
> a b c1
> rows: 1

DROP TABLE TEST;
> ok

-- ---------------------------------------------------------------------------
-- PostgreSQL syntax tests
-- ---------------------------------------------------------------------------

set mode postgresql;
> ok

CREATE TABLE COUNT(X INT);
> ok

INSERT INTO COUNT VALUES(1);
> update count: 1

CREATE FORCE TRIGGER T_COUNT BEFORE INSERT OR UPDATE ON COUNT CALL 'com.Unknown';
> ok

INSERT INTO COUNT VALUES(NULL);
> exception ERROR_CREATING_TRIGGER_OBJECT_3

UPDATE COUNT SET X=2 WHERE X=1;
> exception ERROR_CREATING_TRIGGER_OBJECT_3

DROP TABLE COUNT;
> ok

SET MODE Regular;
> ok

CREATE MEMORY TABLE T(ID INT PRIMARY KEY, V INT);
> ok

CREATE VIEW V1 AS TABLE T;
> ok

CREATE VIEW V2 AS TABLE T;
> ok

CREATE VIEW V3 AS TABLE T;
> ok

CREATE TRIGGER T1 INSTEAD OF INSERT ON V1 FOR EACH ROW AS STRINGDECODE(
'org.h2.api.Trigger create() {
    return new org.h2.api.Trigger() {
        public void fire(Connection conn, Object[] oldRow, Object[] newRow) {
        }
    }\u003B
}');
> ok

CREATE TRIGGER T2 INSTEAD OF UPDATE ON V2 FOR EACH ROW AS STRINGDECODE(
'org.h2.api.Trigger create() {
    return new org.h2.api.Trigger() {
        public void fire(Connection conn, Object[] oldRow, Object[] newRow) {
        }
    }\u003B
}');
> ok

CREATE TRIGGER T3 INSTEAD OF DELETE ON V3 FOR EACH ROW AS STRINGDECODE(
'org.h2.api.Trigger create() {
    return new org.h2.api.Trigger() {
        public void fire(Connection conn, Object[] oldRow, Object[] newRow) {
        }
    }\u003B
}');
> ok

SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, IS_INSERTABLE_INTO, COMMIT_ACTION
    FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC';
> TABLE_CATALOG TABLE_SCHEMA TABLE_NAME TABLE_TYPE IS_INSERTABLE_INTO COMMIT_ACTION
> ------------- ------------ ---------- ---------- ------------------ -------------
> SCRIPT        PUBLIC       T          BASE TABLE YES                null
> SCRIPT        PUBLIC       V1         VIEW       NO                 null
> SCRIPT        PUBLIC       V2         VIEW       NO                 null
> SCRIPT        PUBLIC       V3         VIEW       NO                 null
> rows: 4

SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION, CHECK_OPTION, IS_UPDATABLE, INSERTABLE_INTO,
    IS_TRIGGER_UPDATABLE, IS_TRIGGER_DELETABLE, IS_TRIGGER_INSERTABLE_INTO
    FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'PUBLIC';
> TABLE_CATALOG TABLE_SCHEMA TABLE_NAME VIEW_DEFINITION    CHECK_OPTION IS_UPDATABLE INSERTABLE_INTO IS_TRIGGER_UPDATABLE IS_TRIGGER_DELETABLE IS_TRIGGER_INSERTABLE_INTO
> ------------- ------------ ---------- ------------------ ------------ ------------ --------------- -------------------- -------------------- --------------------------
> SCRIPT        PUBLIC       V1         TABLE "PUBLIC"."T" NONE         NO           NO              NO                   NO                   YES
> SCRIPT        PUBLIC       V2         TABLE "PUBLIC"."T" NONE         NO           NO              YES                  NO                   NO
> SCRIPT        PUBLIC       V3         TABLE "PUBLIC"."T" NONE         NO           NO              NO                   YES                  NO
> rows: 3

SELECT * FROM INFORMATION_SCHEMA.TRIGGERS;
> TRIGGER_CATALOG TRIGGER_SCHEMA TRIGGER_NAME EVENT_MANIPULATION EVENT_OBJECT_CATALOG EVENT_OBJECT_SCHEMA EVENT_OBJECT_TABLE ACTION_ORIENTATION ACTION_TIMING IS_ROLLBACK JAVA_CLASS QUEUE_SIZE NO_WAIT REMARKS
> --------------- -------------- ------------ ------------------ -------------------- ------------------- ------------------ ------------------ ------------- ----------- ---------- ---------- ------- -------
> SCRIPT          PUBLIC         T1           INSERT             SCRIPT               PUBLIC              V1                 ROW                INSTEAD OF    FALSE       null       1024       FALSE   null
> SCRIPT          PUBLIC         T2           UPDATE             SCRIPT               PUBLIC              V2                 ROW                INSTEAD OF    FALSE       null       1024       FALSE   null
> SCRIPT          PUBLIC         T3           DELETE             SCRIPT               PUBLIC              V3                 ROW                INSTEAD OF    FALSE       null       1024       FALSE   null
> rows: 3

CREATE TRIGGER T4 BEFORE ROLLBACK ON TEST FOR EACH ROW AS STRINGDECODE(
'org.h2.api.Trigger create() {
    return new org.h2.api.Trigger() {
        public void fire(Connection conn, Object[] oldRow, Object[] newRow) {
        }
    }\u003B
}');
> exception INVALID_TRIGGER_FLAGS_1

CREATE TRIGGER T4 BEFORE SELECT ON TEST FOR EACH ROW AS STRINGDECODE(
'org.h2.api.Trigger create() {
    return new org.h2.api.Trigger() {
        public void fire(Connection conn, Object[] oldRow, Object[] newRow) {
        }
    }\u003B
}');
> exception INVALID_TRIGGER_FLAGS_1

CREATE TRIGGER T4 BEFORE SELECT, ROLLBACK ON TEST FOR EACH STATEMENT AS STRINGDECODE(
'org.h2.api.Trigger create() {
    return new org.h2.api.Trigger() {
        public void fire(Connection conn, Object[] oldRow, Object[] newRow) {
        }
    }\u003B
}');
> exception INVALID_TRIGGER_FLAGS_1

DROP TABLE T CASCADE;
> ok
