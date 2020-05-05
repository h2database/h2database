-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE COUNT(X INT);
> ok

CREATE FORCE TRIGGER T_COUNT BEFORE INSERT ON COUNT CALL "com.Unknown";
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

CREATE TRIGGER T1 BEFORE INSERT, UPDATE ON TEST FOR EACH ROW CALL "org.h2.test.scripts.Trigger1";
> ok

INSERT INTO TEST VALUES ('a', 'b', 'c');
> exception ERROR_EXECUTING_TRIGGER_3

DROP TABLE TEST;
> ok

CREATE TABLE TEST(A VARCHAR, B VARCHAR, C INT);
> ok

CREATE TRIGGER T1 BEFORE INSERT ON TEST FOR EACH ROW CALL "org.h2.test.scripts.Trigger1";
> ok

INSERT INTO TEST VALUES ('1', 'a', 1);
> update count: 1

DROP TRIGGER T1;
> ok

CREATE TRIGGER T1 BEFORE INSERT ON TEST FOR EACH STATEMENT CALL "org.h2.test.scripts.Trigger1";
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

CREATE FORCE TRIGGER T_COUNT BEFORE INSERT OR UPDATE ON COUNT CALL "com.Unknown";
> ok

INSERT INTO COUNT VALUES(NULL);
> exception ERROR_CREATING_TRIGGER_OBJECT_3

UPDATE COUNT SET X=2 WHERE X=1;
> exception ERROR_CREATING_TRIGGER_OBJECT_3

SET MODE Regular;
> ok
