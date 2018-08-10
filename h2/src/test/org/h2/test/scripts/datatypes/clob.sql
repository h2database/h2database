-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(C1 CLOB, C2 CHARACTER LARGE OBJECT, C3 TINYTEXT, C4 TEXT, C5 MEDIUMTEXT, C6 LONGTEXT, C7 NTEXT,
    C8 NCLOB);
> ok

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE
> ----------- --------- --------- ----------------------
> C1          2005      CLOB      CLOB
> C2          2005      CLOB      CHARACTER LARGE OBJECT
> C3          2005      CLOB      TINYTEXT
> C4          2005      CLOB      TEXT
> C5          2005      CLOB      MEDIUMTEXT
> C6          2005      CLOB      LONGTEXT
> C7          2005      CLOB      NTEXT
> C8          2005      CLOB      NCLOB
> rows (ordered): 8

DROP TABLE TEST;
> ok
