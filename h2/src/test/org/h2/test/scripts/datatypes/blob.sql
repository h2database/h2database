-- Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (http://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(B1 BLOB, B2 BINARY LARGE OBJECT, B3 TINYBLOB, B4 MEDIUMBLOB, B5 LONGBLOB, B6 IMAGE, B7 OID);
> ok

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE
> ----------- --------- --------- -------------------
> B1          2004      BLOB      BLOB
> B2          2004      BLOB      BINARY LARGE OBJECT
> B3          2004      BLOB      TINYBLOB
> B4          2004      BLOB      MEDIUMBLOB
> B5          2004      BLOB      LONGBLOB
> B6          2004      BLOB      IMAGE
> B7          2004      BLOB      OID
> rows (ordered): 7

DROP TABLE TEST;
> ok
