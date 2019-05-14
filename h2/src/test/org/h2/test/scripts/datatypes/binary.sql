-- Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

SELECT TYPE_NAME, PRECISION, PREFIX, SUFFIX, PARAMS, MINIMUM_SCALE, MAXIMUM_SCALE FROM INFORMATION_SCHEMA.TYPE_INFO
    WHERE TYPE_NAME IN ('BINARY', 'VARBINARY', 'LONGVARBINARY');
> TYPE_NAME     PRECISION  PREFIX SUFFIX PARAMS MINIMUM_SCALE MAXIMUM_SCALE
> ------------- ---------- ------ ------ ------ ------------- -------------
> BINARY        2147483647 X'     '      LENGTH 0             0
> LONGVARBINARY 2147483647 X'     '      LENGTH 0             0
> VARBINARY     2147483647 X'     '      LENGTH 0             0
> rows: 3

CREATE TABLE TEST(B1 VARBINARY, B2 BINARY VARYING, B3 BINARY, B4 RAW, B5 BYTEA, B6 LONG RAW, B7 LONGVARBINARY);
> ok

SELECT COLUMN_NAME, DATA_TYPE, TYPE_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE TYPE_NAME COLUMN_TYPE
> ----------- --------- --------- --------------
> B1          -3        VARBINARY VARBINARY
> B2          -3        VARBINARY BINARY VARYING
> B3          -3        VARBINARY BINARY
> B4          -3        VARBINARY RAW
> B5          -3        VARBINARY BYTEA
> B6          -3        VARBINARY LONG RAW
> B7          -3        VARBINARY LONGVARBINARY
> rows (ordered): 7

DROP TABLE TEST;
> ok
