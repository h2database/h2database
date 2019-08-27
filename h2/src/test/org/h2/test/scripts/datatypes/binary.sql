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

CREATE MEMORY TABLE TEST AS (VALUES X'11' || X'25');
> ok

SCRIPT NOPASSWORDS NOSETTINGS TABLE TEST;
> SCRIPT
> ---------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "C1" VARBINARY(2) );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> INSERT INTO "PUBLIC"."TEST" VALUES (X'1125');
> rows: 4

EXPLAIN SELECT C1 || X'10' FROM TEST;
>> SELECT ("C1" || X'10') FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

DROP TABLE TEST;
> ok

SELECT X'11' || CAST(NULL AS VARBINARY);
>> null

SELECT CAST(NULL AS VARBINARY) || X'11';
>> null

SELECT X'1';
> exception HEX_STRING_ODD_1

SELECT X'1' '1';
> exception HEX_STRING_ODD_1

SELECT X' 1 2  3 4 ';
>> 1234

SELECT X'1 2 3';
> exception HEX_STRING_ODD_1

SELECT X'~';
> exception HEX_STRING_WRONG_1

SELECT X'G';
> exception HEX_STRING_WRONG_1

SELECT X'TT';
> exception HEX_STRING_WRONG_1

SELECT X' TT';
> exception HEX_STRING_WRONG_1

SELECT X'AB' 'CD';
>> abcd

SELECT X'AB' /* comment*/ 'CD' 'EF';
>> abcdef

SELECT X'AB' 'CX';
> exception HEX_STRING_WRONG_1

SELECT 0xabcd;
>> 43981

SET MODE MSSQLServer;
> ok

SELECT 0x, 0x12ab;
> X X12ab
> - -----
>   12ab
> rows: 1

SELECT 0xZ;
> exception HEX_STRING_WRONG_1

SET MODE MySQL;
> ok

SELECT 0x, 0x12ab;
> X X12ab
> - -----
>   12ab
> rows: 1

SELECT 0xZ;
> exception HEX_STRING_WRONG_1

SET MODE Regular;
> ok
