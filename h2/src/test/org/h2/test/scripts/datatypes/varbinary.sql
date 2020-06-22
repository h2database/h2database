-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(B1 VARBINARY, B2 BINARY VARYING, B3 RAW, B4 BYTEA, B5 LONG RAW, B6 LONGVARBINARY);
> ok

SELECT COLUMN_NAME, DATA_TYPE,  COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE      COLUMN_TYPE
> ----------- -------------- --------------
> B1          BINARY VARYING VARBINARY
> B2          BINARY VARYING BINARY VARYING
> B3          BINARY VARYING RAW
> B4          BINARY VARYING BYTEA
> B5          BINARY VARYING LONG RAW
> B6          BINARY VARYING LONGVARBINARY
> rows (ordered): 6

DROP TABLE TEST;
> ok

CREATE MEMORY TABLE TEST AS (VALUES X'11' || X'25');
> ok

SCRIPT NOPASSWORDS NOSETTINGS TABLE TEST;
> SCRIPT
> --------------------------------------------------------------
> -- 1 +/- SELECT COUNT(*) FROM PUBLIC.TEST;
> CREATE MEMORY TABLE "PUBLIC"."TEST"( "C1" BINARY VARYING(2) );
> CREATE USER IF NOT EXISTS "SA" PASSWORD '' ADMIN;
> INSERT INTO "PUBLIC"."TEST" VALUES (X'1125');
> rows: 4

EXPLAIN SELECT C1 || X'10' FROM TEST;
>> SELECT "C1" || X'10' FROM "PUBLIC"."TEST" /* PUBLIC.TEST.tableScan */

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
>> X'1234'

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
>> X'abcd'

SELECT X'AB' /* comment*/ 'CD' 'EF';
>> X'abcdef'

SELECT X'AB' 'CX';
> exception HEX_STRING_WRONG_1

SELECT 0xabcd;
>> 43981

SET MODE MSSQLServer;
> ok

SELECT 0x, 0x12ab;
>
> --- -------
> X'' X'12ab'
> rows: 1

SELECT 0xZ;
> exception HEX_STRING_WRONG_1

SET MODE MySQL;
> ok

SELECT 0x, 0x12ab;
> X'' X'12ab'
> --- -------
> X'' X'12ab'
> rows: 1

SELECT 0xZ;
> exception HEX_STRING_WRONG_1

SET MODE Regular;
> ok

EXPLAIN VALUES X'';
>> VALUES (X'')

CREATE TABLE T(C VARBINARY(0));
> exception INVALID_VALUE_2
