-- Copyright 2004-2024 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(A INT COMMENT NULL, B INT COMMENT '', C INT COMMENT 'comment 1', D INT COMMENT 'comment 2');
> ok

SELECT COLUMN_NAME, REMARKS FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST';
> COLUMN_NAME REMARKS
> ----------- ---------
> A           null
> B           null
> C           comment 1
> D           comment 2
> rows: 4

COMMENT ON COLUMN TEST.A IS 'comment 3';
> ok

COMMENT ON COLUMN TEST.B IS 'comment 4';
> ok

COMMENT ON COLUMN TEST.C IS NULL;
> ok

COMMENT ON COLUMN TEST.D IS '';
> ok

SELECT COLUMN_NAME, REMARKS FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TEST';
> COLUMN_NAME REMARKS
> ----------- ---------
> A           comment 3
> B           comment 4
> C           null
> D           null
> rows: 4

DROP TABLE TEST;
> ok

CREATE USER U1 COMMENT NULL PASSWORD '1';
> ok

CREATE USER U2 COMMENT '' PASSWORD '1';
> ok

CREATE USER U3 COMMENT 'comment' PASSWORD '1';
> ok

SELECT USER_NAME, REMARKS FROM INFORMATION_SCHEMA.USERS WHERE USER_NAME LIKE 'U_';
> USER_NAME REMARKS
> --------- -------
> U1        null
> U2        null
> U3        comment
> rows: 3

DROP USER U1;
> ok

DROP USER U2;
> ok

DROP USER U3;
> ok
