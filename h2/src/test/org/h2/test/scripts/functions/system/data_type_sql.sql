-- Copyright 2004-2023 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--
CREATE CONSTANT C VALUE 12;
> ok

CREATE DOMAIN D AS CHAR(3);
> ok

CREATE TABLE T (C VARCHAR(10));
> ok

CREATE ALIAS R FOR "java.lang.Math.max(long,long)";
> ok

SELECT ID, DATA_TYPE_SQL('PUBLIC', 'C', 'CONSTANT', ID) FROM (VALUES NULL, 'TYPE', 'X') T(ID);
> ID   DATA_TYPE_SQL('PUBLIC', 'C', 'CONSTANT', ID)
> ---- --------------------------------------------
> TYPE INTEGER
> X    null
> null null
> rows: 3

SELECT ID, DATA_TYPE_SQL('PUBLIC', 'D', 'DOMAIN', ID) FROM (VALUES NULL, 'TYPE', 'X') T(ID);
> ID   DATA_TYPE_SQL('PUBLIC', 'D', 'DOMAIN', ID)
> ---- ------------------------------------------
> TYPE CHARACTER(3)
> X    null
> null null
> rows: 3

SELECT ID, DATA_TYPE_SQL('PUBLIC', 'T', 'TABLE', ID) FROM (VALUES NULL, '0', '1', '2', 'X') T(ID);
> ID   DATA_TYPE_SQL('PUBLIC', 'T', 'TABLE', ID)
> ---- -----------------------------------------
> 0    null
> 1    CHARACTER VARYING(10)
> 2    null
> X    null
> null null
> rows: 5

SELECT ID, DATA_TYPE_SQL('PUBLIC', 'R_1', 'ROUTINE', ID) FROM (VALUES NULL, 'RESULT', '0', '1', '2', '3', 'X') T(ID);
> ID     DATA_TYPE_SQL('PUBLIC', 'R_1', 'ROUTINE', ID)
> ------ ---------------------------------------------
> 0      null
> 1      BIGINT
> 2      BIGINT
> 3      null
> RESULT BIGINT
> X      null
> null   null
> rows: 7

SELECT DATA_TYPE_SQL(S, O, T, I) FROM (VALUES
    (NULL, 'C', 'CONSTANT', 'TYPE'),
    ('X', 'C', 'CONSTANT', 'TYPE'),
    ('PUBLIC', NULL, 'CONSTANT', 'TYPE'),
    ('PUBLIC', 'X', 'CONSTANT', 'TYPE'),
    ('PUBLIC', 'C', NULL, 'TYPE'),
    (NULL, 'D', 'DOMAIN', 'TYPE'),
    ('X', 'D', 'DOMAIN', 'TYPE'),
    ('PUBLIC', NULL, 'DOMAIN', 'TYPE'),
    ('PUBLIC', 'X', 'DOMAIN', 'TYPE'),
    ('PUBLIC', 'D', NULL, 'TYPE'),
    (NULL, 'T', 'TABLE', '1'),
    ('X', 'T', 'TABLE', '1'),
    ('PUBLIC', NULL, 'TABLE', '1'),
    ('PUBLIC', 'X', 'TABLE', '1'),
    ('PUBLIC', 'T', NULL, '1'),
    (NULL, 'R_1', 'ROUTINE', '1'),
    ('X', 'R_1', 'ROUTINE', '1'),
    ('PUBLIC', NULL, 'ROUTINE', '1'),
    ('PUBLIC', 'R_0', 'ROUTINE', '1'),
    ('PUBLIC', 'R_2', 'ROUTINE', '1'),
    ('PUBLIC', 'R_Z', 'ROUTINE', '1'),
    ('PUBLIC', 'X', 'ROUTINE', '1'),
    ('PUBLIC', 'X_1', 'ROUTINE', '1'),
    ('PUBLIC', 'R_1', NULL, '1'),
    ('PUBLIC', 'T', 'X', '1')
    ) T(S, O, T, I);
> DATA_TYPE_SQL(S, O, T, I)
> -------------------------
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> null
> rows: 25

DROP CONSTANT C;
> ok

DROP DOMAIN D;
> ok

DROP TABLE T;
> ok

DROP ALIAS R;
> ok
